#include "ws_executor_context.h"
#include "ws_bucket_map.h"
#include "ws_mailbox_table.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_class_stats.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/mailbox_lockfree.h>
#include <ydb/library/actors/core/probes.h>
#include <ydb/library/actors/util/datetime.h>

namespace NActors::NWorkStealing {

    TWSExecutorContext::TWSExecutorContext(
        TWorkerId workerId,
        TActorSystem* actorSystem,
        IExecutorPool* pool)
        : TExecutorThread(workerId, actorSystem, pool, TString("ws_ctx"))
    {
    }

    void TWSExecutorContext::SetupTLS() {
        ThreadCtx.ExecutionStats = &ExecutionStats;
        ThreadCtx.ActivityContext.ActorSystemIndex = ActorSystemIndex;
        ThreadCtx.ActivityContext.ElapsingActorActivity = ActorSystemIndex;
        NHPTimer::STime now = GetCycleCountFast();
        ThreadCtx.ActivityContext.StartOfProcessingEventTS = now;
        ThreadCtx.ActivityContext.ActivationStartTS = now;
        TlsThreadContext = &ThreadCtx;
    }

    void TWSExecutorContext::ClearTLS() {
        TlsThreadContext = nullptr;
    }

    bool TWSExecutorContext::ExecuteSingleEvent(TMailbox* mailbox, NHPTimer::STime& hpnow) {
        std::unique_ptr<IEventHandle> ev = mailbox->Pop();
        if (!ev) {
            return false;
        }

        NHPTimer::STime hpprev = TlsThreadContext->UpdateStartOfProcessingEventTS(hpnow);
        ExecutionStats.AddElapsedCycles(ActorSystemIndex, hpnow - hpprev);

        TActorId recipient = ev->GetRecipientRewrite();
        IActor* actor = mailbox->FindActor(recipient.LocalId());
        if (!actor) {
            actor = mailbox->FindAlias(recipient.LocalId());
            if (actor) {
                ev->Rewrite(ev->GetTypeRewrite(), actor->SelfId());
                recipient = ev->GetRecipientRewrite();
            }
        }

        TActorContext ctx(*mailbox, *this, hpnow, recipient);
        TlsActivationContext = &ctx;
        TAutoPtr<IEventHandle> evAuto = ev.release();

        if (actor) {
            ui32 activityType = actor->GetActivityType().GetIndex();
            NProfiling::TMemoryTagScope::Reset(activityType);
            TlsThreadContext->ActivityContext.ElapsingActorActivity.store(activityType, std::memory_order_release);

            CurrentRecipient = recipient;
            CurrentActorScheduledEventsCounter = 0;

            actor->Receive(evAuto);

            hpnow = GetCycleCountFast();
            hpprev = TlsThreadContext->UpdateStartOfProcessingEventTS(hpnow);

            // Accumulate class stats locally; flush on class pointer change
            if (auto* cs = actor->GetClassStats()) {
                if (cs != Accum_.LastClassStats) {
                    FlushClassStats();
                    Accum_.LastClassStats = cs;
                }
                Accum_.ClassMessages++;
                Accum_.ClassCycles += (hpnow - hpprev);
            }

            // Accumulate mailbox stats locally; flushed in FinishMailbox
            if (!Accum_.MboxStats) {
                Accum_.MboxStats = WsMailboxTable_
                    ? WsMailboxTable_->GetStats(mailbox->Hint) : nullptr;
                Accum_.FirstEventHpprev = hpprev;
            }
            if (Accum_.MboxStats) {
                NHPTimer::STime eventCycles = hpnow - hpprev;
                Accum_.Events++;
                Accum_.TotalExecCycles += eventCycles;
                if (static_cast<ui64>(eventCycles) > Accum_.MaxExecCycles)
                    Accum_.MaxExecCycles = eventCycles;
            }

            if (!DyingActors.empty()) {
                for (const auto& dying : DyingActors) {
                    if (auto* cs = dying->GetClassStats()) {
                        cs->ActorsDestroyed.fetch_add(1, std::memory_order_relaxed);
                        cs->LifetimeCyclesSum.fetch_add(
                            GetCycleCountFast() - dying->GetCreatedAtCycles(), std::memory_order_relaxed);
                    }
                }
                DropUnregistered();
                actor = nullptr;
            }

            if (mailbox->IsEmpty()) {
                mailbox->LockToFree();
            }

            ExecutionStats.AddElapsedCycles(activityType, hpnow - hpprev);
            NHPTimer::STime elapsed = hpnow - hpprev;
            mailbox->AddElapsedCycles(elapsed);
            if (actor) {
                actor->AddElapsedTicks(elapsed);
            }

            CurrentRecipient = TActorId();
        } else {
            TAutoPtr<IEventHandle> nonDelivered = IEventHandle::ForwardOnNondelivery(std::move(evAuto), TEvents::TEvUndelivered::ReasonActorUnknown);
            if (nonDelivered.Get()) {
                ActorSystem->Send(nonDelivered);
            } else {
                ExecutionStats.IncrementNonDeliveredEvents();
            }
            hpnow = GetCycleCountFast();
            hpprev = TlsThreadContext->UpdateStartOfProcessingEventTS(hpnow);
            ExecutionStats.AddElapsedCycles(ActorSystemIndex, hpnow - hpprev);
        }

        TlsActivationContext = nullptr;
        NProfiling::TMemoryTagScope::Reset(0);

        return true;
    }

    void TWSExecutorContext::FlushClassStats() {
        if (Accum_.ClassMessages > 0 && Accum_.LastClassStats) {
            Accum_.LastClassStats->MessagesProcessed.fetch_add(
                Accum_.ClassMessages, std::memory_order_relaxed);
            Accum_.LastClassStats->ExecutionCycles.fetch_add(
                Accum_.ClassCycles, std::memory_order_relaxed);
            Accum_.ClassMessages = 0;
            Accum_.ClassCycles = 0;
        }
    }

    void TWSExecutorContext::FlushAllStats(NHPTimer::STime hpnow) {
        FlushClassStats();
        if (auto* ms = Accum_.MboxStats) {
            // Idle time: from last batch end to first event of this batch
            NHPTimer::STime lastEnd = ms->LastExecutionEndCycles.load(
                std::memory_order_relaxed);
            if (lastEnd && Accum_.FirstEventHpprev) {
                NHPTimer::STime idleCycles = Accum_.FirstEventHpprev - lastEnd;
                ms->TotalIdleCycles.fetch_add(idleCycles, std::memory_order_relaxed);
                auto curMax = ms->MaxIdleCycles.load(std::memory_order_relaxed);
                if (static_cast<ui64>(idleCycles) > curMax)
                    ms->MaxIdleCycles.store(idleCycles, std::memory_order_relaxed);
                auto curMin = ms->MinIdleCycles.load(std::memory_order_relaxed);
                if (static_cast<ui64>(idleCycles) < curMin)
                    ms->MinIdleCycles.store(idleCycles, std::memory_order_relaxed);
            }
            ms->EventsProcessed.fetch_add(Accum_.Events, std::memory_order_relaxed);
            ms->TotalExecutionCycles.fetch_add(Accum_.TotalExecCycles,
                std::memory_order_relaxed);
            auto curMax = ms->MaxExecutionCycles.load(std::memory_order_relaxed);
            if (Accum_.MaxExecCycles > curMax)
                ms->MaxExecutionCycles.store(Accum_.MaxExecCycles,
                    std::memory_order_relaxed);
            ms->LastExecutionEndCycles.store(hpnow, std::memory_order_relaxed);
        }
        Accum_.Reset();
    }

    void TWSExecutorContext::FinishMailbox(TMailbox* mailbox) {
        NHPTimer::STime hpnow = GetCycleCountFast();
        FlushAllStats(hpnow);
        TlsThreadContext->ActivityContext.ElapsingActorActivity.store(ActorSystemIndex, std::memory_order_release);
        TlsThreadContext->ActivityContext.ActivationStartTS.store(hpnow, std::memory_order_release);

        if (mailbox->IsEmpty() && !mailbox->IsFree()) {
            mailbox->LockToFree();
        }

        if (mailbox->IsFree() && mailbox->CanReclaim()) {
            if (BucketMap_) {
                BucketMap_->ResetBucket(mailbox->Hint);
            }
            if (SlotAllocator_) {
                SlotAllocator_->Free(mailbox->Hint);
            } else {
                ThreadCtx.FreeMailbox(mailbox);
            }
        } else if (!mailbox->IsFree()) {
            mailbox->Unlock(ThreadCtx.Pool(), hpnow, RevolvingWriteCounter);
        }
        // IsFree && !CanReclaim: LockToFree drained late-arriving events into
        // EventHead. These were already processed by prior ExecuteSingleEvent
        // calls (Pop drains EventHead). If we reach here, it means events
        // arrived between the last Pop (which returned nullptr) and LockToFree.
        // The mailbox is free with pending events — let them be cleaned up
        // by the mailbox table reclamation.
    }

} // namespace NActors::NWorkStealing
