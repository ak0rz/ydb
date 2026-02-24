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

    void TWSExecutorContext::CommitLocalCursor() {
        if (LocalHead_ && CurrentBatchMailbox_) {
            CurrentBatchMailbox_->ReattachEventChain(LocalHead_, LocalTail_);
            LocalHead_ = nullptr;
            LocalTail_ = nullptr;
        }
        CurrentBatchMailbox_ = nullptr;
    }

    bool TWSExecutorContext::ExecuteSingleEvent(TMailbox* mailbox, NHPTimer::STime& hpnow) {
        // Pop from local cursor to avoid writing mailbox->EventHead per event.
        // EventHead shares a cache line with NextEventPtr (producer CAS target);
        // writing it on every Pop causes false sharing under high sender count.
        if (!LocalHead_) {
            // Local cursor exhausted — refill from mailbox
            LocalHead_ = mailbox->DrainToLocal(LocalTail_);
            CurrentBatchMailbox_ = mailbox;
        }

        IEventHandle* rawEv = LocalHead_;
        if (rawEv) {
            LocalHead_ = reinterpret_cast<IEventHandle*>(
                rawEv->NextLinkPtr.load(std::memory_order_relaxed));
            rawEv->NextLinkPtr.store(0, std::memory_order_relaxed);
            if (!LocalHead_) {
                LocalTail_ = nullptr;
            }
        }

        if (!rawEv) {
            LocalTail_ = nullptr;
            CurrentBatchMailbox_ = nullptr;
            return false;
        }

        std::unique_ptr<IEventHandle> ev(rawEv);

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

        if (mailbox->IsFree()) {
            // Mailbox was freed inside ExecuteSingleEvent (LockToFree after
            // last actor PassAway'd). Return the hint to the allocator so
            // it can be reused by future Register() calls.
            if (SlotAllocator_) {
                SlotAllocator_->Free(mailbox->Hint);
            }
            return;
        }

        // Reclaim empty mailboxes (last actor called PassAway).
        // Check: no actors, no pre-processed events in EventHead.
        // TryLockToFree uses CAS (0 → MarkerFree), so it fails safely
        // if a concurrent Push arrived — we fall through to Unlock.
        if (mailbox->IsEmpty() && !mailbox->EventHead
                && mailbox->TryLockToFree())
        {
            if (SlotAllocator_) {
                SlotAllocator_->Free(mailbox->Hint);
            }
            return;
        }

        mailbox->Unlock(ThreadCtx.Pool(), hpnow, RevolvingWriteCounter);
    }

} // namespace NActors::NWorkStealing
