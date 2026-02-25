#include "ws_executor_context.h"
#include "ws_bucket_map.h"
#include "ws_mailbox_table.h"
#include "ws_poll.h"

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

    TWSExecutorContext::~TWSExecutorContext() = default;

    void TWSExecutorContext::SetStealIterator(std::unique_ptr<IStealIterator> iter) {
        StealIterator_ = std::move(iter);
    }

    IStealIterator* TWSExecutorContext::GetStealIterator() const {
        return StealIterator_.get();
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
        // No-op: Vyukov MPSC queue doesn't need local cursor management.
        // Head is updated by Pop() directly (no false-sharing with Tail).
    }

    void TWSExecutorContext::DispatchEvent(TMailbox* mailbox, std::unique_ptr<IEventHandle> ev, NHPTimer::STime& hpnow) {
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

            // Accumulate mailbox stats locally; flushed in FinalizeActivation
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
    }

    bool TWSExecutorContext::ExecuteSingleEvent(TMailbox* mailbox, NHPTimer::STime& hpnow) {
        std::unique_ptr<IEventHandle> ev = mailbox->Pop();
        if (!ev) {
            return false;
        }
        DispatchEvent(mailbox, std::move(ev), hpnow);
        if (mailbox->IsEmpty()) {
            mailbox->LockToFree();
        }
        return true;
    }

    TWSExecutorContext::TActivationResult TWSExecutorContext::ExecuteActivation(
        TMailbox* mailbox, ui32 eventBudget, NHPTimer::STime deadline, NHPTimer::STime& hpnow)
    {
        TActivationResult result;
        NHPTimer::STime hpBefore = hpnow;
        ESnapshotResult snapshotResult;
        {
            TMailboxSnapshot snapshot(mailbox, snapshotResult);
            while (eventBudget > 0) {
                auto ev = snapshot.Pop();
                if (!ev) break;
                DispatchEvent(mailbox, std::move(ev), hpnow);
                --eventBudget;
                ++result.EventsProcessed;
                if (mailbox->IsEmpty()) break;
                if (hpnow >= deadline) break;
            }
        }

        // Update bucket stats with total activation cost
        if (BucketMap_ && result.EventsProcessed > 0) {
            uint64_t totalCycles = static_cast<uint64_t>(hpnow - hpBefore);
            BucketMap_->UpdateAfterExecution(mailbox->Hint, totalCycles);
        }

        return result;
    }

    bool TWSExecutorContext::FinalizeActivation(TMailbox* mailbox, i16 slotIdx, NHPTimer::STime hpnow) {
        FlushAllStats(hpnow);
        TlsThreadContext->ActivityContext.ElapsingActorActivity.store(ActorSystemIndex, std::memory_order_release);
        TlsThreadContext->ActivityContext.ActivationStartTS.store(hpnow, std::memory_order_release);

        // Stamp affinity before any state transition — after unlock/free
        // another thread may read LastPoolSlotIdx via RouteActivation.
        mailbox->LastPoolSlotIdx = static_cast<ui16>(slotIdx + 1);

        if (mailbox->IsFree()) {
            if (SlotAllocator_) {
                SlotAllocator_->Free(mailbox->Hint);
            }
            return true;
        }

        if (mailbox->IsEmpty()) {
            mailbox->LockToFree();
            if (SlotAllocator_) {
                SlotAllocator_->Free(mailbox->Hint);
            }
            return true;
        }

        return mailbox->TryUnlock();
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
        // TryLockToFree checks Head==StubPtr, Stub==0, and CAS on Tail,
        // so it fails safely if a concurrent Push arrived.
        if (mailbox->IsEmpty() && mailbox->CanReclaim()) {
            if (SlotAllocator_) {
                SlotAllocator_->Free(mailbox->Hint);
            }
            return;
        }

        mailbox->Unlock(ThreadCtx.Pool(), hpnow, RevolvingWriteCounter);
    }

} // namespace NActors::NWorkStealing
