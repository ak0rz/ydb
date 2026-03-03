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

    void TWSExecutorContext::DispatchEvent(TMailbox* mailbox, IEventHandle* ev, NHPTimer::STime& hpnow) {
        IActor* actor = mailbox->ResolveActor(ev);
        DispatchEvent(mailbox, actor, ev, hpnow);
    }

    void TWSExecutorContext::DispatchEvent(TMailbox* mailbox, IActor* actor, IEventHandle* ev, NHPTimer::STime& hpnow) {
        NHPTimer::STime hpprev = TlsThreadContext->UpdateStartOfProcessingEventTS(hpnow);
        ExecutionStats.AddElapsedCycles(ActorSystemIndex, hpnow - hpprev);

        TActorId recipient = ev->GetRecipientRewrite();

        TActorContext ctx(*mailbox, *this, hpnow, recipient);
        TlsActivationContext = &ctx;

        // TAutoPtr wraps the event for Receive compatibility.
        // If Receive (or ForwardOnNondelivery) takes ownership, evAuto becomes
        // null and its destructor is a no-op. Otherwise evAuto destructor frees.
        TAutoPtr<IEventHandle> evAuto(ev);

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
        } else if (Y_UNLIKELY(evAuto->GetTypeRewrite() == TEvents::TSystem::RegisterActor)) {
            auto* reg = evAuto->CastAsLocal<TEvents::TEvRegisterActor>();
            mailbox->AttachActor(reg->LocalActorId, reg->Actor);
            reg->Actor = nullptr; // consumer took ownership
            hpnow = GetCycleCountFast();
            hpprev = TlsThreadContext->UpdateStartOfProcessingEventTS(hpnow);
            ExecutionStats.AddElapsedCycles(ActorSystemIndex, hpnow - hpprev);
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

    EPopResult TWSExecutorContext::ExecuteSingleEvent(TMailbox* mailbox, NHPTimer::STime& hpnow) {
        return mailbox->Pop([&](IActor* actor, IEventHandle* ev) {
            DispatchEvent(mailbox, actor, ev, hpnow);
        });
    }

    TWSExecutorContext::TActivationResult TWSExecutorContext::ExecuteActivation(
        TMailbox* mailbox, ui32 eventBudget, NHPTimer::STime deadline, NHPTimer::STime& hpnow,
        i16 slotIdx)
    {
        TActivationResult result;
        result.Hint = mailbox->Hint;
        NHPTimer::STime hpBefore = hpnow;

        // Stamp affinity before processing events. Must happen before
        // Pop can CAS → Idle, since after Idle the mailbox must not be
        // accessed. On the non-Idle path FinalizeActivation used to
        // stamp this, but the Idle path skipped it — causing the
        // receiver to bounce via PowerOfTwoHash during startup.
        mailbox->LastPoolSlotIdx = static_cast<ui16>(slotIdx + 1);

        // Dispatch events inside the Pop callback — before the deferred CAS.
        // This guarantees exclusive consumer access during DispatchEvent.
        // After Pop returns Idle, the mailbox must NOT be accessed (a new
        // consumer may already be running on it).
        while (eventBudget > 0) {
            EPopResult popResult = mailbox->Pop([&](IActor* actor, IEventHandle* ev) {
                DispatchEvent(mailbox, actor, ev, hpnow);
                // Cache emptiness while we still hold exclusive access.
                // After the CAS → Idle (which happens after this callback
                // returns), reading mailbox fields is a data race.
                result.MailboxWasEmpty = mailbox->IsEmpty();
            });
            if (popResult == EPopResult::Empty) break;
            --eventBudget;
            ++result.EventsProcessed;
            if (popResult == EPopResult::Idle) {
                result.IsIdle = true;
                break;
            }
            if (hpnow >= deadline) break;
        }

        // Update bucket stats with total activation cost.
        // Uses cached hint (safe after Idle).
        if (BucketMap_ && result.EventsProcessed > 0) {
            uint64_t totalCycles = static_cast<uint64_t>(hpnow - hpBefore);
            BucketMap_->UpdateAfterExecution(result.Hint, totalCycles);
        }

        return result;
    }

    bool TWSExecutorContext::FinalizeActivation(TMailbox* mailbox, NHPTimer::STime hpnow) {
        FlushAllStats(hpnow);
        TlsThreadContext->ActivityContext.ElapsingActorActivity.store(ActorSystemIndex, std::memory_order_release);
        TlsThreadContext->ActivityContext.ActivationStartTS.store(hpnow, std::memory_order_release);

        // LastPoolSlotIdx already stamped by ExecuteActivation (before
        // any Pop can CAS → Idle). No need to re-stamp here.

        // Try to idle the mailbox. TryUnlock checks Head+Stub+Tail
        // to ensure no push is in progress and the queue is truly idle.
        bool isIdle = mailbox->TryUnlock();

        // Reclaim empty+idle mailboxes (last actor called PassAway and
        // queue is idle). Safe because TryUnlock succeeded — no concurrent
        // consumer can start until a new Push arrives.
        if (isIdle && mailbox->IsEmpty()) {
            if (SlotAllocator_) {
                SlotAllocator_->Free(mailbox->Hint);
            }
        }

        return isIdle;
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

    void TWSExecutorContext::FlushStatsOnly(NHPTimer::STime hpnow) {
        FlushAllStats(hpnow);
        TlsThreadContext->ActivityContext.ElapsingActorActivity.store(ActorSystemIndex, std::memory_order_release);
        TlsThreadContext->ActivityContext.ActivationStartTS.store(hpnow, std::memory_order_release);
    }

    void TWSExecutorContext::FreeHint(ui32 hint) {
        if (SlotAllocator_) {
            SlotAllocator_->Free(hint);
        }
    }

    void TWSExecutorContext::FinishMailbox(TMailbox* mailbox) {
        NHPTimer::STime hpnow = GetCycleCountFast();
        FlushAllStats(hpnow);
        TlsThreadContext->ActivityContext.ElapsingActorActivity.store(ActorSystemIndex, std::memory_order_release);
        TlsThreadContext->ActivityContext.ActivationStartTS.store(hpnow, std::memory_order_release);

        // Reclaim empty mailboxes (last actor called PassAway).
        if (mailbox->IsEmpty() && mailbox->CanReclaim()) {
            if (SlotAllocator_) {
                SlotAllocator_->Free(mailbox->Hint);
            }
            return;
        }

        mailbox->Unlock(ThreadCtx.Pool(), hpnow, RevolvingWriteCounter);
    }

} // namespace NActors::NWorkStealing
