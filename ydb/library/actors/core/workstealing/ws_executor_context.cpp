#include "ws_executor_context.h"

#include <ydb/library/actors/core/actor.h>
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

            if (auto* cs = actor->GetClassStats()) {
                cs->MessagesProcessed.fetch_add(1, std::memory_order_relaxed);
                cs->ExecutionCycles.fetch_add(hpnow - hpprev, std::memory_order_relaxed);
            }

            if (auto* mboxStats = ThreadCtx.WorkerContext.MailboxTable->GetStats(mailbox->Hint)) {
                NHPTimer::STime eventCycles = hpnow - hpprev;

                // Mailbox idle time: from last execution end to start of this event
                NHPTimer::STime lastEnd = mboxStats->LastExecutionEndCycles.load(std::memory_order_relaxed);
                if (lastEnd) {
                    NHPTimer::STime idleCycles = hpprev - lastEnd;
                    mboxStats->TotalIdleCycles.fetch_add(idleCycles, std::memory_order_relaxed);
                    auto prevMaxIdle = mboxStats->MaxIdleCycles.load(std::memory_order_relaxed);
                    if (static_cast<ui64>(idleCycles) > prevMaxIdle)
                        mboxStats->MaxIdleCycles.store(idleCycles, std::memory_order_relaxed);
                    auto prevMinIdle = mboxStats->MinIdleCycles.load(std::memory_order_relaxed);
                    if (static_cast<ui64>(idleCycles) < prevMinIdle)
                        mboxStats->MinIdleCycles.store(idleCycles, std::memory_order_relaxed);
                }

                // Execution stats
                mboxStats->EventsProcessed.fetch_add(1, std::memory_order_relaxed);
                mboxStats->TotalExecutionCycles.fetch_add(eventCycles, std::memory_order_relaxed);
                auto prevMax = mboxStats->MaxExecutionCycles.load(std::memory_order_relaxed);
                if (static_cast<ui64>(eventCycles) > prevMax)
                    mboxStats->MaxExecutionCycles.store(eventCycles, std::memory_order_relaxed);

                mboxStats->LastExecutionEndCycles.store(hpnow, std::memory_order_relaxed);
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

    void TWSExecutorContext::FinishMailbox(TMailbox* mailbox) {
        NHPTimer::STime hpnow = GetCycleCountFast();
        TlsThreadContext->ActivityContext.ElapsingActorActivity.store(ActorSystemIndex, std::memory_order_release);
        TlsThreadContext->ActivityContext.ActivationStartTS.store(hpnow, std::memory_order_release);

        if (mailbox->IsEmpty() && !mailbox->IsFree()) {
            mailbox->LockToFree();
        }

        if (mailbox->IsFree() && mailbox->CanReclaim()) {
            ThreadCtx.FreeMailbox(mailbox);
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
