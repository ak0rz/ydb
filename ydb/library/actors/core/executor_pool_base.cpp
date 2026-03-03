#include "actorsystem.h"
#include "activity_guard.h"
#include "actor.h"
#include "executor_pool_base.h"
#include "executor_pool_basic_feature_flags.h"
#include "executor_thread.h"
#include "mailbox.h"
#include "probes.h"
#include "debug.h"
#include <ydb/library/actors/util/datetime.h>

namespace NActors {
    LWTRACE_USING(ACTORLIB_PROVIDER);

    void DoActorInit(TActorSystem* sys, IActor* actor, const TActorId& self, const TActorId& owner) {
        actor->SelfActorId = self;
        actor->CreatedAtCycles_ = GetCycleCountFast();
        if (actor->ClassStats_) {
            actor->ClassStats_->ActorsCreated.fetch_add(1, std::memory_order_relaxed);
        }
        actor->Registered(sys, owner);
    }

    TExecutorPoolBaseMailboxed::TExecutorPoolBaseMailboxed(ui32 poolId)
        : IExecutorPool(poolId)
        , ActorSystem(nullptr)
        , MailboxTableHolder(new TMailboxTable)
        , MailboxTable(MailboxTableHolder.Get())
    {}

    TExecutorPoolBaseMailboxed::~TExecutorPoolBaseMailboxed() {
        MailboxTableHolder.Destroy();
    }

#if defined(ACTORSLIB_COLLECT_EXEC_STATS)
    void TExecutorPoolBaseMailboxed::RecalculateStuckActors(TExecutorThreadStats& stats) const {
        if (!ActorSystem || !ActorSystem->MonitorStuckActors()) {
            return;
        }

        const TMonotonic now = ActorSystem->Monotonic();

        std::fill(stats.StuckActorsByActivity.begin(), stats.StuckActorsByActivity.end(), 0);

        with_lock (StuckObserverMutex) {
            for (size_t i = 0; i < Actors.size(); ++i) {
                IActor *actor = Actors[i];
                Y_ABORT_UNLESS(actor->StuckIndex == i);
                const TDuration delta = now - actor->LastReceiveTimestamp;
                if (delta > TDuration::Seconds(30)) {
                    ++stats.StuckActorsByActivity[actor->GetActivityType().GetIndex()];
                }
            }
        }
    }
#endif

    TExecutorPoolBase::TExecutorPoolBase(ui32 poolId, ui32 threads, TAffinity* affinity, bool useRingQueue)
        : TExecutorPoolBaseMailboxed(poolId)
        , PoolThreads(threads)
        , UseRingQueueValue(useRingQueue)
        , ThreadsAffinity(affinity)
    {
        if (useRingQueue) {
            Activations.emplace<TRingActivationQueueV4>(threads);
        } else {
            Activations.emplace<TUnorderedCacheActivationQueue>();
        }
    }

    TExecutorPoolBase::~TExecutorPoolBase() {
        while (std::visit([](auto &x){return x.Pop(0);}, Activations))
            ;
    }

    TMailbox* TExecutorPoolBaseMailboxed::ResolveMailbox(ui32 hint) {
        return MailboxTable->Get(hint);
    }

    ui64 TExecutorPoolBaseMailboxed::AllocateID() {
        return ActorSystem->AllocateIDSpace(1);
    }

    bool TExecutorPoolBaseMailboxed::Send(std::unique_ptr<IEventHandle>& ev) {
        Y_DEBUG_ABORT_UNLESS(ev->GetRecipientRewrite().PoolID() == PoolId);
#ifdef ACTORSLIB_COLLECT_EXEC_STATS
        RelaxedStore(&ev->SendTime, (::NHPTimer::STime)GetCycleCountFast());
#endif
        if (TlsThreadContext) {
            TlsThreadContext->IsCurrentRecipientAService = ev->Recipient.IsService();
        }

        if (TMailbox* mailbox = MailboxTable->Get(ev->GetRecipientRewrite().Hint())) {
            switch (mailbox->Push(ev)) {
                case EMailboxPush::Pushed:
                    return true;
                case EMailboxPush::Locked:
                    mailbox->ScheduleMoment = GetCycleCountFast();
                    ScheduleActivation(mailbox);
                    return true;
            }
        }

        return false;
    }

    bool TExecutorPoolBaseMailboxed::SpecificSend(std::unique_ptr<IEventHandle>& ev) {
        Y_DEBUG_ABORT_UNLESS(ev->GetRecipientRewrite().PoolID() == PoolId);
#ifdef ACTORSLIB_COLLECT_EXEC_STATS
        RelaxedStore(&ev->SendTime, (::NHPTimer::STime)GetCycleCountFast());
#endif
        if (TlsThreadContext) {
            TlsThreadContext->IsCurrentRecipientAService = ev->Recipient.IsService();
        }

        if (TMailbox* mailbox = MailboxTable->Get(ev->GetRecipientRewrite().Hint())) {
            switch (mailbox->Push(ev)) {
                case EMailboxPush::Pushed:
                    return true;
                case EMailboxPush::Locked:
                    mailbox->ScheduleMoment = GetCycleCountFast();
                    SpecificScheduleActivation(mailbox);
                    return true;
            }
        }

        return false;
    }

    void TExecutorPoolBase::ScheduleActivation(TMailbox* mailbox) {
        if (UseRingQueue()) {
            ScheduleActivationEx(mailbox, 0);
        } else {
            ScheduleActivationEx(mailbox, AtomicIncrement(ActivationsRevolvingCounter));
        }
    }

    Y_FORCE_INLINE bool IsAllowedToCapture(IExecutorPool *self) {
        if (TlsThreadContext->Pool() != self || TlsThreadContext->CheckCapturedSendingType(ESendingType::Tail)) {
            return false;
        }
        return !TlsThreadContext->CheckSendingType(ESendingType::Common);
    }

    Y_FORCE_INLINE bool IsTailSend(IExecutorPool *self) {
        return TlsThreadContext->Pool() == self && TlsThreadContext->CheckSendingType(ESendingType::Tail) && !TlsThreadContext->CheckCapturedSendingType(ESendingType::Tail);
    }

    void TExecutorPoolBase::SpecificScheduleActivation(TMailbox* mailbox) {
        if (NFeatures::IsCommon() && IsAllowedToCapture(this) || IsTailSend(this)) {
            mailbox = TlsThreadContext->CaptureMailbox(mailbox);
        }
        if (!mailbox) {
            return;
        }
        if (UseRingQueueValue) {
            ScheduleActivationEx(mailbox, 0);
        } else {
            ScheduleActivationEx(mailbox, AtomicIncrement(ActivationsRevolvingCounter));
        }
    }

    TActorId TExecutorPoolBaseMailboxed::Register(IActor* actor, TMailboxType::EType, ui64 revolvingWriteCounter, const TActorId& parentId) {
        TMailboxCache empty;
        return Register(actor, empty, revolvingWriteCounter, parentId);
    }

    TActorId TExecutorPoolBaseMailboxed::Register(IActor* actor, TMailboxCache& cache, ui64 revolvingWriteCounter, const TActorId& parentId) {
        NHPTimer::STime hpstart = GetCycleCountFast();
        TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_REGISTER, false> activityGuard(hpstart);
#ifdef ACTORSLIB_COLLECT_EXEC_STATS
        ui32 at = actor->GetActivityType().GetIndex();
        Y_DEBUG_ABORT_UNLESS(at < Stats.ActorsAliveByActivity.size());
        if (at >= Stats.MaxActivityType()) {
            at = TActorTypeOperator::GetActorActivityIncorrectIndex();
            Y_ABORT_UNLESS(at < Stats.ActorsAliveByActivity.size());
        }
        AtomicIncrement(Stats.ActorsAliveByActivity[at]);
#endif
        AtomicIncrement(ActorRegistrations);

        TMailbox* mailbox = cache ? cache.Allocate() : MailboxTable->Allocate();
        const ui64 localActorId = AllocateID();
        const TActorId actorId(ActorSystem->NodeId, PoolId, localActorId, mailbox->Hint);

        // Push deferred registration event
        auto* regEvent = new TEvents::TEvRegisterActor();
        regEvent->Actor = actor;
        regEvent->LocalActorId = localActorId;
        regEvent->ParentId = parentId;
        auto ev = std::make_unique<IEventHandle>(actorId, TActorId(), regEvent);
        EMailboxPush pushResult = mailbox->Push(ev);
        if (pushResult == EMailboxPush::Locked) {
            // Normal case: mailbox was Idle, we locked it — schedule for execution
            mailbox->ScheduleMoment = GetCycleCountFast();
            ScheduleActivationEx(mailbox, ++revolvingWriteCounter);
        }
        // Pushed: a stale push already locked/scheduled this recycled mailbox.
        // The consumer will process stale events (non-delivery) then our
        // TEvRegisterActor which attaches the actor.

        // Eager init on registering thread - sets SelfActorId, CreatedAtCycles_, ClassStats_ and calls Registered() on the actor.
        // This is needed to ensure that the actor is fully initialized and registered before any other producer can send messages
        // to it (e.g. from the parent during bootstrap).
        DoActorInit(ActorSystem, actor, actorId, parentId);

#ifdef ACTORSLIB_COLLECT_EXEC_STATS
        if (ActorSystem->MonitorStuckActors()) {
            with_lock (StuckObserverMutex) {
                Y_ABORT_UNLESS(actor->StuckIndex == Max<size_t>());
                actor->StuckIndex = Actors.size();
                Actors.push_back(actor);
            }
        }
#endif

        actor = nullptr;

        NHPTimer::STime elapsed = GetCycleCountFast() - hpstart;
        if (elapsed > 1000000) {
            LWPROBE(SlowRegisterNew, PoolId, NHPTimer::GetSeconds(elapsed) * 1000.0);
        }

        return actorId;
    }

    TActorId TExecutorPoolBaseMailboxed::Register(IActor* actor, TMailbox* mailbox, const TActorId& parentId) {
        NHPTimer::STime hpstart = GetCycleCountFast();
        TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_REGISTER, false> activityGuard(hpstart);
#ifdef ACTORSLIB_COLLECT_EXEC_STATS
        ui32 at = actor->GetActivityType().GetIndex();
        if (at >= Stats.MaxActivityType())
            at = 0;
        AtomicIncrement(Stats.ActorsAliveByActivity[at]);
#endif
        AtomicIncrement(ActorRegistrations);

        // Empty mailboxes are currently pending for reclamation
        Y_ABORT_UNLESS(!mailbox->IsEmpty(),
            "RegisterWithSameMailbox called on an empty mailbox");

        const ui64 localActorId = AllocateID();
        mailbox->AttachActor(localActorId, actor);

        const TActorId actorId(ActorSystem->NodeId, PoolId, localActorId, mailbox->Hint);
        DoActorInit(ActorSystem, actor, actorId, parentId);

#ifdef ACTORSLIB_COLLECT_EXEC_STATS
        if (ActorSystem->MonitorStuckActors()) {
            with_lock (StuckObserverMutex) {
                Y_ABORT_UNLESS(actor->StuckIndex == Max<size_t>());
                actor->StuckIndex = Actors.size();
                Actors.push_back(actor);
            }
        }
#endif

        NHPTimer::STime elapsed = GetCycleCountFast() - hpstart;
        if (elapsed > 1000000) {
            LWPROBE(SlowRegisterAdd, PoolId, NHPTimer::GetSeconds(elapsed) * 1000.0);
        }

        return actorId;
    }

    TActorId TExecutorPoolBaseMailboxed::RegisterAlias(TMailbox* mailbox, IActor* actor) {
        Y_ABORT_UNLESS(!mailbox->IsEmpty(),
            "RegisterAlias called on an empty mailbox");

        Y_DEBUG_ABORT_UNLESS(mailbox->FindActor(actor->SelfId().LocalId()) == actor,
            "RegisterAlias called for an actor that is not register in the mailbox");

        const ui64 localActorId = AllocateID();
        mailbox->AttachAlias(localActorId, actor);
        return TActorId(ActorSystem->NodeId, PoolId, localActorId, mailbox->Hint);
    }

    void TExecutorPoolBaseMailboxed::UnregisterAlias(TMailbox* mailbox, const TActorId& actorId) {
        Y_DEBUG_ABORT_UNLESS(actorId.Hint() == mailbox->Hint);
        Y_DEBUG_ABORT_UNLESS(actorId.PoolID() == PoolId);
        Y_DEBUG_ABORT_UNLESS(actorId.NodeId() == ActorSystem->NodeId);
        mailbox->DetachAlias(actorId.LocalId());
    }

    TAffinity* TExecutorPoolBase::Affinity() const {
        return ThreadsAffinity.Get();
    }

    bool TExecutorPoolBaseMailboxed::Cleanup() {
        return MailboxTable->Cleanup();
    }

    ui32 TExecutorPoolBase::GetThreads() const {
        return PoolThreads;
    }

    TMailboxTable* TExecutorPoolBaseMailboxed::GetMailboxTable() const {
        return MailboxTable;
    }

    bool TExecutorPoolBase::UseRingQueue() const {
        return UseRingQueueValue;
    }
}
