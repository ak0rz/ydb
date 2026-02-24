#include "executor_pool_ws.h"
#include "thread_driver.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_class_stats.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/mailbox_lockfree.h>
#include <ydb/library/actors/core/probes.h>
#include <ydb/library/actors/util/datetime.h>

#include <algorithm>


namespace NActors::NWorkStealing {

    // Deferred re-injection mechanism.
    //
    // When Execute() calls mailbox->Unlock() and TryUnlock fails (events were
    // pushed during execution), Unlock calls pool->ScheduleActivationEx() to
    // re-inject the hint. Without deferral, the hint enters another slot's
    // queue while the first worker is still inside Execute(). Another worker
    // can then drain+pop+execute the same mailbox concurrently — crash.
    //
    // Fix: only defer ScheduleActivationEx for the CURRENTLY EXECUTING mailbox.
    // Other mailboxes (activated by Send during Receive) are routed immediately.
    static thread_local TMailbox* CurrentlyExecutingMailbox = nullptr;
    static thread_local TMailbox* DeferredReinjection = nullptr;


    TWSExecutorPool* TWSExecutorPool::LastCreated = nullptr;

    TVector<TWSExecutorPool*>& TWSExecutorPool::AllPools() {
        static TVector<TWSExecutorPool*> pools;
        return pools;
    }

    TWSExecutorPool* TWSExecutorPool::FindPool(const TString& name) {
        for (auto* p : AllPools()) {
            if (p->GetName() == name) {
                return p;
            }
        }
        return nullptr;
    }

    TWSExecutorPool::TWSExecutorPool(const TWSExecutorPoolConfig& cfg)
        : TExecutorPoolBaseMailboxed(cfg.PoolId)
        , WsConfig_(cfg.WsConfig)
        , Slots_(cfg.MaxSlotCount)
        , PoolName_(cfg.PoolName)
        , TimePerMailbox_(cfg.TimePerMailbox)
        , TimePerMailboxTsValue_(NHPTimer::GetClockRate() * cfg.TimePerMailbox.SecondsFloat())
        , EventsPerMailboxValue_(cfg.EventsPerMailbox)
        , MinSlotCount_(cfg.MinSlotCount)
        , MaxSlotCount_(cfg.MaxSlotCount)
        , DefaultSlotCount_(cfg.DefaultSlotCount)
        , Priority_(cfg.Priority)
    {
        LastCreated = this;
        AllPools().push_back(this);
    }

    TWSExecutorPool::~TWSExecutorPool() {
        if (LastCreated == this) {
            LastCreated = nullptr;
        }
        auto& pools = AllPools();
        pools.erase(std::remove(pools.begin(), pools.end(), this), pools.end());
    }

    void TWSExecutorPool::SetDriver(IDriver* driver) {
        Driver_ = driver;
    }

    void TWSExecutorPool::Prepare(TActorSystem* actorSystem, NSchedulerQueue::TReader** scheduleReaders, ui32* scheduleSz) {
        ActorSystem = actorSystem;

        ScheduleReaders_.Reset(new NSchedulerQueue::TReader[MaxSlotCount_]);
        ScheduleWriters_.Reset(new NSchedulerQueue::TWriter[MaxSlotCount_]);

        for (i16 i = 0; i < MaxSlotCount_; ++i) {
            ScheduleWriters_[i].Init(ScheduleReaders_[i]);
        }

        *scheduleReaders = ScheduleReaders_.Get();
        *scheduleSz = MaxSlotCount_;

        // Create one TWSExecutorContext per slot
        Contexts_.reserve(MaxSlotCount_);
        for (i16 i = 0; i < MaxSlotCount_; ++i) {
            Contexts_.push_back(std::make_unique<TWSExecutorContext>(i, actorSystem, this));
        }

        // Create TWsMailboxTable (flatter segments, index-based free lists)
        WsMailboxTable_ = std::make_unique<TWsMailboxTable>();
        auto* wsTable = WsMailboxTable_.get();

        // Create per-slot allocators
        SlotAllocators_.resize(MaxSlotCount_);
        for (i16 i = 0; i < MaxSlotCount_; ++i) {
            SlotAllocators_[i].Init(wsTable);
        }

        // Wire WsMailboxTable onto each slot for cost-aware stealing
        for (i16 i = 0; i < MaxSlotCount_; ++i) {
            Slots_[i].WsMailboxTable = wsTable;
        }

        // Register all slots with the driver and wire per-worker callbacks
        if (Driver_) {
            Driver_->RegisterSlots(Slots_.data(), static_cast<size_t>(MaxSlotCount_));
            auto* pool = this;
            for (i16 i = 0; i < MaxSlotCount_; ++i) {
                auto* ctx = Contexts_[i].get();
                TWorkerCallbacks callbacks;
                i16 slotIdx = i;
                callbacks.Execute = [ctx, wsTable, pool, slotIdx](ui32 hint, NHPTimer::STime& hpnow) -> bool {
                    TMailbox* mailbox = wsTable->Get(hint);
                    if (!mailbox) {
                        return false;
                    }

                    CurrentlyExecutingMailbox = mailbox;
                    DeferredReinjection = nullptr;

                    NHPTimer::STime hpBefore = hpnow;
                    bool processed = ctx->ExecuteSingleEvent(mailbox, hpnow);

                    // Stamp affinity BEFORE any unlock — after unlock another
                    // thread may read LastPoolSlotIdx via RouteActivation.
                    mailbox->LastPoolSlotIdx = static_cast<ui16>(slotIdx + 1);

                    if (!processed) {
                        ctx->FinishMailbox(mailbox);
                        CurrentlyExecutingMailbox = nullptr;

                        // Process deferred re-activation from Unlock path.
                        if (DeferredReinjection) {
                            TMailbox* deferred = DeferredReinjection;
                            DeferredReinjection = nullptr;
                            pool->RouteActivation(deferred);
                        }
                        return false;
                    }

                    // Inline bucket eviction: update bucket stats after each event
                    if (pool->BucketMap_) {
                        uint64_t eventCycles = static_cast<uint64_t>(hpnow - hpBefore);
                        pool->BucketMap_->UpdateAfterExecution(hint, eventCycles);
                    }

                    // Event processed, mailbox stays locked for more.
                    mailbox->LastPoolSlotIdx = static_cast<ui16>(slotIdx + 1);
                    CurrentlyExecutingMailbox = nullptr;
                    DeferredReinjection = nullptr;
                    return true;
                };
                callbacks.Overflow = [this, wsTable](ui32 hint) {
                    // Reset sticky routing — this activation is being load-shed
                    if (auto* mailbox = wsTable->Get(hint)) {
                        mailbox->LastPoolSlotIdx = 0;
                    }
                    // Fresh power-of-two routing to least-loaded slot
                    int target = Router_->Route(hint, 0);
                    if (target >= 0 && Driver_) {
                        Driver_->WakeSlot(&Slots_[target]);
                    }
                };
                callbacks.BeginBatch = [wsTable](ui32 hint) -> ui64 {
                    // Drain pending events from NextEventPtr into EventHead.
                    // Returns the count of drained events — used by PollSlot
                    // to decide continuation eligibility. Single-event
                    // activations (self-sends) skip the ring to prevent
                    // monopolization.
                    if (TMailbox* mailbox = wsTable->Get(hint)) {
                        // Skip contended DrainPending if EventHead already has
                        // preprocessed events. Pop() will call PreProcessEvents
                        // on-demand when EventHead runs out. This avoids the
                        // expensive CAS on NextEventPtr when many senders push
                        // to the same mailbox concurrently.
                        if (mailbox->EventHead) {
                            // Return 2 to signal "has events" so PollSlot
                            // routes to ring (preBatchCount > 1).
                            return 2;
                        }
                        return mailbox->DrainPending();
                    }
                    return 0;
                };
                callbacks.EndBatch = [ctx](ui32 /*hint*/) {
                    ctx->CommitLocalCursor();
                };
                callbacks.Setup = [ctx]() {
                    ctx->SetupTLS();
                };
                callbacks.Teardown = [ctx]() {
                    ctx->ClearTLS();
                };
                if (i == 0 && WsConfig_.AdaptiveScaling) {
                    callbacks.AdaptiveEval = [this]() {
                        if (AdaptiveScaler_) {
                            AdaptiveScaler_->Evaluate();
                        }
                    };
                }
                Driver_->SetWorkerCallbacks(&Slots_[i], std::move(callbacks));
            }

            // Activate default number of slots
            for (i16 i = 0; i < DefaultSlotCount_ && i < MaxSlotCount_; ++i) {
                Driver_->ActivateSlot(&Slots_[i]);
            }
        }

        ActiveSlotCount_.store(DefaultSlotCount_, std::memory_order_relaxed);

        // Create the activation router
        Router_ = std::make_unique<TActivationRouter>(Slots_.data(), Slots_.size());

        // Create bucket map if bucketing is enabled
        if (WsConfig_.SlotBucketing) {
            TBucketConfig bucketConfig;
            bucketConfig.CostThresholdCycles = WsConfig_.BucketCostThresholdCycles;
            bucketConfig.DowngradeThresholdCycles = WsConfig_.BucketDowngradeThresholdCycles;
            bucketConfig.MinSamplesForClassification = WsConfig_.BucketMinSamples;
            bucketConfig.EmaAlphaQ16 = WsConfig_.BucketEmaAlphaQ16;
            bucketConfig.MinActiveForBucketing = WsConfig_.BucketMinActiveSlots;

            BucketMap_ = std::make_unique<TBucketMap>(wsTable, bucketConfig);

            // Initial state: boundary=0 (all fast, no partitioning).
            // Reclassify() will set the boundary once heavy actors are detected.
            BucketMap_->SetActiveCount(static_cast<ui16>(DefaultSlotCount_));

            Router_->SetBucketMap(BucketMap_.get());

            for (auto& ctx : Contexts_) {
                ctx->SetBucketMap(BucketMap_.get());
            }

            if (Driver_) {
                // Dynamic cast not available; use static interface
                // TThreadDriver is the only IDriver implementation
                static_cast<TThreadDriver*>(Driver_)->SetBucketMap(BucketMap_.get());
            }
        }

        // Wire WsMailboxTable and slot allocators into executor contexts
        for (i16 i = 0; i < MaxSlotCount_; ++i) {
            Contexts_[i]->SetWsMailboxTable(wsTable);
            Contexts_[i]->SetSlotAllocator(&SlotAllocators_[i]);
        }

        // Create adaptive scaler if enabled
        if (WsConfig_.AdaptiveScaling) {
            AdaptiveScaler_ = std::make_unique<TAdaptiveScaler>(
                [this](i16 threads) { SetFullThreadCount(threads); },
                [this]() -> i16 { return ActiveSlotCount_.load(std::memory_order_relaxed); },
                Slots_.data(), MaxSlotCount_, WsConfig_);

            if (BucketMap_) {
                AdaptiveScaler_->SetBucketMap(BucketMap_.get());
            }
        }
    }

    void TWSExecutorPool::Start() {
        // Driver manages workers; nothing extra needed here
    }

    void TWSExecutorPool::PrepareStop() {
        StopFlag_.store(true, std::memory_order_release);

        // Deactivate all slots via Driver
        if (Driver_) {
            for (i16 i = 0; i < MaxSlotCount_; ++i) {
                if (Slots_[i].GetState() == ESlotState::Active) {
                    Driver_->DeactivateSlot(&Slots_[i]);
                }
            }
        }

        ActiveSlotCount_.store(0, std::memory_order_relaxed);
    }

    void TWSExecutorPool::Shutdown() {
        // Drain slot allocators back to global pool
        for (auto& alloc : SlotAllocators_) {
            alloc.Drain();
        }
        // Driver manages worker threads; nothing else to do here
    }

    TMailbox* TWSExecutorPool::ResolveMailbox(ui32 hint) {
        return WsMailboxTable_ ? WsMailboxTable_->Get(hint) : MailboxTable->Get(hint);
    }

    bool TWSExecutorPool::Send(std::unique_ptr<IEventHandle>& ev) {
        Y_DEBUG_ABORT_UNLESS(ev->GetRecipientRewrite().PoolID() == PoolId);
#ifdef ACTORSLIB_COLLECT_EXEC_STATS
        RelaxedStore(&ev->SendTime, (::NHPTimer::STime)GetCycleCountFast());
#endif
        if (TlsThreadContext) {
            TlsThreadContext->IsCurrentRecipientAService = ev->Recipient.IsService();
        }

        if (TMailbox* mailbox = WsMailboxTable_->Get(ev->GetRecipientRewrite().Hint())) {
            switch (mailbox->Push(ev)) {
                case EMailboxPush::Pushed:
                    return true;
                case EMailboxPush::Locked:
                    mailbox->ScheduleMoment = GetCycleCountFast();
                    ScheduleActivation(mailbox);
                    return true;
                case EMailboxPush::Free:
                    Y_DEBUG_ABORT("WS Send: Push returned Free for hint=%" PRIu32 " recipient=%s",
                        ev->GetRecipientRewrite().Hint(),
                        ev->GetRecipientRewrite().ToString().c_str());
                    break;
            }
        }

        return false;
    }

    bool TWSExecutorPool::SpecificSend(std::unique_ptr<IEventHandle>& ev) {
        Y_DEBUG_ABORT_UNLESS(ev->GetRecipientRewrite().PoolID() == PoolId);
#ifdef ACTORSLIB_COLLECT_EXEC_STATS
        RelaxedStore(&ev->SendTime, (::NHPTimer::STime)GetCycleCountFast());
#endif
        if (TlsThreadContext) {
            TlsThreadContext->IsCurrentRecipientAService = ev->Recipient.IsService();
        }

        if (TMailbox* mailbox = WsMailboxTable_->Get(ev->GetRecipientRewrite().Hint())) {
            switch (mailbox->Push(ev)) {
                case EMailboxPush::Pushed:
                    return true;
                case EMailboxPush::Locked:
                    mailbox->ScheduleMoment = GetCycleCountFast();
                    SpecificScheduleActivation(mailbox);
                    return true;
                case EMailboxPush::Free:
                    Y_DEBUG_ABORT("WS SpecificSend: Push returned Free for hint=%" PRIu32 " recipient=%s",
                        ev->GetRecipientRewrite().Hint(),
                        ev->GetRecipientRewrite().ToString().c_str());
                    break;
            }
        }

        return false;
    }

    TActorId TWSExecutorPool::Register(IActor* actor, TMailboxType::EType /*mailboxType*/, ui64 revolvingWriteCounter, const TActorId& parentId) {
        NHPTimer::STime hpstart = GetCycleCountFast();
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

        // Determine slot from TLS (worker thread always knows its slot)
        ui32 hint;
        if (TlsThreadContext && TlsThreadContext->WorkerId() < static_cast<TWorkerId>(SlotAllocators_.size())) {
            hint = SlotAllocators_[TlsThreadContext->WorkerId()].Allocate();
        } else {
            // External thread (system init) — fall back to global pool
            size_t got = WsMailboxTable_->AllocateBatch(&hint, 1);
            Y_ABORT_UNLESS(got > 0, "Failed to allocate mailbox hint");
        }

        TMailbox* mailbox = WsMailboxTable_->Get(hint);
        Y_ABORT_UNLESS(mailbox);
        mailbox->LockFromFree();

        // Stamp initial execution end time so the first event's idle time
        // is measured from allocation
        if (auto* execStats = WsMailboxTable_->GetStats(mailbox->Hint)) {
            execStats->LastExecutionEndCycles.store(GetCycleCountFast(), std::memory_order_relaxed);
        }

        const ui64 localActorId = AllocateID();
        mailbox->AttachActor(localActorId, actor);

        const TActorId actorId(ActorSystem->NodeId, PoolId, localActorId, mailbox->Hint);
        DoActorInit(ActorSystem, actor, actorId, parentId);

        // Stuck actor monitoring is handled by TExecutorPoolBaseMailboxed
        // (accesses IActor::StuckIndex via friend declaration).
        // WS Register bypasses that; monitoring uses standard counters.

        // Once we unlock the mailbox the actor starts running
        actor = nullptr;

        mailbox->Unlock(this, GetCycleCountFast(), revolvingWriteCounter);

        NHPTimer::STime elapsed = GetCycleCountFast() - hpstart;
        (void)elapsed;

        return actorId;
    }

    TActorId TWSExecutorPool::Register(IActor* actor, TMailboxCache& /*cache*/, ui64 revolvingWriteCounter, const TActorId& parentId) {
        // WS pools ignore the cache; use slot allocator instead
        return Register(actor, TMailboxType::LockFreeIntrusive, revolvingWriteCounter, parentId);
    }

    bool TWSExecutorPool::Cleanup() {
        if (WsMailboxTable_) {
            return WsMailboxTable_->Cleanup();
        }
        return MailboxTable->Cleanup();
    }

    TMailboxTable* TWSExecutorPool::GetMailboxTable() const {
        // WS pools use WsMailboxTable; return nullptr to signal callers
        // should not use the legacy TMailboxTable path.
        return nullptr;
    }

    TMailbox* TWSExecutorPool::GetReadyActivation(ui64 /*revolvingCounter*/) {
        // WS workers poll via PollSlot, not through this method
        return nullptr;
    }

    void TWSExecutorPool::ScheduleActivation(TMailbox* mailbox) {
        RouteActivation(mailbox);
    }

    void TWSExecutorPool::SpecificScheduleActivation(TMailbox* mailbox) {
        RouteActivation(mailbox);
    }

    void TWSExecutorPool::ScheduleActivationEx(TMailbox* mailbox, ui64 /*revolvingCounter*/) {
        if (mailbox == CurrentlyExecutingMailbox) {
            // Called from inside Execute → Unlock → TryUnlock failed.
            // Defer re-injection until Execute returns to prevent concurrent execution.
            DeferredReinjection = mailbox;
            return;
        }
        RouteActivation(mailbox);
    }

    void TWSExecutorPool::RouteActivation(TMailbox* mailbox) {
        if (!Router_) {
            return;
        }

        if (auto* stats = WsMailboxTable_->GetStats(mailbox->Hint)) {
            stats->ActivationCount.fetch_add(1, std::memory_order_relaxed);
        }

        // Bucket prediction for unclassified mailboxes
        if (BucketMap_) {
            if (!BucketMap_->IsClassified(mailbox->Hint)) {
                // Try to predict from actor-class stats
                ui32 hint = mailbox->Hint;
                mailbox->ForEach([this, hint](ui64 /*actorId*/, IActor* actor) {
                    if (!actor) return;
                    if (auto* cs = actor->GetClassStats()) {
                        ui64 msgs = cs->MessagesProcessed.load(std::memory_order_relaxed);
                        if (msgs > 0) {
                            ui64 cycles = cs->ExecutionCycles.load(std::memory_order_relaxed);
                            uint8_t predicted = BucketMap_->PredictFromClassCost(cycles / msgs);
                            if (predicted != 0) {
                                BucketMap_->SetBucket(hint, predicted);
                            }
                        }
                    }
                });
            }

            BucketMap_->TrackActive(mailbox->Hint);
        }

        int slotIdx = Router_->Route(mailbox->Hint, mailbox->LastPoolSlotIdx);

        if (slotIdx < 0) {
            return;
        }

        // Wake only the worker owning the target slot.
        if (Driver_) {
            Driver_->WakeSlot(&Slots_[slotIdx]);
        }
    }

    void TWSExecutorPool::Schedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) {
        Schedule(deadline - ActorSystem->Timestamp(), ev, cookie, workerId);
    }

    void TWSExecutorPool::Schedule(TMonotonic deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) {
        const auto current = ActorSystem->Monotonic();
        if (deadline < current) {
            deadline = current;
        }

        Y_DEBUG_ABORT_UNLESS(workerId < MaxSlotCount_);
        ScheduleWriters_[workerId % MaxSlotCount_].Push(deadline.MicroSeconds(), ev.Release(), cookie);
    }

    void TWSExecutorPool::Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) {
        const auto deadline = ActorSystem->Monotonic() + delta;
        Y_DEBUG_ABORT_UNLESS(workerId < MaxSlotCount_);
        ScheduleWriters_[workerId % MaxSlotCount_].Push(deadline.MicroSeconds(), ev.Release(), cookie);
    }

    TAffinity* TWSExecutorPool::Affinity() const {
        // Driver manages CPU affinity, not the pool
        return nullptr;
    }

    ui64 TWSExecutorPool::TimePerMailboxTs() const {
        return TimePerMailboxTsValue_;
    }

    ui32 TWSExecutorPool::EventsPerMailbox() const {
        return EventsPerMailboxValue_;
    }

    TString TWSExecutorPool::GetName() const {
        return PoolName_;
    }

    ui32 TWSExecutorPool::GetThreads() const {
        return static_cast<ui32>(ActiveSlotCount_.load(std::memory_order_relaxed));
    }

    float TWSExecutorPool::GetThreadCount() const {
        return static_cast<float>(ActiveSlotCount_.load(std::memory_order_relaxed));
    }

    i16 TWSExecutorPool::GetFullThreadCount() const {
        return ActiveSlotCount_.load(std::memory_order_relaxed);
    }

    void TWSExecutorPool::SetFullThreadCount(i16 threads) {
        if (threads < MinSlotCount_) {
            threads = MinSlotCount_;
        }
        if (threads > MaxSlotCount_) {
            threads = MaxSlotCount_;
        }

        i16 current = ActiveSlotCount_.load(std::memory_order_relaxed);

        if (threads > current) {
            // Activate more slots
            for (i16 i = current; i < threads && i < MaxSlotCount_; ++i) {
                if (Driver_ && Slots_[i].GetState() == ESlotState::Inactive) {
                    Driver_->ActivateSlot(&Slots_[i]);
                }
            }
        } else if (threads < current) {
            // Deactivate excess slots (from the end)
            for (i16 i = current - 1; i >= threads; --i) {
                if (Driver_ && Slots_[i].GetState() == ESlotState::Active) {
                    Driver_->DeactivateSlot(&Slots_[i]);
                }
            }
        }

        ActiveSlotCount_.store(threads, std::memory_order_relaxed);

        // Refresh the router's view of active slots
        if (Router_) {
            Router_->RefreshActiveSlots();
        }

        // Update bucket map's active count (it recomputes boundary internally)
        if (BucketMap_) {
            BucketMap_->SetActiveCount(static_cast<ui16>(threads));
        }
    }

    float TWSExecutorPool::GetDefaultThreadCount() const {
        return static_cast<float>(DefaultSlotCount_);
    }

    i16 TWSExecutorPool::GetDefaultFullThreadCount() const {
        return DefaultSlotCount_;
    }

    float TWSExecutorPool::GetMinThreadCount() const {
        return static_cast<float>(MinSlotCount_);
    }

    i16 TWSExecutorPool::GetMinFullThreadCount() const {
        return MinSlotCount_;
    }

    float TWSExecutorPool::GetMaxThreadCount() const {
        return static_cast<float>(MaxSlotCount_);
    }

    i16 TWSExecutorPool::GetMaxFullThreadCount() const {
        return MaxSlotCount_;
    }

    i16 TWSExecutorPool::GetPriority() const {
        return Priority_;
    }

    TCpuConsumption TWSExecutorPool::GetThreadCpuConsumption(i16 threadIdx) {
        if (threadIdx < 0 || threadIdx >= MaxSlotCount_) {
            return TCpuConsumption{0.0, 0.0};
        }
        double load = Slots_[threadIdx].LoadEstimate.load(std::memory_order_relaxed);
        return TCpuConsumption{load, load};
    }

    TWsSlotCountersSnapshot TWSExecutorPool::AggregateCounters() const {
        TWsSlotCountersSnapshot total;
        for (size_t i = 0; i < Slots_.size(); ++i) {
            total.Add(Slots_[i].Counters.Snapshot());
        }
        return total;
    }

    void TWSExecutorPool::ResetCounters() {
        for (size_t i = 0; i < Slots_.size(); ++i) {
            Slots_[i].Counters.Reset();
        }
    }

    void TWSExecutorPool::DumpCounters(const char* label) const {
        AggregateCounters().Dump(label);
    }

    uint64_t TWSExecutorPool::AdaptiveInflateEvents() const {
        return AdaptiveScaler_ ? AdaptiveScaler_->InflateEvents() : 0;
    }

    uint64_t TWSExecutorPool::AdaptiveDeflateEvents() const {
        return AdaptiveScaler_ ? AdaptiveScaler_->DeflateEvents() : 0;
    }

    void TWSExecutorPool::DumpSlots(IOutputStream& out) const {
        i16 active = ActiveSlotCount_.load(std::memory_order_relaxed);
        out << "  pool=" << PoolName_ << " active=" << active << "/" << MaxSlotCount_ << Endl;
        for (i16 i = 0; i < MaxSlotCount_; ++i) {
            auto state = Slots_[i].GetState();
            const char* stateStr = "???";
            switch (state) {
                case ESlotState::Inactive: stateStr = "Inactive"; break;
                case ESlotState::Initializing: stateStr = "Initializing"; break;
                case ESlotState::Active: stateStr = "Active"; break;
                case ESlotState::Draining: stateStr = "Draining"; break;
            }
            int ringCount = Slots_[i].ContinuationCount.load(std::memory_order_relaxed);
            out << "  slot[" << i << "]: state=" << stateStr
                << " queue=" << Slots_[i].SizeEstimate()
                << " ring=" << ringCount
                << " spinning=" << Slots_[i].WorkerSpinning.load(std::memory_order_relaxed)
                << " executing=" << Slots_[i].Executing.load(std::memory_order_relaxed);
            if (ringCount > 0) {
                out << " ring_items=[";
                for (int j = 0; j < ringCount && j < 8; ++j) {
                    if (j > 0) out << ",";
                    out << Slots_[i].RingSnapshot[j].load(std::memory_order_relaxed);
                }
                out << "]";
            }
            // Per-slot watched-hint PollSlot tracing
            out << Endl;
        }
    }

} // namespace NActors::NWorkStealing
