#include "executor_pool_ws.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/mailbox_lockfree.h>
#include <ydb/library/actors/util/datetime.h>


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
    }

    TWSExecutorPool::~TWSExecutorPool() {
        if (LastCreated == this) {
            LastCreated = nullptr;
        }
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

        // Wire MailboxTable onto each slot for cost-aware stealing
        TMailboxTable* mboxTable = MailboxTable;
        for (i16 i = 0; i < MaxSlotCount_; ++i) {
            Slots_[i].MailboxTable = mboxTable;
        }

        // Register all slots with the driver and wire per-worker callbacks
        if (Driver_) {
            Driver_->RegisterSlots(Slots_.data(), static_cast<size_t>(MaxSlotCount_));
            auto* pool = this;
            for (i16 i = 0; i < MaxSlotCount_; ++i) {
                auto* ctx = Contexts_[i].get();
                TWorkerCallbacks callbacks;
                i16 slotIdx = i;
                callbacks.Execute = [ctx, mboxTable, pool, slotIdx](ui32 hint, NHPTimer::STime& hpnow) -> bool {
                    TMailbox* mailbox = mboxTable->Get(hint);
                    if (!mailbox) {
                        return false;
                    }

                    CurrentlyExecutingMailbox = mailbox;
                    DeferredReinjection = nullptr;

                    bool processed = ctx->ExecuteSingleEvent(mailbox, hpnow);

                    // Stamp affinity BEFORE any unlock — after unlock another
                    // thread may read LastPoolSlotIdx via RouteActivation.
                    mailbox->LastPoolSlotIdx = static_cast<ui16>(slotIdx + 1);

                    if (!processed) {
                        // No event available. Finalize (unlock/free) the mailbox.
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

                    // Event processed, mailbox stays locked for more.
                    mailbox->LastPoolSlotIdx = static_cast<ui16>(slotIdx + 1);
                    CurrentlyExecutingMailbox = nullptr;
                    DeferredReinjection = nullptr;
                    return true;
                };
                callbacks.Setup = [ctx]() {
                    ctx->SetupTLS();
                };
                callbacks.Teardown = [ctx]() {
                    ctx->ClearTLS();
                };
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
        // Driver manages worker threads; nothing to do here
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

        int slotIdx = Router_->Route(mailbox->Hint, mailbox->LastPoolSlotIdx);

        // Wake only the worker owning the target slot.
        if (slotIdx >= 0 && Driver_) {
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

} // namespace NActors::NWorkStealing
