#pragma once

#include "ws_slot.h"
#include "ws_config.h"
#include "ws_counters.h"
#include "activation_router.h"
#include "driver.h"
#include "ws_executor_context.h"

#include <ydb/library/actors/core/executor_pool_base.h>
#include <ydb/library/actors/core/scheduler_queue.h>

namespace NActors::NWorkStealing {

    // Config for a WS executor pool
    struct TWSExecutorPoolConfig {
        ui32 PoolId = 0;
        TString PoolName;
        i16 MinSlotCount = 1;
        i16 MaxSlotCount = 32;
        i16 DefaultSlotCount = 4;
        TDuration TimePerMailbox = TDuration::MilliSeconds(10);
        ui32 EventsPerMailbox = 100;
        i16 Priority = 0;
        TWsConfig WsConfig;
    };

    class TWSExecutorPool: public TExecutorPoolBaseMailboxed {
    public:
        explicit TWSExecutorPool(const TWSExecutorPoolConfig& cfg);
        ~TWSExecutorPool();

        // Inject driver reference (called by TCpuManager before Start)
        void SetDriver(IDriver* driver);

        // Access slots (for Driver integration)
        TSlot* GetSlots() {
            return Slots_.data();
        }
        size_t GetSlotCount() const {
            return Slots_.size();
        }
        const TWsConfig& GetWsConfig() const {
            return WsConfig_;
        }

        // --- IActorThreadPool lifecycle ---
        void Prepare(TActorSystem* actorSystem, NSchedulerQueue::TReader** scheduleReaders, ui32* scheduleSz) override;
        void Start() override;
        void PrepareStop() override;
        void Shutdown() override;

        // --- IExecutorPool ---
        TMailbox* GetReadyActivation(ui64 revolvingCounter) override;
        void ScheduleActivation(TMailbox* mailbox) override;
        void SpecificScheduleActivation(TMailbox* mailbox) override;
        void ScheduleActivationEx(TMailbox* mailbox, ui64 revolvingCounter) override;

        void Schedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) override;
        void Schedule(TMonotonic deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) override;
        void Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) override;

        TAffinity* Affinity() const override;

        ui64 TimePerMailboxTs() const override;
        ui32 EventsPerMailbox() const override;

        // --- Thread/slot count (harmonizer compatibility) ---
        TString GetName() const override;
        ui32 GetThreads() const override;
        float GetThreadCount() const override;
        i16 GetFullThreadCount() const override;
        void SetFullThreadCount(i16 threads) override;
        float GetDefaultThreadCount() const override;
        i16 GetDefaultFullThreadCount() const override;
        float GetMinThreadCount() const override;
        i16 GetMinFullThreadCount() const override;
        float GetMaxThreadCount() const override;
        i16 GetMaxFullThreadCount() const override;
        i16 GetPriority() const override;
        TCpuConsumption GetThreadCpuConsumption(i16 threadIdx) override;

        // Aggregate counters across all slots, reset, and dump to stderr.
        TWsSlotCountersSnapshot AggregateCounters() const;
        void ResetCounters();
        void DumpCounters(const char* label) const;

        // Debug: last created pool instance (for benchmark introspection).
        static TWSExecutorPool* LastCreated;

    private:
        void RouteActivation(TMailbox* mailbox);

        TWsConfig WsConfig_;

        TVector<TSlot> Slots_;
        std::unique_ptr<TActivationRouter> Router_;

        IDriver* Driver_ = nullptr;

        // Scheduler queue (one writer per slot for Schedule() calls)
        TArrayHolder<NSchedulerQueue::TReader> ScheduleReaders_;
        TArrayHolder<NSchedulerQueue::TWriter> ScheduleWriters_;

        // Active slot tracking
        std::atomic<i16> ActiveSlotCount_{0};

        const TString PoolName_;
        const TDuration TimePerMailbox_;
        const ui64 TimePerMailboxTsValue_;
        const ui32 EventsPerMailboxValue_;
        const i16 MinSlotCount_;
        const i16 MaxSlotCount_;
        const i16 DefaultSlotCount_;
        const i16 Priority_;

        std::atomic_bool StopFlag_{false};

        // One TWSExecutorContext per slot (created in Prepare)
        TVector<std::unique_ptr<TWSExecutorContext>> Contexts_;
    };

} // namespace NActors::NWorkStealing
