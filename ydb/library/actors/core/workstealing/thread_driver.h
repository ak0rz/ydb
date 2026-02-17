#pragma once

#include "driver.h"
#include "ws_config.h"
#include "ws_poll.h"
#include "topology.h"

#include <ydb/library/actors/util/threadparkpad.h>
#include <util/system/thread.h>

#include <atomic>
#include <memory>
#include <vector>

namespace NActors::NWorkStealing {

    // Thread-based driver implementation.
    // Creates one worker thread per slot (for simplicity in the first iteration).
    // Workers are CPU-pinned and poll their assigned slot.
    class TThreadDriver: public IDriver {
    public:
        explicit TThreadDriver(const TWsConfig& config);
        ~TThreadDriver() override;

        // IDriver interface
        void Prepare(const TCpuTopology& topology) override;
        void Start() override;
        void PrepareStop() override;
        void Shutdown() override;

        void RegisterSlots(TSlot* slots, size_t count) override;
        void ActivateSlot(TSlot* slot) override;
        void DeactivateSlot(TSlot* slot) override;
        void WakeSlot(TSlot* slot) override;

        void SetWorkerCallbacks(TSlot* slot, TWorkerCallbacks callbacks) override;

        std::unique_ptr<IStealIterator> MakeStealIterator(TSlot* exclude) override;

    private:
        struct TWorker {
            TSlot* Slot = nullptr;
            std::unique_ptr<TThread> Thread;
            NActors::TThreadParkPad ParkPad;
            std::atomic<bool> ShouldStop{false};
            ui16 WorkerIndex = 0;
            ui16 GroupIndex = 0;      // which slot group (pool) this worker belongs to
            TCpuId AssignedCpu = 0;   // CPU to pin to
            TWorkerCallbacks Callbacks;
        };

        struct TSlotGroup {
            std::vector<TWorker*> Workers; // workers in registration order (= topology order)
        };

        void WorkerLoop(TWorker& worker);

        TWsConfig Config_;
        TCpuTopology Topology_;

        std::vector<std::unique_ptr<TWorker>> Workers_;
        std::vector<TSlot*> AllSlots_; // all registered slots
        std::vector<TSlotGroup> Groups_;
        std::vector<TCpuId> GlobalCpuOrder_;   // all CPUs in topology order from seed
        size_t NextCpuOffset_ = 0;            // next available position in GlobalCpuOrder_

        std::atomic<bool> Started_{false};
        std::atomic<bool> Stopping_{false};
    };

    // Topology-ordered steal iterator for TThreadDriver.
    // Iterates over registered slots in circular order starting from the
    // worker's position, limited to MaxStealNeighbors per scan.
    class TTopologyStealIterator: public IStealIterator {
    public:
        TTopologyStealIterator(const std::vector<TSlot*>& slots, TSlot* exclude, size_t maxProbe);
        TTopologyStealIterator(std::vector<TSlot*> orderedSlots, size_t maxProbe);

        TSlot* Next() override;
        void Reset() override;

    private:
        std::vector<TSlot*> Slots_; // all slots except self, in circular order
        size_t MaxProbe_;           // max neighbors to scan per steal attempt
        size_t Index_ = 0;
        size_t Probed_ = 0;
    };

} // namespace NActors::NWorkStealing
