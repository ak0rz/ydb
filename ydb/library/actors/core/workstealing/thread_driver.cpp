#include "thread_driver.h"

#include <ydb/library/actors/util/affinity.h>
#include <ydb/library/actors/util/datetime.h>
#include <util/system/yassert.h>

#include <algorithm>

namespace NActors::NWorkStealing {

    // --- TTopologyStealIterator ---

    TTopologyStealIterator::TTopologyStealIterator(
        const std::vector<TSlot*>& slots,
        TSlot* exclude,
        size_t maxProbe)
        : MaxProbe_(maxProbe)
    {
        // Find exclude position and build circular neighbor list starting after it.
        // Each slot gets a different starting position, distributing steal pressure.
        size_t excludePos = 0;
        for (size_t i = 0; i < slots.size(); ++i) {
            if (slots[i] == exclude) {
                excludePos = i;
                break;
            }
        }

        Slots_.reserve(slots.size() - 1);
        for (size_t i = 1; i < slots.size(); ++i) {
            Slots_.push_back(slots[(excludePos + i) % slots.size()]);
        }
    }

    TTopologyStealIterator::TTopologyStealIterator(
        std::vector<TSlot*> orderedSlots,
        size_t maxProbe)
        : Slots_(std::move(orderedSlots))
        , MaxProbe_(maxProbe)
    {
    }

    TSlot* TTopologyStealIterator::Next() {
        if (Probed_ >= MaxProbe_ || Index_ >= Slots_.size()) {
            return nullptr;
        }
        ++Probed_;
        return Slots_[Index_++];
    }

    void TTopologyStealIterator::Reset() {
        // Advance starting position for next steal cycle so we don't
        // always probe the same neighbors first.
        if (Slots_.size() > 0) {
            size_t advance = (Probed_ > 0) ? Probed_ : 1;
            // Rotate: move front elements to back
            if (advance < Slots_.size()) {
                std::rotate(Slots_.begin(), Slots_.begin() + advance, Slots_.end());
            }
        }
        Index_ = 0;
        Probed_ = 0;
    }

    // --- TThreadDriver ---

    TThreadDriver::TThreadDriver(const TWsConfig& config)
        : Config_(config)
        , Topology_(TCpuTopology::MakeFlat(0))
    {
    }

    TThreadDriver::~TThreadDriver() {
        if (Started_.load(std::memory_order_relaxed)) {
            Shutdown();
        }
    }

    void TThreadDriver::Prepare(const TCpuTopology& topology) {
        Topology_ = topology;
        if (topology.GetCpuCount() > 0) {
            TCpuId seed = 0;
            auto neighbors = topology.GetNeighborsOrdered(seed);
            GlobalCpuOrder_.reserve(1 + neighbors.size());
            GlobalCpuOrder_.push_back(seed);
            GlobalCpuOrder_.insert(GlobalCpuOrder_.end(), neighbors.begin(), neighbors.end());
        }
    }

    void TThreadDriver::Start() {
        Started_.store(true, std::memory_order_release);

        struct TContext {
            TThreadDriver* Driver;
            TWorker* Worker;
        };

        for (auto& worker : Workers_) {
            if (!worker->Slot) {
                continue;
            }
            auto* ctx = new TContext{this, worker.get()};
            worker->Thread = std::make_unique<TThread>(
                TThread::TParams(
                    +[](void* arg) -> void* {
                        auto* c = static_cast<TContext*>(arg);
                        c->Driver->WorkerLoop(*c->Worker);
                        delete c;
                        return nullptr;
                    },
                    ctx)
                    .SetName("ws_worker"));
            worker->Thread->Start();
        }
    }

    void TThreadDriver::PrepareStop() {
        Stopping_.store(true, std::memory_order_release);
        for (auto& worker : Workers_) {
            worker->ParkPad.Interrupt();
        }
    }

    void TThreadDriver::Shutdown() {
        if (!Started_.load(std::memory_order_acquire)) {
            return;
        }

        Stopping_.store(true, std::memory_order_release);

        for (auto& worker : Workers_) {
            worker->ShouldStop.store(true, std::memory_order_release);
            worker->ParkPad.Interrupt();
        }

        for (auto& worker : Workers_) {
            if (worker->Thread && worker->Thread->Running()) {
                worker->Thread->Join();
            }
        }

        Started_.store(false, std::memory_order_release);
    }

    void TThreadDriver::RegisterSlots(TSlot* slots, size_t count) {
        TSlotGroup group;
        ui16 groupIdx = static_cast<ui16>(Groups_.size());

        for (size_t i = 0; i < count; ++i) {
            TSlot* slot = &slots[i];
            AllSlots_.push_back(slot);

            auto worker = std::make_unique<TWorker>();
            worker->Slot = slot;
            worker->WorkerIndex = static_cast<ui16>(Workers_.size());
            worker->GroupIndex = groupIdx;
            slot->DriverData = worker.get();

            // Assign CPU from global topology order
            if (!GlobalCpuOrder_.empty()) {
                worker->AssignedCpu = GlobalCpuOrder_[
                    (NextCpuOffset_ + i) % GlobalCpuOrder_.size()];
            }

            group.Workers.push_back(worker.get());
            Workers_.push_back(std::move(worker));
        }

        NextCpuOffset_ += count;
        Groups_.push_back(std::move(group));
    }

    void TThreadDriver::ActivateSlot(TSlot* slot) {
        slot->TryTransition(ESlotState::Inactive, ESlotState::Initializing);
        slot->TryTransition(ESlotState::Initializing, ESlotState::Active);

        // Wake the worker associated with this slot
        auto* worker = static_cast<TWorker*>(slot->DriverData);
        if (worker) {
            worker->ParkPad.Unpark();
        }
    }

    void TThreadDriver::DeactivateSlot(TSlot* slot) {
        slot->TryTransition(ESlotState::Active, ESlotState::Draining);
    }

    void TThreadDriver::WakeSlot(TSlot* slot) {
        // Fast path: if the worker is actively spinning, it will find the
        // injected work on its next PollSlot iteration — skip the wake.
        if (slot->WorkerSpinning.load(std::memory_order_seq_cst)) {
            return;
        }
        // Worker is parked or about to park. Unpark it.
        // The Dekker protocol in WorkerLoop ensures no missed wakes.
        slot->Counters.Wakes.fetch_add(1, std::memory_order_relaxed);
        auto* worker = static_cast<TWorker*>(slot->DriverData);
        if (worker) {
            worker->ParkPad.Unpark();
        }
    }

    std::unique_ptr<IStealIterator> TThreadDriver::MakeStealIterator(TSlot* exclude) {
        auto* worker = static_cast<TWorker*>(exclude->DriverData);
        if (!worker || Groups_.empty()) {
            return std::make_unique<TTopologyStealIterator>(AllSlots_, exclude, Config_.MaxStealNeighbors);
        }

        // Build slot list from same group only, excluding self.
        // Start from worker's position + 1, wrap around (closest neighbors first).
        auto& group = Groups_[worker->GroupIndex];
        std::vector<TSlot*> groupSlots;
        groupSlots.reserve(group.Workers.size() - 1);

        size_t selfPos = 0;
        for (size_t i = 0; i < group.Workers.size(); ++i) {
            if (group.Workers[i]->Slot == exclude) {
                selfPos = i;
                break;
            }
        }
        for (size_t j = 1; j < group.Workers.size(); ++j) {
            size_t idx = (selfPos + j) % group.Workers.size();
            groupSlots.push_back(group.Workers[idx]->Slot);
        }

        return std::make_unique<TTopologyStealIterator>(std::move(groupSlots), Config_.MaxStealNeighbors);
    }

    void TThreadDriver::SetWorkerCallbacks(TSlot* slot, TWorkerCallbacks callbacks) {
        auto* worker = static_cast<TWorker*>(slot->DriverData);
        if (worker) {
            worker->Callbacks = std::move(callbacks);
        }
    }

    void TThreadDriver::WorkerLoop(TWorker& worker) {
        // Pin to assigned CPU
        if (!GlobalCpuOrder_.empty()) {
            TAffinity affinity(TCpuMask(worker.AssignedCpu));
            affinity.Set();
        }

        if (worker.Callbacks.Setup) {
            worker.Callbacks.Setup();
        }

        auto stealIterator = MakeStealIterator(worker.Slot);
        TPollState pollState;
        ui64 lastLocalWorkTs = GetCycleCountFast();
        // Adaptive spin: start with short threshold after wake, extend to full
        // threshold once the slot proves it has steady local work.
        ui64 spinThreshold = Config_.MinSpinThresholdCycles;

        worker.Slot->WorkerSpinning.store(true, std::memory_order_release);

        while (!worker.ShouldStop.load(std::memory_order_acquire)) {
            ESlotState slotState = worker.Slot->GetState();

            if (slotState == ESlotState::Draining) {
                // Drain remaining activations without stealing — slot is winding down.
                EPollResult result = PollSlot(*worker.Slot, nullptr, worker.Callbacks.Execute, Config_, pollState);
                if (result == EPollResult::Busy) {
                    lastLocalWorkTs = GetCycleCountFast();
                    continue;
                }

                // Queue appears empty. Dekker: announce not-spinning, re-check
                // for items from in-flight Route() calls that saw Active before
                // the transition.
                worker.Slot->WorkerSpinning.store(false, std::memory_order_seq_cst);
                if (worker.Slot->HasWork()) {
                    worker.Slot->WorkerSpinning.store(true, std::memory_order_release);
                    continue;
                }

                // Complete deactivation.
                worker.Slot->TryTransition(ESlotState::Draining, ESlotState::Inactive);

                // Final safety net: execute items that raced with the transition.
                // After Inactive, no new Route() calls will target this slot.
                while (auto item = worker.Slot->Pop()) {
                    if (worker.Callbacks.Execute) {
                        worker.Callbacks.Execute(*item);
                    }
                }

                worker.ParkPad.Park();
                worker.Slot->WorkerSpinning.store(true, std::memory_order_release);
                if (worker.ShouldStop.load(std::memory_order_acquire)) {
                    break;
                }
                lastLocalWorkTs = GetCycleCountFast();
                spinThreshold = Config_.MinSpinThresholdCycles;
                continue;
            }

            if (slotState != ESlotState::Active) {
                // Inactive or Initializing — park and wait for activation.
                worker.Slot->WorkerSpinning.store(false, std::memory_order_seq_cst);
                worker.ParkPad.Park();
                worker.Slot->WorkerSpinning.store(true, std::memory_order_release);
                if (worker.ShouldStop.load(std::memory_order_acquire)) {
                    break;
                }
                lastLocalWorkTs = GetCycleCountFast();
                spinThreshold = Config_.MinSpinThresholdCycles;
                continue;
            }

            EPollResult result = PollSlot(*worker.Slot, stealIterator.get(), worker.Callbacks.Execute, Config_, pollState);

            if (pollState.HadLocalWork) {
                lastLocalWorkTs = GetCycleCountFast();
                // Local work found — earn full spin time
                spinThreshold = Config_.SpinThresholdCycles;
            }

            // Park after spinning long enough without local work.
            // Only local work (activations from this slot's own queues) resets
            // the spin timer. Stolen work keeps the worker productive but does
            // not extend spinning — the worker is helping another slot and
            // should eventually park if its own slot stays idle.
            // Check on both Idle and stolen-Busy to ensure workers that keep
            // stealing but have no local work eventually park.
            if (result == EPollResult::Idle ||
                (result == EPollResult::Busy && !pollState.HadLocalWork))
            {
                ui64 now = GetCycleCountFast();
                if (now - lastLocalWorkTs > spinThreshold) {
                    // Dekker protocol: announce intent to park, then re-check.
                    // Paired with seq_cst load in WakeSlot. Either:
                    //  - WakeSlot sees WorkerSpinning=false → calls Unpark
                    //  - We see the injection → skip park
                    worker.Slot->WorkerSpinning.store(false, std::memory_order_seq_cst);

                    if (worker.Slot->HasWork()) {
                        // Work arrived between last poll and now — don't park
                        worker.Slot->WorkerSpinning.store(true, std::memory_order_release);
                        lastLocalWorkTs = GetCycleCountFast();
                        spinThreshold = Config_.MinSpinThresholdCycles;
                        continue;
                    }

                    worker.Slot->Counters.Parks.fetch_add(1, std::memory_order_relaxed);
                    worker.ParkPad.Park();
                    worker.Slot->WorkerSpinning.store(true, std::memory_order_release);
                    if (worker.ShouldStop.load(std::memory_order_acquire)) {
                        break;
                    }
                    pollState = TPollState{};
                    lastLocalWorkTs = GetCycleCountFast();
                    spinThreshold = Config_.MinSpinThresholdCycles;
                }
            }
        }

        worker.Slot->WorkerSpinning.store(false, std::memory_order_release);

        if (worker.Callbacks.Teardown) {
            worker.Callbacks.Teardown();
        }
    }

} // namespace NActors::NWorkStealing
