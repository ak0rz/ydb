#include "harmonizer_adapter.h"

#include <ydb/library/actors/util/datetime.h>

namespace NActors::NWorkStealing {

    THarmonizerAdapter::THarmonizerAdapter(TSlot* slots, i16 maxSlotCount)
        : Slots_(slots)
        , MaxSlotCount_(maxSlotCount)
    {
    }

    TSlotCpuConsumption THarmonizerAdapter::GetSlotCpuConsumption(i16 slotIdx) const {
        if (slotIdx < 0 || slotIdx >= MaxSlotCount_ || !Slots_) {
            return {};
        }

        const TSlotStats& stats = Slots_[slotIdx].Stats;
        uint64_t busy = stats.BusyCycles.load(std::memory_order_relaxed);
        uint64_t idle = stats.IdleCycles.load(std::memory_order_relaxed);
        double cpuUs = Ts2Us(busy);
        double elapsedUs = Ts2Us(busy + idle);
        return {cpuUs, elapsedUs};
    }

    void THarmonizerAdapter::CollectPoolStats(TSlotStats& aggregated, i16 activeSlotCount) const {
        aggregated.ActivationsExecuted = 0;
        aggregated.BusyCycles.store(0, std::memory_order_relaxed);
        aggregated.IdleCycles.store(0, std::memory_order_relaxed);
        aggregated.ExecTimeAccumNs = 0;
        if (!Slots_) {
            return;
        }

        i16 count = (activeSlotCount < MaxSlotCount_) ? activeSlotCount : MaxSlotCount_;
        uint64_t totalBusy = 0;
        uint64_t totalIdle = 0;
        for (i16 i = 0; i < count; ++i) {
            const TSlotStats& s = Slots_[i].Stats;
            aggregated.ActivationsExecuted += s.ActivationsExecuted;
            totalBusy += s.BusyCycles.load(std::memory_order_relaxed);
            totalIdle += s.IdleCycles.load(std::memory_order_relaxed);
            aggregated.ExecTimeAccumNs += s.ExecTimeAccumNs;
        }
        aggregated.BusyCycles.store(totalBusy, std::memory_order_relaxed);
        aggregated.IdleCycles.store(totalIdle, std::memory_order_relaxed);
    }

    void THarmonizerAdapter::UpdateLoadEstimates(i16 activeSlotCount) {
        if (!Slots_) {
            return;
        }

        i16 count = (activeSlotCount < MaxSlotCount_) ? activeSlotCount : MaxSlotCount_;
        for (i16 i = 0; i < count; ++i) {
            const TSlotStats& s = Slots_[i].Stats;
            uint64_t busy = s.BusyCycles.load(std::memory_order_relaxed);
            uint64_t idle = s.IdleCycles.load(std::memory_order_relaxed);
            uint64_t total = busy + idle;
            double load = (total > 0)
                              ? static_cast<double>(busy) / static_cast<double>(total)
                              : 0.0;
            Slots_[i].LoadEstimate.store(load, std::memory_order_relaxed);
        }
    }

    void THarmonizerAdapter::ResetCounters(i16 activeSlotCount) {
        if (!Slots_) {
            return;
        }

        i16 count = (activeSlotCount < MaxSlotCount_) ? activeSlotCount : MaxSlotCount_;
        for (i16 i = 0; i < count; ++i) {
            Slots_[i].Stats.BusyCycles.store(0, std::memory_order_relaxed);
            Slots_[i].Stats.IdleCycles.store(0, std::memory_order_relaxed);
        }
    }

} // namespace NActors::NWorkStealing
