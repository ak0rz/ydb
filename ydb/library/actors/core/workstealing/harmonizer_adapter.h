#pragma once

#include "ws_slot.h"

#include <cstdint>

namespace NActors::NWorkStealing {

    // CPU consumption snapshot for a single slot, expressed in microseconds.
    // Mirrors the NActors::TCpuConsumption layout but avoids pulling in executor_pool.h.
    struct TSlotCpuConsumption {
        double CpuUs = 0;
        double ElapsedUs = 0;
    };

    // Collects and translates slot stats into harmonizer-compatible metrics.
    // Used by TWSExecutorPool to implement GetThreadCpuConsumption.
    class THarmonizerAdapter {
    public:
        THarmonizerAdapter(TSlot* slots, i16 maxSlotCount);

        // Snapshot per-slot load for harmonizer consumption reporting.
        // Returns consumed CPU and elapsed time in microseconds for the given slot index.
        TSlotCpuConsumption GetSlotCpuConsumption(i16 slotIdx) const;

        // Aggregate all active slot stats into pool-level stats.
        void CollectPoolStats(TSlotStats& aggregated, i16 activeSlotCount) const;

        // Update load estimates for all active slots based on their busy/idle cycles.
        // Called periodically (e.g., every LoadWindowNs).
        void UpdateLoadEstimates(i16 activeSlotCount);

        // Reset per-slot cycle counters (called after each harmonizer window).
        void ResetCounters(i16 activeSlotCount);

    private:
        TSlot* Slots_;
        i16 MaxSlotCount_;
    };

} // namespace NActors::NWorkStealing
