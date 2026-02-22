#pragma once

#include "ws_slot.h"
#include "ws_config.h"

#include <cstdint>
#include <functional>

namespace NActors::NWorkStealing {

    class TBucketMap;

    // Adaptive controller that deflates idle slots (farthest-from-core first)
    // and inflates when load increases, with hysteresis and cooldown.
    //
    // Evaluate() is called periodically from worker 0. It scans active slots,
    // computes utilization, and calls SetThreadCount to adjust the active count.
    //
    // When bucket map is set, calls Reclassify() which handles boundary
    // management internally (demand-driven, not scaler-driven).
    class TAdaptiveScaler {
    public:
        TAdaptiveScaler(
            std::function<void(i16)> setThreadCount,
            std::function<i16()> getActiveCount,
            TSlot* slots, i16 maxSlotCount,
            const TWsConfig& config);

        // Set bucket map for periodic reclassification (nullptr = disabled)
        void SetBucketMap(TBucketMap* bucketMap);

        // Called periodically from worker 0 to evaluate and adjust slot count.
        void Evaluate();

        uint64_t InflateEvents() const { return InflateEvents_; }
        uint64_t DeflateEvents() const { return DeflateEvents_; }

    private:
        std::function<void(i16)> SetThreadCount_;
        std::function<i16()> GetActiveCount_;
        TSlot* Slots_;
        i16 MaxSlotCount_;
        const TWsConfig& Config_;
        TBucketMap* BucketMap_ = nullptr;
        uint64_t LastChangeCycles_ = 0;
        uint64_t InflateEvents_ = 0;
        uint64_t DeflateEvents_ = 0;
    };

} // namespace NActors::NWorkStealing
