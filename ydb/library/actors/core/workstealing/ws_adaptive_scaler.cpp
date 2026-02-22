#include "ws_adaptive_scaler.h"
#include "ws_bucket_map.h"

#include <ydb/library/actors/util/datetime.h>

#include <algorithm>

namespace NActors::NWorkStealing {

    TAdaptiveScaler::TAdaptiveScaler(
        std::function<void(i16)> setThreadCount,
        std::function<i16()> getActiveCount,
        TSlot* slots, i16 maxSlotCount,
        const TWsConfig& config)
        : SetThreadCount_(std::move(setThreadCount))
        , GetActiveCount_(std::move(getActiveCount))
        , Slots_(slots)
        , MaxSlotCount_(maxSlotCount)
        , Config_(config)
    {
    }

    void TAdaptiveScaler::SetBucketMap(TBucketMap* bucketMap) {
        BucketMap_ = bucketMap;
    }

    void TAdaptiveScaler::Evaluate() {
        if (!Config_.AdaptiveScaling) {
            return;
        }

        ui64 now = GetCycleCountFast();

        // Cooldown check — skip if too soon after last change
        if (LastChangeCycles_ > 0 && (now - LastChangeCycles_) < Config_.AdaptiveCooldownCycles) {
            return;
        }

        i16 activeCount = GetActiveCount_();
        if (activeCount <= 0) {
            return;
        }

        // Scan active slots: read-and-reset busy/idle cycles
        i16 busySlots = 0;
        uint32_t maxQueueDepth = 0;

        for (i16 i = 0; i < activeCount && i < MaxSlotCount_; ++i) {
            uint64_t busy = Slots_[i].Stats.BusyCycles.exchange(0, std::memory_order_relaxed);
            uint64_t idle = Slots_[i].Stats.IdleCycles.exchange(0, std::memory_order_relaxed);
            uint64_t total = busy + idle;

            if (total > 0) {
                double util = static_cast<double>(busy) / static_cast<double>(total);
                if (util > Config_.SlotBusyThreshold) {
                    ++busySlots;
                }
            }

            uint32_t queueDepth = static_cast<uint32_t>(Slots_[i].SizeEstimate())
                                + Slots_[i].ContinuationCount.load(std::memory_order_relaxed);
            maxQueueDepth = std::max(maxQueueDepth, queueDepth);
        }

        double busyFraction = static_cast<double>(busySlots) / static_cast<double>(activeCount);

        // Inflate: high utilization or queue pressure
        if (busyFraction >= Config_.InflateUtilThreshold
            || maxQueueDepth > Config_.QueuePressureThreshold)
        {
            if (activeCount < MaxSlotCount_) {
                i16 growth = std::max<i16>(1, activeCount / 4);  // +25% geometric
                i16 newCount = std::min<i16>(MaxSlotCount_, activeCount + growth);
                SetThreadCount_(newCount);
                LastChangeCycles_ = now;
                ++InflateEvents_;
            }
            return;
        }

        // Deflate: low utilization
        if (busyFraction < Config_.DeflateUtilThreshold) {
            i16 headroom = std::max<i16>(1, busySlots / 4);  // +25% headroom
            i16 target = busySlots + headroom;
            // Never drop more than 50% per step (stability)
            i16 halfCurrent = activeCount / 2;
            i16 newCount = std::max(target, halfCurrent);
            newCount = std::max<i16>(1, newCount);  // SetFullThreadCount clamps to MinSlotCount

            if (newCount < activeCount) {
                SetThreadCount_(newCount);
                LastChangeCycles_ = now;
                ++DeflateEvents_;
            }
        }

        // Bucket reclassification: the bucket map handles its own boundary
        // management internally (demand-driven based on heavy actor count).
        if (BucketMap_) {
            BucketMap_->Reclassify();
        }
    }

} // namespace NActors::NWorkStealing
