#include "activation_router.h"
#include "ws_bucket_map.h"

namespace NActors::NWorkStealing {

    TActivationRouter::TActivationRouter(TSlot* slots, size_t slotCount)
        : Slots_(slots)
        , SlotCount_(slotCount)
    {
        RefreshActiveSlots();
    }

    void TActivationRouter::SetBucketMap(TBucketMap* bucketMap) {
        BucketMap_ = bucketMap;
    }

    int TActivationRouter::Route(ui32 hint, ui16 lastSlotIdx) {
        ui16 activeCount = ActiveCount_.load(std::memory_order_acquire);
        if (activeCount == 0) {
            return -1;
        }

        // Compute bucket slot range
        ui16 begin = 0;
        ui16 end = activeCount;

        if (BucketMap_) {
            ui16 boundary = BucketMap_->GetBucketBoundary();

            // boundary == 0 means bucketing is disabled (all fast)
            if (boundary > 0 && boundary < activeCount) {
                uint8_t bucket = BucketMap_->GetBucket(hint);
                if (bucket == 1) {
                    // Heavy: [0, boundary)
                    begin = 0;
                    end = boundary;
                } else {
                    // Fast: [boundary, activeCount)
                    begin = boundary;
                    end = activeCount;
                }
            }
        }

        ui16 rangeSize = end - begin;

        // Sticky routing: try the last slot that executed this mailbox,
        // but only if it's within the current bucket range and not overloaded.
        if (lastSlotIdx > 0 && lastSlotIdx <= activeCount) {
            size_t idx = lastSlotIdx - 1;

            // Check if sticky slot is within the bucket range
            bool inBucket = (idx >= begin && idx < end);

            if (inBucket && Slots_[idx].GetState() == ESlotState::Active) {
                size_t stickyLoad = Slots_[idx].SizeEstimate()
                                  + Slots_[idx].ContinuationCount.load(std::memory_order_relaxed);
                if (stickyLoad <= 1) {
                    Slots_[idx].Push(hint);
                    return static_cast<int>(idx);
                }
                // Compare with a hash-derived peer within bucket range
                size_t peer = begin + ((hint ^ 0x9e3779b9u) % rangeSize);
                size_t peerLoad = Slots_[peer].SizeEstimate()
                                + Slots_[peer].ContinuationCount.load(std::memory_order_relaxed);
                if (stickyLoad <= peerLoad * 2 + 2) {
                    Slots_[idx].Push(hint);
                    return static_cast<int>(idx);
                }
            }
            // If lastSlotIdx is outside the bucket range (mailbox was reclassified),
            // fall through to fresh p2 routing into the correct bucket.
        }

        // Fallback: power-of-two hash choice within bucket range
        return PowerOfTwoHash(hint, begin, end);
    }

    void TActivationRouter::RefreshActiveSlots() {
        ui16 count = 0;
        for (size_t i = 0; i < SlotCount_; ++i) {
            if (Slots_[i].GetState() == ESlotState::Active) {
                ++count;
            } else {
                break;
            }
        }
        ActiveCount_.store(count, std::memory_order_release);
    }

    int TActivationRouter::PowerOfTwoHash(ui32 hint, ui16 begin, ui16 end) {
        ui16 rangeSize = end - begin;
        if (rangeSize == 0) {
            return -1;
        }

        if (rangeSize == 1) {
            if (Slots_[begin].GetState() == ESlotState::Active) {
                Slots_[begin].Push(hint);
                return static_cast<int>(begin);
            }
            return -1;
        }

        ui32 h1 = hint;
        ui32 h2 = hint ^ 0x5bd1e995u;
        h2 = (h2 >> 16) ^ (h2 * 0x45d9f3bu);

        size_t idxA = begin + (h1 % rangeSize);
        size_t idxB = begin + (h2 % rangeSize);
        if (idxB == idxA) {
            idxB = begin + ((idxA - begin + 1) % rangeSize);
        }

        size_t loadA = Slots_[idxA].SizeEstimate()
                     + Slots_[idxA].ContinuationCount.load(std::memory_order_relaxed);
        size_t loadB = Slots_[idxB].SizeEstimate()
                     + Slots_[idxB].ContinuationCount.load(std::memory_order_relaxed);
        size_t first = (loadA <= loadB) ? idxA : idxB;
        size_t second = (first == idxA) ? idxB : idxA;

        if (Slots_[first].GetState() == ESlotState::Active) {
            Slots_[first].Push(hint);
            return static_cast<int>(first);
        }
        if (Slots_[second].GetState() == ESlotState::Active) {
            Slots_[second].Push(hint);
            return static_cast<int>(second);
        }

        // All candidates transitioning away — try any active slot in range
        for (ui16 i = begin; i < end; ++i) {
            if (Slots_[i].GetState() == ESlotState::Active) {
                Slots_[i].Push(hint);
                return static_cast<int>(i);
            }
        }
        return -1;
    }

} // namespace NActors::NWorkStealing
