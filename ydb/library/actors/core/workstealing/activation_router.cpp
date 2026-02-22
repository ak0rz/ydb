#include "activation_router.h"

namespace NActors::NWorkStealing {

    TActivationRouter::TActivationRouter(TSlot* slots, size_t slotCount)
        : Slots_(slots)
        , SlotCount_(slotCount)
    {
        RefreshActiveSlots();
    }

    int TActivationRouter::Route(ui32 hint, ui16 lastSlotIdx) {
        ui16 activeCount = ActiveCount_.load(std::memory_order_acquire);
        if (activeCount == 0) {
            return -1;
        }

        // Sticky routing: try the last slot that executed this mailbox,
        // but only if it isn't overloaded (queue depth ≤ 2× a random peer).
        // This preserves cache locality while letting work spread when load
        // is unbalanced (e.g., 10 actor pairs on 32 slots).
        if (lastSlotIdx > 0 && lastSlotIdx <= activeCount) {
            size_t idx = lastSlotIdx - 1;
            if (Slots_[idx].GetState() == ESlotState::Active) {
                size_t stickyLoad = Slots_[idx].SizeEstimate()
                                  + Slots_[idx].ContinuationCount.load(std::memory_order_relaxed);
                if (stickyLoad <= 1) {
                    // Slot is empty or near-empty — always use it.
                    Slots_[idx].Push(hint);
                    return static_cast<int>(idx);
                }
                // Compare with a hash-derived peer.
                size_t peer = (hint ^ 0x9e3779b9u) % activeCount;
                size_t peerLoad = Slots_[peer].SizeEstimate()
                                + Slots_[peer].ContinuationCount.load(std::memory_order_relaxed);
                if (stickyLoad <= peerLoad * 2 + 2) {
                    Slots_[idx].Push(hint);
                    return static_cast<int>(idx);
                }
            }
        }

        // Fallback: power-of-two hash choice among active slots
        return PowerOfTwoHash(hint, activeCount);
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

    int TActivationRouter::PowerOfTwoHash(ui32 hint, ui16 activeCount) {
        if (activeCount == 1) {
            if (Slots_[0].GetState() == ESlotState::Active) {
                Slots_[0].Push(hint);
                return 0;
            }
            return -1;
        }

        ui32 h1 = hint;
        ui32 h2 = hint ^ 0x5bd1e995u;
        h2 = (h2 >> 16) ^ (h2 * 0x45d9f3bu);

        size_t idxA = h1 % activeCount;
        size_t idxB = h2 % activeCount;
        if (idxB == idxA) {
            idxB = (idxA + 1) % activeCount;
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

        // All candidates transitioning away — try any active slot
        for (ui16 i = 0; i < activeCount; ++i) {
            if (Slots_[i].GetState() == ESlotState::Active) {
                Slots_[i].Push(hint);
                return static_cast<int>(i);
            }
        }
        return -1;
    }

} // namespace NActors::NWorkStealing
