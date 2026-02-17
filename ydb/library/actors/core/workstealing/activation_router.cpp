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

        // Sticky routing: try the last slot that executed this mailbox
        if (lastSlotIdx > 0 && lastSlotIdx <= activeCount) {
            size_t idx = lastSlotIdx - 1;
            if (Slots_[idx].GetState() == ESlotState::Active) {
                if (Slots_[idx].Inject(hint)) {
                    return static_cast<int>(idx);
                }
                // Inject failed (slot transitioning away) -- fall through to hash
            }
        }

        // Fallback: power-of-two hash choice among active slots
        return PowerOfTwoHash(hint, activeCount);
    }

    void TActivationRouter::RefreshActiveSlots() {
        // Count contiguous active slots from the beginning.
        // SetFullThreadCount guarantees active slots are 0..N-1.
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
            if (Slots_[0].Inject(hint)) {
                return 0;
            }
            return -1;
        }

        // Derive two candidate indices from the hint using mix functions.
        // This is deterministic per-hint (no shared mutable state).
        ui32 h1 = hint;
        ui32 h2 = hint ^ 0x5bd1e995u;
        h2 = (h2 >> 16) ^ (h2 * 0x45d9f3bu);

        size_t idxA = h1 % activeCount;
        size_t idxB = h2 % activeCount;
        if (idxB == idxA) {
            idxB = (idxA + 1) % activeCount;
        }

        // Choose the slot with lower estimated load, fall back to the other
        size_t first = (Slots_[idxA].SizeEstimate() <= Slots_[idxB].SizeEstimate())
                            ? idxA
                            : idxB;
        size_t second = (first == idxA) ? idxB : idxA;

        if (Slots_[first].Inject(hint)) {
            return static_cast<int>(first);
        }
        if (Slots_[second].Inject(hint)) {
            return static_cast<int>(second);
        }

        // All candidates transitioning away — try any active slot
        for (ui16 i = 0; i < activeCount; ++i) {
            if (Slots_[i].Inject(hint)) {
                return static_cast<int>(i);
            }
        }
        return -1;
    }

} // namespace NActors::NWorkStealing
