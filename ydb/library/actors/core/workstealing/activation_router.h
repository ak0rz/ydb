#pragma once

#include "ws_slot.h"

#include <util/system/types.h>

#include <atomic>
#include <cstddef>

namespace NActors::NWorkStealing {

    // Routes mailbox activations to slots using sticky routing with
    // hash-based power-of-two random fallback.
    //
    // Thread safety: all methods are safe to call from any thread concurrently.
    // No shared mutable state — uses atomic slot count and hint-based hashing.
    //
    // Routing algorithm:
    // 1. Read mailbox->LastPoolSlotIdx (1-based, 0 = fresh/unassigned)
    // 2. If non-zero: try that slot; if Active, inject there (sticky routing)
    // 3. If zero or target slot not Active: power-of-two hash choice
    //    - Derive 2 candidate slots from hint, choose the one with lower SizeEstimate()
    // 4. Inject into chosen slot's MPSC queue
    class TActivationRouter {
    public:
        TActivationRouter(TSlot* slots, size_t slotCount);

        // Route an activation to a slot. Returns the slot index chosen,
        // or -1 if no active slot is available.
        int Route(ui32 hint, ui16 lastSlotIdx);

        // Update the active slot count. Call when slots change state.
        // Active slots must always be contiguous from index 0.
        void RefreshActiveSlots();

    private:
        TSlot* Slots_;
        size_t SlotCount_;
        std::atomic<ui16> ActiveCount_{0};

        int PowerOfTwoHash(ui32 hint, ui16 activeCount);
    };

} // namespace NActors::NWorkStealing
