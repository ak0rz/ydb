#include "ws_slot.h"

#include <util/system/yassert.h>

namespace NActors::NWorkStealing {

    bool TSlot::IsValidTransition(ESlotState from, ESlotState to) {
        switch (from) {
            case ESlotState::Inactive:
                return to == ESlotState::Initializing;
            case ESlotState::Initializing:
                return to == ESlotState::Active;
            case ESlotState::Active:
                return to == ESlotState::Draining;
            case ESlotState::Draining:
                return to == ESlotState::Inactive;
        }
        return false;
    }

    bool TSlot::TryTransition(ESlotState from, ESlotState to) {
        if (!IsValidTransition(from, to)) {
            return false;
        }
        return State_.compare_exchange_strong(from, to,
                                              std::memory_order_acq_rel, std::memory_order_relaxed);
    }

    ESlotState TSlot::GetState() const {
        return State_.load(std::memory_order_relaxed);
    }

    bool TSlot::Inject(ui32 hint) {
        if (State_.load(std::memory_order_acquire) != ESlotState::Active) {
            return false;
        }
        InjectionQueue_.Push(hint);
        return true;
    }

    size_t TSlot::DrainInjectionQueue(size_t maxBatch) {
        size_t count = 0;
        while (count < maxBatch) {
            auto item = InjectionQueue_.TryPop();
            if (!item) {
                break;
            }
            // Push into local Chase-Lev deque. If full, re-inject into MPSC
            // so the item is not lost.
            if (!WorkDeque_.Push(*item)) {
                InjectionQueue_.Push(*item);
                break;
            }
            ++count;
        }
        return count;
    }

    std::optional<ui32> TSlot::PopActivation() {
        return WorkDeque_.PopOwner();
    }

    void TSlot::Reinject(ui32 hint) {
        InjectionQueue_.Push(hint);
    }

    size_t TSlot::StealHalf(ui32* out, size_t max) {
        ESlotState state = State_.load(std::memory_order_acquire);
        if (state != ESlotState::Active && state != ESlotState::Draining) {
            return 0;
        }
        return WorkDeque_.StealHalf(out, max);
    }

    bool TSlot::PushStolen(ui32 hint) {
        return WorkDeque_.Push(hint);
    }

    size_t TSlot::SizeEstimate() const {
        return WorkDeque_.SizeEstimate();
    }

} // namespace NActors::NWorkStealing
