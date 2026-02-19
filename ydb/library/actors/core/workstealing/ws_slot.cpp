#include "ws_slot.h"

#include <util/system/yassert.h>

#include <algorithm>

namespace NActors::NWorkStealing {

    TSlot::TSlot() = default;

    TSlot::~TSlot() = default;

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

    void TSlot::Push(ui32 hint) {
        Queue_.Push(hint);
        ApproxSize_.fetch_add(1, std::memory_order_relaxed);
    }

    std::optional<ui32> TSlot::Pop() {
        auto result = Queue_.TryPop();
        if (!result) {
            return std::nullopt;
        }
        ApproxSize_.fetch_sub(1, std::memory_order_relaxed);
        return result;
    }

    size_t TSlot::StealHalf(ui32* out, size_t max) {
        ESlotState state = State_.load(std::memory_order_acquire);
        if (state != ESlotState::Active && state != ESlotState::Draining) {
            return 0;
        }
        size_t estimate = SizeEstimate();
        size_t target = std::min(std::max(estimate / 2, size_t(1)), max);
        size_t count = 0;
        while (count < target) {
            auto item = Queue_.TryPop();
            if (!item) {
                break;
            }
            ApproxSize_.fetch_sub(1, std::memory_order_relaxed);
            out[count++] = *item;
        }
        return count;
    }

    bool TSlot::HasWork() const {
        return ApproxSize_.load(std::memory_order_relaxed) > 0;
    }

    size_t TSlot::SizeEstimate() const {
        i64 size = ApproxSize_.load(std::memory_order_relaxed);
        return (size > 0) ? static_cast<size_t>(size) : 0;
    }

} // namespace NActors::NWorkStealing
