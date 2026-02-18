#include "ws_slot.h"

#include <ydb/library/actors/core/thread_context.h>
#include <ydb/library/actors/queues/activation_queue.h>

#include <util/system/yassert.h>

#include <algorithm>

namespace NActors::NWorkStealing {

    TSlot::TSlot()
        : Queue_(std::make_unique<NActors::TRingActivationQueueV4>(8))
    {
    }

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
        Queue_->Push(hint, 0);
        ApproxSize_.fetch_add(1, std::memory_order_relaxed);
    }

    std::optional<ui32> TSlot::Pop() {
        ui64 counter = PopCounter_.fetch_add(1, std::memory_order_relaxed);
        ui32 result = Queue_->Pop(counter);
        if (result == 0) {
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
            ui64 counter = PopCounter_.fetch_add(1, std::memory_order_relaxed);
            ui32 item = Queue_->Pop(counter);
            if (item == 0) {
                break;
            }
            ApproxSize_.fetch_sub(1, std::memory_order_relaxed);
            out[count++] = item;
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
