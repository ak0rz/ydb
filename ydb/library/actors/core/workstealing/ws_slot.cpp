#include "ws_slot.h"
#include "ws_mailbox_table.h"

#include <util/system/yassert.h>

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

    bool TSlot::Push(ui32 hint) {
        ESlotState state = State_.load(std::memory_order_acquire);
        if (state != ESlotState::Active && state != ESlotState::Draining) {
            return false;
        }
        Queue_.Push(hint);
        ApproxSize_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    std::optional<ui32> TSlot::Pop() {
        auto result = Queue_.TryPop();
        if (!result) {
            return std::nullopt;
        }
        ApproxSize_.fetch_sub(1, std::memory_order_relaxed);
        return result;
    }

    size_t TSlot::Steal(ui32* out, size_t maxCount, NHPTimer::STime cyclesBudget) {
        // Allow stealing from any state — acts as a safety net for
        // activations stranded in Inactive/Draining slots.
        NHPTimer::STime totalCost = 0;
        size_t count = 0;
        while (count < maxCount) {
            auto item = Queue_.TryPop();
            if (!item) {
                break;
            }
            ApproxSize_.fetch_sub(1, std::memory_order_relaxed);
            out[count++] = *item;

            // Accumulate estimated cost from mailbox stats
            TMailboxExecStats* stats = nullptr;
            if (WsMailboxTable) {
                stats = WsMailboxTable->GetStats(*item);
            }
            if (stats) {
                ui64 events = stats->EventsProcessed.load(std::memory_order_relaxed);
                ui64 cycles = stats->TotalExecutionCycles.load(std::memory_order_relaxed);
                if (events > 0) {
                    totalCost += static_cast<NHPTimer::STime>(cycles / events);
                }
            }
            if (totalCost >= cyclesBudget) {
                break;
            }
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
