#pragma once

#include "ws_counters.h"

#include "mpmc_unbounded_queue.h"

#include <util/system/hp_timer.h>
#include <util/system/types.h>

#include <atomic>
#include <cstdint>
#include <optional>

namespace NActors { class TMailboxTable; }

namespace NActors::NWorkStealing {

    enum class ESlotState: uint8_t {
        Inactive,     // slot is idle, not assigned to a worker
        Initializing, // driver assigned slot to a worker, worker not yet confirmed
        Active,       // worker is running, accepts injections
        Draining,     // harmonizer requested deflation, no new injections
    };

    struct TSlotStats {
        uint64_t ActivationsExecuted = 0;
        uint64_t IdleCycles = 0;
        uint64_t BusyCycles = 0;
        uint64_t ExecTimeAccumNs = 0;
    };

    // Scheduling slot for the work-stealing runtime.
    //
    // Each slot owns a TMPMCUnboundedQueue for activations.
    // Any thread can Push/Pop activations; Steal pops a budget-aware batch for stealers.
    //
    // State machine:
    //   Inactive -> Initializing  (driver assigns to worker)
    //   Initializing -> Active    (worker wakes and confirms)
    //   Active -> Draining        (harmonizer requests deflation)
    //   Draining -> Inactive      (queues empty, steals done)
    //
    // All other transitions are rejected.
    struct alignas(64) TSlot {
        TSlot();
        ~TSlot();

        // --- State machine ---

        bool TryTransition(ESlotState from, ESlotState to);
        ESlotState GetState() const;

        // --- Push/Pop API ---

        // Push an activation into this slot's MPMC queue.
        void Push(ui32 hint);

        // Pop the next activation. Returns nullopt if the queue is empty.
        std::optional<ui32> Pop();

        // --- Stealer API (any thread) ---

        // Steal activations from this slot's MPMC queue until the accumulated
        // estimated cost reaches cyclesBudget or maxCount items are taken.
        // When MailboxTable is set, uses per-mailbox avg cycles/event for cost.
        // When MailboxTable is nullptr (tests), falls back to maxCount-limited.
        size_t Steal(ui32* out, size_t maxCount, NHPTimer::STime cyclesBudget);

        // --- Metrics ---

        size_t SizeEstimate() const;
        bool HasWork() const;

        // --- Driver integration ---

        std::atomic<bool> WorkerSpinning{false};
        std::atomic<bool> Executing{false};   // true while inside executeCallback
        std::atomic<uint8_t> ContinuationCount{0};  // ring occupancy, read by router
        void* DriverData = nullptr;
        NActors::TMailboxTable* MailboxTable = nullptr;  // set by pool init, used for cost-aware stealing

        // --- Stats ---

        TSlotStats Stats;
        TWsSlotCounters Counters;
        std::atomic<double> LoadEstimate{0.0};

    private:
        std::atomic<ESlotState> State_{ESlotState::Inactive};

        TMPMCUnboundedQueue<65536> Queue_;
        alignas(64) std::atomic<i64> ApproxSize_{0};

        static bool IsValidTransition(ESlotState from, ESlotState to);
    };

} // namespace NActors::NWorkStealing
