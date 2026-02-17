#pragma once

#include "chase_lev_deque.h"
#include "vyukov_mpsc_queue.h"
#include "ws_counters.h"

#include <util/system/types.h>

#include <atomic>
#include <cstdint>
#include <optional>

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
    // Each slot owns an injection queue (MPSC, multiple producers push activations)
    // and a local work deque (Chase-Lev SPMC, owner pops, stealers steal).
    //
    // State machine:
    //   Inactive -> Initializing  (driver assigns to worker)
    //   Initializing -> Active    (worker wakes and confirms)
    //   Active -> Draining        (harmonizer requests deflation)
    //   Draining -> Inactive      (queues empty, steals done)
    //
    // All other transitions are rejected.
    struct alignas(64) TSlot {
        // --- State machine ---

        // Attempt a state transition via CAS.
        // Returns true if the transition was performed.
        bool TryTransition(ESlotState from, ESlotState to);

        // Current state (relaxed read -- use for diagnostics / non-critical checks).
        ESlotState GetState() const;

        // --- Producer API (any thread) ---

        // Inject an activation hint into this slot's MPSC queue.
        // Returns false if the slot is not Active.
        bool Inject(ui32 hint);

        // --- Owner API (single consumer thread) ---

        // Drain up to maxBatch items from the injection queue into the local deque.
        // Returns the number of items drained.
        size_t DrainInjectionQueue(size_t maxBatch);

        // Pop the next activation from the local deque (LIFO for the owner).
        std::optional<ui32> PopActivation();

        // Push an activation back into the MPSC queue for rescheduling.
        void Reinject(ui32 hint);

        // Push a stolen activation directly into the local work deque.
        // Only called by the owner thread after stealing from a neighbor.
        // Returns false if the deque is full.
        bool PushStolen(ui32 hint);

        // --- Stealer API (any thread) ---

        // Steal up to half the items from the local deque.
        // Writes stolen hints to out[0..N-1], returns N.
        // Only operates when state is Active or Draining.
        size_t StealHalf(ui32* out, size_t max);

        // --- Metrics ---

        // Approximate total work in this slot (deque size estimate).
        size_t SizeEstimate() const;

        // --- Stats ---

        // Check if injection queue has pending items (consumer thread only).
        bool HasPendingInjections() const;

        // --- Driver integration ---

        // True when the owning worker is actively polling (not parked).
        // Used by WakeSlot to skip redundant Unpark calls.
        // Protocol: worker sets false (seq_cst) before parking, checks MPSC
        // after (Dekker). WakeSlot reads (seq_cst) — if true, skips wake.
        std::atomic<bool> WorkerSpinning{false};

        // Opaque pointer for driver use (e.g., TWorker*). Eliminates hash map.
        void* DriverData = nullptr;

        // --- Stats ---

        TSlotStats Stats;
        TWsSlotCounters Counters;
        std::atomic<double> LoadEstimate{0.0};

    private:
        std::atomic<ESlotState> State_{ESlotState::Inactive};

        TVyukovMPSCQueue<ui32, 64> InjectionQueue_;
        TChaseLevDeque<ui32, 256> WorkDeque_;

        // Validates that the (from -> to) transition is allowed by the state machine.
        static bool IsValidTransition(ESlotState from, ESlotState to);
    };

} // namespace NActors::NWorkStealing
