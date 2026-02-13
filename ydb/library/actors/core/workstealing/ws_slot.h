#pragma once

// Per-worker pool slot for the work-stealing scheduler.
//
// TPoolSlot is the central per-worker struct composing a local deque,
// injection queue, LIFO slot, and load metric into a single scheduling unit.
// Queues and scheduling state belong to slots, not threads — when the
// harmonizer reassigns a physical core, the slot's injection queue stays
// in the pool; late-arriving pushes remain in the correct pool.
//
// Layout strategy: separate cache lines for contended atomics (read/written
// by multiple threads) vs owner-only fields (written only by the occupant).
// The struct is aligned to 64 bytes.
//
// State machine (CAS-enforced):
//   Inactive → Initializing  (thread claims slot)
//   Initializing → Active    (setup complete)
//   Active → Draining        (harmonizer requests deflation)
//   Draining → Inactive      (drain complete)

#include <ydb/library/actors/core/workstealing/local_deque.h>
#include <ydb/library/actors/core/workstealing/injection_queue.h>

#include <ydb/library/actors/core/defs.h>

#include <library/cpp/threading/chunk_queue/queue.h>

#include <util/system/defaults.h>

#include <atomic>

namespace NActors {

    enum class ESlotState : ui32 {
        Inactive,       // slot not in use (no occupant, not visible to stealers)
        Initializing,   // being set up by a thread claiming it
        Active,         // running — poll/steal/execute cycle
        Draining,       // deflating — drain queues then go Inactive
    };

    struct alignas(64) TPoolSlot {
        // ── Contended atomics (read/written by multiple threads) ──────────
        //
        // Each padded to its own cache line to avoid false sharing.
        // Access patterns differ per field (see design doc Part 2 §Slot
        // field access classification):
        //
        //   State        — written by harmonizer + occupant, read by pushers/stealers
        //   Parked       — seq_cst store by occupant, acquire load by pushers
        //   InStealTopo  — written by occupant during deflation, read by stealers
        //   LoadEstimate — written by occupant every ~1ms, read by random-2 pickers
        //   Occupant     — written by occupant on attach, read by pushers for unpark

        NThreading::TPadded<std::atomic<ESlotState>> State{ESlotState::Inactive};
        NThreading::TPadded<std::atomic<bool>> Parked{false};
        NThreading::TPadded<std::atomic<bool>> InStealTopo{false};
        NThreading::TPadded<std::atomic<ui64>> LoadEstimate{0};
        NThreading::TPadded<std::atomic<TWorkerId>> Occupant{Max<TWorkerId>()};

        // ── Owner-only fields (written only by the occupant thread) ───────

        ui32 LifoSlot = 0;                          // single-item fast path (0 = empty)
        ui32 LifoConsecutiveCount = 0;              // starvation guard counter

        TBoundedSPMCDeque<256> LocalDeque;
        TInjectionQueue InjectionQueue;

        ui64 ExecTimeAccum = 0;                     // accumulated exec ns in current window
        ui64 PrevAccum = 0;                         // previous window's accumulation
        ui64 LastSnapshotTs = 0;                    // timestamp of last load estimate snapshot

        // ── State machine ─────────────────────────────────────────────────

        // CAS-based state transition. Returns true if the current state
        // matched `expected` and was changed to `desired`. Only the 4 valid
        // transitions are allowed; all others return false without touching
        // the atomic.
        bool TryTransition(ESlotState expected, ESlotState desired) {
            if (!IsValidTransition(expected, desired)) {
                return false;
            }
            return State.compare_exchange_strong(
                expected, desired,
                std::memory_order_acq_rel,
                std::memory_order_acquire);
        }

        static bool IsValidTransition(ESlotState from, ESlotState to) {
            switch (from) {
                case ESlotState::Inactive:      return to == ESlotState::Initializing;
                case ESlotState::Initializing:  return to == ESlotState::Active;
                case ESlotState::Active:        return to == ESlotState::Draining;
                case ESlotState::Draining:      return to == ESlotState::Inactive;
            }
            return false;
        }

        // ── LIFO slot access (owner-only, non-atomic) ────────────────────

        void SetLifo(ui32 hint) {
            LifoSlot = hint;
        }

        // Returns the hint stored in the LIFO slot, or 0 if empty.
        // Clears the slot after taking.
        ui32 TakeLifo() {
            ui32 hint = LifoSlot;
            LifoSlot = 0;
            return hint;
        }

        // ── Starvation guard ──────────────────────────────────────────────

        // Returns true if LIFO consecutive count >= limit (starvation
        // detected — caller should demote to deque). Increments count
        // on false, resets on true.
        bool CheckStarvationGuard(ui32 limit) {
            if (LifoConsecutiveCount >= limit) {
                LifoConsecutiveCount = 0;
                return true;
            }
            ++LifoConsecutiveCount;
            return false;
        }

        // Called after a non-LIFO dispatch (deque pop or injection drain).
        void ResetStarvationGuard() {
            LifoConsecutiveCount = 0;
        }

        // ── Load estimate ─────────────────────────────────────────────────

        // Snapshot logic: if enough time has elapsed since the last
        // snapshot, store the delta to LoadEstimate, rotate accumulators,
        // and reset the timestamp.
        void UpdateLoadEstimate(ui64 nowNs, ui64 windowNs) {
            if (nowNs - LastSnapshotTs >= windowNs) {
                ui64 delta = ExecTimeAccum - PrevAccum;
                LoadEstimate.store(delta, std::memory_order_relaxed);
                PrevAccum = ExecTimeAccum;
                LastSnapshotTs = nowNs;
            }
        }
    };

} // namespace NActors
