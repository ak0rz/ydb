#pragma once

#include "ws_slot.h"
#include "ws_config.h"

#include <util/system/hp_timer.h>
#include <util/system/types.h>

#include <cstddef>
#include <functional>
#include <optional>

namespace NActors::NWorkStealing {

    enum class EPollResult: ui8 {
        Busy, // slot had work and executed it
        Idle, // no work found (local or stolen)
    };

    // Abstract interface for topology-ordered steal iteration.
    // Driver provides this per worker. Iterates over neighbor slots
    // in topology order (L1 -> L2 -> L3 -> NUMA -> cross-NUMA).
    class IStealIterator {
    public:
        virtual ~IStealIterator() = default;

        // Returns next slot to steal from, or nullptr if exhausted.
        virtual TSlot* Next() = 0;

        // Reset the iterator for a new steal cycle.
        virtual void Reset() = 0;
    };

    // Callback type for executing a single event from a mailbox activation.
    // Called with the activation hint (mailbox index).
    // Returns true if an event was processed (more events might remain).
    // Returns false if no event was available (mailbox was finalized).
    // hpnow is input-output: on entry, the caller's current cycle timestamp
    // (used by the callback for pre-event stats); on exit, updated to
    // post-event GetCycleCountFast(). This eliminates redundant rdtsc
    // calls — one GetCycleCountFast() per event total.
    using TExecuteCallback = std::function<bool(ui32 hint, NHPTimer::STime& hpnow)>;

    // Callback for ring overflow — reroutes activation to another slot.
    using TOverflowCallback = std::function<void(ui32 hint)>;

    // Called before each activation batch. Returns the number of
    // pre-existing events (used to decide continuation eligibility).
    // Single-event activations (self-sends) skip the continuation ring.
    using TBeginBatchCallback = std::function<ui64(ui32 hint)>;

    // Called after each activation batch. Used by WS to commit
    // local batch cursors back to EventHead.
    using TEndBatchCallback = std::function<void(ui32 hint)>;

    // Stack-allocated fixed-capacity FIFO ring for hot continuations.
    // Single-threaded — accessed only by the owning worker.
    struct TContinuationRing {
        static constexpr uint8_t kMaxCapacity = 8;

        bool Empty() const { return Count == 0; }
        bool Full() const { return Count >= Capacity; }
        uint8_t Size() const { return Count; }

        bool Push(ui32 activation) {
            if (Full()) {
                return false;
            }
            Items[Tail] = activation;
            Tail = (Tail + 1) % kMaxCapacity;
            ++Count;
            return true;
        }

        std::optional<ui32> Pop() {
            if (Empty()) {
                return std::nullopt;
            }
            ui32 val = Items[Head];
            Head = (Head + 1) % kMaxCapacity;
            --Count;
            return val;
        }

        void FlushTo(TSlot& slot) {
            while (!Empty()) {
                slot.Push(*Pop());
            }
        }

        // Copy current ring contents to a snapshot array on the slot.
        void SnapshotTo(TSlot& slot) const {
            for (uint8_t i = 0; i < Count; ++i) {
                slot.RingSnapshot[i].store(Items[(Head + i) % kMaxCapacity],
                                           std::memory_order_relaxed);
            }
            for (uint8_t i = Count; i < kMaxCapacity; ++i) {
                slot.RingSnapshot[i].store(0, std::memory_order_relaxed);
            }
        }

        ui32 Items[kMaxCapacity] = {};
        uint8_t Head = 0;
        uint8_t Tail = 0;
        uint8_t Count = 0;
        uint8_t Capacity = 4;  // set from TWsConfig::ContinuationRingCapacity
    };

    // Per-slot polling state, persists across PollSlot calls within a worker.
    //
    // Tracks steal backoff (ConsecutiveIdle, NextStealAtIdle, StealInterval)
    // and the continuation ring — mailboxes that exhausted their time budget
    // while still having work. Ring items are interleaved with queue items
    // on subsequent PollSlot calls for fair execution.
    //
    // INVARIANT: Mailboxes in the ring are still locked. The worker
    // MUST flush them (push to queue) before parking, draining, or shutting
    // down. See thread_driver.cpp for flush points.
    struct TPollState {
        uint32_t ConsecutiveIdle = 0;
        uint32_t NextStealAtIdle = 0; // next ConsecutiveIdle value at which to attempt stealing
        uint32_t StealInterval = 0;   // current interval between steal attempts (exponential backoff)
        bool HadLocalWork = false;    // set by PollSlot: true = local work executed, false = stolen or idle
        TContinuationRing Ring;       // hot continuations: activations that survived time budget
    };

    // Core polling routine for a single slot. Unified interleaved execution
    // with continuation ring and time-based batching.
    //
    // Execution loop:
    //   PopInterleaved alternates between ring (proven hot items) and queue
    //   (new arrivals). Each activation runs for up to MailboxBatchCycles.
    //   When time expires and other work is available, the current activation
    //   is saved to ring tail and the next item is executed (inline swap).
    //   If nothing else is available, the same activation continues with a
    //   reset deadline. Budget exhaustion saves to ring; ring full triggers
    //   overflow callback for redistribution.
    //
    // Stealing:
    //   If no work found, try stealing from neighbors via stealIterator
    //   with exponential backoff. Stolen items are executed directly from
    //   a stack buffer (no reinjection).
    //
    // Returns Busy if any event was processed, Idle otherwise.
    EPollResult PollSlot(
        TSlot& slot,
        IStealIterator* stealIterator,
        const TExecuteCallback& executeCallback,
        const TOverflowCallback& overflowCallback,
        const TWsConfig& config,
        TPollState& pollState,
        const TBeginBatchCallback& beginBatchCallback = {},
        const TEndBatchCallback& endBatchCallback = {});

} // namespace NActors::NWorkStealing
