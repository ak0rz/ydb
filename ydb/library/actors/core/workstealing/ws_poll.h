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

    // Per-slot polling state, persists across PollSlot calls within a worker.
    //
    // Tracks steal backoff (ConsecutiveIdle, NextStealAtIdle, StealInterval)
    // and the hot continuation — a mailbox that exhausted Phase 2's event
    // budget while still having work. The continuation receives priority
    // processing on the next PollSlot call (Phase 1).
    //
    // INVARIANT: A mailbox in HotContinuation is still locked. The worker
    // MUST flush it (push to queue) before parking, draining, or shutting
    // down. See thread_driver.cpp for flush points.
    struct TPollState {
        uint32_t ConsecutiveIdle = 0;
        uint32_t NextStealAtIdle = 0; // next ConsecutiveIdle value at which to attempt stealing
        uint32_t StealInterval = 0;   // current interval between steal attempts (exponential backoff)
        bool HadLocalWork = false;    // set by PollSlot: true = local work executed, false = stolen or idle
        std::optional<ui32> HotContinuation; // Phase 2 leftover: activation that still has events
    };

    // Core polling routine for a single slot. Two-phase execution with
    // one-shot hot continuation and time-based batching.
    //
    // Phase 1 — Hot continuation (one-shot):
    //   If HotContinuation is set, process it first with a fresh
    //   MaxExecBatch budget. If it still has events, push to queue
    //   (NOT saved back as continuation). Gives priority to hot
    //   mailboxes without monopolizing Phase 1 across calls.
    //
    // Phase 2 — Queue processing:
    //   Pop activations and execute with a separate MaxExecBatch budget.
    //   Each mailbox runs for up to MailboxBatchCycles before checking
    //   the queue for interleaving. If budget is exhausted while a
    //   mailbox still has events, it becomes the next HotContinuation.
    //
    // Stealing:
    //   If both phases found no work, try stealing from neighbors via
    //   stealIterator with exponential backoff. Stolen items are
    //   executed directly from a stack buffer (no reinjection).
    //
    // Returns Busy if any event was processed, Idle otherwise.
    EPollResult PollSlot(
        TSlot& slot,
        IStealIterator* stealIterator,
        const TExecuteCallback& executeCallback,
        const TWsConfig& config,
        TPollState& pollState);

} // namespace NActors::NWorkStealing
