#pragma once

#include "ws_slot.h"
#include "ws_config.h"

#include <util/system/types.h>

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

    class TWSExecutorContext;

    // Core polling routine for a single slot. Self-sufficient activation
    // execution using TMailboxSnapshot for batch event processing.
    //
    // Execution loop:
    //   Pops one item from ring (hot) and one from queue (new). Each
    //   activation is resolved to a mailbox and processed via a snapshot
    //   until budget, time, or queue exhaustion. Survivors go back to ring
    //   (or queue if single-event activation to prevent monopolization).
    //
    // Stealing:
    //   If no work found, try stealing from neighbors via stealIterator
    //   with exponential backoff. Stolen items are executed directly.
    //   Skipped automatically when the slot is in Draining state.
    //
    // Returns Busy if any event was processed, Idle otherwise.
    // Reads PollState from slot (valid between Acquire/Release).
    EPollResult PollSlot(TSlot& slot, TWSExecutorContext& ctx);

    // Test overload: pops items but skips execution (null ctx/wsTable).
    // Reads Config from slot (falls back to defaults if null).
    EPollResult PollSlot(TSlot& slot, IStealIterator* stealIterator = nullptr);

} // namespace NActors::NWorkStealing
