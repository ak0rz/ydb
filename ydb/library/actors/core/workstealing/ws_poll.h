#pragma once

#include "ws_slot.h"
#include "ws_config.h"

#include <util/system/types.h>

#include <cstddef>
#include <functional>

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

    // Callback type for executing a mailbox activation.
    // Called with the activation hint (mailbox index).
    // Returns true if the mailbox still has pending events (budget depleted),
    // false if the mailbox is empty after execution.
    using TExecuteCallback = std::function<bool(ui32 hint)>;

    // Per-slot state for polling. Tracks consecutive idle polls to
    // control steal frequency (backoff).
    struct TPollState {
        uint32_t ConsecutiveIdle = 0;
        uint32_t NextStealAtIdle = 0; // next ConsecutiveIdle value at which to attempt stealing
        uint32_t StealInterval = 0;   // current interval between steal attempts (exponential backoff)
        bool HadLocalWork = false;    // set by PollSlot: true = local work executed, false = stolen or idle
    };

    // Core polling routine for a single slot.
    //
    // Algorithm:
    // 1. Pop and execute from our MPMC queue (up to config.MaxExecBatch)
    //    - For each: call executeCallback(hint)
    //    - If callback returns true (budget depleted): push hint back
    //    - Return Busy if any work was done
    // 2. No local work: try stealing from neighbors via stealIterator
    //    - StealHalf into stack buffer, push stolen items into our queue
    //    - Execute stolen items (up to remaining budget)
    //    - Return Busy if any work was done
    // 3. Nothing found anywhere: return Idle
    EPollResult PollSlot(
        TSlot& slot,
        IStealIterator* stealIterator,
        const TExecuteCallback& executeCallback,
        const TWsConfig& config,
        TPollState& pollState);

} // namespace NActors::NWorkStealing
