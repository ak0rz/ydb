#pragma once

#include "ws_slot.h"
#include "topology.h"

#include <memory>

namespace NActors::NWorkStealing {

    class IStealIterator;
    class TWSExecutorContext;

    // Abstract driver interface. Owns system-wide workers pinned to CPUs.
    // Decouples thread management from executor pools.
    class IDriver {
    public:
        virtual ~IDriver() = default;

        // Lifecycle
        virtual void Prepare(const TCpuTopology& topology) = 0;
        virtual void Start() = 0;
        virtual void PrepareStop() = 0;
        virtual void Shutdown() = 0;

        // Slot management
        virtual void RegisterSlots(TSlot* slots, size_t count) = 0; // pool registers a batch of slots (one group per pool)
        virtual void ActivateSlot(TSlot* slot) = 0;   // harmonizer inflates
        virtual void DeactivateSlot(TSlot* slot) = 0; // harmonizer deflates
        virtual void WakeSlot(TSlot* slot) = 0;       // wake the worker owning this slot

        // Set executor context for a worker. Called by the pool after RegisterSlots.
        virtual void SetWorkerContext(TSlot* slot, TWSExecutorContext* ctx) = 0;

        // Create a steal iterator for a worker, excluding the given slot
        virtual std::unique_ptr<IStealIterator> MakeStealIterator(TSlot* exclude) = 0;
    };

} // namespace NActors::NWorkStealing
