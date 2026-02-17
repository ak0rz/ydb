#pragma once

#include "ws_slot.h"
#include "ws_poll.h"
#include "topology.h"

#include <functional>
#include <memory>

namespace NActors::NWorkStealing {

    // Callbacks provided by the pool per worker slot.
    struct TWorkerCallbacks {
        TExecuteCallback Execute;       // Called per activation (ui32 hint -> bool preempted)
        std::function<void()> Setup;    // Called once at worker thread start (TLS setup)
        std::function<void()> Teardown; // Called once at worker thread end (TLS cleanup)
    };

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
        virtual void RegisterSlot(TSlot* slot) = 0;   // pool registers a slot
        virtual void ActivateSlot(TSlot* slot) = 0;   // harmonizer inflates
        virtual void DeactivateSlot(TSlot* slot) = 0; // harmonizer deflates
        virtual void WakeSlot(TSlot* slot) = 0;       // wake the worker owning this slot

        // Set per-worker callbacks for a slot. Called by the pool after RegisterSlot.
        virtual void SetWorkerCallbacks(TSlot* slot, TWorkerCallbacks callbacks) = 0;

        // Create a steal iterator for a worker, excluding the given slot
        virtual std::unique_ptr<IStealIterator> MakeStealIterator(TSlot* exclude) = 0;
    };

} // namespace NActors::NWorkStealing
