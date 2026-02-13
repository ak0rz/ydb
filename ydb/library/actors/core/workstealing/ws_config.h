#pragma once

// Pool-wide configuration for work-stealing scheduler.
//
// Plain struct, no methods. Values read by the poll/steal functions (Step 5+)
// and the thread driver (Step 8). Defaults match the design doc Part 2.

#include <util/system/defaults.h>

namespace NActors {

    struct TWsConfig {
        ui32 MaxSlots = 64;
        ui32 DequeSize = 256;                // unused for now (template param), documented
        ui32 StarvationGuardLimit = 3;       // LIFO consecutive dispatches before demotion
        ui32 DrainBudget = 512;              // max items drained per deflation pass
        ui32 SpinThreshold = 100;            // iterations before parking
        ui64 LoadWindowNs = 1'000'000;       // 1ms — load estimate snapshot interval
        ui64 TimePerMailboxTs = 0;           // inherited from pool config
        ui32 EventsPerMailbox = 100;         // inherited from pool config
    };

} // namespace NActors
