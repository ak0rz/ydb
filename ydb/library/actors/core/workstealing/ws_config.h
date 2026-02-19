#pragma once

#include <cstddef>
#include <cstdint>

namespace NActors::NWorkStealing {

    struct TWsConfig {
        size_t MaxExecBatch = 64;             // max events to execute per PollSlot call
        uint64_t MailboxBatchCycles = 50000;    // max cycles per mailbox before push-back (~17us at 3GHz)
        uint64_t SpinThresholdCycles = 100000;  // max spin cycles before parking (~33us at 3GHz)
        uint64_t MinSpinThresholdCycles = 10000; // initial spin after wake (~3us at 3GHz)
        uint64_t LoadWindowNs = 1000000;      // 1ms -- load estimate window
        uint32_t StarvationGuardLimit = 3;    // consecutive idle cycles before forced steal
        uint32_t MaxStealNeighbors = 3;       // max neighbors to probe per steal attempt
        uint16_t MaxSlots = 128;              // max slots per pool (configurable)
        uint32_t EventsPerMailbox = 100;      // max events per mailbox execution
        uint64_t TimePerMailboxNs = 1000000;  // 1ms -- max time per mailbox execution
        uint32_t ParkAfterIdlePolls = 64;     // park after this many consecutive idle PollSlot calls
    };

} // namespace NActors::NWorkStealing
