#pragma once

#include <cstddef>
#include <cstdint>

namespace NActors::NWorkStealing {

    struct TBucketConfig;

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
        uint8_t ContinuationRingCapacity = 4;  // max items in continuation ring (1-8)

        // Adaptive slot scaling
        bool AdaptiveScaling = false;               // master switch
        uint64_t AdaptiveEvalCycles = 30000000;     // ~10ms at 3GHz between evaluations
        uint64_t AdaptiveCooldownCycles = 90000000; // ~30ms cooldown after scaling change
        double InflateUtilThreshold = 0.8;          // inflate when >=80% of slots busy
        double DeflateUtilThreshold = 0.3;          // deflate when <30% of slots busy
        double SlotBusyThreshold = 0.1;             // slot considered "busy" above 10% util
        uint32_t QueuePressureThreshold = 16;       // inflate if any slot queue depth >16
        uint64_t AdaptiveParkNs = 50000;              // 50us — timed park for eval worker

        // Adaptive slot bucketing
        bool SlotBucketing = false;                    // master switch
        uint64_t BucketCostThresholdCycles = 100000;   // above → heavy bucket (~33us at 3GHz)
        uint64_t BucketDowngradeThresholdCycles = 50000; // below → fast (hysteresis)
        uint32_t BucketMinSamples = 64;                // events before classification
        uint16_t BucketEmaAlphaQ16 = 6554;             // ~0.1 in Q16.16
        uint16_t BucketMinActiveSlots = 4;               // disable bucketing below this many active slots
    };

} // namespace NActors::NWorkStealing
