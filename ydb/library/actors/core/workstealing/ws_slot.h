#pragma once

#include "ws_counters.h"

#include "mpmc_unbounded_queue.h"

#include <util/system/hp_timer.h>
#include <util/system/types.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <optional>

namespace NActors::NWorkStealing {

    class TWsMailboxTable;
    struct TWsConfig;

    // Callback for ring overflow — reroutes activation to another slot.
    using TOverflowCallback = std::function<void(ui32 hint)>;

    enum class ESlotState: uint8_t {
        Inactive,     // slot is idle, not assigned to a worker
        Initializing, // driver assigned slot to a worker, worker not yet confirmed
        Active,       // worker is running, accepts injections
        Draining,     // harmonizer requested deflation, no new injections
    };

    struct TSlotStats {
        uint64_t ActivationsExecuted = 0;
        std::atomic<uint64_t> IdleCycles{0};
        std::atomic<uint64_t> BusyCycles{0};
        uint64_t ExecTimeAccumNs = 0;
    };

    struct TSlot; // forward — needed by TContinuationRing::FlushTo/SnapshotTo

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

        // Defined in ws_slot.cpp (needs complete TSlot)
        void FlushTo(TSlot& slot);
        void SnapshotTo(TSlot& slot) const;

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
    // down. See TSlot::ReleaseFromWorker / FlushRing.
    struct TPollState {
        uint32_t ConsecutiveIdle = 0;
        uint32_t NextStealAtIdle = 0; // next ConsecutiveIdle value at which to attempt stealing
        uint32_t StealInterval = 0;   // current interval between steal attempts (exponential backoff)
        bool HadLocalWork = false;    // set by PollSlot: true = local work executed, false = stolen or idle
        TContinuationRing Ring;       // hot continuations: activations that survived time budget
    };

    // Scheduling slot for the work-stealing runtime.
    //
    // Each slot owns a TMPMCUnboundedQueue for activations.
    // Any thread can Push/Pop activations; Steal pops a budget-aware batch for stealers.
    //
    // State machine:
    //   Inactive -> Initializing  (driver assigns to worker)
    //   Initializing -> Active    (worker wakes and confirms)
    //   Active -> Draining        (harmonizer requests deflation)
    //   Draining -> Inactive      (queues empty, steals done)
    //
    // All other transitions are rejected.
    struct alignas(64) TSlot {
        TSlot();
        ~TSlot();

        // --- State machine ---

        bool TryTransition(ESlotState from, ESlotState to);
        ESlotState GetState() const;

        // --- Push/Pop API ---

        // Push an activation into this slot's MPMC queue.
        // Returns false if the slot is not Active (caller must reroute).
        bool Push(ui32 hint);

        // Pop the next activation. Returns nullopt if the queue is empty.
        std::optional<ui32> Pop();

        // --- Stealer API (any thread) ---

        // Steal activations from this slot's MPMC queue until the accumulated
        // estimated cost reaches cyclesBudget or maxCount items are taken.
        // When MailboxTable is set, uses per-mailbox avg cycles/event for cost.
        // When MailboxTable is nullptr (tests), falls back to maxCount-limited.
        size_t Steal(ui32* out, size_t maxCount, NHPTimer::STime cyclesBudget);

        // --- Metrics ---

        size_t SizeEstimate() const;
        bool HasWork() const;

        // --- Driver integration ---

        std::atomic<bool> WorkerSpinning{false};
        std::atomic<bool> Executing{false};   // true while inside executeCallback
        std::atomic<uint8_t> ContinuationCount{0};  // ring occupancy, read by router
        std::atomic<ui32> RingSnapshot[8] = {};       // snapshot of ring items (updated by worker)

        void* DriverData = nullptr;
        TWsMailboxTable* WsMailboxTable = nullptr;  // set by pool init (WS pools), used for cost-aware stealing
        uint32_t AssignedCpu = 0;  // CPU id assigned by topology ordering (set by driver)

        // --- Pool-assigned fields (set during Prepare) ---

        i16 SlotIdx = -1;                         // slot index within the pool
        const TWsConfig* Config = nullptr;         // pool's WS config
        TOverflowCallback Overflow;                // drain reroute callback
        std::function<void()> AdaptiveEval;        // adaptive scaling eval (slot 0 only)

        // --- Stats ---

        TSlotStats Stats;
        TWsSlotCounters Counters;
        std::atomic<double> LoadEstimate{0.0};

        // --- Worker lifecycle ---

        // Worker acquires this slot for polling.
        // Resets PollState, sets ring capacity from Config, sets WorkerSpinning=true.
        void AcquireForWorker();

        // Worker releases this slot.
        // Flushes ring to queue, sets WorkerSpinning=false (seq_cst for Dekker),
        // resets PollState.
        void ReleaseFromWorker();

        // Flush ring items to queue without releasing.
        // Used in drain path when items need to be re-processed.
        void FlushRing();

        // Access current PollState (valid between Acquire/Release).
        TPollState& GetPollState() { return PollState_; }

    private:
        TPollState PollState_;
        std::atomic<ESlotState> State_{ESlotState::Inactive};

        TMPMCUnboundedQueue<65536> Queue_;
        alignas(64) std::atomic<i64> ApproxSize_{0};

        static bool IsValidTransition(ESlotState from, ESlotState to);
    };

} // namespace NActors::NWorkStealing
