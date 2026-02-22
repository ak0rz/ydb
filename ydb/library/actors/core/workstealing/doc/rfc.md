# RFC: Work-Stealing Activation Runtime for YDB Actor System

## Status

Implemented (Steps 0-16 complete, plus post-v1 optimizations including continuation ring, adaptive slot scaling, and load classification buckets). Benchmarks on 2×EPYC 9654 (384 threads) with compact SMT-LLC-NUMA topology pinning show WS+adaptive+bucketing vs basic pool: ping-pong 2-2.5× at scale, chain 1.5-1.9× consistently, star 1.1-2.5× at low-mid pairs, pipeline up to 5.2× at few actors but basic wins at high pipeline counts, storage-node 1.4-2× at 64+ threads. CPU efficiency dramatically better: ping-pong 384t/10p uses 5% CPU for 2.3× throughput vs basic at 98%. See Section 14.

## 1. Problem Statement

The YDB actor system dispatches activations through a single shared MPMC ring queue per executor pool (`TMPMCRingQueueV4Correct<20>`). Every push increments a shared `Tail` counter via `fetch_add`; every pop increments a shared `Head` counter via `fetch_add`. Both are serializing atomics that require exclusive cache line ownership.

On modern many-core hardware, cache coherence round-trip latency between chiplets and sockets bounds the throughput of a single contended atomic counter to roughly 5-12M ops/sec on x86 and 3-8M ops/sec on ARM, regardless of core count. YDB generates 15-50M activations/sec under production OLTP workloads. The queue saturates well before CPU capacity is exhausted.

The [contention analysis](contention-analysis.md) quantifies this in detail. Key findings:

| Configuration | Queue throughput ceiling | YDB demand |
|---------------|------------------------|------------|
| 192 threads, 1 socket (EPYC 9654) | 10-15M act/sec | 15-50M act/sec |
| 384 threads, 2 sockets (EPYC 9654) | 4-6M act/sec | 15-50M act/sec |

At 384 threads, the overhead ratio is 8-150x: threads spend more time in CAS retry loops and coherence waits than executing actors.

### Target Hardware

- **AMD EPYC 9654/9755** (Zen 4/5): 96-128 cores per socket, 12-16 CCDs per socket, cross-CCD coherence 40-80ns, cross-socket 120-200ns
- **NVIDIA Grace** (Neoverse V2): 144 cores, CMN-700 mesh, LL/SC more contention-sensitive than x86 LOCK XADD


## 2. Proposed Architecture

Replace the single shared MPMC queue with per-slot local queues and work stealing between slots. Decouple thread management from pools into a system-wide Driver. The implementation is opt-in, co-exists with the existing runtime at compile time, and is gated by configuration.

### System Overview

```mermaid
graph TD
    AS[TActorSystem] --> PM[TCpuManager]

    PM --> P1[TWSExecutorPool 1]
    PM --> P2[TWSExecutorPool 2]
    PM --> DR[TThreadDriver]

    P1 --> S1_0["Slot 0 (MPMC Queue)"]
    P1 --> S1_1["Slot 1 (MPMC Queue)"]
    P1 --> S1_N["Slot N (MPMC Queue)"]

    P2 --> S2_0["Slot 0 (MPMC Queue)"]
    P2 --> S2_1["Slot 1 (MPMC Queue)"]

    DR --> W0["Worker 0<br/>CPU 0"]
    DR --> W1["Worker 1<br/>CPU 1"]
    DR --> WN["Worker N<br/>CPU N"]

    W0 -.->|polls| S1_0
    W0 -.->|polls| S2_0
    W1 -.->|polls| S1_1
    WN -.->|polls| S1_N
    WN -.->|polls| S2_1

    W1 -->|steals from| S1_0
    WN -->|steals from| S1_1
```

**Key properties:**
- Pools own slot arrays and route activations to slots. Pools do not own threads.
- Driver owns CPU-pinned workers. Workers poll their assigned slots and steal from neighbors.
- A worker may poll slots from multiple pools (configurable, subject to latency budgets).
- Slot 0 of each pool has a wake mechanism to unpark its assigned worker.

### Slot State Machine

Each slot has a lifecycle managed by the Driver in response to harmonizer inflation/deflation.

```mermaid
stateDiagram-v2
    [*] --> Inactive
    Inactive --> Initializing : Driver assigns slot to worker
    Initializing --> Active : Worker wakes, confirms assignment
    Active --> Draining : Harmonizer deflates pool
    Draining --> Inactive : Queues empty, steal complete
    Active --> Active : Normal polling
```

- **Inactive:** not polled, not accepting activations.
- **Initializing:** assigned but worker has not yet started polling. Activations not routed here.
- **Active:** polled by worker, accepts activations, can be stolen from.
- **Draining:** no new activations routed, but existing work and steals continue until empty.

### Polling Routine

The core loop executed by each worker for each assigned slot. Uses a **continuation ring** with **time-based batching** for execution, and **topology-aware work stealing** when idle.

- **Seeding:** If the continuation ring has items from a previous PollSlot (mailboxes that exhausted the event budget), pop one to seed the loop. This gives hot mailboxes first-execution priority. When the ring is empty, this is a no-op -- zero overhead on the common path.

- **Queue processing:** Pop activations from the MPMC queue and execute with a `MaxExecBatch` event budget. Each mailbox runs for up to `MailboxBatchCycles` (~17us at 3GHz). When the time slice expires, check the queue inline: if other work is waiting, push current to queue tail and pop next (inline swap); if empty, reset the deadline and continue the same mailbox. If the budget is exhausted while a mailbox still has events, that mailbox is saved to the continuation ring for the next PollSlot call.

- **Stealing:** If no work found, try stealing from neighbor slots with exponential backoff. Stolen items are executed directly from a stack buffer (no reinjection into the queue).

```mermaid
flowchart TD
    START([PollSlot]) --> SEED{Ring<br/>has items?}

    SEED -->|yes| RPOP["Pop one from ring<br/><i>(priority seeding)</i>"]
    RPOP --> EXEC
    SEED -->|no| POP

    POP{Pop from<br/>queue?} -->|activation| EXEC["Execute events for<br/>up to MailboxBatchCycles"]
    EXEC --> DRAIN{Mailbox<br/>drained?}
    DRAIN -->|yes| BUDGET
    DRAIN -->|no, deadline hit| PEEK{Queue has<br/>other work?}
    PEEK -->|yes| SWAP["Push current to queue,<br/>pop next"]
    PEEK -->|no| RESET["Reset deadline,<br/>continue same mailbox"]
    SWAP --> EXEC
    RESET --> EXEC

    BUDGET{Budget<br/>remaining?} -->|yes| POP
    DRAIN -->|no, budget exhausted| SAVE
    BUDGET -->|no, activation live| SAVE

    SAVE["Save to<br/>continuation ring"] --> BUSY(["Return Busy"])
    BUDGET -->|no, drained| BUSY

    POP -->|empty| STEAL{Steal from<br/>neighbors?}
    STEAL -->|found items| EXEC_STOLEN["Execute directly<br/>from steal buffer"]
    EXEC_STOLEN --> BUSY2(["Return Busy"])
    STEAL -->|nothing| IDLE(["Return Idle"])
```

**Key details:**
- **Continuation ring seeding:** The ring holds activations that exhausted the entire event budget -- proven hottest. One item is popped per PollSlot for priority execution. When the ring is empty, seeding falls through to `slot.Pop()` -- identical to the pre-ring code path.
- **Inline interleaving:** When a mailbox's time slice expires, the loop checks the queue inline. If another activation is waiting, it pushes the current activation to queue tail and pops the next -- swapping without leaving the inner loop. Only budget-exhaustion leftovers go to the ring (not time swaps), ensuring the ring holds only the hottest actors. If the queue is empty, the deadline resets and the same mailbox continues -- avoiding unnecessary MPMC push/pop overhead and false steal potential.
- **Direct steal execution:** Stolen items are executed directly from a stack buffer on the stealer's stack. Only items that still have events after execution are pushed to the local queue. This avoids re-steal races.
- `HadLocalWork` flag distinguishes local work from stolen work, used by the Worker loop for parking decisions.
- Before probing a victim, the stealer checks `SizeEstimate() == 0` and `Executing == false` and skips empty/idle slots.
- The ring's `ContinuationCount` is exported to the activation router, which includes it in load estimates for routing decisions. This prevents the router from overloading slots whose MPMC queue looks empty but whose ring is occupied.

### Continuation Ring

#### Problem: Fan-In Budget Exhaustion

In fan-in workloads (N senders -> 1 receiver), the receiver mailbox accumulates events faster than PollSlot can drain them. When `MaxExecBatch` (64 events) is exhausted while the receiver still has work, the original (pre-continuation) approach pushed the activation to the **end** of the slot's MPMC queue:

```
Before: PollSlot exhausts budget on receiver (R)
Queue state: [S1, S2, S3, ..., SN]
             <- R pushed here ->  [S1, S2, S3, ..., SN, R]
```

With N senders on the same slot, R waits behind all of them before getting another turn. Each sender generates one more event for R, so when R finally runs again, it has N new events but the same 64-event budget. Effective receiver throughput drops to roughly `1/(N+1)` of slot capacity.

For cheap events (~100 cycles, e.g. star receiver doing `counter++`), the 64-event budget is exhausted after ~6,400 cycles -- well before the 50,000-cycle `MailboxBatchCycles` deadline. The push-to-tail is the bottleneck, not time-based interleaving.

#### Attempt 1: Persistent Continuation

First fix: save the hot mailbox in `TPollState::HotContinuation` across PollSlot calls. Each call starts by processing the continuation first, then saves it back if it still has work.

**Result:** Star 4t/10p improved from 157K to 228K (+45%). But star 8t/10p **regressed** from 303K to 71K -- a 4x slowdown.

**Root cause -- synchronized contention:** Persistent continuation forces all workers to start each PollSlot with their sender continuation (Phase 1). All senders execute simultaneously and call `Send(receiver, msg)`, which routes to `slot.Push(receiverHint)`. With 8 workers doing this in lockstep:

```
Worker 0: Phase1(sender_0) -> Send(receiver) -> slot[R].Push(hint) --+
Worker 1: Phase1(sender_1) -> Send(receiver) -> slot[R].Push(hint) --+  8 concurrent
Worker 2: Phase1(sender_2) -> Send(receiver) -> slot[R].Push(hint) --+  CAS operations
  ...                                                                 |  on same cache line
Worker 7: Phase1(sender_7) -> Send(receiver) -> slot[R].Push(hint) --+
```

Each `Push` is a `fetch_add` on the MPMC queue's tail counter. Under 8-way contention, the cache line bounces between L1 caches, inflating per-event cost from ~700 cycles (uncontended) to ~6,000 cycles. The `MailboxBatchCycles` deadline (50,000 cycles) is hit after only ~8 events instead of 64, wasting 87% of the event budget.

Diagnostic counters confirmed this: `Phase1Deadlines ~ BusyPolls` (virtually every Phase 1 hit the time deadline), and `Phase1Execs / BusyPolls ~ 8` (only 8 events per Phase 1, not 64).

#### Attempt 2: One-Shot Continuation

Phase 1 consumes the continuation but does **not** save it back. If the mailbox still has events after Phase 1's budget, it's pushed to the queue -- where Phase 2 and other activations interleave fairly. Only Phase 2's leftover (if any) becomes the next continuation.

This breaks the synchronization: Phase 1 is brief, workers desynchronize in Phase 2, and the continuation mechanism only engages when there's actual sustained load. One-shot eliminated the 8t regression while preserving the fan-in improvement.

#### Solution: Continuation Ring with Queue-Only Swaps

The one-shot approach used a single `std::optional<ui32> HotContinuation`. The continuation ring replaces it with a `TContinuationRing` (stack-allocated FIFO, capacity 4, configurable up to 8) that can hold multiple hot activations across PollSlot calls.

**Key design principle:** Only budget-exhaustion leftovers enter the ring. Time-based inline swaps push to the queue, not the ring. This ensures the ring holds only the proven hottest actors -- those that consumed the entire `MaxExecBatch` budget.

```
PollSlot call N:
  Seed: Pop from ring -> activation A (priority execution)
  Queue loop: Pop B, execute, time swap -> push B to queue, pop C
              Continue until budget exhausted on activation D
  Save: Push D to ring (proven hottest)

PollSlot call N+1:
  Seed: Pop from ring -> activation D (or A if D drained)
  Queue loop: ...
```

**Why queue-only swaps?** Promoting time-swap items to the ring dilutes it with less-hot actors. In star workloads, this fills the ring with senders that survive one time slice, pushing out the truly hot hub actor. Queue-only swaps keep the ring reserved for budget-exhaustion survivors -- the actors that consistently consume the most events per PollSlot.

**Ring occupancy for routing:** Each slot exports `ContinuationCount` (ring size) as a relaxed atomic. The activation router includes this in load estimates, preventing the router from overloading slots whose MPMC queue looks empty but whose ring is occupied.

#### Flush Invariants

A locked mailbox must never be abandoned in the continuation ring. The worker must flush the ring (push all items to queue) before:

1. **Draining** (slot transitioning to `Inactive`): flush before Dekker `WorkerSpinning=false` so `HasWork()` sees it
2. **Parking** (spin timeout): flush before Dekker protocol
3. **Shutdown** (worker loop exit): flush before teardown

See `thread_driver.cpp:WorkerLoop()` for the three flush points.

### Activation Routing

When `ScheduleActivation` is called, the pool routes the activation to a slot:

```mermaid
flowchart TD
    SCHED([ScheduleActivation]) --> READ[Read mailbox.LastPoolSlotIdx]
    READ --> ZERO{Zero?}
    ZERO -->|yes, fresh mailbox| P2R[Power-of-two hash:<br/>pick 2 slots by hash,<br/>choose least loaded]
    ZERO -->|no, sticky| CHECK[Check sticky slot load]
    CHECK --> EMPTY{Load <= 1?}
    EMPTY -->|yes| INJECT[Inject to sticky slot]
    EMPTY -->|no| COMPARE[Compare with<br/>hash-derived peer]
    COMPARE --> BALANCED{"stickyLoad <=<br/>peerLoad * 2 + 2?"}
    BALANCED -->|yes, not overloaded| INJECT
    BALANCED -->|no, overloaded| P2R
    P2R --> INJECT2[Inject to chosen slot]
    INJECT --> WAKE{Need wake?}
    INJECT2 --> WAKE
    WAKE -->|worker parked| WAKEUP[Wake via Dekker protocol]
    WAKE -->|worker spinning| DONE([Done, skip wake])
    WAKEUP --> DONE
```

After executing a mailbox, the slot writes its own index to `mailbox.LastPoolSlotIdx` (before unlocking the mailbox), providing cache-locality stickiness. The load-aware check prevents hot sticky slots from accumulating all activations when load is unbalanced (e.g. 10 actor pairs on 32 slots).

**Deferred reinjection:** When `ScheduleActivationEx` is called for the mailbox currently being executed (e.g., from `TryUnlock` inside `Execute`), the reinjection is deferred until after `Execute` completes. This prevents a race where the mailbox re-enters the slot's queues while still locked for execution.


## 3. Data Structures

### MPMC Unbounded Queue (current)

`TMPMCUnboundedQueue<SegmentSize>` -- segment-based, lock-free, unbounded.

The original plan used Chase-Lev (SPMC) + Vyukov (MPSC) dual queues per slot. During implementation, this was replaced with a single MPMC unbounded queue that supports both concurrent push and pop from any thread, plus batch `StealHalf`. This simplified the slot API (no drain step) and eliminated the injection-to-deque copy bottleneck.

| Operation | Caller | Contention | Memory ordering |
|-----------|--------|------------|-----------------|
| `Push(T)` | Any thread | `fetch_add` on segment tail | Release on slot write |
| `Pop()` | Any thread | CAS on segment head | Acquire on slot read |
| `StealHalf(out, max)` | Any stealer | Snapshot + batch CAS | Seq_cst on size snapshot |

Segments of `SegmentSize` slots are allocated on demand. Empty segments are reclaimed via DEBRA (Deferred Epoch-Based Reclamation) -- a lightweight epoch scheme that defers `delete` until all threads have passed through a quiescent state. This avoids unbounded memory growth while remaining lock-free.

`SizeEstimate()` returns an approximate queue depth via a relaxed atomic counter, used by the activation router for load-aware decisions without traversing segments.

### Design Note: Why MPMC Instead of Chase-Lev + Vyukov

The original RFC specified Chase-Lev SPMC deque (owner pop, stealer steal-half) + Vyukov MPSC queue (multi-producer injection, single-consumer drain). This required a drain step in PollSlot to copy items from MPSC to Chase-Lev before processing. The drain introduced:
1. An extra copy per activation on the hot path
2. Complexity in bounding the drain batch vs. Chase-Lev capacity
3. A single-consumer bottleneck on the MPSC queue

The MPMC queue eliminates all three: any thread can push (replacing MPSC), any thread can pop (replacing Chase-Lev owner pop), and StealHalf provides batch stealing. The trade-off is slightly higher per-operation cost (CAS instead of relaxed store for push), but this is offset by eliminating the drain copy.


## 4. Driver Design

### IDriver Interface

```cpp
class IDriver {
public:
    virtual void Prepare(const TCpuTopology& topology) = 0;
    virtual void Start() = 0;
    virtual void PrepareStop() = 0;
    virtual void Shutdown() = 0;

    virtual void RegisterSlot(TSlot* slot) = 0;         // pool registers a slot
    virtual void ActivateSlot(TSlot* slot) = 0;         // harmonizer inflates
    virtual void DeactivateSlot(TSlot* slot) = 0;       // harmonizer deflates
    virtual void WakeSlot(TSlot* slot) = 0;             // wake worker owning this slot

    virtual void SetWorkerCallbacks(TSlot* slot, TWorkerCallbacks callbacks) = 0;
    virtual std::unique_ptr<IStealIterator> MakeStealIterator(TSlot* exclude) = 0;
};
```

The pool calls `RegisterSlot` for each slot during setup, then `SetWorkerCallbacks` to provide per-slot execute/setup/teardown callbacks. `WakeSlot` unparks the worker owning a specific slot (called when an activation is routed to a parked worker). The interface isolates pools and slots from threading details.

### TThreadDriver

First implementation: one `TThread` + `TThreadParkPad` per registered slot. Each worker polls its assigned slot.

**Worker loop:** Calls `PollSlot()` in a loop. `PollSlot` returns `Busy` (work executed) or `Idle` (nothing found). The parking strategy combines **time-based spinning with local work distinction** and **adaptive spin thresholds**:

- **Local work** (activations from the slot's own queues): resets the spin timer and promotes the spin threshold to `SpinThresholdCycles` (100K cycles, ~33us). `PollSlot` sets `pollState.HadLocalWork = true`.
- **Stolen work** (items taken from neighbor queues): does NOT reset the spin timer. `PollSlot` sets `pollState.HadLocalWork = false`.
- **Idle**: spin timer keeps ticking. When `now - lastLocalWorkTs > spinThreshold`, the worker parks via `TThreadParkPad`.
- **Adaptive threshold:** Workers start with `MinSpinThresholdCycles` (10K cycles, ~3us) after each wake or startup. Only local work promotes the threshold to the full `SpinThresholdCycles`. Workers that never receive local work park within ~3us instead of ~33us.

This design ensures idle workers park fast (saving CPU), while workers with steady local work spin long enough to avoid park/wake overhead. Workers that only steal from neighbors spin briefly then park -- they're helping but shouldn't burn CPU indefinitely.

**Steal ordering:** `TTopologyStealIterator` iterates over all registered slots in circular order (excluding self), probing up to `MaxStealNeighbors` (default 3) per steal round. The starting position rotates after each steal cycle to distribute steal pressure. True topology-ordered stealing (L3/CCD/NUMA proximity) is prepared by `TCpuTopology` but not yet wired into the iterator.

**Wake elimination via Dekker protocol:** Each slot has an atomic `WorkerSpinning` flag and a `DriverData` pointer (eliminates hash map lookup). The protocol:

1. Worker sets `WorkerSpinning = true` (release) when entering the poll loop.
2. Before parking: worker sets `WorkerSpinning = false` (seq_cst), then re-checks `HasPendingInjections()`. If work arrived, cancel park and continue spinning.
3. `WakeSlot()` reads `WorkerSpinning` (seq_cst). If true, the worker is actively polling and will find the work -- skip Unpark entirely.

The seq_cst ordering on both sides forms a Dekker-like protocol: either the worker sees the injection (and doesn't park) or `WakeSlot` sees `WorkerSpinning=false` (and calls Unpark). This eliminates >99.99% of wakes in benchmarks -- millions of redundant `Unpark()` calls reduced to single digits.

### TCpuTopology

Discovers CPU relationships from Linux sysfs (`/sys/devices/system/cpu/*/topology/`, `/sys/devices/system/node/*/distance`). Non-Linux builds fall back to flat (equidistant) topology. Provides `GetNeighborsOrdered(cpuId)` returning all CPUs sorted by proximity.


## 5. Mailbox Execution Model

### Single-Event with Time-Based Batching

The execute callback processes **one event at a time** from the mailbox. PollSlot calls it in a loop for up to `MailboxBatchCycles` (default 50,000 cycles, ~17us at 3GHz), then pushes the activation hint back into the MPMC queue if events remain.

```
ExecuteCallback(hint) -> bool
    true:  event processed, more may remain
    false: mailbox drained and finalized (unlocked)
```

**Why single-event + time-based batch:**

The basic pool's `TExecutorThread::Execute()` processes up to 100 events per mailbox visit, holding the mailbox locked. For work-stealing, this creates two problems:

1. **Starvation:** A self-sending actor (events always in mailbox) would monopolize the worker indefinitely if we processed all events before returning.
2. **Star regression:** Pure single-event processing (batch size = 1) adds MPMC queue push/pop overhead per event. In fan-in workloads (N senders -> 1 receiver), this overhead dominates because the receiver's mailbox always has events and gets pushed/popped every cycle.

The time-based batch is the compromise: process events from one mailbox for up to ~17us, then push back and give other activations a turn. This amortizes queue overhead (fixing star regression) while maintaining fairness (fixing starvation).

**TSAN-critical ordering:** `mailbox->LastPoolSlotIdx` must be written BEFORE `FinishMailbox()` (which calls `Unlock`). After unlock, another thread can read `LastPoolSlotIdx` via `RouteActivation`. Writing after unlock is a data race.


## 6. TActivationContext Strategy

### Problem

`TActivationContext` holds a `TExecutorThread& ExecutorThread`. All static methods (`Send`, `Schedule`, `Register`, etc.) proxy through this reference. The WS runtime has no `TExecutorThread` -- workers are Driver threads polling slots, not executor threads.

### Solution

`TWSExecutorContext` inherits `TExecutorThread` but is never started as a thread.

`TExecutorThread` inherits `ISimpleThread` (= `TThread`). An unstarted `TThread` is a small inert object. The WS context constructor initializes the base class with the pool and actor system references, then never calls `Start()`.

Each Driver worker holds one `TWSExecutorContext` per assigned pool. Before executing a mailbox:

```
TlsActivationContext = TActorContext(mailbox, *wsExecutorContext, eventStart, selfId)
TlsThreadContext = &wsExecutorContext->ThreadCtx
```

All existing code paths work unchanged: `TActivationContext::Send()` calls `ExecutorThread.Send()` which calls `ActorSystem->Send()`. No virtual dispatch, no branching on the hot path.

This approach works independently of PR #34266 (which makes `ExecutorThread` private). The shim IS a `TExecutorThread`, so the reference is valid regardless of access level.


## 7. Configuration Schema

### TCpuManagerConfig Extension

```cpp
struct TWorkStealingPoolConfig {
    ui32 PoolId = 0;
    TString PoolName;
    i16 MinSlotCount = 1;
    i16 MaxSlotCount = 32;
    i16 DefaultSlotCount = 4;
    TDuration TimePerMailbox = TDuration::MilliSeconds(10);
    ui32 EventsPerMailbox = 100;
    i16 Priority = 0;
    NWorkStealing::TWsConfig WsConfig;  // see below
};

struct TWorkStealingConfig {
    bool Enabled = false;
    TVector<TWorkStealingPoolConfig> Pools;
};

struct TWsConfig {
    size_t MaxExecBatch = 64;              // max events per PollSlot call (across all mailboxes)
    uint64_t MailboxBatchCycles = 50000;   // max cycles per mailbox before push-back (~17us at 3GHz)
    uint64_t SpinThresholdCycles = 100000; // max spin cycles before parking (~33us at 3GHz)
    uint64_t MinSpinThresholdCycles = 10000; // initial spin after wake (~3us at 3GHz)
    uint64_t LoadWindowNs = 1000000;       // 1ms -- load estimate window
    uint32_t StarvationGuardLimit = 3;     // consecutive idle cycles before first steal attempt
    uint32_t MaxStealNeighbors = 3;        // max neighbors to probe per steal attempt
    uint16_t MaxSlots = 128;               // max slots per pool
    uint32_t EventsPerMailbox = 100;       // max events per mailbox execution
    uint64_t TimePerMailboxNs = 1000000;   // 1ms -- max time per mailbox execution
    uint8_t ContinuationRingCapacity = 4;  // max items in continuation ring (1-8)
    uint32_t ParkAfterIdlePolls = 64;      // (unused, kept for future use)

    // Adaptive slot scaling (see Section 9)
    bool AdaptiveScaling = false;               // master switch
    uint64_t AdaptiveEvalCycles = 30000000;     // ~10ms at 3GHz between evaluations
    uint64_t AdaptiveCooldownCycles = 90000000; // ~30ms cooldown after scaling change
    double InflateUtilThreshold = 0.8;          // inflate when >=80% of active slots busy
    double DeflateUtilThreshold = 0.3;          // deflate when <30% of active slots busy
    double SlotBusyThreshold = 0.1;             // slot considered "busy" if >10% utilization
    uint32_t QueuePressureThreshold = 16;       // inflate if any slot queue depth exceeds this

    // Load classification buckets (see Section 10)
    bool SlotBucketing = false;                          // master switch
    uint64_t BucketCostThresholdCycles = 100000;         // heavy classification threshold
    uint64_t BucketDowngradeThresholdCycles = 50000;     // hysteresis for heavy->fast downgrade
    uint32_t BucketMinSamples = 64;                      // min events before reclassification
    uint16_t BucketEmaAlphaQ16 = 6554;                   // ~0.1 in Q16.16 fixed-point
    uint16_t BucketMinActiveSlots = 4;                   // disable bucketing below this
};
```

When `WorkStealing` is absent or `Enabled` is false, no WS code is instantiated. Matching pools are created as `TWSExecutorPool`; non-matching pools remain `TBasicExecutorPool`. The driver configuration (worker count, topology) is currently derived automatically from the registered slot count.


## 8. Harmonizer Integration

`TWSExecutorPool` implements the `IExecutorPool` interface including thread-count methods. The harmonizer sees it as a regular pool:

| Harmonizer action | WS translation |
|-------------------|----------------|
| `SetFullThreadCount(N+1)` | `Driver->ActivateSlot()`: Inactive -> Initializing -> Active |
| `SetFullThreadCount(N-1)` | `Driver->DeactivateSlot()`: Active -> Draining -> Inactive |
| `GetThreadCpuConsumption(i)` | Returns slot `i`'s load estimate (stub, returns zero) |
| `GetThreads()` / `GetThreadCount()` | Returns active slot count |

Per-slot counters track executions, drain/steal/idle/busy polls, parks, wakes, and stolen items. These are exposed via `AggregateCounters()` for diagnostics. Full `TCpuConsumption` integration with the harmonizer (mapping per-slot execution time to harmonizer's per-thread view) is not yet implemented -- current benchmarks use fixed slot counts.


## 9. Adaptive Slot Scaling

### Problem

When a WS pool is configured with many slots (e.g. 384 on a 2-socket EPYC) but the workload only needs a few, all workers spin through PollSlot, attempt steals, and burn CPU. The spin threshold (Section 4) parks workers after ~33us of idle spinning, but the activation router treats all Active slots equally -- routing work to high-index slots wakes their workers, which spin briefly and park again. The result is 100% CPU on a workload that basic pool handles at 2%.

Additionally, `TSlotStats.BusyCycles` and `IdleCycles` were never written by the worker loop, so `LoadEstimate` was always 0 and any external harmonizer got no signal.

### Solution: TAdaptiveScaler

A self-contained controller that runs from worker 0, monitors per-slot utilization, and adjusts the active slot count via `SetFullThreadCount`. Slots are deflated from the end (highest index first = topologically farthest, since `RegisterSlots` assigns CPUs from `GlobalCpuOrder_`). Inflation adds from the next available index.

```
+------------------------------------------------------------------+
|                        TWSExecutorPool                           |
|                                                                  |
|  Slots:  [0][1][2]...[N-1][N]...[MaxSlots-1]                   |
|           ^              ^  ^                                    |
|           |   Active     |  |  Inactive                          |
|           |<------------>|  |<------------>|                     |
|           |              |  |              |                     |
|  Deflate removes --------+  |              |                     |
|  from the end (farthest)    |              |                     |
|                             |              |                     |
|  Inflate activates ---------+              |                     |
|  next available                            |                     |
|                                                                  |
|  +---------------------+                                        |
|  |  TAdaptiveScaler    | <- Evaluate() called from Worker 0      |
|  |  every ~10ms        |                                        |
|  |                     |                                        |
|  |  Reads:  BusyCycles, IdleCycles, QueueDepth                  |
|  |  Writes: SetFullThreadCount(newCount)                        |
|  +---------------------+                                        |
+------------------------------------------------------------------+
```

### Load Tracking

Workers write cycle counters around every PollSlot call and park:

```
Active state:
    pollStart = rdtsc()
    result = PollSlot(...)
    elapsed = rdtsc() - pollStart

    if (result == Busy)  -> BusyCycles.fetch_add(elapsed, relaxed)
    else                 -> IdleCycles.fetch_add(elapsed, relaxed)

Park (all 3 sites):
    parkStart = rdtsc()
    ParkPad.Park()
    IdleCycles.fetch_add(rdtsc() - parkStart, relaxed)
```

`BusyCycles` and `IdleCycles` are `std::atomic<uint64_t>` -- workers write via `fetch_add(relaxed)`, the scaler reads via `exchange(0, relaxed)` (atomic read-and-reset). Zero overhead on x86_64 (TSO), silences TSAN.

### Evaluate Algorithm

Worker 0 calls `Evaluate()` every `AdaptiveEvalCycles` (~10ms). The algorithm:

```mermaid
flowchart TD
    START([Evaluate]) --> ENABLED{Adaptive<br/>enabled?}
    ENABLED -->|no| SKIP([Return])
    ENABLED -->|yes| COOL{Cooldown<br/>elapsed?}
    COOL -->|no| SKIP

    COOL -->|yes| SCAN[Scan active slots:<br/>exchange BusyCycles->0<br/>exchange IdleCycles->0<br/>compute per-slot util]

    SCAN --> COUNT["Count busy slots<br/>(util > SlotBusyThreshold)<br/>Track max queue depth"]

    COUNT --> FRAC["busyFraction =<br/>busySlots / activeCount"]

    FRAC --> INF_CHECK{"busyFraction >= 0.8<br/>OR maxQueue > 16?"}
    INF_CHECK -->|yes| INFLATE["newCount = min(max,<br/>active + active/4)<br/><i>+25% geometric</i>"]
    INFLATE --> APPLY

    INF_CHECK -->|no| DEF_CHECK{"busyFraction < 0.3?"}
    DEF_CHECK -->|no| DEADBAND([Return<br/><i>no change</i>])

    DEF_CHECK -->|yes| DEFLATE["target = busy + busy/4<br/>halfCurrent = active / 2<br/>newCount = max(target, half)<br/>newCount = max(1, newCount)"]
    DEFLATE --> APPLY[SetFullThreadCount<br/>record cooldown timestamp]
    APPLY --> DONE([Return])
```

### Hysteresis and Stability

The algorithm has three mechanisms to prevent oscillation:

1. **Deadband:** No action when utilization is between 30% and 80%. Only extreme underload or overload triggers scaling.

2. **Cooldown:** After any scaling change, the scaler waits `AdaptiveCooldownCycles` (~30ms) before evaluating again. This gives the system time to stabilize at the new slot count.

3. **Geometric rate limiting:** Deflation never removes more than 50% of active slots per step. Inflation grows by 25% per step. This prevents wild swings:

```
Deflation convergence (384 slots, 10 genuinely busy):

Step 1:  384 -> 192  (halve: max(target=12, half=192) = 192)
Step 2:  192 ->  96  (halve: max(target=12, half=96)  =  96)
Step 3:   96 ->  48  (halve: max(target=12, half=48)  =  48)
Step 4:   48 ->  24  (halve: max(target=12, half=24)  =  24)
Step 5:   24 ->  24  (deadband: 10/24=42% > 30%, no action)

Total convergence: ~4 steps x (10ms eval + 30ms cooldown) ~ 160ms
```

### Topology Awareness

The scaler inherits topology awareness from `SetFullThreadCount`, which activates slots 0..N-1 and deactivates from N-1 down. Since `RegisterSlots()` assigns CPUs from `GlobalCpuOrder_` (topology-sorted: SMT -> L3 -> NUMA -> distance), deflating higher-index slots removes topologically farthest workers first. No additional topology logic is needed in the scaler.

```
CPU assignment order (GlobalCpuOrder_):
  [core0-SMT0, core0-SMT1, core1-SMT0, ..., coreN-SMTM]
   <- nearest ---------------------------------------- farthest ->

Slot index:     0    1    2    ...    N-2   N-1
                ^                           ^
                |                           |
        always active              deflated first
```

### Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `AdaptiveScaling` | `false` | Master switch. When false, no scaler is created. |
| `AdaptiveEvalCycles` | 30,000,000 | Minimum cycles between evaluations (~10ms at 3GHz). |
| `AdaptiveCooldownCycles` | 90,000,000 | Minimum cycles after a scaling change before the next evaluation (~30ms at 3GHz). |
| `InflateUtilThreshold` | 0.8 | Inflate when >=80% of active slots are busy. |
| `DeflateUtilThreshold` | 0.3 | Deflate when <30% of active slots are busy. |
| `SlotBusyThreshold` | 0.1 | A slot is counted as "busy" if its utilization exceeds 10%. |
| `QueuePressureThreshold` | 16 | Inflate if any active slot's queue depth (MPMC size + continuation ring) exceeds this, regardless of utilization. Prevents throughput loss when few slots are overloaded. |

When `AdaptiveScaling` is enabled, set `MinSlotCount = 1` in `TWorkStealingPoolConfig` to allow full deflation. `SetFullThreadCount` clamps to `MinSlotCount`, so the pool always retains at least one active slot.

### Integration Wiring

```mermaid
sequenceDiagram
    participant Pool as TWSExecutorPool
    participant Scaler as TAdaptiveScaler
    participant Driver as TThreadDriver
    participant W0 as Worker 0

    Note over Pool: Prepare()
    Pool->>Driver: RegisterSlots(slots, maxCount)
    Pool->>Driver: SetWorkerCallbacks(slot[0], {AdaptiveEval: ...})
    Pool->>Scaler: create(SetFullThreadCount, GetActiveCount, slots, config)

    Note over W0: WorkerLoop (Active state)
    loop Every AdaptiveEvalCycles
        W0->>Scaler: Evaluate()
        Scaler->>Scaler: Scan BusyCycles/IdleCycles (exchange->0)
        Scaler->>Scaler: Compute busyFraction, check thresholds

        alt Deflate needed
            Scaler->>Pool: SetFullThreadCount(newCount)
            Pool->>Driver: DeactivateSlot(slot[N-1])
            Driver->>Driver: Slot -> Draining -> Inactive
        else Inflate needed
            Scaler->>Pool: SetFullThreadCount(newCount)
            Pool->>Driver: ActivateSlot(slot[N])
            Driver->>Driver: Slot -> Initializing -> Active
        end
    end
```

### Benchmark Results: Adaptive vs Non-Adaptive

Hardware: 2x AMD EPYC 9654 (384 threads), 10s measurement, 2s warmup.

#### Chain 384t/1000p -- the worst case

| Pool | ops/s | CPU% | Active Slots | Deflate Events |
|------|------:|-----:|-------------:|---------------:|
| basic | 265,469 | 1.4% | 384 | -- |
| WS | 114,627 | **97.8%** | 384 | -- |
| **WS adaptive** | **393,695** | **3.1%** | **12** | 6 |

Adaptive delivers **3.4x throughput** vs non-adaptive WS and **1.5x vs basic**, while dropping CPU from 97.8% to 3.1%. The scaler deflated 384->12 slots in ~160ms.

#### Star 192t/100p

| Pool | ops/s | CPU% | Active Slots | Deflate Events |
|------|------:|-----:|-------------:|---------------:|
| basic | 66,129 | 54.3% | 192 | -- |
| WS | 39,552 | 51.9% | 192 | -- |
| WS adaptive | 59,441 | 52.5% | 191 | 0 |

No deflation -- 100 senders genuinely load slots (42% busy fraction is in the deadband). Throughput improved 39K->59K from the load tracking changes.

#### Ping-pong 384t/10p

| Pool | ops/s | CPU% | Active Slots |
|------|------:|-----:|-------------:|
| basic | 1,422,386 | 61.6% | 384 |
| WS | 2,502,198 | 5.2% | 384 |
| WS adaptive | 2,523,442 | 5.2% | 383 |

WS already efficient here (spin threshold parks idle workers). Adaptive adds no overhead -- no regression.

#### Pipeline 384t/100p

| Pool | ops/s | CPU% | Active Slots |
|------|------:|-----:|-------------:|
| basic | 10,479,137 | 98.3% | 384 |
| WS | 3,550,636 | 93.4% | 384 |
| WS adaptive | 3,268,030 | 93.0% | 384 |

No deflation -- 500 actors (100 pipelines x 5 stages) genuinely load all 384 slots. The WS-vs-basic throughput gap is a separate issue (per-slot queue overhead, not idle spinning).


## 10. Load Classification Buckets

### Overview

When fast actors (e.g. ping-pong pairs doing lightweight message passing) share a pool with heavy actors (e.g. CPU-intensive computation), work stealing spreads both kinds across all slots indiscriminately. A heavy actor occupying a slot for milliseconds blocks fast actors on the same slot, inflating their tail latency. Load classification buckets partition the slot range into two regions -- heavy and fast -- so that heavy actors are isolated from fast ones.

### Architecture

Two buckets: bucket 0 (heavy) and bucket 1 (fast). Default for new mailboxes: fast.

**Slot layout:** `heavy=[0, boundary), fast=[boundary, activeCount)`. When `boundary==0`, no partitioning -- all slots are equivalent (bucketing disabled or not enough active slots).

Heavy slots occupy the beginning of the slot array so that deflation (which removes from the end, highest index first) only removes fast slots, preserving heavy actor isolation. The scaler never deflates below the heavy boundary.

**Demand-driven boundary:** `HeavySlotTarget` = count of classified heavy actors from the last `Reclassify()` sweep. The actual boundary is computed as `BucketBoundary = min(HeavySlotTarget, activeCount - 1)`. Zero means disabled.

**Auto-disable:** Bucketing turns off when `activeCount < MinActiveForBucketing` (default 4). With too few slots, partitioning creates degenerate ranges (e.g. 1 heavy + 1 fast) that reduce steal opportunities.

### Classification Signal

Per-mailbox exponential moving average (EMA) of event execution cost, stored in Q16.16 fixed-point (alpha=0.1, approximately 6554 in Q16.16 representation).

**Threshold:** 100,000 cycles (~33us at 3GHz). A mailbox whose EMA exceeds this threshold is classified as heavy.

**Hysteresis:** A heavy mailbox must drop below 50,000 cycles (~17us) to be downgraded back to fast. This prevents oscillation for borderline actors.

**Per-mailbox state:** `TMailboxBucketInfo` (8 bytes total):
- `BucketId` (atomic u8): current bucket assignment (0=heavy, 1=fast)
- `Classified` (atomic u8): whether the mailbox has been classified at least once
- `EmaAvgCostQ16` (atomic u32): EMA of event cost in Q16.16 fixed-point

These are stored in `TBucketMap`'s own parallel array (same indexing as `TMailboxTable`), lazily allocated per line via CAS. All bucket code lives in `workstealing/` -- the common `mailbox_lockfree` sources are not modified.

### Three Classification Points

1. **Predict from actor class:** On first schedule, if the actor class's historical average cost exceeds the threshold, route to heavy immediately. This avoids cold-start misrouting for known-heavy actor types.

2. **Inline eviction:** After each event execution, if the measured cost exceeds the threshold and the mailbox is currently classified as fast, atomically flip `BucketId` to heavy. The next activation routes to the heavy bucket immediately. This provides sub-millisecond reaction to sudden cost spikes.

3. **Periodic reclassify (~10ms):** Sweeps recently-active mailboxes tracked via a lock-free `TActiveHintSet` (capacity 4096). For each active mailbox:
   - Updates the EMA with recent event costs
   - Applies hysteresis for heavy-to-fast downgrade (only for mailboxes with >= `BucketMinSamples` events since last reclassification)
   - Counts heavy actors
   - Updates `HeavySlotTarget`
   - Calls `RecalcBoundary()` to adjust the slot partition

### Bucket-Aware Routing

The activation router reads `BucketMap->GetBucket(hint)` and `GetBucketBoundary()`:
- Heavy hints route to `[0, boundary)`
- Fast hints route to `[boundary, activeCount)`
- If `boundary==0`, no partitioning -- the full range `[0, activeCount)` is used
- Degenerate ranges (begin >= end) fall back to the full range for correctness

### Bucket-Aware Stealing

The steal iterator partitions neighbors into two groups: same-bucket first, then cross-bucket. Workers rebuild the iterator when a generation counter indicates the boundary has changed.

Cross-bucket stealing remains allowed for correctness -- a worker must never deadlock because work exists in a different bucket. The ordering preference ensures that same-bucket work is found first, minimizing interference.

### Scaler Integration

The adaptive scaler (Section 9) is decoupled from bucketing. The scaler performs global inflate/deflate based on utilization. `TBucketMap::Reclassify()` is called from the scaler's evaluate callback and owns the bucket boundary internally via `RecalcBoundary()`. The scaler does not need to know about buckets -- it only adjusts `activeCount`, and the bucket map recomputes the boundary accordingly.

### Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `SlotBucketing` | `false` | Master switch. When false, no bucket map is created. |
| `BucketCostThresholdCycles` | 100,000 | EMA cost above which a mailbox is classified as heavy (~33us at 3GHz). |
| `BucketDowngradeThresholdCycles` | 50,000 | EMA cost below which a heavy mailbox is downgraded to fast (~17us). |
| `BucketMinSamples` | 64 | Minimum events before a mailbox can be reclassified in the periodic sweep. |
| `BucketEmaAlphaQ16` | 6554 | EMA smoothing factor in Q16.16 (~0.1). Higher values = faster response. |
| `BucketMinActiveSlots` | 4 | Bucketing auto-disables below this active slot count. |

### Key Files

- `ws_bucket_map.h/cpp` -- `TBucketMap`, `TBucketConfig`, `TMailboxBucketInfo`, `TActiveHintSet`
- `activation_router.cpp` -- bucket-aware range routing
- `thread_driver.cpp` -- bucket-ordered steal iterator
- `ws_executor_context.cpp` -- `ResetBucket` on mailbox reclamation
- `ut/ws_bucket_map_ut.cpp` -- 12 unit tests


## 11. NUMA Considerations

The architecture is NUMA-ready by design, but NUMA-specific optimizations are deferred until benchmarks confirm single-NUMA improvement.

**Already built in:**
- `TCpuTopology` discovers NUMA node distances from sysfs
- Steal iterator ordering includes NUMA distance (same-NUMA before cross-NUMA)
- Driver worker-to-CPU pinning respects NUMA placement

**Deferred:**
- NUMA-local-first slot inflation (prefer activating slots on the same NUMA node)
- Per-NUMA-node slot allocation for large pools
- NUMA-aware power-of-two redistribution (prefer same-NUMA slots in routing)
- NUMA-aware mailbox memory allocation


## 12. Prior Art

### Go Runtime Scheduler

Per-P (processor) local run queue (bounded ring, 256 slots) + global run queue + work stealing from random other P. Each goroutine schedules onto its last P for locality. When local queue is empty, steal half from a random P or take from global queue.

**Relevance:** Direct inspiration for the per-slot model with sticky routing and steal-half semantics.

### Rust Tokio

Per-worker LIFO slot (single most recent task for temporal locality) + per-worker SPMC deque + global injection queue. Workers steal from random other workers when idle.

**Relevance:** Validates the MPSC injection + SPMC steal pattern at production scale. Tokio's injection queue maps to our per-slot MPSC.

### Cilk / Intel TBB

Chase-Lev deque (SPAA 2005) originated in Cilk for fork-join work stealing. Intel TBB adopted the same structure. Stealers take from the opposite end of the deque (FIFO for stealers, LIFO for owner), providing good cache behavior for divide-and-conquer workloads.

**Relevance:** The Chase-Lev deque was used in the initial prototype before switching to the MPMC unbounded queue.

### Java ForkJoinPool

Per-worker bounded deques with work stealing. Uses `volatile` fields and Unsafe CAS. Work-stealing order is random; no topology awareness.

**Relevance:** Demonstrates work stealing in managed runtimes with bounded deques and dynamic worker scaling (similar to our harmonizer-driven slot inflation).

### SPDK

Storage Performance Development Kit uses pollers (non-blocking poll functions) and reactors (threads that loop over pollers). Interrupt-driven mode parks reactors when idle and wakes them on I/O completion.

**Relevance:** Reference for Driver interface design. An SPDK-based driver could replace `TThreadDriver` to integrate YDB actors with SPDK's reactor loop.


## 13. Implementation Summary

All 17 steps are complete. 168 unit tests pass, including stress tests with concurrent stealers and TSAN verification.

```
Step 0  Contention Analysis           done
Step 1  RFC Document                  done (this document)
Step 2  Chase-Lev SPMC Deque          done (replaced by MPMC in Step 5b)
Step 3  Vyukov MPSC Queue             done (replaced by MPMC in Step 5b)
Step 5b MPMC Unbounded Queue          done (segment-based, DEBRA reclamation)
Step 4  CPU Topology Discovery        done (sysfs parser, flat fallback)
Step 5  Slot Struct + State Machine   done (MPMC queue, 4-state FSM)
Step 6  Activation Router             done (load-aware sticky + power-of-two)
Step 7  Poll + Steal Functions        done (time-based batch execution)
Step 8  Driver + TThreadDriver        done (one thread per slot, Dekker wake)
Step 9  TActivationContext Shim       done (inherits TExecutorThread, never started)
Step 10 WS Executor Pool              done (IExecutorPool impl, deferred reinjection)
Step 11 Feature Flags + Config        done (opt-in via TCpuManagerConfig)
Step 12 CPU Manager Integration       done (creates TWSExecutorPool + TThreadDriver)
Step 13 Harmonizer Adapter            done (basic: slot count maps to thread count)
Step 14 Existing Test Parameterization done (stress + integration tests)
Step 15 Data Structure Benchmarks     done (MPMC queue microbenchmarks)
Step 16 System-level A/B Benchmarks   done (ping-pong, star, chain; CSV + CPU util)
```

### Post-v1 Optimizations (chronological)

1. **Adaptive spin + steal reduction** (Step 8): Workers start with MinSpinThresholdCycles (~3us) and only promote to full threshold on local work. Pre-steal `SizeEstimate()` check skips empty victims.
2. **Dekker wake elimination** (Step 8): `WorkerSpinning` flag + seq_cst protocol eliminates >99.99% of Unpark syscalls.
3. **MPMC unbounded queue** (Step 5b): Replaced Chase-Lev + Vyukov dual-queue per slot with a single MPMC queue. Simplified API, eliminated drain step and injection-to-deque copy.
4. **Load-aware sticky routing** (Step 6): Sticky slot compared against hash-derived peer; falls back to power-of-two when overloaded (stickyLoad > peerLoad * 2 + 2).
5. **Single-event execution with push-back** (Step 7/16): Replaced batch-then-reinject model with single-event callback + push-back. Prevents self-send starvation.
6. **Time-based mailbox batching** (Step 7): Process events from same mailbox for up to `MailboxBatchCycles` before push-back. Amortizes queue overhead, recovering star performance.
7. **TSAN race fixes**: (a) Write `LastPoolSlotIdx` before `FinishMailbox`/`Unlock`; (b) atomic counters for cross-thread size reads in stress tests.
8. **Continuation ring**: Replaced single `HotContinuation` with `TContinuationRing` (FIFO, capacity 4). Budget-exhaustion leftovers saved to ring; time-swap items go to queue only. Ring occupancy exported via `ContinuationCount` for load-aware routing. See [Continuation Ring](#continuation-ring).
9. **Atomic load tracking** (Section 9): `TSlotStats.BusyCycles` and `IdleCycles` converted to `std::atomic<uint64_t>`. Workers write via `fetch_add(relaxed)` around PollSlot and park calls. Enables `LoadEstimate` computation and adaptive scaling.
10. **Adaptive slot scaling** (Section 9): `TAdaptiveScaler` deflates idle slots (farthest-from-core first) and inflates when load increases, with hysteresis and cooldown. Eliminates the 97.8% CPU regression on overprovisioned workloads (chain 384t/1000p -> 3.1% CPU, 3.4x throughput).
11. **Load classification buckets** (Section 10): Two-bucket system (fast/heavy) with per-mailbox EMA cost tracking, inline eviction, periodic reclassify, and demand-driven boundary. Heavy slots at beginning, auto-disable below MinActiveForBucketing. Bucket-proof benchmark: +107% fast throughput at 50K heavy work, +48% at 500K.


## 14. Benchmark Results

For scenario descriptions, CLI reference, and how to run on remote hardware,
see [benchmarks.md](benchmarks.md).

### Hardware

2x AMD EPYC 9654 (96 cores/socket, 192 physical cores, 384 threads with SMT). 5-second measurement, 2-second warmup. Compact SMT-LLC-NUMA topology pinning via taskset.

Topology masks: 1=CPU 0, 16=0-7,192-199 (1 CCD+SMT), 64=0-31,192-223 (4 CCDs+SMT), 96=0-47,192-239 (6 CCDs+SMT), 192=0-95,192-287 (1 socket+SMT), 384=0-383 (all).

WS configuration: adaptive scaling + slot bucketing enabled.

### Ping-Pong (10 pairs)

| Threads | Basic ops/s | Basic CPU% | WS ops/s | WS CPU% | WS active | Ratio |
|---------|----------:|----------:|--------:|-------:|----------:|------:|
| 1 | 357K | 100% | 395K | 100% | 1 | 1.11x |
| 16 | 4,291K | 100% | 4,010K | 89% | 16 | 0.93x |
| 64 | 1,712K | 99% | 3,448K | 25% | 31 | **2.01x** |
| 96 | 1,432K | 99% | 3,291K | 12% | 16 | **2.30x** |
| 192 | 1,284K | 84% | 3,194K | 9% | 47 | **2.49x** |
| 384 | 1,094K | 98% | 2,498K | 5% | 47 | **2.28x** |

### Ping-Pong (100 pairs)

| Threads | Basic ops/s | Basic CPU% | WS ops/s | WS CPU% | WS active | Ratio |
|---------|----------:|----------:|--------:|-------:|----------:|------:|
| 1 | 361K | 100% | 396K | 100% | 1 | 1.10x |
| 16 | 4,401K | 100% | 4,468K | 94% | 16 | 1.02x |
| 64 | 10,532K | 99% | 9,957K | 100% | 64 | 0.95x |
| 96 | 13,111K | 99% | 11,033K | 99% | 96 | 0.84x |
| 192 | 12,740K | 99% | 11,574K | 67% | 192 | 0.91x |
| 384 | 5,855K | 99% | 10,836K | 39% | 238 | **1.85x** |

### Star (10 pairs)

| Threads | Basic ops/s | Basic CPU% | WS ops/s | WS CPU% | WS active | Ratio |
|---------|----------:|----------:|--------:|-------:|----------:|------:|
| 1 | 40K | 100% | 100K | 100% | 1 | **2.51x** |
| 16 | 534K | 94% | 607K | 69% | 15 | **1.14x** |
| 64 | 478K | 24% | 617K | 17% | 1 | **1.29x** |
| 96 | 405K | 16% | 409K | 12% | 1 | 1.01x |
| 192 | 401K | 8% | 491K | 6% | 1 | **1.22x** |
| 384 | 333K | 4% | 448K | 3% | 1 | **1.35x** |

### Star (100 pairs)

| Threads | Basic ops/s | Basic CPU% | WS ops/s | WS CPU% | WS active | Ratio |
|---------|----------:|----------:|--------:|-------:|----------:|------:|
| 1 | 4K | 100% | 8K | 100% | 1 | **1.91x** |
| 16 | 53K | 98% | 108K | 94% | 16 | **2.06x** |
| 64 | 24K | 99% | 59K | 100% | 64 | **2.49x** |
| 96 | 63K | 99% | 41K | 96% | 96 | 0.64x |
| 192 | 59K | 55% | 51K | 52% | 191 | 0.87x |
| 384 | 48K | 28% | 52K | 26% | 191 | 1.08x |

### Chain (10 pairs)

| Threads | Basic ops/s | Basic CPU% | WS ops/s | WS CPU% | WS active | Ratio |
|---------|----------:|----------:|--------:|-------:|----------:|------:|
| 1 | 361K | 100% | 399K | 100% | 1 | **1.11x** |
| 16 | 499K | 36% | 576K | 62% | 15 | **1.15x** |
| 64 | 292K | 8% | 486K | 16% | 31 | **1.67x** |
| 96 | 296K | 5% | 507K | 9% | 13 | **1.71x** |
| 192 | 297K | 3% | 537K | 5% | 13 | **1.81x** |
| 384 | 243K | 1% | 423K | 3% | 13 | **1.74x** |

### Chain (100 pairs)

| Threads | Basic ops/s | Basic CPU% | WS ops/s | WS CPU% | WS active | Ratio |
|---------|----------:|----------:|--------:|-------:|----------:|------:|
| 1 | 362K | 100% | 402K | 100% | 1 | **1.11x** |
| 16 | 491K | 33% | 508K | 94% | 16 | 1.04x |
| 64 | 291K | 9% | 500K | 24% | 16 | **1.72x** |
| 96 | 287K | 5% | 506K | 13% | 12 | **1.76x** |
| 192 | 288K | 3% | 422K | 8% | 16 | **1.46x** |
| 384 | 207K | 2% | 397K | 4% | 16 | **1.92x** |

### Reincarnation (10 pairs)

| Threads | Basic ops/s | Basic CPU% | WS ops/s | WS CPU% | WS active | Ratio |
|---------|----------:|----------:|--------:|-------:|----------:|------:|
| 1 | 247K | 100% | 249K | 100% | 1 | 1.01x |
| 16 | 2,317K | 99% | 1,039K | 70% | 16 | **0.45x** |
| 64 | 1,087K | 97% | 772K | 19% | 25 | **0.71x** |
| 96 | 1,007K | 98% | 652K | 21% | 37 | **0.65x** |
| 192 | 819K | 99% | 553K | 14% | 46 | **0.68x** |
| 384 | 600K | 98% | 502K | 5% | 22 | **0.84x** |

### Reincarnation (100 pairs)

| Threads | Basic ops/s | Basic CPU% | WS ops/s | WS CPU% | WS active | Ratio |
|---------|----------:|----------:|--------:|-------:|----------:|------:|
| 1 | 247K | 100% | 251K | 100% | 1 | 1.02x |
| 16 | 2,379K | 98% | 2,277K | 91% | 16 | 0.96x |
| 64 | 1,178K | 86% | 1,087K | 77% | 64 | 0.92x |
| 96 | 1,135K | 90% | 1,073K | 69% | 96 | 0.95x |
| 192 | 1,024K | 94% | 933K | 61% | 192 | 0.91x |
| 384 | 652K | 95% | 924K | 32% | 297 | **1.42x** |

### Pipeline (10 pairs)

| Threads | Basic ops/s | Basic CPU% | WS ops/s | WS CPU% | WS active | Ratio |
|---------|----------:|----------:|--------:|-------:|----------:|------:|
| 1 | 95K | 100% | 89K | 100% | 1 | 0.94x |
| 16 | 1,208K | 100% | 618K | 94% | 16 | **0.51x** |
| 64 | 2,562K | 99% | 2,780K | 81% | 64 | **1.09x** |
| 96 | 2,419K | 99% | 2,710K | 52% | 95 | **1.12x** |
| 192 | 888K | 99% | 2,674K | 26% | 58 | **3.01x** |
| 384 | 516K | 99% | 2,698K | 14% | 58 | **5.23x** |

### Pipeline (100 pairs)

| Threads | Basic ops/s | Basic CPU% | WS ops/s | WS CPU% | WS active | Ratio |
|---------|----------:|----------:|--------:|-------:|----------:|------:|
| 1 | 93K | 100% | 89K | 100% | 1 | 0.95x |
| 16 | 1,174K | 100% | 920K | 100% | 16 | **0.78x** |
| 64 | 4,020K | 99% | 2,398K | 100% | 64 | **0.60x** |
| 96 | 5,309K | 99% | 2,870K | 98% | 96 | **0.54x** |
| 192 | 7,942K | 99% | 3,029K | 91% | 192 | **0.38x** |
| 384 | 13,173K | 97% | 3,084K | 45% | 297 | **0.23x** |

### Storage-Node (8 vdisks, 32 clients)

| Threads | Basic ops/s | Basic CPU% | WS ops/s | WS CPU% | WS active | Ratio |
|---------|----------:|----------:|--------:|-------:|----------:|------:|
| 1 | 202K | 100% | 207K | 100% | 1 | 1.02x |
| 16 | 2,164K | 100% | 2,035K | 100% | 16 | 0.94x |
| 64 | 1,114K | 99% | 1,582K | 74% | 64 | **1.42x** |
| 96 | 1,109K | 99% | 1,510K | 61% | 95 | **1.36x** |
| 192 | 1,378K | 41% | 1,525K | 30% | 95 | **1.11x** |
| 384 | 740K | 89% | 1,488K | 11% | 58 | **2.01x** |

### Bucket-Proof Results

Isolated scenario: 64 threads, 8 fast ping-pong pairs + 8 heavy CPU-bound actors sharing one pool. Measures whether bucketing protects fast actors from heavy interference.

**bucket-proof (50K heavy work cycles)**

| Bucketing | Fast ops/s | Fast latency | Heavy ops/s | Active | CPU% |
|-----------|----------:|------------:|----------:|-------:|-----:|
| OFF | 6,737K | 0.15us | 377K | 63 | 30% |
| ON | 13,922K | 0.07us | 375K | 22 | 31% |

**+107% fast throughput, -53% fast latency.** Heavy throughput unchanged.

**bucket-proof (500K heavy work cycles)**

| Bucketing | Fast ops/s | Fast latency | Heavy ops/s | Active | CPU% |
|-----------|----------:|------------:|----------:|-------:|-----:|
| OFF | 6,605K | 0.15us | 377K | 63 | 30% |
| ON | 9,750K | 0.10us | 378K | 22 | 27% |

**+48% fast throughput, -33% fast latency.** Heavy throughput unchanged.

### Analysis

**Consistent WS wins:**

- **Ping-pong (few actors):** 2-2.5x at 64+ threads. Adaptive deflation concentrates work on fewer slots instead of spreading across hundreds of idle threads. At 384t/10p, WS delivers 2.28x throughput at 5% CPU vs basic at 98%.

- **Chain:** 1.1-1.9x across nearly all configurations. Sequential workload benefits from sticky routing and adaptive deflation. Basic pool suffers at scale because 10-100 actors spread across hundreds of threads burn CPU in empty queue polling.

- **Star (few senders):** 1.1-2.5x. Continuation ring + time-based batching give the hub actor priority. At 1t/10p, WS achieves 2.51x from better single-thread scheduling alone.

- **Storage-node:** 1.1-2x at 64+ threads. Realistic mixed workload (8 vdisks with logging, compaction, 32 clients). WS handles the mix of light and heavy actors well. At 384t, WS delivers 2.01x at 11% CPU vs basic at 89%.

**Basic pool wins:**

- **Pipeline (100 pairs):** Basic scales linearly (13.2M at 384t) while WS plateaus at ~3M. With 500+ active actors (100 pipelines x 5 stages), basic's shared MPMC queue amortizes well and all threads stay productive. WS per-slot overhead does not pay off when actors outnumber slots.

- **Reincarnation (10 pairs):** Basic 2x faster at 16t. Actor creation/destruction pattern stresses registration and mailbox allocation, where the basic pool's simpler activation path has less overhead.

- **Star (100 pairs, 96+ threads):** Slight basic advantage. Many senders per slot cause push contention on the receiver's slot.

**CPU efficiency:**

WS uses dramatically less CPU for equal or better throughput at overprovisioned thread counts:
- Ping-pong 384t/10p: WS 5% CPU for 2.3x throughput (vs basic at 98%)
- Chain 384t/10p: WS 3% CPU for 1.7x throughput (vs basic at 1%)
- Pipeline 384t/10p: WS 14% CPU for 5.2x throughput (vs basic at 99%)
- Storage-node 384t: WS 11% CPU for 2.0x throughput (vs basic at 89%)

**Bucketing impact:**

Load classification buckets (Section 10) deliver strong isolation when fast and heavy actors share a pool. The bucket-proof scenario shows +107% fast throughput / -53% latency at 50K heavy work, with zero impact on heavy throughput. For already-isolated workloads (multi-pool, mixed-pool without heavy/fast distinction), bucketing is neutral.


## 15. Known Weak Spots and Next Steps

### Weak Spots to Investigate

| Pattern | Symptom | Root Cause Hypothesis | Investigation Path |
|---------|---------|----------------------|-------------------|
| Pipeline, high pair count | 0.23x at 384t/100p | Per-slot queue overhead does not amortize when actors >> slots. Basic's shared MPMC scales linearly with thread count for this pattern. | Profile MPMC push/pop overhead per slot. Consider routing multiple pipeline stages to same slot. Batch activation routing for pipeline-local sends. |
| Reincarnation, low pair count | 0.45x at 16t/10p | Actor creation/destruction path through WS executor context has more overhead than basic's direct mailbox allocation + MPMC push. | Profile Register/Unregister hot path. Minimize TWSExecutorContext overhead for short-lived actors. |
| Star, 100 pairs at 96-192t | 0.64-0.87x | Many senders per slot cause MPMC push contention on the receiver's slot. With 100 senders and ~96 slots, each slot still has ~1 sender but the receiver slot gets 100 concurrent pushes. | Measure per-slot push contention via profiling. Consider receiver-side batching or sender-side local aggregation. |
| WS throughput plateau at ~3M pipeline ops | Caps at ~3M regardless of thread count | WS per-event overhead (rdtsc, load estimate, routing) creates a per-slot ceiling. Basic has lower per-event overhead and scales with more threads. | Profile per-event overhead breakdown. Consider amortizing rdtsc across N events. Skip routing for self-sends within same slot. |
| Chain 100p at 192t: WS uses 8% CPU but only 1.46x | More CPU than basic (3%) for modest gain | Adaptive scaler settles at 16 active slots but chain(100) could use fewer. Steal attempts on 16 slots with only ~1 active chain link waste cycles. | Tune deflation convergence speed. Consider work-notify instead of steal-probe for chain-like sequential patterns. |
| Ping-pong 100p at 96t: 0.84x | Rare case where basic wins on a "WS-friendly" workload | 100 pairs = 200 actors on 96 slots is a perfect fit for basic's shared queue. WS overhead does not pay off when utilization is already high. | Accept as inherent -- WS overhead only pays when there is slack to exploit. |

### Next Investigation Priorities

1. **Pipeline throughput ceiling** -- highest-impact regression. Profile and optimize the per-event hot path.
2. **Reincarnation performance** -- frequent actor creation is common in production (query actors, session actors). Optimize WS registration path.
3. **Cross-socket stealing** -- current steal iterator is topology-aware but not yet wired to prefer same-NUMA neighbors. Profile cross-socket steal latency.
4. **Harmonizer integration** -- current benchmarks use the adaptive scaler. Full harmonizer integration (per-pool CPU consumption reporting) is needed for production deployment.


## References

1. Chase, D. and Lev, Y. "Dynamic Circular Work-Stealing Deque." SPAA 2005.
2. Le, N.M., Pop, A., Cohen, A., and Zappa Nardelli, F. "Correct and Efficient Work-Stealing for Weak Memory Models." PPoPP 2013.
3. Vyukov, D. "Intrusive MPSC node-based queue." 1024cores.net, 2010.
4. Go runtime scheduler. `runtime/proc.go` in the Go source tree.
5. Tokio (Rust). `tokio/src/runtime/scheduler/multi_thread/`.
6. Lozi et al. "Remote Core Locking." USENIX ATC 2012.
7. Dice, Lev, Moir. "Scalable Statistics Counters." SPAA 2013.
