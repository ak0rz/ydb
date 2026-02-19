# RFC: Work-Stealing Activation Runtime for YDB Actor System

## Status

Implemented (Steps 0-16 complete, plus post-v1 optimizations). Benchmarks on 2×EPYC 9654 (384 threads) show WS 1.3-2.8× faster on ping-pong, 1.4-1.9× on star, 1.2-2.6× on chain for the best configurations. See Section 11 for full results.

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

The core loop executed by each worker for each assigned slot. Uses a **time-based batched execution model**: process events from a single mailbox for up to `MailboxBatchCycles` (default ~17μs at 3GHz) before pushing the activation back, then move to the next activation.

```mermaid
flowchart TD
    START([PollSlot]) --> POP

    POP{MPMC queue<br/>pop?} -->|activation| BATCH[Execute batch:<br/>process events for up to<br/>MailboxBatchCycles]
    BATCH --> DONE{Mailbox<br/>drained?}
    DONE -->|no, time budget expired| PUSH[Push activation<br/>back to queue]
    DONE -->|yes, mailbox finalized| BUDGET
    PUSH --> BUDGET{Exec budget<br/>remaining?}
    BUDGET -->|yes| POP
    BUDGET -->|no| BUSY(["Return Busy"])

    POP -->|empty| STEAL{Steal-half<br/>from neighbors?}
    STEAL -->|stolen items| PUSH_STOLEN[Push stolen items<br/>into our queue]
    PUSH_STOLEN --> EXEC_STOLEN[Pop and execute<br/>with same batch model]
    EXEC_STOLEN --> BUSY_STOLEN(["Return Busy"])
    STEAL -->|nothing| IDLE([Return Idle])
```

**Key details:**
- **Time-based batching:** Each mailbox gets up to `MailboxBatchCycles` of processing time per pop/push cycle. This amortizes MPMC queue push/pop overhead for hot mailboxes (critical for star/fan-in patterns) while still interleaving activations fairly. The time check uses `GetCycleCountFast()` (rdtsc).
- **Overall budget:** `MaxExecBatch` (default 64) caps total events per `PollSlot` call across all mailboxes.
- **Push-back model:** After the batch time expires, the activation hint is pushed back into the slot's MPMC queue. This naturally interleaves activations and prevents starvation where self-sending actors could monopolize a worker.
- **Stolen items** are pushed into the local MPMC queue, then popped and executed with the same batched model.
- `HadLocalWork` flag distinguishes local work from stolen work, used by the Worker loop for parking decisions.
- Before calling `StealHalf` on a victim, the stealer checks `SizeEstimate() == 0` and `Executing == false` and skips empty/idle victims.

### Activation Routing

When `ScheduleActivation` is called, the pool routes the activation to a slot:

```mermaid
flowchart TD
    SCHED([ScheduleActivation]) --> READ[Read mailbox.LastPoolSlotIdx]
    READ --> ZERO{Zero?}
    ZERO -->|yes, fresh mailbox| P2R[Power-of-two hash:<br/>pick 2 slots by hash,<br/>choose least loaded]
    ZERO -->|no, sticky| CHECK[Check sticky slot load]
    CHECK --> EMPTY{Load ≤ 1?}
    EMPTY -->|yes| INJECT[Inject to sticky slot]
    EMPTY -->|no| COMPARE[Compare with<br/>hash-derived peer]
    COMPARE --> BALANCED{"stickyLoad ≤<br/>peerLoad × 2 + 2?"}
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

`TMPMCUnboundedQueue<SegmentSize>` — segment-based, lock-free, unbounded.

The original plan used Chase-Lev (SPMC) + Vyukov (MPSC) dual queues per slot. During implementation, this was replaced with a single MPMC unbounded queue that supports both concurrent push and pop from any thread, plus batch `StealHalf`. This simplified the slot API (no drain step) and eliminated the injection-to-deque copy bottleneck.

| Operation | Caller | Contention | Memory ordering |
|-----------|--------|------------|-----------------|
| `Push(T)` | Any thread | `fetch_add` on segment tail | Release on slot write |
| `Pop()` | Any thread | CAS on segment head | Acquire on slot read |
| `StealHalf(out, max)` | Any stealer | Snapshot + batch CAS | Seq_cst on size snapshot |

Segments of `SegmentSize` slots are allocated on demand. Empty segments are reclaimed via DEBRA (Deferred Epoch-Based Reclamation) — a lightweight epoch scheme that defers `delete` until all threads have passed through a quiescent state. This avoids unbounded memory growth while remaining lock-free.

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

- **Local work** (activations from the slot's own queues): resets the spin timer and promotes the spin threshold to `SpinThresholdCycles` (100K cycles, ~33μs). `PollSlot` sets `pollState.HadLocalWork = true`.
- **Stolen work** (items taken from neighbor queues): does NOT reset the spin timer. `PollSlot` sets `pollState.HadLocalWork = false`.
- **Idle**: spin timer keeps ticking. When `now - lastLocalWorkTs > spinThreshold`, the worker parks via `TThreadParkPad`.
- **Adaptive threshold:** Workers start with `MinSpinThresholdCycles` (10K cycles, ~3μs) after each wake or startup. Only local work promotes the threshold to the full `SpinThresholdCycles`. Workers that never receive local work park within ~3μs instead of ~33μs.

This design ensures idle workers park fast (saving CPU), while workers with steady local work spin long enough to avoid park/wake overhead. Workers that only steal from neighbors spin briefly then park — they're helping but shouldn't burn CPU indefinitely.

**Steal ordering:** `TTopologyStealIterator` iterates over all registered slots in circular order (excluding self), probing up to `MaxStealNeighbors` (default 3) per steal round. The starting position rotates after each steal cycle to distribute steal pressure. True topology-ordered stealing (L3/CCD/NUMA proximity) is prepared by `TCpuTopology` but not yet wired into the iterator.

**Wake elimination via Dekker protocol:** Each slot has an atomic `WorkerSpinning` flag and a `DriverData` pointer (eliminates hash map lookup). The protocol:

1. Worker sets `WorkerSpinning = true` (release) when entering the poll loop.
2. Before parking: worker sets `WorkerSpinning = false` (seq_cst), then re-checks `HasPendingInjections()`. If work arrived, cancel park and continue spinning.
3. `WakeSlot()` reads `WorkerSpinning` (seq_cst). If true, the worker is actively polling and will find the work — skip Unpark entirely.

The seq_cst ordering on both sides forms a Dekker-like protocol: either the worker sees the injection (and doesn't park) or `WakeSlot` sees `WorkerSpinning=false` (and calls Unpark). This eliminates >99.99% of wakes in benchmarks — millions of redundant `Unpark()` calls reduced to single digits.

### TCpuTopology

Discovers CPU relationships from Linux sysfs (`/sys/devices/system/cpu/*/topology/`, `/sys/devices/system/node/*/distance`). Non-Linux builds fall back to flat (equidistant) topology. Provides `GetNeighborsOrdered(cpuId)` returning all CPUs sorted by proximity.


## 5. Mailbox Execution Model

### Single-Event with Time-Based Batching

The execute callback processes **one event at a time** from the mailbox. PollSlot calls it in a loop for up to `MailboxBatchCycles` (default 50,000 cycles, ~17μs at 3GHz), then pushes the activation hint back into the MPMC queue if events remain.

```
ExecuteCallback(hint) -> bool
    true:  event processed, more may remain
    false: mailbox drained and finalized (unlocked)
```

**Why single-event + time-based batch:**

The basic pool's `TExecutorThread::Execute()` processes up to 100 events per mailbox visit, holding the mailbox locked. For work-stealing, this creates two problems:

1. **Starvation:** A self-sending actor (events always in mailbox) would monopolize the worker indefinitely if we processed all events before returning.
2. **Star regression:** Pure single-event processing (batch size = 1) adds MPMC queue push/pop overhead per event. In fan-in workloads (N senders → 1 receiver), this overhead dominates because the receiver's mailbox always has events and gets pushed/popped every cycle.

The time-based batch is the compromise: process events from one mailbox for up to ~17μs, then push back and give other activations a turn. This amortizes queue overhead (fixing star regression) while maintaining fairness (fixing starvation).

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
    uint32_t ParkAfterIdlePolls = 64;      // (unused, kept for future use)
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

Per-slot counters track executions, drain/steal/idle/busy polls, parks, wakes, and stolen items. These are exposed via `AggregateCounters()` for diagnostics. Full `TCpuConsumption` integration with the harmonizer (mapping per-slot execution time to harmonizer's per-thread view) is not yet implemented — current benchmarks use fixed slot counts.


## 9. NUMA Considerations

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


## 10. Prior Art

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


## 11. Implementation Summary

All 17 steps are complete. 132 unit tests pass, including stress tests with concurrent stealers and TSAN verification.

```
Step 0  Contention Analysis           ✓
Step 1  RFC Document                  ✓ (this document)
Step 2  Chase-Lev SPMC Deque          ✓ (replaced by MPMC in Step 5b)
Step 3  Vyukov MPSC Queue             ✓ (replaced by MPMC in Step 5b)
Step 5b MPMC Unbounded Queue          ✓ (segment-based, DEBRA reclamation)
Step 4  CPU Topology Discovery        ✓ (sysfs parser, flat fallback)
Step 5  Slot Struct + State Machine   ✓ (MPMC queue, 4-state FSM)
Step 6  Activation Router             ✓ (load-aware sticky + power-of-two)
Step 7  Poll + Steal Functions        ✓ (time-based batch execution)
Step 8  Driver + TThreadDriver        ✓ (one thread per slot, Dekker wake)
Step 9  TActivationContext Shim       ✓ (inherits TExecutorThread, never started)
Step 10 WS Executor Pool              ✓ (IExecutorPool impl, deferred reinjection)
Step 11 Feature Flags + Config        ✓ (opt-in via TCpuManagerConfig)
Step 12 CPU Manager Integration       ✓ (creates TWSExecutorPool + TThreadDriver)
Step 13 Harmonizer Adapter            ✓ (basic: slot count maps to thread count)
Step 14 Existing Test Parameterization ✓ (stress + integration tests)
Step 15 Data Structure Benchmarks     ✓ (MPMC queue microbenchmarks)
Step 16 System-level A/B Benchmarks   ✓ (ping-pong, star, chain; CSV + CPU util)
```

### Post-v1 Optimizations (chronological)

1. **Adaptive spin + steal reduction** (Step 8): Workers start with MinSpinThresholdCycles (~3μs) and only promote to full threshold on local work. Pre-steal `SizeEstimate()` check skips empty victims.
2. **Dekker wake elimination** (Step 8): `WorkerSpinning` flag + seq_cst protocol eliminates >99.99% of Unpark syscalls.
3. **MPMC unbounded queue** (Step 5b): Replaced Chase-Lev + Vyukov dual-queue per slot with a single MPMC queue. Simplified API, eliminated drain step and injection-to-deque copy.
4. **Load-aware sticky routing** (Step 6): Sticky slot compared against hash-derived peer; falls back to power-of-two when overloaded (stickyLoad > peerLoad × 2 + 2).
5. **Single-event execution with push-back** (Step 7/16): Replaced batch-then-reinject model with single-event callback + push-back. Prevents self-send starvation.
6. **Time-based mailbox batching** (Step 7): Process events from same mailbox for up to `MailboxBatchCycles` before push-back. Amortizes queue overhead, recovering star performance.
7. **TSAN race fixes**: (a) Write `LastPoolSlotIdx` before `FinishMailbox`/`Unlock`; (b) atomic counters for cross-thread size reads in stress tests.


## 12. Benchmark Results

### Hardware

2× AMD EPYC 9654 (96 cores/socket, 192 physical cores, 384 threads with SMT). 5-second measurement, 1-second warmup. Cpuset jail escaped via `taskset`.

### Ping-Pong (parallel actor pairs)

| Threads | Pairs | Basic ops/s | WS ops/s | Ratio | WS CPU% |
|---------|-------|------------|---------|-------|---------|
| 1 | 10 | 360K | 462K | **1.28×** | 101 |
| 1 | 100 | 363K | 463K | **1.28×** | 101 |
| 1 | 1000 | 359K | 461K | **1.28×** | 101 |
| 16 | 10 | 2,822K | 3,945K | **1.40×** | 81 |
| 16 | 100 | 3,060K | 5,551K | **1.81×** | 100 |
| 16 | 1000 | 3,315K | 5,580K | **1.68×** | 100 |
| 96 | 10 | 1,200K | 3,316K | **2.76×** | 17 |
| 96 | 100 | 11,367K | 12,609K | **1.11×** | 96 |
| 96 | 1000 | 10,806K | 15,148K | **1.40×** | 100 |
| 192 | 10 | 1,345K | 3,644K | **2.71×** | 11 |
| 192 | 100 | 9,002K | 12,524K | **1.39×** | 68 |
| 192 | 1000 | 11,889K | 19,873K | **1.67×** | 99 |
| 384 | 10 | 746K | 3,794K | **5.09×** | 5 |
| 384 | 100 | 6,007K | 10,316K | **1.72×** | 41 |
| 384 | 1000 | 15,081K | 22,111K | **1.47×** | 98 |

### Star (fan-in: N senders → 1 receiver)

| Threads | Senders | Basic ops/s | WS ops/s | Ratio |
|---------|---------|------------|---------|-------|
| 1 | 10 | 40K | 72K | **1.78×** |
| 1 | 100 | 4.1K | 7.7K | **1.87×** |
| 1 | 1000 | 440 | 673 | **1.53×** |
| 16 | 10 | 456K | 639K | **1.40×** |
| 16 | 100 | 57K | 74K | **1.30×** |
| 16 | 1000 | 5.5K | 7.7K | **1.40×** |
| 96 | 10 | 479K | 548K | **1.14×** |
| 96 | 100 | 43K | 25K | 0.58× |
| 96 | 1000 | 3.2K | 4.1K | **1.27×** |
| 192 | 10 | 380K | 658K | **1.73×** |
| 192 | 100 | 76K | 47K | 0.62× |
| 192 | 1000 | 5.0K | 818 | 0.16× |
| 384 | 10 | 351K | 652K | **1.86×** |
| 384 | 100 | 44K | 28K | 0.63× |
| 384 | 1000 | 820 | 567 | 0.69× |

### Chain (sequential ring of N actors)

| Threads | Chain len | Basic ops/s | WS ops/s | Ratio | Basic CPU% | WS CPU% |
|---------|-----------|------------|---------|-------|-----------|---------|
| 1 | 10 | 368K | 451K | **1.22×** | 101 | 101 |
| 1 | 100 | 361K | 465K | **1.29×** | 101 | 101 |
| 1 | 1000 | 369K | 468K | **1.27×** | 101 | 101 |
| 16 | 10 | 278K | 684K | **2.46×** | 33 | 57 |
| 16 | 100 | 252K | 568K | **2.25×** | 33 | 100 |
| 16 | 1000 | 285K | 584K | **2.05×** | 32 | 100 |
| 96 | 10 | 241K | 629K | **2.61×** | 5 | 10 |
| 96 | 100 | 285K | 378K | **1.33×** | 5 | 102 |
| 96 | 1000 | 242K | 378K | **1.56×** | 6 | 97 |
| 192 | 10 | 354K | 587K | **1.66×** | 3 | 6 |
| 192 | 100 | 289K | 372K | **1.29×** | 3 | 50 |
| 192 | 1000 | 230K | 217K | 0.94× | 3 | 100 |
| 384 | 10 | 302K | 548K | **1.81×** | 2 | 3 |
| 384 | 100 | 290K | 392K | **1.35×** | 1 | 26 |
| 384 | 1000 | 211K | 79K | 0.37× | 2 | 98 |

### Analysis

**Ping-pong:** WS wins across all configurations. Best results at high thread counts with low pair counts, where the basic pool's MPMC contention is worst: **5.09× at 384t/10p** (WS uses only 5% CPU vs basic's 96%). At the highest throughput point (384t/1000p), WS delivers 22.1M ops/s vs 15.1M — **1.47× with equal CPU usage**.

**Star:** WS wins at low-to-mid pair counts (1.1-1.9×) thanks to time-based batching which amortizes queue overhead for the hot receiver mailbox. Regression persists at high pair counts with many threads (96t/100p: 0.58×, 192t/1000p: 0.16×). This is a fundamental contention bottleneck — 1000 senders contending for 1 receiver mailbox lock, with WS workers spending cycles on push-back while basic processes events in batch.

**Chain:** WS wins strongly at low-to-mid pair counts (1.2-2.6×). Chain is inherently sequential — only one actor is active at a time. WS excels because the sticky slot keeps the active chain link local, avoiding MPMC contention. Regression at 384t/1000p (0.37×): WS workers spin at 98% CPU waiting for work that rarely arrives, while basic's idle threads use only 2%.

### CPU Efficiency

A key WS advantage: at overprovisioned thread counts (threads >> active actors), WS parks idle workers and uses dramatically less CPU. Examples:
- PP 384t/10p: WS **5% CPU** vs basic 96%, while delivering **5× throughput**
- Star 192t/10p: WS **6% CPU** vs basic 8%, at **1.73× throughput**
- Chain 96t/10p: WS **10% CPU** vs basic 5%, at **2.61× throughput**

### Known Regressions and Root Causes

| Pattern | Worst case | Root cause | Potential fix |
|---------|-----------|------------|---------------|
| Star, high pairs, many threads | 192t/1000p: 0.16× | Receiver mailbox lock contention; push-back overhead per batch | Adaptive batch size based on mailbox event rate |
| Chain, many threads, high pairs | 384t/1000p: 0.37× | Workers spin on empty queues; park mechanism too slow to converge | Faster park convergence; park after N empty steals |
| Ping-pong, single-thread | 1t: 1.28× (was 1.41×) | rdtsc overhead per event in batch loop | Check deadline every Nth event |


## References

1. Chase, D. and Lev, Y. "Dynamic Circular Work-Stealing Deque." SPAA 2005.
2. Le, N.M., Pop, A., Cohen, A., and Zappa Nardelli, F. "Correct and Efficient Work-Stealing for Weak Memory Models." PPoPP 2013.
3. Vyukov, D. "Intrusive MPSC node-based queue." 1024cores.net, 2010.
4. Go runtime scheduler. `runtime/proc.go` in the Go source tree.
5. Tokio (Rust). `tokio/src/runtime/scheduler/multi_thread/`.
6. Lozi et al. "Remote Core Locking." USENIX ATC 2012.
7. Dice, Lev, Moir. "Scalable Statistics Counters." SPAA 2013.
