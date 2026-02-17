# Cache Contention Analysis: MPMC Activation Queue on Many-Core Systems

## Executive Summary

The YDB actor system dispatches activations (ready-to-run actor mailbox hints)
through a single shared MPMC ring queue per executor pool. The production
implementation (`TMPMCRingQueueV4Correct<20>`) uses two padded atomic counters
-- `Head` and `Tail` -- that every producer and consumer must modify on every
operation via `fetch_add` or `compare_exchange_strong`. On a dual-socket AMD
EPYC 9654 (192 cores / 384 threads, 24 CCDs), this creates a serialization point: the
cache line holding `Tail` bounces between all producer cores, and the cache
line holding `Head` bounces between all consumer cores. Effective throughput
saturates once coherence round-trip latency dominates, regardless of how many
cores are added.

Modern many-core topologies -- AMD Zen 4 with chiplet-based CCDs, ARM
Neoverse V2 (NVIDIA Grace) with mesh interconnects -- have deep NUMA
hierarchies where cross-CCD and cross-socket coherence traffic costs 4-20x
more than local L3 access. A single contended atomic counter forces all cores
into a global serialization queue whose throughput is bounded by `1 /
round_trip_latency`, yielding roughly 5-12M ops/sec on x86 and 3-8M ops/sec
on ARM regardless of core count. YDB under production load generates 10-50M+
activations per second across pools, so the queue becomes the bottleneck well
before CPU capacity is exhausted.

A work-stealing architecture replaces the single shared queue with per-slot
structures (MPSC injection queues + SPMC Chase-Lev deques), eliminating the
global contention point. This document quantifies the problem and establishes
the performance case for the transition.


## 1. Current Queue Implementation

### 1.1 Structure

The activation queue is defined in `ydb/library/actors/queues/activation_queue.h`.
The production variant `TRingActivationQueueV4` wraps:

```
TMPMCRingQueueV4Correct<20>   -- primary bounded ring, 2^20 = 1M slots
TUnorderedCache<ui32, 512, 4>  -- overflow "overtaken" queue
```

`TMPMCRingQueueV4Correct` (in `mpmc_ring_queue_v4_correct.h`) is a bounded
MPMC ring queue with the following layout:

```cpp
NThreading::TPadded<std::atomic<ui64>> Tail{0};    // cache line 0
NThreading::TPadded<std::atomic<ui64>> Head{0};    // cache line 1
NThreading::TPadded<TArrayHolder<std::atomic<ui64>>> Buffer;  // cache line 2
// ... additional padded fields for overtaken slot tracking
```

`TPadded` (from `library/cpp/threading/chunk_queue/queue.h`) pads each field
to a full cache line (64 bytes), preventing false sharing between `Head` and
`Tail`.

### 1.2 Push Path (Producers)

Every `Send()` that schedules an activation calls `TryPush`:

```cpp
bool TryPush(ui32 val) {
    for (ui32 it = 0;; ++it) {
        ui64 currentTail = Tail.fetch_add(1, std::memory_order_relaxed);  // (A)
        // ...
        // CAS on Buffer[slotIdx] to claim the slot                       // (B)
        if (currentSlot.compare_exchange_strong(expected, newSlotValue,
                std::memory_order_acq_rel)) {
            return true;
        }
        // On CAS failure: check Head to decide if queue is full          // (C)
        ui64 currentHead = Head.load(std::memory_order_acquire);
        // ...
    }
}
```

**Critical contention points:**
- **(A)** `Tail.fetch_add(1)` -- every producer unconditionally increments the
  same atomic. On x86, this compiles to `LOCK XADD`, which requires exclusive
  ownership of the cache line.
- **(B)** `Buffer[slotIdx].compare_exchange_strong` -- slot-level CAS. Due to
  the `ConvertIdx` scatter function (bit-interleaving), consecutive producers
  typically hit different cache lines in the buffer. This is not the primary
  bottleneck.
- **(C)** On retry/failure, producers load `Head` (read-sharing with consumers
  who write to it).

### 1.3 Pop Path (Consumers)

Every executor thread calls `TryPop`:

```cpp
std::optional<ui32> TryPop() {
    // First: check and possibly drain overtaken slots (CAS on OvertakenSlots)
    // ...
    ui64 currentHead = Head.fetch_add(1, std::memory_order_relaxed);  // (D)
    // CAS on Buffer[slotIdx] to extract the value                     // (E)
    // On empty slot: SpinLockPause() and retry
}
```

**Critical contention points:**
- **(D)** `Head.fetch_add(1)` -- every consumer unconditionally increments the
  same atomic. Same serialization as the `Tail` on the producer side.
- **(E)** Buffer slot CAS, mitigated by index scattering.

### 1.4 Overtaken Queue

When the ring is full or producers "overtake" consumers, items spill into
`TUnorderedCache`, which itself contains multiple sharded buckets but
introduces additional atomics: `OvertakenSlots` (CAS loop),
`ReadRevolvingCounter` and `WriteRevolvingCounter` (`fetch_add` each).

### 1.5 Summary of Atomic Operations Per Activation

| Operation | Push | Pop |
|-----------|------|-----|
| `Tail.fetch_add` | 1 (unconditional) | 0-1 (on failure path) |
| `Head.fetch_add` | 0 | 1 (unconditional) |
| `Head.load` | 0-1 (on retry) | 0 |
| `Tail.load` | 0 | 0-1 (on failure) |
| `Buffer[i].CAS` | 1+ | 1+ |
| `OvertakenSlots.CAS` | 0-1 | 0-1 |

**Minimum per activation round-trip: 2 serializing atomics** (`Tail.fetch_add`
on push + `Head.fetch_add` on pop), each requiring exclusive cache line
ownership.


## 2. Hardware Topology and Coherence Costs

### 2.1 AMD EPYC 9654 (Zen 4, Genoa)

| Property | Value |
|----------|-------|
| Sockets | 2 |
| CCDs per socket | 12 |
| Cores per CCD | 8 (16 threads with SMT) |
| Cores per socket | 96 (192 threads with SMT) |
| Total cores (2 sockets) | 192 (384 threads with SMT) |
| L1d / L1i per core | 32KB / 32KB |
| L2 per core | 1MB |
| L3 per CCD (shared) | 32MB |
| Total L3 per socket | 384MB |
| Intra-CCD L3 hit | ~10ns |
| Cross-CCD (same socket, Infinity Fabric) | ~40-80ns |
| Cross-socket (xGMI) | ~120-200ns |

**Cache coherence protocol:** MOESI, extended for multi-chip. A `LOCK XADD`
on a cache line in Modified state on a remote CCD requires:

1. Invalidation request to current owner via Infinity Fabric
2. Data transfer (writeback + forward)
3. Acknowledgement

This round-trip is the floor for serialized atomic throughput.

### 2.2 NVIDIA Grace (Neoverse V2)

| Property | Value |
|----------|-------|
| Cores | 144 (72 per "superchip" in Grace Hopper) |
| L1d per core | 64KB |
| L2 per core | 1MB |
| System-level cache (SLC) | 117MB shared |
| Interconnect | CMN-700 mesh |
| Coherence | Directory-based (MESI derivative) |
| Intra-mesh hop (nearby) | ~15-25ns |
| Cross-mesh (far cores) | ~40-60ns |
| Cross-socket (NVLink-C2C) | ~80-150ns |

**ARM `LDXR/STXR` (LL/SC):** Unlike x86 `LOCK XADD`, ARM uses load-linked /
store-conditional. Under contention, `STXR` fails (without bus lock) and the
entire loop retries. This makes ARM *more sensitive* to contention than x86:
failed SC operations waste cycles without making progress, and the retry loop
generates additional coherence traffic.

### 2.3 NVIDIA BlueField-3 DPU

| Property | Value |
|----------|-------|
| Cores | 16 ARM Cortex-A78 |
| Interconnect | CMN-650 mesh |
| Cross-core coherence | ~20-40ns |

Lower core count, but the DPU processes network packets at line rate and
generates activation bursts. Even 16 cores contending on a single atomic can
reduce throughput below packet processing requirements.


## 3. Throughput Degradation Model

### 3.1 Serialized Atomic Throughput

When N threads contend on a single atomic counter, throughput is bounded by:

```
T_max = 1 / RTT_coherence
```

where `RTT_coherence` is the round-trip time for a cache line to transfer
between contending cores. This is independent of N once N exceeds the
pipelining depth of the coherence protocol (~2-4 on most implementations).

**Estimated maximum `fetch_add` throughput on a single counter:**

| Topology | RTT (ns) | Max ops/sec |
|----------|----------|-------------|
| Intra-CCD (Zen 4) | ~10 | ~100M |
| Cross-CCD, same socket (Zen 4) | ~40-80 | ~12-25M |
| Cross-socket (Zen 4) | ~120-200 | ~5-8M |
| Intra-mesh, nearby (Grace) | ~15-25 | ~40-67M |
| Cross-mesh, far (Grace) | ~40-60 | ~17-25M |
| Cross-socket (Grace) | ~80-150 | ~7-12M |

### 3.2 Effective Queue Throughput

Each activation requires at minimum two serializing atomics (push `Tail` +
pop `Head`). The two counters are on separate cache lines, so they can
pipeline independently. The queue's throughput is bounded by the slower of the
two:

```
T_queue = min(T_tail, T_head)
```

With uniform distribution of producers and consumers across CCDs and sockets:

| Configuration | Estimated queue throughput |
|---------------|--------------------------|
| 8 threads, 1 CCD | ~50-80M act/sec |
| 32 threads, 4 CCDs, 1 socket | ~12-20M act/sec |
| 384 threads (SMT), 24 CCDs, 2 sockets | ~4-6M act/sec |

### 3.3 CAS Retry Amplification

The V4Correct implementation uses `fetch_add` for `Head`/`Tail` reservation
followed by CAS on the buffer slot. Under high contention, the `fetch_add`
succeeds but the slot CAS may fail when producers and consumers interleave.
Failed CAS operations:

- Waste the reserved slot (producing "overtaken" entries)
- Trigger spill to `TUnorderedCache` (additional atomics)
- Force subsequent consumers into the OvertakenSlots CAS loop

The overtaken path adds 2-3 more serializing atomics per activation. Under
sustained contention at 128+ cores, 10-30% of operations may take the
overtaken path, further reducing effective throughput.

### 3.4 YDB Activation Rate Requirements

| Workload | Estimated activations/sec |
|----------|--------------------------|
| Idle / light OLTP | 0.5-2M |
| Moderate OLTP (10K QPS) | 5-15M |
| Heavy OLTP (50K+ QPS) | 20-50M |
| Bulk scan / export | 10-30M |
| Mixed (production) | 15-40M |

**At 128+ cores, the queue throughput ceiling (5-15M act/sec) is below the
demand of moderate-to-heavy workloads (15-50M act/sec).** Cores spin in CAS
retry loops and `SpinLockPause()` instead of executing actors.

### 3.5 Wasted Core-Cycles

At 384 threads contending for ~5M act/sec of queue throughput:

- Each thread gets ~13K successful operations/sec
- Time per operation: ~77us (almost entirely coherence wait + CAS retries)
- Useful work per activation (actor `Receive`): typically 0.5-10us
- **Overhead ratio: 8-150x the useful work is spent on queue contention**


## 4. Why Work-Stealing Eliminates This Bottleneck

### 4.1 Per-Slot MPSC Injection Queue

External producers (threads from other pools or I/O completion) inject
activations into a target slot's MPSC queue. The MPSC queue uses a single
atomic (`Head` pointer, XCHG-based intrusive list). Only the owning slot
drains this queue (single consumer, no contention on drain). N producers
contend on one cache line, but N is typically small (activations are
distributed across slots by mailbox affinity).

**Throughput:** If K producers target the same slot, throughput is
`1 / RTT_between_producers` for that slot, but total system throughput scales
with the number of slots.

### 4.2 Chase-Lev SPMC Work-Stealing Deque

Each slot maintains a local deque:

- **Owner pushes/pops from the "top"** (private end): no atomics needed for
  push; pop uses a single `load-acquire` to check if stealers are present,
  fast path is contention-free.
- **Stealers pop from the "bottom"**: `CAS` on the `bottom` index, but
  stealers only appear when their own deque is empty, so contention is rare
  and transient.

**Throughput per slot (owner, no stealing):** Limited only by L1 cache
latency, ~500M-1000M ops/sec.

### 4.3 Topology-Aware Stealing

Stealing follows the memory hierarchy:

1. **L3-local** (same CCD): ~10ns RTT, steal from sibling cores first
2. **Socket-local** (cross-CCD): ~40-80ns RTT, steal from same-socket CCDs
3. **Cross-socket**: ~120-200ns RTT, steal as last resort

This ensures that the common case (local work available) is contention-free,
and stealing pays coherence costs only when necessary and proportional to the
distance.

### 4.4 Expected Throughput Scaling

| Configuration | MPMC queue (current) | Work-stealing (proposed) |
|---------------|---------------------|--------------------------|
| 8 threads, 1 CCD | ~50-80M | ~500M+ (local deque) |
| 32 threads, 4 CCDs | ~12-20M | ~200-400M |
| 128 threads, 12 CCDs | ~10-15M | ~100-300M |
| 384 threads, 2 sockets | ~4-6M | ~80-200M |

Work-stealing throughput scales approximately linearly with core count until
the stealing rate becomes significant (which only happens under severe
imbalance). Even in the worst case (all work injected into one slot, all
others stealing), throughput degrades to the stealing bandwidth, which is
distributed across multiple steal targets rather than serialized on a single
counter.


## 5. Architecture Comparison

| Aspect | Current (MPMC ring) | Proposed (work-stealing) |
|--------|--------------------|-----------------------|
| **Queue type** | Single shared MPMC per pool | Per-slot MPSC inject + SPMC deque |
| **Push contention** | All producers on one `Tail` atomic | Distributed across slot inject queues |
| **Pop contention** | All consumers on one `Head` atomic | Owner pops locally (no contention) |
| **Overflow handling** | Spill to `TUnorderedCache` (more atomics) | Deque grows dynamically; no overflow |
| **NUMA awareness** | None | Topology-aware steal order |
| **Throughput scaling** | O(1) -- bounded by single cache line | O(cores) up to topology limits |
| **Latency (uncontended)** | ~20-50ns (fetch_add + CAS) | ~5-10ns (local deque push/pop) |
| **Latency (256 threads)** | ~200-1000ns (coherence + retries) | ~10-80ns (local or near-steal) |
| **Fairness** | FIFO within ring (approx.) | Per-slot FIFO + steal rebalancing |
| **Complexity** | Moderate (CAS loops, overtaken tracking) | Higher (deque + steal protocol + affinity) |
| **Cache footprint** | 2 hot cache lines (Head, Tail) | 1 hot line per active slot |
| **Worst case** | All cores serialize | All cores steal from one slot |


## 6. Conclusion

The current MPMC activation queue has a hard throughput ceiling determined by
cache coherence round-trip latency on two globally contended cache lines. On
dual-socket AMD EPYC 9654 systems (192 cores / 384 threads), this ceiling is approximately 4-6M
activations/sec -- well below the 15-50M act/sec that YDB generates under
production OLTP workloads. The queue's design cannot be incrementally improved:
the fundamental issue is that all threads serialize on the same two counters.

A work-stealing architecture eliminates this serialization point by giving each
executor slot its own local deque with contention-free owner access. External
injection uses per-slot MPSC queues that distribute contention across N
independent cache lines instead of 1. Topology-aware stealing ensures that
cross-CCD and cross-socket coherence traffic occurs only when necessary,
keeping the common case (local work) fast.

The expected throughput improvement is 10-40x at 128+ cores, with near-linear
scaling up to topology boundaries.


## References

1. Chase, D. and Lev, Y. "Dynamic Circular Work-Stealing Deque." *SPAA 2005*.
   -- The foundational lock-free deque used by most work-stealing runtimes.

2. Le, N.M., Pop, A., Cohen, A., and Zappa Nardelli, F. "Correct and
   Efficient Work-Stealing for Weak Memory Models." *PPoPP 2013*.
   -- Correctness proof and optimized implementation for ARM/POWER.

3. Lozi, J.-P., Lepers, B., Funston, J., Gaud, F., Quema, V., and Fedorova,
   A. "Remote Core Locking: Migrating Critical-Section Execution to Improve
   the Performance of Multithreaded Applications." *USENIX ATC 2012*.
   -- Demonstrates that contended locks on multi-socket NUMA systems are
   dominated by coherence traffic; delegation to a single core can be faster.

4. Dice, D., Lev, Y., and Moir, M. "Scalable Statistics Counters." *SPAA
   2013*. -- Per-core counters with combining; directly relevant to why
   centralized atomics do not scale.

5. Go runtime scheduler design. `runtime/proc.go` in the Go source tree.
   -- Per-P local run queues (size 256), global run queue, work stealing.
   Direct inspiration for the per-slot model.

6. Tokio (Rust async runtime). `tokio/src/runtime/scheduler/multi_thread/`.
   -- Per-worker LIFO slot + SPMC deque + global injection queue. Demonstrates
   the pattern in production at scale.

7. AMD. "AMD EPYC 9004 Series Processors Architecture." *AMD Technical
   Reference*, 2023. -- CCD topology, Infinity Fabric latencies, MOESI
   coherence protocol details.

8. NVIDIA. "NVIDIA Grace CPU Superchip Architecture." *NVIDIA Technical Brief*,
   2023. -- Neoverse V2 core, CMN-700 mesh, directory-based coherence.
