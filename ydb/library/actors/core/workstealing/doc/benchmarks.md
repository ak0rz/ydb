# WS System Benchmarks

A/B benchmark comparing the work-stealing executor pool (`ws`) against the
baseline shared-queue executor pool (`basic`) across five messaging topologies.

Source: `ydb/library/actors/core/workstealing/bench/system/system_bench.cpp`

## Building

```bash
./ya make -j32 ydb/library/actors/core/workstealing/bench/system
```

Binary:
`ydb/library/actors/core/workstealing/bench/system/ws_system_bench`

## CLI Reference

```
ws_system_bench [OPTIONS]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--pool-type` | `both` | `basic`, `ws`, or `both` |
| `--scenario` | `all` | `ping-pong`, `star`, `chain`, `reincarnation`, `storage-node`, or `all` |
| `--threads` | `1,2,4,8` | Comma-separated thread counts |
| `--pairs` | `10,100` | Comma-separated actor counts (pairs / senders / chain length) |
| `--duration` | `5` | Measurement seconds per scenario |
| `--warmup` | `1` | Warmup seconds before measurement (counters reset after) |
| `--spin-threshold` | `0` | WS spin threshold in CPU cycles (0 = default 100k) |
| `--min-spin-threshold` | `0` | WS min spin threshold in CPU cycles (0 = default 10k) |
| `--vdisks` | `8` | Number of VDisk actor groups (storage-node scenario) |
| `--clients` | `32` | Number of client actors (storage-node scenario) |
| `--put-ratio` | `0.5` | Fraction of requests that are puts vs gets (0.0–1.0) |
| `--put-work` | `500` | CPU iterations per put in skeleton (~1 iter ≈ 1 ns) |
| `--log-work` | `300` | CPU iterations per log write in logger |
| `--get-work` | `5000` | CPU iterations per get in query actor |
| `--compaction-work` | `50000` | CPU iterations per compaction burst |
| `--compaction-period` | `50` | Compaction trigger interval in ms |

### Output

CSV on stdout, one row per (pool_type, scenario, threads, pairs) combination:

```
pool_type,scenario,threads,actor_pairs,ops_per_sec,avg_latency_us,cpu_seconds,cpu_util_pct
```

For WS runs, a comment line is printed before each CSV row with internal
counters (exec, stolen, steal attempts, idle/busy polls, parks/wakes).

## Scenarios

All scenarios count events processed by actor `Receive` handlers. One
"operation" = one event delivered and processed. The five topologies stress
different aspects of the scheduler.

### 1. Ping-Pong

Symmetric point-to-point messaging. Tests raw scheduler throughput with
minimal contention on the activation queues.

```mermaid
graph LR
    subgraph "× N pairs"
        A[Ping] -- Pong --> B[Pong]
        B -- Ping --> A
    end
```

**Setup:** N pairs of actors. Each pair exchanges Ping/Pong messages in a
tight loop. Every delivered event increments a shared counter.

**What it measures:**
- Per-event scheduling overhead (push + pop + execute)
- Cache locality of activation routing (same-slot affinity)
- Steal efficiency under balanced load (each pair is independent)

**Scaling:** Perfectly parallel — N pairs = N independent message streams.
With `--pairs 100 --threads 96`, each thread services ~1 pair on average.

### 2. Star (Fan-In)

Many-to-one messaging. Tests how the scheduler handles a single hot mailbox
receiving from many senders.

```mermaid
graph TD
    S1[Sender 1] --> R[Receiver]
    S2[Sender 2] --> R
    S3[Sender 3] --> R
    SN["Sender N"] --> R

    S1 -- "self-loop" --> S1
    S2 -- "self-loop" --> S2
    S3 -- "self-loop" --> S3
    SN -- "self-loop" --> SN
```

**Setup:** 1 receiver + N senders. Each sender sends a message to the
receiver, then sends a self-message to keep itself running. The receiver
just increments the counter.

**What it measures:**
- Contention on a single mailbox (all senders target the same actor)
- Mailbox lock throughput under fan-in pressure
- Queue fairness — whether the receiver gets enough CPU time

**Known characteristics:** The receiver's mailbox becomes a serialization
point. The `basic` pool handles this better because its shared queue
naturally funnels all activations to any available thread. WS per-slot
queues can leave the receiver pinned to one slot while other slots spin
idle, making this topology a known WS weakness.

### 3. Chain (Ring)

Sequential token-passing through a ring of actors. Tests cross-actor
scheduling latency with strict ordering.

```mermaid
graph LR
    A1[Actor 1] --> A2[Actor 2]
    A2 --> A3[Actor 3]
    A3 --> A4["..."]
    A4 --> AN[Actor N]
    AN --> A1
```

**Setup:** N actors wired in a ring. A single token message circulates:
each actor receives it, increments the counter, and forwards to the next.

**What it measures:**
- Activation-to-execution latency (time between Send and Receive)
- Cross-slot routing overhead (sequential chain defeats locality)
- Wake-up efficiency (only one actor is runnable at any time)

**Known characteristics:** Only one message is in flight at a time, so
parallelism is 1 regardless of thread count. This scenario measures
scheduling latency, not throughput. WS benefits from slot affinity — if
consecutive chain actors land on the same slot, no cross-slot hop is
needed.

### 4. Reincarnation

Actor lifecycle stress test. Tests mailbox allocation, registration, and
garbage collection throughput.

```mermaid
graph TD
    subgraph "× N chains"
        A1["Actor (gen 1)"] -- "Register + Send" --> A2["Actor (gen 2)"]
        A2 -- "Register + Send" --> A3["Actor (gen 3)"]
        A3 -- "Register + Send" --> A4["..."]
    end
    A1 -. PassAway .-> X1((dead))
    A2 -. PassAway .-> X2((dead))
    A3 -. PassAway .-> X3((dead))
```

**Setup:** N independent reincarnation chains. Each actor receives one
message, registers a successor actor, sends it a message, then dies
(`PassAway`). The successor repeats the cycle.

**What it measures:**
- `TMailboxTable::Allocate` / `Free` throughput under contention
- Actor registration cost (`Register` → mailbox lock → slot routing)
- Steal effectiveness for short-lived activations (each mailbox processes
  exactly one event before being freed)

**Known characteristics:** High mailbox churn — every event creates and
destroys a mailbox. The WS pool's slot-affine routing helps here: newly
registered actors often land on the same slot as their parent, reducing
cross-slot hops.

### 5. Storage Node

Composite scenario modeling YDB storage node actor patterns under heavy load.
Combines fan-in serialization, pipeline processing, concurrent child actors,
and periodic background CPU bursts.

```mermaid
graph TD
    subgraph "× C clients"
        CL[StorageClient]
    end

    subgraph "× V VDisk groups"
        SK[Skeleton] --> LOG[Logger]
        LOG --> SK
        SK --> COMP[CompactionActor]
        COMP --> SK
        SK -.-> Q1[QueryActor]
        SK -.-> Q2[QueryActor]
    end

    CL -- "put/get" --> SK
    Q1 -. response .-> CL
    Q2 -. response .-> CL
    SK -- "put response" --> CL
```

**Setup:** V VDisk groups × C client actors. Each VDisk group contains three
long-lived actors (Skeleton, Logger, CompactionActor). Clients send randomized
put/get requests to random VDisks. Puts flow through a pipeline
(Skeleton → Logger → Skeleton → Client). Gets spawn a one-shot TQueryActor
that does CPU work and responds directly to the client. Compaction fires
periodically via `Schedule()`, sending work to the CompactionActor.

**What it measures:**
- Fan-in serialization: multiple clients target the same Skeleton (like real
  SkeletonFront → Skeleton routing)
- Pipeline latency: put requests traverse Skeleton → Logger → Skeleton → Client
  (3 hops, modeling the real PDisk log write pipeline)
- Child actor spawning: gets create one-shot query actors on the executor pool
  (like real VGet query actors)
- Background CPU bursts: compaction creates periodic heavy work items that
  compete with request processing for CPU time
- Mixed workload scheduling: lightweight messages (control flow) interleaved
  with CPU-intensive actors (query, compaction)

**Tuning knobs:**
- `--put-ratio` controls the put/get mix; higher values increase pipeline
  traffic, lower values increase child actor spawning
- `--put-work` / `--log-work` control per-message CPU cost in the pipeline
- `--get-work` controls CPU cost of spawned query actors
- `--compaction-work` / `--compaction-period` control background CPU pressure

**Known characteristics:** The Skeleton actors are serialization points (like
the real VDisk Skeleton), creating natural fan-in bottlenecks. With many
clients targeting few VDisks, this resembles the star topology's hot-mailbox
pattern but with additional pipeline depth and child actor dynamics. WS
benefits from slot affinity keeping each VDisk group's actors local;
basic benefits from its shared queue distributing Skeleton activations.

## Reading the WS Counter Lines

Each WS run prints a comment line before the CSV row:

```
# [ws/ping-pong t=96 p=100] exec=60806856 drained=0 stolen=23624694 steal_att=21979295 idle=114529781 busy=38353689 parks=280259 wakes=280257
```

| Counter | Meaning |
|---------|---------|
| `exec` | Total events executed across all slots |
| `drained` | Events executed during slot draining (deflation) |
| `stolen` | Total activations stolen from other slots |
| `steal_att` | Total steal attempts (includes misses) |
| `idle` | Idle `PollSlot` calls (no work found) |
| `busy` | Busy `PollSlot` calls (work executed) |
| `parks` | Thread park events (futex wait) |
| `wakes` | Thread wake events (futex wake) |

Key ratios to watch:
- **stolen / steal_att** — steal hit rate. Low ratio = lots of wasted probes.
- **parks / wakes** — should be roughly equal. Large parks with few wakes
  means threads are parking unnecessarily.
- **idle / busy** — high idle ratio means threads are spinning without work.
  Compare with `cpu_util_pct` to see if spin is consuming CPU.

## Interpreting Results

### What "ops_per_sec" means per scenario

| Scenario | One "op" |
|----------|----------|
| ping-pong | One Ping or Pong delivered and processed |
| star | One message delivered to the receiver |
| chain | One forward hop in the ring |
| reincarnation | One actor created, messaged, and destroyed |
| storage-node | One client request completed (put pipeline or get query) |

### Typical performance profiles

| Scenario | WS strength | WS weakness |
|----------|-------------|-------------|
| ping-pong | Slot affinity keeps pairs local, avoids shared queue contention | — |
| star | — | Single hot receiver serializes on one slot; basic's shared queue distributes better |
| chain | Low latency from slot affinity for consecutive actors | Only 1 message in flight, can't exploit parallelism |
| reincarnation | Budget-aware stealing reduces churn; new actors inherit parent's slot | High mailbox alloc/free overhead is pool-independent |
| storage-node | Slot affinity keeps VDisk actor groups local; pipeline hops stay on-slot | Skeleton fan-in from many clients can bottleneck on one slot |
