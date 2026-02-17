#pragma once

#include <atomic>
#include <cstdint>
#include <cstdio>

namespace NActors::NWorkStealing {

    // Snapshot of counters for aggregation and reporting (plain values, copyable).
    struct TWsSlotCountersSnapshot {
        uint64_t Executions = 0;
        uint64_t StolenItems = 0;
        uint64_t StealAttempts = 0;
        uint64_t IdlePolls = 0;
        uint64_t BusyPolls = 0;
        uint64_t DrainedItems = 0;
        uint64_t Parks = 0;
        uint64_t Wakes = 0;

        void Add(const TWsSlotCountersSnapshot& other) {
            Executions += other.Executions;
            StolenItems += other.StolenItems;
            StealAttempts += other.StealAttempts;
            IdlePolls += other.IdlePolls;
            BusyPolls += other.BusyPolls;
            DrainedItems += other.DrainedItems;
            Parks += other.Parks;
            Wakes += other.Wakes;
        }

        void Dump(const char* label) const {
            std::fprintf(stderr,
                         "# [%s] exec=%lu drained=%lu stolen=%lu "
                         "steal_att=%lu idle=%lu busy=%lu parks=%lu wakes=%lu\n",
                         label, Executions, DrainedItems, StolenItems,
                         StealAttempts, IdlePolls, BusyPolls, Parks, Wakes);
        }
    };

    // Per-slot diagnostic counters. Most are incremented by the single owner
    // thread, but Wakes is incremented from any thread (via WakeSlot), and
    // Reset/Snapshot may be called from the main thread while workers run.
    // Use relaxed atomics for TSAN correctness with zero overhead on x86_64.
    struct TWsSlotCounters {
        std::atomic<uint64_t> Executions{0};
        std::atomic<uint64_t> StolenItems{0};
        std::atomic<uint64_t> StealAttempts{0};
        std::atomic<uint64_t> IdlePolls{0};
        std::atomic<uint64_t> BusyPolls{0};
        std::atomic<uint64_t> DrainedItems{0};
        std::atomic<uint64_t> Parks{0};
        std::atomic<uint64_t> Wakes{0};

        void Reset() {
            Executions.store(0, std::memory_order_relaxed);
            StolenItems.store(0, std::memory_order_relaxed);
            StealAttempts.store(0, std::memory_order_relaxed);
            IdlePolls.store(0, std::memory_order_relaxed);
            BusyPolls.store(0, std::memory_order_relaxed);
            DrainedItems.store(0, std::memory_order_relaxed);
            Parks.store(0, std::memory_order_relaxed);
            Wakes.store(0, std::memory_order_relaxed);
        }

        TWsSlotCountersSnapshot Snapshot() const {
            return {
                Executions.load(std::memory_order_relaxed),
                StolenItems.load(std::memory_order_relaxed),
                StealAttempts.load(std::memory_order_relaxed),
                IdlePolls.load(std::memory_order_relaxed),
                BusyPolls.load(std::memory_order_relaxed),
                DrainedItems.load(std::memory_order_relaxed),
                Parks.load(std::memory_order_relaxed),
                Wakes.load(std::memory_order_relaxed),
            };
        }
    };

} // namespace NActors::NWorkStealing
