#pragma once

#include "ws_mailbox_table.h"

#include <atomic>
#include <cstdint>
#include <memory>

namespace NActors::NWorkStealing {

    struct TBucketConfig {
        uint64_t CostThresholdCycles = 100000;     // above → heavy bucket
        uint64_t DowngradeThresholdCycles = 50000;  // below → fast (hysteresis)
        uint32_t MinSamplesForClassification = 64;
        uint16_t EmaAlphaQ16 = 6554;               // ~0.1 in Q16.16
        ui16 MinActiveForBucketing = 4;             // disable bucketing below this
    };

    // Per-mailbox bucket classification for adaptive slot bucketing.
    // Single writer: the executor holding the mailbox lock, or the
    // periodic reclassify sweep (which only touches recently-active hints).
    struct TMailboxBucketInfo {
        std::atomic<uint8_t> BucketId{0};       // 0=fast, 1=heavy
        std::atomic<uint8_t> Classified{0};     // 1=has enough samples
        uint16_t Padding = 0;
        std::atomic<uint32_t> EmaAvgCostQ16{0}; // EMA of avg cost in Q16.16 fixed-point
    };

    static_assert(sizeof(TMailboxBucketInfo) == 8);

    // Lock-free fixed-size set of recently-active mailbox hints for
    // periodic reclassification. Insert is racy (duplicates possible,
    // eviction on collision) — acceptable for a best-effort sweep.
    class TActiveHintSet {
    public:
        static constexpr uint32_t Capacity = 4096;
        static constexpr uint32_t Empty = UINT32_MAX;

        void Insert(ui32 hint);
        uint32_t Size() const;

        // Iterate: calls fn(hint) for each non-empty slot, then clears.
        template <typename TFn>
        void DrainAndClear(TFn&& fn) {
            for (uint32_t i = 0; i < Capacity; ++i) {
                uint32_t h = Hints_[i].exchange(Empty, std::memory_order_relaxed);
                if (h != Empty) {
                    fn(h);
                }
            }
            Size_.store(0, std::memory_order_relaxed);
        }

        void Clear() {
            for (uint32_t i = 0; i < Capacity; ++i) {
                Hints_[i].store(Empty, std::memory_order_relaxed);
            }
            Size_.store(0, std::memory_order_relaxed);
        }

    private:
        std::atomic<uint32_t> Hints_[Capacity] = {};
        std::atomic<uint32_t> Size_{0};
    };

    // Classification engine: assigns mailboxes to bucket 0 (fast) or
    // bucket 1 (heavy) based on per-event execution cost.
    //
    // Slot layout: heavy slots at the beginning [0, boundary),
    // fast slots after [boundary, activeCount). Deflation removes from
    // the end (fast slots), so heavy slots are always preserved.
    // When boundary == 0, bucketing is effectively disabled (all fast).
    //
    // Owns its own parallel array of TMailboxBucketInfo, indexed by
    // mailbox hint using the same line/index encoding as TMailboxTable.
    // Lines are allocated lazily on first access.
    //
    // Three classification points:
    //   A) PredictBucket — actor-class stats for unclassified mailboxes
    //   B) UpdateAfterExecution — inline eviction on costly events
    //   C) Reclassify — periodic EMA-based sweep + demand-driven boundary
    class TBucketMap {
    public:
        TBucketMap(TWsMailboxTable* mboxTable, const TBucketConfig& config);
        ~TBucketMap();

        // Current bucket for a mailbox (fast path, relaxed load)
        uint8_t GetBucket(ui32 hint) const;

        // Inline eviction: if cost > threshold and currently fast, flip to heavy
        void UpdateAfterExecution(ui32 hint, uint64_t eventCycles);

        // Predict bucket for a newly registered mailbox from actor-class stats.
        // classAvgCost = ExecutionCycles / MessagesProcessed from TActorClassStats.
        uint8_t PredictFromClassCost(uint64_t classAvgCost) const;

        // Check if a mailbox has been classified (has enough samples or was predicted)
        bool IsClassified(ui32 hint) const;

        // Reset bucket info for a reclaimed mailbox (prevents stale classification
        // from leaking to the next actor occupying the same hint).
        void ResetBucket(ui32 hint);

        // Mark a mailbox as classified with a specific bucket
        void SetBucket(ui32 hint, uint8_t bucket);

        // Track an active hint for periodic reclassification
        void TrackActive(ui32 hint);

        // Periodic sweep: count heavy actors, recompute boundary, apply hysteresis.
        void Reclassify();

        // Bucket boundary: heavy = [0, boundary), fast = [boundary, activeCount).
        // When boundary == 0, no partitioning (all slots serve all work).
        ui16 GetBucketBoundary() const;

        // Update active slot count. Recomputes boundary to preserve heavy slot target.
        void SetActiveCount(ui16 count);

        // Generation counter: incremented on boundary change, checked by workers
        // to rebuild steal iterators.
        uint32_t GetGeneration() const;

    private:
        // Lazily-allocated parallel array of bucket info, same segment
        // structure as TWsMailboxTable but owned entirely by TBucketMap.
        struct alignas(64) TBucketLine {
            TMailboxBucketInfo Infos[TWsMailboxTable::MailboxesPerLine];
        };

        TMailboxBucketInfo* GetBucketInfo(ui32 hint);
        const TMailboxBucketInfo* GetBucketInfo(ui32 hint) const;

        // Recompute BucketBoundary_ from HeavySlotTarget_ and ActiveCount_.
        void RecalcBoundary();

    private:
        TWsMailboxTable* MailboxTable_;
        TBucketConfig Config_;
        std::atomic<ui16> BucketBoundary_{0}; // 0 = no partitioning (all fast)
        std::atomic<ui16> ActiveCount_{0};
        std::atomic<uint32_t> Generation_{0};
        TActiveHintSet ActiveHints_;

        // How many heavy slots are needed (set by Reclassify, read by RecalcBoundary)
        ui16 HeavySlotTarget_ = 0;

        // Heap-allocated array of line pointers, indexed by lineIndex.
        std::unique_ptr<std::atomic<TBucketLine*>[]> Lines_;
    };

} // namespace NActors::NWorkStealing
