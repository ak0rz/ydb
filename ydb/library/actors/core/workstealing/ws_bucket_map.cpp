#include "ws_bucket_map.h"

#include <algorithm>

namespace NActors::NWorkStealing {

    // --- TActiveHintSet ---

    void TActiveHintSet::Insert(ui32 hint) {
        uint32_t idx = hint % Capacity;
        uint32_t expected = Empty;
        if (Hints_[idx].compare_exchange_strong(expected, hint, std::memory_order_relaxed)) {
            Size_.fetch_add(1, std::memory_order_relaxed);
        } else if (expected != hint) {
            // Collision: overwrite (acceptable for best-effort tracking)
            Hints_[idx].store(hint, std::memory_order_relaxed);
        }
    }

    uint32_t TActiveHintSet::Size() const {
        return Size_.load(std::memory_order_relaxed);
    }

    // --- TBucketMap ---

    TBucketMap::TBucketMap(TMailboxTable* mboxTable, const TBucketConfig& config)
        : MailboxTable_(mboxTable)
        , Config_(config)
    {
        Lines_.reset(new std::atomic<TBucketLine*>[TMailboxTable::LinesCount]{});
        ActiveHints_.Clear();
    }

    TBucketMap::~TBucketMap() {
        for (size_t i = 0; i < TMailboxTable::LinesCount; ++i) {
            delete Lines_[i].load(std::memory_order_relaxed);
        }
    }

    TMailboxBucketInfo* TBucketMap::GetBucketInfo(ui32 hint) {
        ui32 lineIndex = (hint >> TMailboxTable::LineIndexShift) & TMailboxTable::LineIndexMask;
        if (lineIndex >= TMailboxTable::LinesCount) [[unlikely]] {
            return nullptr;
        }

        auto* line = Lines_[lineIndex].load(std::memory_order_acquire);
        if (!line) {
            // Lazy allocation: allocate a new line and try to publish it
            auto* newLine = new TBucketLine{};
            TBucketLine* expected = nullptr;
            if (Lines_[lineIndex].compare_exchange_strong(expected, newLine, std::memory_order_release)) {
                line = newLine;
            } else {
                // Another thread won the race
                delete newLine;
                line = expected;
            }
        }

        return &line->Infos[hint & TMailboxTable::MailboxIndexMask];
    }

    const TMailboxBucketInfo* TBucketMap::GetBucketInfo(ui32 hint) const {
        ui32 lineIndex = (hint >> TMailboxTable::LineIndexShift) & TMailboxTable::LineIndexMask;
        if (lineIndex >= TMailboxTable::LinesCount) [[unlikely]] {
            return nullptr;
        }

        auto* line = Lines_[lineIndex].load(std::memory_order_acquire);
        if (!line) {
            return nullptr; // const path doesn't allocate
        }

        return &line->Infos[hint & TMailboxTable::MailboxIndexMask];
    }

    uint8_t TBucketMap::GetBucket(ui32 hint) const {
        if (auto* bi = GetBucketInfo(hint)) {
            return bi->BucketId.load(std::memory_order_relaxed);
        }
        return 0; // default: fast
    }

    void TBucketMap::UpdateAfterExecution(ui32 hint, uint64_t eventCycles) {
        auto* bi = GetBucketInfo(hint);
        if (!bi) {
            return;
        }

        // Inline eviction: if this single event exceeded the threshold
        // and the mailbox is currently in the fast bucket, flip to heavy.
        if (eventCycles > Config_.CostThresholdCycles) {
            uint8_t current = bi->BucketId.load(std::memory_order_relaxed);
            if (current == 0) {
                bi->BucketId.store(1, std::memory_order_relaxed);
                bi->Classified.store(1, std::memory_order_relaxed);
            }
        }

        // Update EMA: ema = ema * (1 - alpha) + sample * alpha
        // All in Q16.16 fixed point
        uint32_t oldEma = bi->EmaAvgCostQ16.load(std::memory_order_relaxed);
        uint32_t sampleQ16 = static_cast<uint32_t>(std::min<uint64_t>(eventCycles, 0xFFFFu) << 16);
        uint32_t alpha = Config_.EmaAlphaQ16;
        // ema_new = ema_old + alpha * (sample - ema_old) / 65536
        int64_t diff = static_cast<int64_t>(sampleQ16) - static_cast<int64_t>(oldEma);
        int64_t newEma = static_cast<int64_t>(oldEma) + (diff * alpha) / 65536;
        if (newEma < 0) newEma = 0;
        bi->EmaAvgCostQ16.store(static_cast<uint32_t>(newEma), std::memory_order_relaxed);

        // Count toward classification
        auto* stats = MailboxTable_->GetStats(hint);
        if (stats) {
            uint64_t events = stats->EventsProcessed.load(std::memory_order_relaxed);
            if (events >= Config_.MinSamplesForClassification) {
                bi->Classified.store(1, std::memory_order_relaxed);
            }
        }
    }

    uint8_t TBucketMap::PredictFromClassCost(uint64_t classAvgCost) const {
        return (classAvgCost > Config_.CostThresholdCycles) ? 1 : 0;
    }

    bool TBucketMap::IsClassified(ui32 hint) const {
        if (auto* bi = GetBucketInfo(hint)) {
            return bi->Classified.load(std::memory_order_relaxed) != 0;
        }
        return false;
    }

    void TBucketMap::ResetBucket(ui32 hint) {
        ui32 lineIndex = (hint >> TMailboxTable::LineIndexShift) & TMailboxTable::LineIndexMask;
        if (lineIndex >= TMailboxTable::LinesCount) [[unlikely]] {
            return;
        }
        auto* line = Lines_[lineIndex].load(std::memory_order_acquire);
        if (!line) {
            return;
        }
        auto& info = line->Infos[hint & TMailboxTable::MailboxIndexMask];
        info.BucketId.store(0, std::memory_order_relaxed);
        info.Classified.store(0, std::memory_order_relaxed);
        info.EmaAvgCostQ16.store(0, std::memory_order_relaxed);
    }

    void TBucketMap::SetBucket(ui32 hint, uint8_t bucket) {
        if (auto* bi = GetBucketInfo(hint)) {
            bi->BucketId.store(bucket, std::memory_order_relaxed);
            bi->Classified.store(1, std::memory_order_relaxed);
        }
    }

    void TBucketMap::TrackActive(ui32 hint) {
        ActiveHints_.Insert(hint);
    }

    void TBucketMap::Reclassify() {
        ui16 heavyCount = 0;

        // Sweep recently-active mailboxes: apply hysteresis, count heavy actors.
        // Only apply EMA-based reclassification for mailboxes with enough samples;
        // the Q16.16 EMA needs time to converge, so premature downgrade of
        // inline-evicted actors must be avoided.
        ActiveHints_.DrainAndClear([this, &heavyCount](uint32_t hint) {
            auto* bi = GetBucketInfo(hint);
            if (!bi) {
                return;
            }

            // Check if we have enough samples for reliable EMA
            auto* stats = MailboxTable_->GetStats(hint);
            uint64_t events = stats ? stats->EventsProcessed.load(std::memory_order_relaxed) : 0;

            if (events >= Config_.MinSamplesForClassification) {
                // Read the EMA cost (extract from Q16.16 → cycles)
                uint32_t emaQ16 = bi->EmaAvgCostQ16.load(std::memory_order_relaxed);
                uint64_t emaCycles = emaQ16 >> 16;

                uint8_t currentBucket = bi->BucketId.load(std::memory_order_relaxed);

                if (currentBucket == 0 && emaCycles > Config_.CostThresholdCycles) {
                    // Promote to heavy
                    bi->BucketId.store(1, std::memory_order_relaxed);
                } else if (currentBucket == 1 && emaCycles < Config_.DowngradeThresholdCycles) {
                    // Downgrade to fast (hysteresis: must be well below threshold)
                    bi->BucketId.store(0, std::memory_order_relaxed);
                }
            }

            // Count heavy actors AFTER reclassification
            if (bi->BucketId.load(std::memory_order_relaxed) == 1) {
                ++heavyCount;
            }
        });

        // Update heavy slot target and recompute boundary
        HeavySlotTarget_ = heavyCount;
        RecalcBoundary();
    }

    void TBucketMap::RecalcBoundary() {
        ui16 active = ActiveCount_.load(std::memory_order_relaxed);

        // Disable bucketing when too few slots are active or no heavy actors exist
        if (active < Config_.MinActiveForBucketing || HeavySlotTarget_ == 0) {
            ui16 old = BucketBoundary_.load(std::memory_order_relaxed);
            if (old != 0) {
                BucketBoundary_.store(0, std::memory_order_release);
                Generation_.fetch_add(1, std::memory_order_release);
            }
            return;
        }

        // Allocate one slot per heavy actor, capped so at least 1 fast slot remains
        ui16 heavySlots = std::min(HeavySlotTarget_, static_cast<ui16>(active - 1));
        ui16 newBoundary = heavySlots;

        ui16 old = BucketBoundary_.load(std::memory_order_relaxed);
        if (old != newBoundary) {
            BucketBoundary_.store(newBoundary, std::memory_order_release);
            Generation_.fetch_add(1, std::memory_order_release);
        }
    }

    ui16 TBucketMap::GetBucketBoundary() const {
        return BucketBoundary_.load(std::memory_order_acquire);
    }

    void TBucketMap::SetActiveCount(ui16 count) {
        ActiveCount_.store(count, std::memory_order_release);
        RecalcBoundary();
    }

    uint32_t TBucketMap::GetGeneration() const {
        return Generation_.load(std::memory_order_acquire);
    }

} // namespace NActors::NWorkStealing
