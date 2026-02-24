#pragma once

#include "mpmc_unbounded_queue.h"

#include <ydb/library/actors/core/mailbox_lockfree.h>

#include <atomic>
#include <cstdint>
#include <limits>

namespace NActors::NWorkStealing {

    // Per-mailbox execution/idle statistics, stored in a parallel segment
    // array in TWsMailboxTable so that TMailbox stays at 64 bytes.
    // Single-writer (mailbox locked during execution), but monitoring may
    // read from another thread, so all counters use relaxed atomics.
    struct alignas(64) TMailboxExecStats {
        std::atomic<ui64> EventsProcessed{0};
        std::atomic<ui64> TotalExecutionCycles{0};
        std::atomic<ui64> MaxExecutionCycles{0};
        std::atomic<ui64> ActivationCount{0};
        std::atomic<ui64> LastExecutionEndCycles{0};
        std::atomic<ui64> TotalIdleCycles{0};
        std::atomic<ui64> MaxIdleCycles{0};
        std::atomic<ui64> MinIdleCycles{std::numeric_limits<ui64>::max()};

        void Reset() noexcept;
    };

    static_assert(sizeof(TMailboxExecStats) == 64, "TMailboxExecStats must be one cache line");

    // Flatter mailbox table for WS pools.
    //
    // 32K mailboxes per segment (2MB = 1 hugepage) instead of
    // TMailboxTable's 4K per line (256KB). Index array is 4KB
    // (fits in L1) vs ~1MB.
    //
    // Hint encoding:
    //   bits [31:15] = segment index (capped at MaxSegments)
    //   bits [14:0]  = offset within segment (32K entries)
    //   Hint 0 is reserved (never allocated).
    //
    // Thread safety:
    //   Get/GetStats — lock-free, any thread
    //   AllocateBatch/FreeBatch — lock-free (MPMC queue, rare batch path)
    //   Cleanup — single-threaded (shutdown)
    class TWsMailboxTable {
    public:
        static constexpr size_t MaxSegments = 512;
        // 2MB hugepage / sizeof(TMailbox) — kept as power of 2
        static constexpr size_t MailboxesPerSegment = 32768;  // 2^15
        static constexpr int SegmentShift = 15;
        static constexpr ui32 OffsetMask = 0x7FFF;

        // Compatibility constants for TBucketMap (same role as TMailboxTable::*)
        static constexpr size_t LinesCount = MaxSegments;
        static constexpr size_t MailboxesPerLine = MailboxesPerSegment;
        static constexpr int LineIndexShift = SegmentShift;
        static constexpr ui32 LineIndexMask = 0x1FFFF;  // 17 bits (32 - SegmentShift)
        static constexpr ui32 MailboxIndexMask = OffsetMask;

        TWsMailboxTable();
        ~TWsMailboxTable();

        TWsMailboxTable(const TWsMailboxTable&) = delete;
        TWsMailboxTable& operator=(const TWsMailboxTable&) = delete;

        // Hot path: lookup mailbox by hint (one indexed load + offset)
        TMailbox* Get(ui32 hint) const {
            ui32 segIdx = hint >> SegmentShift;
            if (segIdx >= MaxSegments) [[unlikely]] {
                return nullptr;
            }
            auto* seg = Segments_[segIdx].load(std::memory_order_acquire);
            return seg ? &seg[hint & OffsetMask] : nullptr;
        }

        // Hot path: lookup stats by hint
        TMailboxExecStats* GetStats(ui32 hint) const {
            ui32 segIdx = hint >> SegmentShift;
            if (segIdx >= MaxSegments) [[unlikely]] {
                return nullptr;
            }
            auto* seg = StatSegments_[segIdx].load(std::memory_order_acquire);
            return seg ? &seg[hint & OffsetMask] : nullptr;
        }

        // Batch allocate hints from the lock-free shared pool.
        // Returns the number of hints actually allocated (may be less than maxCount
        // if max segments reached).
        size_t AllocateBatch(ui32* out, size_t maxCount);

        // Return hints to the lock-free shared pool.
        void FreeBatch(const ui32* hints, size_t count);

        // Shutdown: iterate all segments, cleanup all mailboxes.
        // Returns true when all mailboxes are clean.
        bool Cleanup();

    private:
        void AllocateAndPublishSegment(size_t segIdx);
        bool TryGrow();
        static void* MapSegment(size_t size);
        static void UnmapSegment(void* ptr, size_t size);

        std::atomic<TMailbox*> Segments_[MaxSegments];
        std::atomic<TMailboxExecStats*> StatSegments_[MaxSegments];

        alignas(64) TMPMCUnboundedQueue<4096, ui32> FreeHints_;
        std::atomic<size_t> NextSegmentIdx_{0};
    };

    // Per-slot hint allocator for WS runtime.
    //
    // LIFO stack of free hints as compact ui32 array (16KB, L1-resident).
    // No atomics on the fast path. Freed hints are immediately reusable
    // on the same slot → same mailbox cache line stays hot.
    //
    // Batch refill/return through TWsMailboxTable (lock-free, rare).
    class TWsSlotAllocator {
    public:
        static constexpr size_t MaxHints = 4096;
        static constexpr size_t RefillCount = 2048;

        TWsSlotAllocator() = default;

        void Init(TWsMailboxTable* table) {
            Table_ = table;
        }

        // Hot path: LIFO pop (no atomics, no mailbox memory touched)
        ui32 Allocate() {
            if (Count_ == 0) [[unlikely]] {
                Refill();
            }
            return Hints_[--Count_];
        }

        // Hot path: LIFO push (no atomics, no mailbox memory touched)
        void Free(ui32 hint) {
            if (Count_ >= MaxHints) [[unlikely]] {
                ReturnExcess();
            }
            Hints_[Count_++] = hint;
        }

        // Return all hints to the global pool (for shutdown)
        void Drain();

    private:
        void Refill();
        void ReturnExcess();

        TWsMailboxTable* Table_ = nullptr;
        ui32 Hints_[MaxHints] = {};
        size_t Count_ = 0;
    };

} // namespace NActors::NWorkStealing
