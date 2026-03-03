#include "ws_mailbox_table.h"

#include <util/system/yassert.h>

#include <cstring>
#include <limits>
#include <new>

#if defined(_linux_)
#include <sys/mman.h>
#endif

namespace NActors::NWorkStealing {

    void TMailboxExecStats::Reset() noexcept {
        EventsProcessed.store(0, std::memory_order_relaxed);
        TotalExecutionCycles.store(0, std::memory_order_relaxed);
        MaxExecutionCycles.store(0, std::memory_order_relaxed);
        ActivationCount.store(0, std::memory_order_relaxed);
        LastExecutionEndCycles.store(0, std::memory_order_relaxed);
        TotalIdleCycles.store(0, std::memory_order_relaxed);
        MaxIdleCycles.store(0, std::memory_order_relaxed);
        MinIdleCycles.store(std::numeric_limits<ui64>::max(), std::memory_order_relaxed);
    }

    namespace {
        static constexpr size_t SegmentBytes = TWsMailboxTable::MailboxesPerSegment * sizeof(TMailbox);
        static constexpr size_t StatSegmentBytes = TWsMailboxTable::MailboxesPerSegment * sizeof(TMailboxExecStats);

        static_assert(SegmentBytes == 2 * 1024 * 1024,
            "Mailbox segment must be exactly 2MB (one hugepage)");
    }

    TWsMailboxTable::TWsMailboxTable() {
        for (size_t i = 0; i < MaxSegments; ++i) {
            Segments_[i].store(nullptr, std::memory_order_relaxed);
            StatSegments_[i].store(nullptr, std::memory_order_relaxed);
        }
    }

    TWsMailboxTable::~TWsMailboxTable() {
        for (size_t i = 0; i < MaxSegments; ++i) {
            auto* seg = Segments_[i].load(std::memory_order_relaxed);
            if (seg) {
                for (size_t j = 0; j < MailboxesPerSegment; ++j) {
                    seg[j].~TMailbox();
                }
                UnmapSegment(seg, SegmentBytes);
            }
            auto* statSeg = StatSegments_[i].load(std::memory_order_relaxed);
            if (statSeg) {
                for (size_t j = 0; j < MailboxesPerSegment; ++j) {
                    statSeg[j].~TMailboxExecStats();
                }
                UnmapSegment(statSeg, StatSegmentBytes);
            }
        }
    }

    void* TWsMailboxTable::MapSegment(size_t size) {
#if defined(_linux_)
        // Try 2MB hugepage-backed mmap
        {
            int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | MAP_POPULATE;
            flags |= (21 << MAP_HUGE_SHIFT);  // 2^21 = 2MB
            void* ptr = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, flags, -1, 0);
            if (ptr != MAP_FAILED) {
                return ptr;
            }
        }
        // Fallback to regular mmap
        {
            void* ptr = ::mmap(nullptr, size, PROT_READ | PROT_WRITE,
                               MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
            if (ptr != MAP_FAILED) {
                return ptr;
            }
        }
        return nullptr;
#else
        void* ptr = ::operator new(size, std::align_val_t{4096}, std::nothrow);
        if (ptr) {
            std::memset(ptr, 0, size);
        }
        return ptr;
#endif
    }

    void TWsMailboxTable::UnmapSegment(void* ptr, size_t size) {
#if defined(_linux_)
        ::munmap(ptr, size);
#else
        (void)size;
        ::operator delete(ptr, std::align_val_t{4096});
#endif
    }

    void TWsMailboxTable::AllocateAndPublishSegment(size_t segIdx) {
        Y_ABORT_UNLESS(segIdx < MaxSegments, "TWsMailboxTable: segment index %zu out of range", segIdx);

        // Allocate mailbox segment (2MB)
        void* segMem = MapSegment(SegmentBytes);
        Y_ABORT_UNLESS(segMem, "TWsMailboxTable: failed to allocate mailbox segment");
        auto* seg = static_cast<TMailbox*>(segMem);

        // Placement-new each mailbox with correct hint and initialized sentinels
        for (size_t i = 0; i < MailboxesPerSegment; ++i) {
            auto* mbox = new (&seg[i]) TMailbox();
            mbox->Hint = static_cast<ui32>((segIdx << SegmentShift) | i);
            // Initialize Vyukov MPSC queue sentinels so every mailbox is always pushable
            mbox->Head.store(mbox->StubPtr(), std::memory_order_relaxed);
            mbox->Tail.store(reinterpret_cast<uintptr_t>(mbox->StubPtr()), std::memory_order_relaxed);
            mbox->Stub.store(0, std::memory_order_relaxed);
        }

        // Allocate stats segment (2MB)
        void* statMem = MapSegment(StatSegmentBytes);
        Y_ABORT_UNLESS(statMem, "TWsMailboxTable: failed to allocate stat segment");
        auto* statSeg = static_cast<TMailboxExecStats*>(statMem);

        for (size_t i = 0; i < MailboxesPerSegment; ++i) {
            new (&statSeg[i]) TMailboxExecStats();
        }

        // Publish stat segment first (readers may try GetStats before Get)
        StatSegments_[segIdx].store(statSeg, std::memory_order_release);
        Segments_[segIdx].store(seg, std::memory_order_release);

        // Push free hints into lock-free queue (skip hint 0 in segment 0)
        size_t startOffset = (segIdx == 0) ? 1 : 0;
        for (size_t i = startOffset; i < MailboxesPerSegment; ++i) {
            FreeHints_.Push(static_cast<ui32>((segIdx << SegmentShift) | i));
        }
    }

    bool TWsMailboxTable::TryGrow() {
        size_t expected = NextSegmentIdx_.load(std::memory_order_acquire);
        if (expected >= MaxSegments) {
            return false;
        }
        if (NextSegmentIdx_.compare_exchange_strong(
                expected, expected + 1, std::memory_order_acq_rel)) {
            // CAS winner: allocate segment and push all hints to queue
            AllocateAndPublishSegment(expected);
        }
        // CAS loser: another thread is allocating — hints will appear shortly
        return true;
    }

    size_t TWsMailboxTable::AllocateBatch(ui32* out, size_t maxCount) {
        size_t count = 0;
        while (count < maxCount) {
            auto hint = FreeHints_.TryPop();
            if (hint) {
                out[count++] = *hint;
                continue;
            }
            // Queue empty — try to grow by allocating a new segment
            if (!TryGrow()) {
                break;  // max segments reached
            }
            // Retry — segment was just allocated, hints being pushed
        }
        return count;
    }

    void TWsMailboxTable::FreeBatch(const ui32* hints, size_t count) {
        for (size_t i = 0; i < count; ++i) {
            FreeHints_.Push(hints[i]);
        }
    }

    bool TWsMailboxTable::Cleanup() {
        bool done = true;
        for (size_t i = 0; i < MaxSegments; ++i) {
            auto* seg = Segments_[i].load(std::memory_order_relaxed);
            if (seg) {
                for (size_t j = 0; j < MailboxesPerSegment; ++j) {
                    if (!seg[j].Cleanup()) {
                        done = false;
                    }
                }
            }
        }
        return done;
    }

    // --- TWsSlotAllocator ---

    void TWsSlotAllocator::Refill() {
        Y_DEBUG_ABORT_UNLESS(Table_);
        size_t got = Table_->AllocateBatch(Hints_ + Count_, RefillCount);
        Count_ += got;
        Y_ABORT_UNLESS(Count_ > 0, "TWsSlotAllocator::Refill: failed to allocate any hints");
    }

    void TWsSlotAllocator::ReturnExcess() {
        Y_DEBUG_ABORT_UNLESS(Table_);
        // Return the bottom half (oldest/coldest hints) to global pool.
        // Keep the top half (newest/hottest = most recently freed).
        size_t returnCount = Count_ / 2;
        Table_->FreeBatch(Hints_, returnCount);
        std::memmove(Hints_, Hints_ + returnCount, (Count_ - returnCount) * sizeof(ui32));
        Count_ -= returnCount;
    }

    void TWsSlotAllocator::Drain() {
        if (Table_ && Count_ > 0) {
            Table_->FreeBatch(Hints_, Count_);
            Count_ = 0;
        }
    }

} // namespace NActors::NWorkStealing
