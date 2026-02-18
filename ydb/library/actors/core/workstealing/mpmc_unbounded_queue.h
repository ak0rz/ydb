#pragma once

#include "debra.h"

#include <util/system/defaults.h>
#include <util/system/yassert.h>

#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <optional>
#include <type_traits>

namespace NActors::NWorkStealing {

// Traits for packing a value and a ready flag into a single atomic cell.
// Defines TAtom — the storage type for std::atomic<TAtom>.
// Linear segments write each cell at most once, so only a READY flag is needed.
//
// Integer types: doubled width (ui8→ui16, ui16→ui32, ui32→ui64),
//   value in low bits, READY at MSB. ui64 stays ui64 with runtime check.
// Pointer types: TAtom = uintptr_t, READY at bit 0 (requires alignment >= 2).
template <typename T, typename = void>
struct TPackedCellTraits;

// Integer types: widen to next power-of-two size to make room for the ready bit.
template <typename T>
struct TPackedCellTraits<T, std::enable_if_t<std::is_integral_v<T>>> {
    using TAtom = std::conditional_t<(sizeof(T) <= 1), ui16,
                  std::conditional_t<(sizeof(T) <= 2), ui32, ui64>>;

    static constexpr TAtom kReadyBit = TAtom(1) << (sizeof(TAtom) * 8 - 1);

    static inline TAtom Pack(T value) {
        TAtom raw = static_cast<TAtom>(value);
        if constexpr (sizeof(T) == sizeof(TAtom)) {
            Y_ABORT_UNLESS((raw & kReadyBit) == 0, "value collides with ready bit");
        }
        return raw | kReadyBit;
    }

    static inline T Unpack(TAtom packed) {
        if constexpr (sizeof(T) < sizeof(TAtom)) {
            return static_cast<T>(packed);
        } else {
            return static_cast<T>(packed & ~kReadyBit);
        }
    }

    static inline bool IsReady(TAtom cell) {
        return cell & kReadyBit;
    }
};

// Pointer types — tag bit 0 (lowest bit).
// Safe for any pointer with natural alignment >= 2.
template <typename T>
struct TPackedCellTraits<T*, void> {
    static_assert(alignof(T) >= 2, "pointer tagging uses bit 0, requires alignof(T) >= 2");

    using TAtom = uintptr_t;

    static constexpr uintptr_t kReadyBit = 1;

    static inline uintptr_t Pack(T* value) {
        return reinterpret_cast<uintptr_t>(value) | kReadyBit;
    }

    static inline T* Unpack(uintptr_t packed) {
        return reinterpret_cast<T*>(packed & ~kReadyBit);
    }

    static inline bool IsReady(uintptr_t cell) {
        return cell & kReadyBit;
    }
};

// Unbounded MPMC queue: linked list of linear segments.
// Each cell packs value + ready flag into a single atomic word.
// Producers: fetch_add on Tail_ (zero inter-producer contention),
// single atomic store to publish value.
// Consumers: CAS on Head_, single atomic load to read.
// Each segment is used once. Pre-allocates segments to keep
// allocation off the hot path.
//
// Exhausted segments are reclaimed via DEBRA epoch-based reclamation.
// Push/TryPop are wrapped in TDebraGuard (epoch announcement).
// On HeadPage_ advance, exhausted segments are retired into a limbo bag
// and freed once all threads have advanced past the retirement epoch.
template <size_t SegmentBytes = 4096, typename T = ui32>
struct alignas(64) TMPMCUnboundedQueue {
private:
    using Traits = TPackedCellTraits<T>;
    using TAtom = typename Traits::TAtom;

    // Linear MPMC segment: fetch_add for producers (zero contention),
    // CAS for consumers. Each segment is used once (not circular).
    // Chain segments via Next_ for unbounded behavior.
    struct alignas(4096) TSegment {
        static_assert(SegmentBytes % 4096 == 0, "SegmentBytes must be a multiple of page size");

        static constexpr size_t kCacheLineSize = 64;
        static constexpr size_t kCellsPerLine = kCacheLineSize / sizeof(std::atomic<TAtom>);

        // Tail_ + Next_ share a cache line (producer-side).
        // Head_ on its own cache line (consumer-side).
        static constexpr size_t kHeaderBytes = kCacheLineSize
            + sizeof(std::atomic<size_t>);
        static constexpr size_t kRawCapacity = (SegmentBytes - kHeaderBytes) / sizeof(std::atomic<TAtom>);
        static constexpr size_t kNumLines = kRawCapacity / kCellsPerLine;
        // Truncate to a clean multiple so the transpose mapping covers every cell.
        static constexpr size_t Capacity = kNumLines * kCellsPerLine;
        static_assert(Capacity > 0, "SegmentBytes too small for even one cell");

        // Spatial indexing: consecutive logical positions land on different
        // cache lines, eliminating false sharing between adjacent producers
        // and between producers/consumers when Head_ ≈ Tail_.
        // Treats the cell array as a (kNumLines × kCellsPerLine) matrix
        // stored row-major, accessed column-major.
        static size_t CellIndex(size_t pos) {
            return (pos % kCellsPerLine) * kNumLines + (pos / kCellsPerLine);
        }

        alignas(kCacheLineSize) std::atomic<size_t> Tail_{0};   // producers: fetch_add
        std::atomic<TSegment*> Next_{nullptr};                   // producers: AdvanceTail
        alignas(kCacheLineSize) std::atomic<size_t> Head_{0};   // consumers: CAS
        std::atomic<TAtom> Cells_[Capacity];

        static inline std::atomic<size_t> AllocCount_{0};
        static inline std::atomic<size_t> FreeCount_{0};

        TSegment() {
            for (size_t i = 0; i < Capacity; ++i) {
                Cells_[i].store(0, std::memory_order_relaxed);
            }
        }

        // Producers: lock-free via fetch_add, no contention between producers.
        // Single atomic store publishes both value and ready flag.
        // Returns false when segment is exhausted (pos >= Capacity).
        bool TryPush(T value) {
            size_t pos = Tail_.fetch_add(1, std::memory_order_relaxed);
            if (pos >= Capacity) {
                return false;
            }
            Cells_[CellIndex(pos)].store(Traits::Pack(value), std::memory_order_release);
            return true;
        }

        // Consumers: CAS-based. Single atomic load reads both ready flag and value.
        // Returns nullopt if empty or next cell not yet written.
        std::optional<T> TryPop() {
            size_t pos = Head_.load(std::memory_order_relaxed);
            for (;;) {
                if (pos >= Capacity) {
                    return std::nullopt;
                }
                TAtom cell = Cells_[CellIndex(pos)].load(std::memory_order_acquire);
                if (!Traits::IsReady(cell)) {
                    return std::nullopt; // not filled yet
                }
                if (Head_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    return Traits::Unpack(cell);
                }
            }
        }

        // True when all Capacity cells have been consumed.
        bool IsExhausted() const {
            return Head_.load(std::memory_order_acquire) >= Capacity;
        }

        static TSegment* Allocate() {
            void* mem = std::aligned_alloc(4096, sizeof(TSegment));
            Y_ABORT_UNLESS(mem, "aligned_alloc failed");
            AllocCount_.fetch_add(1, std::memory_order_relaxed);
            return new (mem) TSegment();
        }

        static void Free(TSegment* seg) {
            seg->~TSegment();
            std::free(seg);
            FreeCount_.fetch_add(1, std::memory_order_relaxed);
        }

        // DEBRA callback for deferred reclamation.
        static void FreeSegment(void* ptr) {
            Free(static_cast<TSegment*>(ptr));
        }
    };

public:
    std::atomic<TSegment*> HeadPage_;
    std::atomic<TSegment*> TailPage_;

    TMPMCUnboundedQueue() {
        TSegment* first = TSegment::Allocate();
        // Pre-allocate one lookahead segment so AdvanceTail
        // can advance without blocking on allocation.
        TSegment* ahead = TSegment::Allocate();
        first->Next_.store(ahead, std::memory_order_relaxed);
        HeadPage_.store(first, std::memory_order_relaxed);
        TailPage_.store(first, std::memory_order_relaxed);
    }

    ~TMPMCUnboundedQueue() {
        // Walk from HeadPage_ (DEBRA reclaims exhausted segments behind it).
        TSegment* seg = HeadPage_.load(std::memory_order_relaxed);
        while (seg) {
            TSegment* next = seg->Next_.load(std::memory_order_relaxed);
            TSegment::Free(seg);
            seg = next;
        }
        // Debra_ destructor frees remaining limbo segments.
    }

    TMPMCUnboundedQueue(const TMPMCUnboundedQueue&) = delete;
    TMPMCUnboundedQueue& operator=(const TMPMCUnboundedQueue&) = delete;

    void Push(T value) {
        TDebraGuard guard(Debra_);
        for (;;) {
            TSegment* tail = TailPage_.load(std::memory_order_acquire);
            if (tail->TryPush(value)) {
                return;
            }
            AdvanceTail(tail);
        }
    }

    std::optional<T> TryPop() {
        TDebraGuard guard(Debra_);
        for (;;) {
            TSegment* head = HeadPage_.load(std::memory_order_acquire);

            if (auto val = head->TryPop()) {
                return val;
            }

            if (!head->IsExhausted()) {
                return std::nullopt;
            }

            TSegment* next = head->Next_.load(std::memory_order_acquire);
            if (!next) {
                return std::nullopt;
            }

            // Try to advance HeadPage_ to the next segment.
            // Winner retires the exhausted segment for deferred reclamation.
            if (HeadPage_.compare_exchange_weak(head, next,
                    std::memory_order_release, std::memory_order_relaxed)) {
                Debra_.Retire(head, &TSegment::FreeSegment);
                Debra_.TryReclaim();
            }
        }
    }

    static constexpr size_t SegmentCapacity() {
        return TSegment::Capacity;
    }

    static size_t SegmentAllocCount() {
        return TSegment::AllocCount_.load(std::memory_order_relaxed);
    }

    static size_t SegmentFreeCount() {
        return TSegment::FreeCount_.load(std::memory_order_relaxed);
    }

    static void ResetSegmentCounters() {
        TSegment::AllocCount_.store(0, std::memory_order_relaxed);
        TSegment::FreeCount_.store(0, std::memory_order_relaxed);
    }

private:
    TDebra Debra_;

    void AdvanceTail(TSegment* tail) {
        TSegment* next = tail->Next_.load(std::memory_order_acquire);
        if (!next) {
            // Lookahead wasn't ready — allocate inline (rare path).
            TSegment* fresh = TSegment::Allocate();
            TSegment* expected = nullptr;
            if (!tail->Next_.compare_exchange_strong(expected, fresh, std::memory_order_release, std::memory_order_relaxed)) {
                TSegment::Free(fresh);
                next = expected;
            } else {
                next = fresh;
            }
        }
        // Advance tail pointer — unblocks producers immediately.
        if (TailPage_.compare_exchange_weak(tail, next, std::memory_order_release, std::memory_order_relaxed)) {
            // Won the advance — pre-allocate lookahead for the next round.
            TSegment* ahead = TSegment::Allocate();
            TSegment* expected = nullptr;
            if (!next->Next_.compare_exchange_strong(expected, ahead, std::memory_order_release, std::memory_order_relaxed)) {
                TSegment::Free(ahead);
            }
        }
    }
};

} // namespace NActors::NWorkStealing
