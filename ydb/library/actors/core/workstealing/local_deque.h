#pragma once

// Chase-Lev work-stealing deque with corrected C11 atomics (Le et al. 2013).
//
// Single-producer (owner) / multiple-consumer (stealers) bounded deque.
// The owner calls Push and Pop from one end (bottom); any thread may call
// Steal from the other end (top). This gives the owner LIFO access (good
// for cache locality) while stealers get FIFO access (good for fairness).
//
// Based on:
//   - Chase & Lev, "Dynamic Circular Work-Stealing Deque", SPAA 2005
//   - Le et al., "Correct and Efficient Work-Stealing for Weak Memory
//     Models", PPoPP 2013 (fixes C11 atomics for non-TSO architectures)
//
// Memory ordering summary (fence/operation labels referenced in comments):
//
//   Push (owner only):
//     relaxed buffer store
//     release fence (F1) — makes buffer write visible before bottom increment
//     relaxed bottom store
//
//   Pop (owner only):
//     relaxed bottom store (decrement)
//     seq_cst fence (F2) — prevents top load from being reordered before
//       bottom store; without this, both owner and stealer can read the
//       same item (double-dispatch)
//     relaxed top load
//     [if single element: seq_cst CAS on top to race with stealers]
//
//   Steal (any thread):
//     acquire top load (L1) — syncs with push's release fence to see buffer
//     seq_cst fence (F3) — orders top load before bottom load; without this,
//       stealer may see new bottom but stale top and compute negative size
//     acquire bottom load (L2)
//     relaxed speculative buffer read
//     seq_cst CAS on top (C1) — mutual exclusion with other stealers and
//       owner's Pop CAS
//
// On x86 (TSO): relaxed ops compile to plain MOV. The only real cost is one
// MFENCE per Pop (F2) and one LOCK CMPXCHG per Steal (C1). Push is free.
// On aarch64: all fences and acquire/release loads compile to real barriers
// (DMB ISH, LDAR, STLR). Every ordering distinction matters on ARM.

#include <library/cpp/threading/chunk_queue/queue.h>

#include <util/system/defaults.h>

#include <atomic>
#include <cstring>
#include <optional>

namespace NActors {

    // Bounded single-producer multiple-consumer deque.
    // Size must be a positive power of 2 (enables & (Size-1) index masking).
    template <i64 Size = 256>
    class TBoundedSPMCDeque {
        static_assert(Size > 0 && (Size & (Size - 1)) == 0,
                      "Size must be a positive power of 2");

    public:
        TBoundedSPMCDeque() {
            // Zero-init buffer to avoid TSAN/MSAN false positives on speculative reads.
            std::memset(Buffer_, 0, sizeof(Buffer_));
        }

        TBoundedSPMCDeque(const TBoundedSPMCDeque&) = delete;
        TBoundedSPMCDeque& operator=(const TBoundedSPMCDeque&) = delete;
        TBoundedSPMCDeque(TBoundedSPMCDeque&&) = delete;
        TBoundedSPMCDeque& operator=(TBoundedSPMCDeque&&) = delete;

        // Owner-only. Pushes a hint to the bottom of the deque.
        // Returns false if the deque is full (caller should handle back-pressure).
        bool Push(ui32 hint) {
            i64 b = Bottom_.load(std::memory_order_relaxed);
            i64 t = Top_.load(std::memory_order_relaxed);

            // Check fullness BEFORE writing the buffer slot.
            // This is critical: if we wrote first and a stealer read that slot
            // before we detected fullness and rolled back, we'd have a data race.
            if (b - t >= Size) {
                return false;
            }

            // Store item into the ring buffer. Relaxed is fine — the release
            // fence below (F1) will make this visible before we publish the
            // new bottom.
            Buffer_[b & (Size - 1)].store(hint, std::memory_order_relaxed);

            // (F1) Release fence: ensures the buffer write above is visible to
            // any thread that subsequently observes the incremented bottom.
            // A release store on bottom would also be correct, but the
            // fence+relaxed pattern makes the ordering intent explicit.
            std::atomic_thread_fence(std::memory_order_release);

            // Publish: stealers will see the new bottom and know a new item
            // is available.
            Bottom_.store(b + 1, std::memory_order_relaxed);
            return true;
        }

        // Owner-only. Pops an item from the bottom of the deque (LIFO).
        // Returns nullopt if the deque is empty.
        std::optional<ui32> Pop() {
            // Optimistic: decrement bottom first, then check if we raced
            // with a stealer.
            i64 b = Bottom_.load(std::memory_order_relaxed) - 1;
            Bottom_.store(b, std::memory_order_relaxed);

            // (F2) seq_cst fence: critical ordering between the bottom store
            // above and the top load below. Without this, on aarch64 (and
            // even on x86 via the store buffer) the top load could be
            // reordered before the bottom store. A stealer could then see
            // the old (higher) bottom, steal the item at index b, and the
            // owner would also read the same item — double-dispatch.
            std::atomic_thread_fence(std::memory_order_seq_cst);

            i64 t = Top_.load(std::memory_order_relaxed);

            if (t < b) {
                // More than one element remaining — no contention with stealers
                // for this particular slot. Relaxed load is safe because we own
                // the bottom region.
                return Buffer_[b & (Size - 1)].load(std::memory_order_relaxed);
            }

            if (t == b) {
                // Exactly one element. A stealer may be trying to take it
                // at the same time via CAS on top. Race them.
                ui32 item = Buffer_[b & (Size - 1)].load(std::memory_order_relaxed);

                // compare_exchange_strong (not _weak): we are NOT in a retry
                // loop, so a spurious failure would incorrectly report empty.
                if (Top_.compare_exchange_strong(t, t + 1,
                        std::memory_order_seq_cst,
                        std::memory_order_relaxed)) {
                    // We won the race. Restore bottom to maintain the invariant
                    // bottom >= top.
                    Bottom_.store(b + 1, std::memory_order_relaxed);
                    return item;
                }

                // Lost the race to a stealer. The item is gone.
                Bottom_.store(b + 1, std::memory_order_relaxed);
                return std::nullopt;
            }

            // t > b: deque was already empty. Restore bottom.
            Bottom_.store(b + 1, std::memory_order_relaxed);
            return std::nullopt;
        }

        // Any thread. Steals an item from the top of the deque (FIFO from
        // stealer's perspective). Returns nullopt if empty or lost the CAS race.
        std::optional<ui32> Steal() {
            // (L1) Acquire load on top: ensures we see buffer writes that were
            // published by the owner's push (via the release fence F1).
            i64 t = Top_.load(std::memory_order_acquire);

            // (F3) seq_cst fence: orders top load (L1) before bottom load (L2).
            // Without this, on aarch64 the bottom load could be reordered before
            // the top load. A stealer might see a new bottom (owner pushed) but
            // a stale top (another stealer already advanced it), computing a
            // negative size and reading garbage.
            std::atomic_thread_fence(std::memory_order_seq_cst);

            // (L2) Acquire load on bottom: ensures we see the latest published
            // bottom from the owner.
            i64 b = Bottom_.load(std::memory_order_acquire);

            if (t >= b) {
                // Empty.
                return std::nullopt;
            }

            // Speculative read: we read the item before the CAS. If the CAS
            // fails (another stealer got it first), this value is discarded.
            // The acquire on L1 ensures we see the buffer write from push's
            // release fence F1.
            ui32 item = Buffer_[t & (Size - 1)].load(std::memory_order_relaxed);

            // (C1) seq_cst CAS: atomically claim the slot by advancing top.
            // seq_cst provides mutual exclusion with:
            //   (a) other stealers racing on the same top slot
            //   (b) the owner's CAS in Pop when the deque has exactly one element
            if (!Top_.compare_exchange_strong(t, t + 1,
                    std::memory_order_seq_cst,
                    std::memory_order_relaxed)) {
                // Lost the race. Another stealer (or the owner) took this slot.
                return std::nullopt;
            }

            return item;
        }

        // Any thread. Steals approximately half the items from this deque and
        // pushes them into `target` (which must be owned by the calling thread).
        // Returns the number of items actually stolen.
        ui32 StealHalf(TBoundedSPMCDeque& target) {
            // Snapshot the approximate size. Individual Steal() calls handle
            // races, so an imprecise count is fine — we may steal fewer than
            // half if we lose CAS races.
            i64 size = GetSize();
            if (size <= 0) {
                return 0;
            }

            // Steal at least 1 item (even if size == 1), up to half.
            i64 toSteal = std::max<i64>(1, size / 2);

            ui32 stolen = 0;
            for (i64 i = 0; i < toSteal; ++i) {
                auto item = Steal();
                if (!item) {
                    break; // Deque drained or lost too many races.
                }
                // Push to target — target is owned by the calling thread,
                // so this is safe. If target is full, stop stealing.
                if (!target.Push(*item)) {
                    break;
                }
                ++stolen;
            }
            return stolen;
        }

        // Any thread. Returns approximate size (may be stale).
        // Named GetSize() rather than Size() to avoid shadowing the template
        // parameter.
        i64 GetSize() const {
            i64 b = Bottom_.load(std::memory_order_relaxed);
            i64 t = Top_.load(std::memory_order_relaxed);
            return b - t;
        }

    private:
        // Bottom_ and Top_ are on separate cache lines via TPadded to avoid
        // false sharing. Bottom_ is written by the owner; Top_ is CAS'd by
        // stealers. Keeping them on different cache lines means the owner's
        // push/pop (which update Bottom_) do not invalidate stealers' cache
        // lines (which access Top_), and vice versa.
        NThreading::TPadded<std::atomic<i64>> Bottom_{0};
        NThreading::TPadded<std::atomic<i64>> Top_{0};

        // Inline ring buffer. For Size=256 with ui32 elements, this is 1KB.
        // Indexed by (bottom or top) & (Size - 1).
        std::atomic<ui32> Buffer_[Size];
    };

} // namespace NActors
