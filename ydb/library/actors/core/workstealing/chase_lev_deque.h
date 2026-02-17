#pragma once

#include <atomic>
#include <array>
#include <optional>
#include <cstddef>

namespace NActors::NWorkStealing {

    // Bounded Chase-Lev SPMC deque.
    //
    // Single-owner, multiple-stealer concurrent deque for work-stealing schedulers.
    // The owner thread pushes and pops from the "bottom" (LIFO for owner).
    // Stealer threads steal from the "top" (FIFO batch steal).
    //
    // Capacity must be a power of 2. When full, Push returns false.
    //
    // Memory orderings follow the original Chase-Lev paper with Le et al. corrections:
    // - Bottom: relaxed loads/stores by owner, acquire load by stealers
    // - Top: seq_cst CAS by stealers (and owner in PopOwner when deque has 1 element)
    // - Buffer slots: relaxed store by owner (happens-before established via Bottom),
    //   acquire load by stealers (after successful CAS on Top)
    //
    // Thread safety:
    // - Push, PopOwner: ONLY called by the owner thread
    // - StealHalf, StealOne, SizeEstimate, Empty: called by any thread
    template <typename T, size_t Capacity>
    class TChaseLevDeque {
        static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of 2");
        static_assert(Capacity > 0);
        static constexpr size_t Mask = Capacity - 1;

        // Cache-line separated to avoid false sharing between owner and stealers
        alignas(64) std::atomic<size_t> Top_{0};
        alignas(64) std::atomic<size_t> Bottom_{0};
        alignas(64) std::array<std::atomic<T>, Capacity> Buffer_{};

    public:
        // Owner: push item to bottom. Returns false if deque is full.
        bool Push(T item) {
            // Relaxed: only owner writes Bottom_, so the owner's view is always current
            size_t b = Bottom_.load(std::memory_order_relaxed);
            // Acquire: need to see the latest Top_ updates from stealers' CAS
            size_t t = Top_.load(std::memory_order_acquire);

            if (b - t >= Capacity) {
                return false; // Full
            }

            // Relaxed: the subsequent release store on Bottom_ will publish this write
            Buffer_[b & Mask].store(item, std::memory_order_relaxed);
            // Release: ensures the Buffer_ store above is visible to any thread
            // that observes Bottom_ > b (i.e., stealers reading this slot)
            Bottom_.store(b + 1, std::memory_order_release);
            return true;
        }

        // Owner: pop item from bottom (LIFO). Returns std::nullopt if empty.
        std::optional<T> PopOwner() {
            // Relaxed: only owner writes Bottom_
            size_t b = Bottom_.load(std::memory_order_relaxed);
            if (b == 0) {
                return std::nullopt; // Never pushed anything
            }

            b = b - 1;
            // Seq_cst: establishes a total order with stealers' seq_cst CAS on Top_.
            // This prevents the race where both owner and stealer claim the last element.
            Bottom_.store(b, std::memory_order_seq_cst);

            // Relaxed: after the seq_cst store above, the total order guarantees
            // we see the latest Top_ value
            size_t t = Top_.load(std::memory_order_relaxed);

            if (t > b) {
                // Deque was already empty before our decrement; restore Bottom_
                Bottom_.store(t, std::memory_order_relaxed);
                return std::nullopt;
            }

            // Relaxed: this slot was written by the owner (us), so no cross-thread concern
            T item = Buffer_[b & Mask].load(std::memory_order_relaxed);

            if (t == b) {
                // Last element -- race with stealers. Use CAS to arbitrate.
                // Seq_cst: must be in the same total order as stealers' CAS
                if (Top_.compare_exchange_strong(t, t + 1, std::memory_order_seq_cst, std::memory_order_relaxed)) {
                    // We won the race
                    Bottom_.store(t + 1, std::memory_order_relaxed);
                    return item;
                } else {
                    // A stealer got it
                    Bottom_.store(t, std::memory_order_relaxed);
                    return std::nullopt;
                }
            }

            // t < b: more than one element, owner always wins (no contention on this slot)
            return item;
        }

        // Stealer: steal up to half the elements from the top.
        // Writes stolen items to output[0..N-1]. Returns N (count stolen).
        // maxOutput limits the number of items to steal.
        //
        // Uses per-item CAS to prevent overlap with the owner's PopOwner.
        // A single batch CAS(Top, t, t+half) is unsafe: between reading the
        // buffer and the CAS, the owner can pop items from the bottom into
        // the stealer's claimed range. Per-item CAS ensures each individual
        // steal follows the Chase-Lev single-steal protocol where the
        // seq_cst total order between PopOwner's Bottom_ store and the
        // stealer's Top_ CAS prevents duplicate claiming.
        size_t StealHalf(T* output, size_t maxOutput) {
            // Estimate how many items to steal
            size_t t = Top_.load(std::memory_order_acquire);
            size_t b = Bottom_.load(std::memory_order_acquire);

            if (t >= b) {
                return 0; // Empty
            }

            size_t size = b - t;
            size_t toSteal = size / 2;
            if (toSteal == 0) {
                toSteal = 1; // Steal at least one
            }
            if (toSteal > maxOutput) {
                toSteal = maxOutput;
            }

            // Steal items one at a time with individual CAS on Top_.
            // Each iteration is a correct single-steal per the Chase-Lev protocol.
            size_t stolen = 0;
            while (stolen < toSteal) {
                t = Top_.load(std::memory_order_acquire);
                b = Bottom_.load(std::memory_order_acquire);
                if (t >= b) {
                    break; // Deque drained
                }

                T item = Buffer_[t & Mask].load(std::memory_order_relaxed);

                if (Top_.compare_exchange_strong(t, t + 1,
                        std::memory_order_seq_cst, std::memory_order_relaxed)) {
                    output[stolen] = item;
                    ++stolen;
                }
                // CAS failure: another stealer advanced Top_; re-read and retry
            }
            return stolen;
        }

        // Stealer: steal exactly one item from the top.
        // Returns std::nullopt if empty or contended.
        std::optional<T> StealOne() {
            // Acquire: synchronizes with owner's release store on Bottom_
            size_t t = Top_.load(std::memory_order_acquire);
            // Acquire: need to see the owner's release store that published new items
            size_t b = Bottom_.load(std::memory_order_acquire);

            if (t >= b) {
                return std::nullopt; // Empty
            }

            // Relaxed: the acquire load of Top_ above establishes happens-before
            T item = Buffer_[t & Mask].load(std::memory_order_relaxed);

            // Seq_cst CAS: same reasoning as in StealHalf
            if (Top_.compare_exchange_strong(t, t + 1, std::memory_order_seq_cst, std::memory_order_relaxed)) {
                return item;
            }

            // Contended: another thread moved Top_
            return std::nullopt;
        }

        // Approximate size (non-atomic snapshot, may be stale).
        size_t SizeEstimate() const {
            // Relaxed: this is only an approximation, no ordering required
            size_t b = Bottom_.load(std::memory_order_relaxed);
            size_t t = Top_.load(std::memory_order_relaxed);
            return b >= t ? b - t : 0;
        }

        // Approximate emptiness check.
        bool Empty() const {
            return SizeEstimate() == 0;
        }
    };

} // namespace NActors::NWorkStealing
