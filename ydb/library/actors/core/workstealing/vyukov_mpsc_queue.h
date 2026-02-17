#pragma once

#include "chase_lev_deque.h"

#include <atomic>
#include <cstddef>
#include <optional>
#include <type_traits>

namespace NActors::NWorkStealing {

    // Intrusive node for the Vyukov MPSC queue.
    template <typename T>
    struct TMPSCNode {
        std::atomic<TMPSCNode*> Next{nullptr};
        T Value{};

        TMPSCNode() = default;
        explicit TMPSCNode(T value)
            : Value(std::move(value))
        {
        }
    };

    // Vyukov MPSC queue -- multiple producers, single consumer.
    //
    // Lock-free, wait-free Push (single XCHG + store).
    // Lock-free TryPop (traverse linked list from stub).
    //
    // Node reclaim pool (optional, enabled when PoolThreshold > 0):
    //   Uses a TChaseLevDeque to recycle nodes between consumer (owner=pusher)
    //   and producers (stealers). Consumer reclaims drained nodes by Push()ing
    //   them into the pool. Producers call StealOne() to get a hot node before
    //   falling back to allocation.
    //
    // Template parameters:
    //   T: value type (should be trivially copyable for best performance)
    //   PoolThreshold: if > 0, enables node pool with this capacity.
    //                  Set to 0 to disable node pooling.
    //
    // Memory model:
    //   Push:
    //     X1: Tail_.exchange(node, acq_rel) -- serializes producers, establishes
    //         happens-before between producer's Value store and consumer's load
    //     S1: prev->Next.store(node, release) -- links node into list
    //   TryPop:
    //     L1: Head_->Next.load(acquire) -- paired with S1
    //     Consumer sees Value written by producer due to X1 acq_rel -> L1 acquire chain
    //
    // Thread safety:
    //   Push: any thread (multiple producers)
    //   TryPop, DrainTo, NonEmpty: ONLY the single consumer thread
    template <typename T, size_t PoolThreshold = 0>
    class TVyukovMPSCQueue {
        // Permanent stub sentinel -- avoids special-casing empty queue
        TMPSCNode<T> Stub_;

        // Consumer reads from Head_ side, producers push to Tail_ side
        alignas(64) TMPSCNode<T>* Head_;              // consumer's read pointer (starts at &Stub_)
        alignas(64) std::atomic<TMPSCNode<T>*> Tail_; // producers XCHG here

        // Node reclaim pool -- only present when PoolThreshold > 0.
        // Consumer pushes reclaimed nodes (owner), producers steal nodes (stealers).
        struct TNodePool {
            TChaseLevDeque<TMPSCNode<T>*, PoolThreshold> Deque;

            // Producer: try to get a recycled node (stealer side)
            TMPSCNode<T>* TakeNode() {
                auto opt = Deque.StealOne();
                return opt.value_or(nullptr);
            }

            // Consumer: reclaim a used node (owner side)
            void ReclaimNode(TMPSCNode<T>* node) {
                node->Next.store(nullptr, std::memory_order_relaxed);
                // Push may fail if pool is full -- that's OK, just delete the node
                if (!Deque.Push(node)) {
                    delete node;
                }
            }

            // Destructor: drain and free all pooled nodes
            ~TNodePool() {
                while (auto opt = Deque.PopOwner()) {
                    delete *opt;
                }
            }
        };

        // Empty struct when pool is disabled (zero overhead)
        struct TNoPool {
            TMPSCNode<T>* TakeNode() {
                return nullptr;
            }
            void ReclaimNode(TMPSCNode<T>* node) {
                delete node;
            }
        };

        [[no_unique_address]]
        std::conditional_t<(PoolThreshold > 0), TNodePool, TNoPool> Pool_;

        TMPSCNode<T>* AllocNode(T value) {
            if constexpr (PoolThreshold > 0) {
                if (auto* node = Pool_.TakeNode()) {
                    node->Value = std::move(value);
                    // Relaxed: node will be published via release store in Push
                    node->Next.store(nullptr, std::memory_order_relaxed);
                    return node;
                }
            }
            return new TMPSCNode<T>(std::move(value));
        }

    public:
        TVyukovMPSCQueue()
            : Head_(&Stub_)
            , Tail_(&Stub_)
        {
        }

        ~TVyukovMPSCQueue() {
            // All producers must have completed their Push() calls before destruction.

            // Drain all remaining items (discard values), free all nodes except stub.
            // After this loop, Head_ points to the last consumed node (or &Stub_
            // if the queue was empty) and Head_->Next is nullptr.
            while (TryPop()) {
                // discard
            }

            // The current Head_ was never reclaimed by TryPop (it's the "live" head
            // awaiting reclamation on the next pop). Free it unless it's the stub.
            if (Head_ != &Stub_) {
                delete Head_;
            }

            // Pool destructor will free pooled nodes automatically (via ~TNodePool)
        }

        TVyukovMPSCQueue(const TVyukovMPSCQueue&) = delete;
        TVyukovMPSCQueue& operator=(const TVyukovMPSCQueue&) = delete;

        // Producer: enqueue value. Wait-free (single XCHG + store).
        void Push(T value) {
            TMPSCNode<T>* node = AllocNode(std::move(value));

            // Relaxed: node will be published via release store below
            node->Next.store(nullptr, std::memory_order_relaxed);

            // X1: acq_rel exchange serializes producers.
            // Release: publishes our Value store to whoever reads this node.
            // Acquire: we need to see the previous tail node's state for the
            // subsequent store to prev->Next.
            TMPSCNode<T>* prev = Tail_.exchange(node, std::memory_order_acq_rel);

            // S1: release store links previous tail to new node.
            // Paired with L1 (acquire load of Next) in TryPop.
            prev->Next.store(node, std::memory_order_release);
        }

        // Consumer: try to dequeue one item. Returns nullopt if empty.
        // Note: may return nullopt even if items were pushed, due to the
        // "incomplete push" window between X1 and S1 in Push().
        std::optional<T> TryPop() {
            TMPSCNode<T>* head = Head_;

            // L1: acquire load, paired with S1 (release store in Push)
            TMPSCNode<T>* next = head->Next.load(std::memory_order_acquire);

            if (next == nullptr) {
                return std::nullopt; // Empty or incomplete push
            }

            // Read value from next node (visible due to X1 acq_rel -> L1 acquire chain)
            T value = std::move(next->Value);

            // Advance head to next node
            Head_ = next;

            // Reclaim old head (but never the stub)
            if (head != &Stub_) {
                Pool_.ReclaimNode(head);
            }

            return value;
        }

        // Consumer: drain up to maxCount items into output buffer.
        // Returns count drained. More efficient than repeated TryPop.
        size_t DrainTo(T* output, size_t maxCount) {
            size_t count = 0;
            while (count < maxCount) {
                auto item = TryPop();
                if (!item) {
                    break;
                }
                output[count++] = std::move(*item);
            }
            return count;
        }

        // Consumer: approximate non-emptiness check.
        // May return false negatives during incomplete pushes.
        bool NonEmpty() const {
            return Head_->Next.load(std::memory_order_acquire) != nullptr;
        }
    };

} // namespace NActors::NWorkStealing
