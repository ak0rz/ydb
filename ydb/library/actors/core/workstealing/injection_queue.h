#pragma once

// Vyukov MPSC (multi-producer single-consumer) intrusive queue.
//
// Any thread may push nodes; a single consumer (the slot occupant) drains
// them in batch. Used as the cross-thread activation channel: senders push
// activation hints here, the occupant periodically drains into its local
// deque.
//
// Based on Dmitry Vyukov's non-intrusive MPSC queue, adapted to intrusive
// nodes (caller manages allocation) with a permanent embedded stub sentinel.
//
// Push is wait-free (single XCHG). Drain is lock-free (acquire chain walk).
//
// Push protocol:
//   1. node->Next.store(nullptr, relaxed)          — init before publishing
//   2. prev = Head_.exchange(node, acq_rel)        — (X1) swing head; get predecessor
//   3. prev->Next.store(node, release)             — (S1) link into chain
//
//   acq_rel on X1: release ensures node->Next=nullptr is visible before the
//   node is reachable through Head_. Acquire ensures we see prev's data so we
//   can safely write prev->Next. On x86 XCHG is inherently a full barrier
//   (LOCK XCHG), so acq_rel is free. On aarch64 it compiles to LDAXR/STLXR.
//
//   Release on S1: pairs with consumer's acquire load on Next (L3), ensuring
//   the consumer sees the full node data (the Hint field).
//
// Drain protocol:
//   1. next = Tail_->Next.load(acquire)            — (L3) see producer's S1
//   2. if next == nullptr: empty or incomplete push — return
//   3. Tail_ = next; output next->Hint; repeat from 1
//
//   The "incomplete-push window" (between X1 and S1) is inherent to this
//   design. During this window the consumer sees next==nullptr even though
//   Head_ != Tail_. The consumer returns empty and the in-progress push
//   becomes visible on the next drain cycle.
//
// Node lifecycle:
//   After drain advances Tail_ past a node, the OLD tail (either the stub
//   or a previously consumed node) is no longer referenced by the queue and
//   may be reused. The NEW tail must remain valid until the next drain
//   advances past it (it serves as the chain sentinel).

#include <util/system/defaults.h>

#include <atomic>

namespace NActors {

    struct TInjectionNode {
        ui32 Hint = 0;
        std::atomic<TInjectionNode*> Next{nullptr};
    };

    class TInjectionQueue {
    public:
        TInjectionQueue()
            : Head_(&Stub_)
            , Tail_(&Stub_)
        {}

        TInjectionQueue(const TInjectionQueue&) = delete;
        TInjectionQueue& operator=(const TInjectionQueue&) = delete;
        TInjectionQueue(TInjectionQueue&&) = delete;
        TInjectionQueue& operator=(TInjectionQueue&&) = delete;

        // Any thread, wait-free. Pushes a node into the queue.
        // The caller must have initialized node->Hint before calling Push.
        // The node must remain valid until it has been drained AND the
        // following drain has advanced past it (since drained nodes
        // temporarily serve as the tail sentinel).
        void Push(TInjectionNode* node) {
            // Initialize the link before publishing. Relaxed is sufficient
            // because the release half of X1 below will make this visible
            // before anyone can reach the node through Head_.
            node->Next.store(nullptr, std::memory_order_relaxed);

            // (X1) Atomically swing Head_ to our node and retrieve the
            // previous head. acq_rel: release publishes node->Next=nullptr;
            // acquire lets us see prev's state so we can safely write
            // prev->Next.
            TInjectionNode* prev = Head_.exchange(node, std::memory_order_acq_rel);

            // (S1) Complete the link. The consumer will load prev->Next with
            // acquire (L3), so this release ensures it sees node->Hint and
            // node->Next.
            prev->Next.store(node, std::memory_order_release);
        }

        // Consumer-only. Drains all currently reachable items, appending
        // their hints to `out` (any container with push_back).
        // Returns the number of items drained.
        //
        // Items pushed concurrently may or may not be visible to this drain.
        // In particular, a push that has completed X1 but not S1 will appear
        // as a nullptr Next — the drain stops and those items will be picked
        // up by the next drain call.
        template <typename TContainer>
        ui32 Drain(TContainer& out) {
            ui32 count = 0;
            for (;;) {
                // (L3) Acquire load on Next: pairs with producer's S1
                // (release store). Ensures we see the pushed node's Hint
                // and its own Next pointer.
                TInjectionNode* next = Tail_->Next.load(std::memory_order_acquire);
                if (!next) {
                    // Either truly empty (Head_ == Tail_) or a push is
                    // in progress (X1 done, S1 pending). In both cases
                    // there's nothing to drain right now.
                    break;
                }
                // Advance the tail. The old Tail_ (stub or previously
                // consumed node) is now free for the caller to reuse.
                Tail_ = next;
                out.push_back(next->Hint);
                ++count;
            }
            return count;
        }

        // Consumer-only. Approximate emptiness check.
        // May return a false negative during concurrent pushes (push in
        // progress between X1 and S1). Never returns a false positive
        // when called from the consumer thread with no concurrent drains.
        bool IsEmpty() const {
            // Check Next first (cheap, consumer-local cache line) before
            // loading Head_ (contended by pushers).
            TInjectionNode* tail = Tail_;
            return tail->Next.load(std::memory_order_acquire) == nullptr
                && Head_.load(std::memory_order_acquire) == tail;
        }

    private:
        // Stub sentinel. Must be declared before Head_ and Tail_ so it
        // is fully constructed when they take its address.
        TInjectionNode Stub_;

        // Head_ is hammered by every pusher (XCHG). Tail_ is written by
        // the consumer on every drain step. Separate cache lines to avoid
        // false sharing between producers and consumer.
        alignas(64) std::atomic<TInjectionNode*> Head_;
        alignas(64) TInjectionNode* Tail_;
    };

} // namespace NActors
