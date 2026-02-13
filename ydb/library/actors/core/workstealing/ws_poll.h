#pragma once

// Poll and steal functions for the work-stealing scheduler.
//
// Two free functions that find the next activation hint for a slot:
//
//   PollNextHint  — owner-only, checks local sources in priority order:
//                   LIFO slot → local deque → injection queue drain → deque
//
//   StealNextHint — owner-only, steals work from neighbor slots' deques
//
// Both return a ui32 hint (0 = idle, nonzero = activation hint to resolve
// via TMailboxTable). These are pure data-structure functions with no TLS
// or actor system dependency — the thread driver (Step 8) wraps them with
// mailbox resolution and execution.

#include <ydb/library/actors/core/workstealing/ws_slot.h>
#include <ydb/library/actors/core/workstealing/ws_config.h>

#include <util/generic/vector.h>

namespace NActors {

    // Precomputed per-slot neighbor list for steal ordering.
    // Built from TTopologyMap, filtered to same-pool active slots.
    struct TStealOrder {
        TVector<ui32> Neighbors;  // slot indices in steal priority order
    };

    namespace NDetail {

        // Adapter that lets TInjectionQueue::DrainAtMost push directly into
        // the local deque. The deque is always empty when we reach the
        // injection drain step (Pop returned nullopt), and DrainAtMost is
        // bounded to 256 items (deque capacity), so Push cannot overflow.
        template <i64 Size>
        struct TDequeInserter {
            TBoundedSPMCDeque<Size>& Deque;

            void push_back(ui32 hint) {
                Deque.Push(hint);
            }
        };

    } // namespace NDetail

    // Owner-only. Returns the next activation hint from local sources:
    //   1. LIFO slot (with starvation guard)
    //   2. Local deque pop
    //   3. Injection queue drain → local deque → pop
    // Returns 0 if all sources empty.
    inline ui32 PollNextHint(TPoolSlot& slot, const TWsConfig& config) {
        // 1. LIFO slot — fast path
        ui32 lifo = slot.TakeLifo();
        if (lifo != 0) {
            if (!slot.CheckStarvationGuard(config.StarvationGuardLimit)) {
                // Guard says OK — return the LIFO hint directly.
                return lifo;
            }
            // Starvation detected — demote LIFO hint to the local deque
            // so it competes fairly with other activations.
            slot.LocalDeque.Push(lifo);
            // Fall through to deque pop.
        }

        // 2. Local deque pop
        auto item = slot.LocalDeque.Pop();
        if (item) {
            slot.ResetStarvationGuard();
            return *item;
        }

        // 3. Drain injection queue → local deque → pop
        NDetail::TDequeInserter<256> inserter{slot.LocalDeque};
        slot.InjectionQueue.DrainAtMost(inserter, 256);

        item = slot.LocalDeque.Pop();
        if (item) {
            slot.ResetStarvationGuard();
            return *item;
        }

        // 4. All sources empty.
        return 0;
    }

    // Owner-only. Tries to steal work from neighbor slots.
    // Iterates neighbors in topology order, skips slots with
    // InStealTopo=false. StealHalf from first eligible neighbor's
    // deque into own deque, pop one.
    // Returns stolen hint or 0.
    inline ui32 StealNextHint(TPoolSlot& slot, TPoolSlot** slots, const TStealOrder& order) {
        for (ui32 neighbor : order.Neighbors) {
            if (slots[neighbor] == nullptr) {
                continue;
            }
            if (!slots[neighbor]->InStealTopo.load(std::memory_order_acquire)) {
                continue;
            }
            ui32 stolen = slots[neighbor]->LocalDeque.StealHalf(slot.LocalDeque);
            if (stolen > 0) {
                auto item = slot.LocalDeque.Pop();
                if (item) {
                    slot.ResetStarvationGuard();
                    return *item;
                }
            }
        }
        return 0;
    }

} // namespace NActors
