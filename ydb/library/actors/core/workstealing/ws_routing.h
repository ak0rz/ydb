#pragma once

// Activation routing for the work-stealing scheduler.
//
// Given an activation hint and a mailbox affinity (LastPoolSlotIdx),
// RouteActivation decides which slot should handle it:
//   - Same-slot: place in LIFO or local deque (no cross-thread cost)
//   - Cross-slot: inject into the target slot's injection queue
//   - Random-2: pick the less-loaded of 2 random Active slots
//
// RedistributeDeque batch-moves items from a full deque to a random-2
// chosen slot's injection queue.
//
// These are pure data-structure operations — no TLS, no mailbox
// resolution, no futex wakes. The caller (pool, Step 7) resolves
// lastSlotIdx from the mailbox and handles unparking.

#include <ydb/library/actors/core/workstealing/ws_slot.h>

#include <util/system/defaults.h>

namespace NActors {

    // xorshift64 PRNG for random-2 slot selection.
    // State must be non-zero on first call.
    inline ui64 WsRand(ui64& state) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        return state;
    }

    namespace NDetail {

        // Scan forward from startIdx to find the first eligible slot.
        // Eligible: not nullptr, Active state, not excludeIdx, not alsoExclude.
        // Returns slotCount sentinel if no eligible slot found.
        inline ui32 ScanForEligible(
            TPoolSlot** slots, ui32 slotCount,
            ui32 startIdx, ui32 excludeIdx,
            ui32 alsoExclude = Max<ui32>())
        {
            for (ui32 i = 0; i < slotCount; ++i) {
                ui32 idx = (startIdx + i) % slotCount;
                if (idx == excludeIdx || idx == alsoExclude) {
                    continue;
                }
                if (slots[idx] == nullptr) {
                    continue;
                }
                if (slots[idx]->State.load(std::memory_order_acquire) != ESlotState::Active) {
                    continue;
                }
                return idx;
            }
            return slotCount;
        }

    } // namespace NDetail

    // Pick the less-loaded of 2 random Active slots.
    // Returns slot index, or slotCount if no eligible slot found.
    inline ui32 PickRandom2Slot(
        TPoolSlot** slots, ui32 slotCount,
        ui32 excludeIdx, ui64& prngState)
    {
        if (slotCount == 0) {
            return 0;
        }

        ui32 start1 = static_cast<ui32>(WsRand(prngState) % slotCount);
        ui32 idx1 = NDetail::ScanForEligible(slots, slotCount, start1, excludeIdx);
        if (idx1 == slotCount) {
            return slotCount;
        }

        ui32 start2 = static_cast<ui32>(WsRand(prngState) % slotCount);
        ui32 idx2 = NDetail::ScanForEligible(slots, slotCount, start2, excludeIdx, idx1);
        if (idx2 == slotCount) {
            return idx1;
        }

        ui64 load1 = slots[idx1]->LoadEstimate.load(std::memory_order_relaxed);
        ui64 load2 = slots[idx2]->LoadEstimate.load(std::memory_order_relaxed);
        return (load1 <= load2) ? idx1 : idx2;
    }

    // Result of routing one activation.
    struct TRouteResult {
        ui32 TargetSlotIdx;    // which slot received the activation
        bool NeedsUnpark;      // target slot was parked — caller should wake
        bool NodeConsumed;     // the TInjectionNode* was used (cross-slot)
    };

    // Route a single activation hint to the appropriate slot.
    // `lastSlotIdx` is raw from mailbox->LastPoolSlotIdx (0 = no affinity,
    // N = slot N-1). `node` is consumed only for cross-slot injection.
    inline TRouteResult RouteActivation(
        ui32 hint, ui32 mySlotIdx, TPoolSlot& mySlot,
        TPoolSlot** slots, ui32 slotCount,
        ui16 lastSlotIdx, TInjectionNode* node,
        ui64& prngState)
    {
        // 1. Decode affinity.
        if (lastSlotIdx != 0) {
            ui32 targetIdx = static_cast<ui32>(lastSlotIdx) - 1;

            // 2. Same-slot fast path.
            if (targetIdx == mySlotIdx) {
                if (mySlot.LifoSlot == 0) {
                    mySlot.SetLifo(hint);
                    return {mySlotIdx, false, false};
                }
                if (mySlot.LocalDeque.Push(hint)) {
                    return {mySlotIdx, false, false};
                }
                // Deque full — fall through to random-2.
            }
            // 3. Cross-slot.
            else if (targetIdx < slotCount
                     && slots[targetIdx] != nullptr
                     && slots[targetIdx]->State.load(std::memory_order_acquire) == ESlotState::Active)
            {
                node->Hint = hint;
                slots[targetIdx]->InjectionQueue.Push(node);
                bool parked = slots[targetIdx]->Parked.load(std::memory_order_acquire);
                return {targetIdx, parked, true};
            }
            // Not Active or out of range — fall through to random-2.
        }

        // 4. Random-2.
        ui32 targetIdx = PickRandom2Slot(slots, slotCount, mySlotIdx, prngState);
        if (targetIdx == slotCount) {
            // No eligible slots — fallback to own slot.
            if (mySlot.LifoSlot == 0) {
                mySlot.SetLifo(hint);
                return {mySlotIdx, false, false};
            }
            if (mySlot.LocalDeque.Push(hint)) {
                return {mySlotIdx, false, false};
            }
            // Last resort: inject to own queue via node.
            node->Hint = hint;
            mySlot.InjectionQueue.Push(node);
            return {mySlotIdx, false, true};
        }

        node->Hint = hint;
        slots[targetIdx]->InjectionQueue.Push(node);
        bool parked = slots[targetIdx]->Parked.load(std::memory_order_acquire);
        return {targetIdx, parked, true};
    }

    // Result of batch redistribution.
    struct TRedistributeResult {
        ui32 Count;
        ui32 TargetSlotIdx;
        bool NeedsUnpark;
    };

    // Batch-redistribute items from a full deque to a random-2 chosen slot.
    // Pops up to `maxItems` from mySlot.LocalDeque, sets node hints, pushes
    // to target's injection queue. Returns count redistributed.
    inline TRedistributeResult RedistributeDeque(
        TPoolSlot& mySlot, ui32 mySlotIdx,
        TPoolSlot** slots, ui32 slotCount,
        TInjectionNode* nodes, ui32 maxItems,
        ui64& prngState)
    {
        ui32 targetIdx = PickRandom2Slot(slots, slotCount, mySlotIdx, prngState);
        if (targetIdx == slotCount) {
            return {0, slotCount, false};
        }

        ui32 count = 0;
        while (count < maxItems) {
            auto item = mySlot.LocalDeque.Pop();
            if (!item) {
                break;
            }
            nodes[count].Hint = *item;
            slots[targetIdx]->InjectionQueue.Push(&nodes[count]);
            ++count;
        }

        bool parked = slots[targetIdx]->Parked.load(std::memory_order_acquire);
        return {count, targetIdx, parked};
    }

} // namespace NActors
