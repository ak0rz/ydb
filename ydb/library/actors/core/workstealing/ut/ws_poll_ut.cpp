#include <ydb/library/actors/core/workstealing/ws_poll.h>

#include <library/cpp/testing/unittest/registar.h>

#include <memory>
#include <vector>

namespace NActors {

Y_UNIT_TEST_SUITE(WsPoll) {

    // Helper: create a default config for tests.
    TWsConfig DefaultConfig() {
        TWsConfig config;
        config.StarvationGuardLimit = 3;
        return config;
    }

    // 1. LIFO dispatch — SetLifo(42) → PollNextHint → returns 42.
    Y_UNIT_TEST(LifoDispatch) {
        TPoolSlot slot;
        TWsConfig config = DefaultConfig();

        slot.SetLifo(42);
        UNIT_ASSERT_VALUES_EQUAL(PollNextHint(slot, config), 42u);

        // After taking, poll should return 0 (idle).
        UNIT_ASSERT_VALUES_EQUAL(PollNextHint(slot, config), 0u);
    }

    // 2. Deque dispatch — Push to deque → PollNextHint → returns hint.
    Y_UNIT_TEST(DequeDispatch) {
        TPoolSlot slot;
        TWsConfig config = DefaultConfig();

        slot.LocalDeque.Push(7);
        UNIT_ASSERT_VALUES_EQUAL(PollNextHint(slot, config), 7u);

        UNIT_ASSERT_VALUES_EQUAL(PollNextHint(slot, config), 0u);
    }

    // 3. Injection drain — Push TInjectionNode to injection queue →
    //    PollNextHint → returns hint (verifies drain + deque transfer).
    Y_UNIT_TEST(InjectionDrain) {
        TPoolSlot slot;
        TWsConfig config = DefaultConfig();

        TInjectionNode node;
        node.Hint = 99;
        slot.InjectionQueue.Push(&node);

        UNIT_ASSERT_VALUES_EQUAL(PollNextHint(slot, config), 99u);

        UNIT_ASSERT_VALUES_EQUAL(PollNextHint(slot, config), 0u);
    }

    // 4. Priority order: LIFO before deque — hint in both LIFO and deque →
    //    first poll returns LIFO hint, second returns deque hint.
    Y_UNIT_TEST(PriorityLifoBeforeDeque) {
        TPoolSlot slot;
        TWsConfig config = DefaultConfig();

        slot.SetLifo(10);
        slot.LocalDeque.Push(20);

        UNIT_ASSERT_VALUES_EQUAL(PollNextHint(slot, config), 10u);
        UNIT_ASSERT_VALUES_EQUAL(PollNextHint(slot, config), 20u);
        UNIT_ASSERT_VALUES_EQUAL(PollNextHint(slot, config), 0u);
    }

    // 5. Priority order: deque before injection — hint in both deque and
    //    injection → first poll returns deque hint, second returns injection hint.
    Y_UNIT_TEST(PriorityDequeBeforeInjection) {
        TPoolSlot slot;
        TWsConfig config = DefaultConfig();

        slot.LocalDeque.Push(30);

        TInjectionNode node;
        node.Hint = 40;
        slot.InjectionQueue.Push(&node);

        UNIT_ASSERT_VALUES_EQUAL(PollNextHint(slot, config), 30u);
        UNIT_ASSERT_VALUES_EQUAL(PollNextHint(slot, config), 40u);
        UNIT_ASSERT_VALUES_EQUAL(PollNextHint(slot, config), 0u);
    }

    // 6. Starvation guard demotes LIFO — do StarvationGuardLimit LIFO
    //    dispatches, then set LIFO again → next poll returns from deque
    //    (LIFO was demoted).
    Y_UNIT_TEST(StarvationGuardDemotesLifo) {
        TPoolSlot slot;
        TWsConfig config = DefaultConfig();
        const ui32 limit = config.StarvationGuardLimit;

        // Dispatch `limit` LIFO hints — these should all succeed via LIFO.
        for (ui32 i = 0; i < limit; ++i) {
            slot.SetLifo(100 + i);
            ui32 hint = PollNextHint(slot, config);
            UNIT_ASSERT_VALUES_EQUAL_C(hint, 100 + i,
                "LIFO dispatch failed at i=" << i);
        }

        // Next LIFO should be demoted: starvation guard fires, LIFO hint
        // goes to the deque, and we pop from the deque instead.
        slot.SetLifo(200);
        slot.LocalDeque.Push(300);

        // The LIFO hint (200) gets demoted to the deque. The poll then
        // pops from the deque. Deque is LIFO (pop from bottom), so 200
        // was pushed last and will be popped first.
        ui32 first = PollNextHint(slot, config);
        ui32 second = PollNextHint(slot, config);

        // Both 200 and 300 should be returned (in either order since they're
        // both in the deque now).
        UNIT_ASSERT_C(
            (first == 200 && second == 300) || (first == 300 && second == 200),
            "expected {200, 300} in any order, got {" << first << ", " << second << "}");
    }

    // 7. Steal from neighbor — push items to neighbor's deque,
    //    set InStealTopo=true → StealNextHint returns hint,
    //    neighbor's deque shrinks.
    Y_UNIT_TEST(StealFromNeighbor) {
        TPoolSlot mySlot;
        TPoolSlot neighborSlot;

        // Push several items to the neighbor's deque.
        for (ui32 i = 1; i <= 10; ++i) {
            neighborSlot.LocalDeque.Push(i);
        }
        neighborSlot.InStealTopo.store(true, std::memory_order_release);

        TPoolSlot* slots[] = {&mySlot, &neighborSlot};
        TStealOrder order;
        order.Neighbors = {1}; // neighbor is at index 1

        ui32 hint = StealNextHint(mySlot, slots, order);
        UNIT_ASSERT_C(hint != 0, "StealNextHint returned 0, expected a stolen hint");

        // Neighbor's deque should have shrunk (StealHalf takes ~half).
        i64 neighborSize = neighborSlot.LocalDeque.GetSize();
        UNIT_ASSERT_C(neighborSize < 10,
            "neighbor deque should have shrunk, size=" << neighborSize);
    }

    // 8. Steal respects InStealTopo — neighbor with InStealTopo=false →
    //    StealNextHint returns 0.
    Y_UNIT_TEST(StealRespectsInStealTopo) {
        TPoolSlot mySlot;
        TPoolSlot neighborSlot;

        for (ui32 i = 1; i <= 10; ++i) {
            neighborSlot.LocalDeque.Push(i);
        }
        // InStealTopo defaults to false.

        TPoolSlot* slots[] = {&mySlot, &neighborSlot};
        TStealOrder order;
        order.Neighbors = {1};

        ui32 hint = StealNextHint(mySlot, slots, order);
        UNIT_ASSERT_VALUES_EQUAL(hint, 0u);

        // Neighbor's deque should be untouched.
        UNIT_ASSERT_VALUES_EQUAL(neighborSlot.LocalDeque.GetSize(), 10);
    }

    // 9. Empty poll — all sources empty → PollNextHint returns 0.
    Y_UNIT_TEST(EmptyPollReturnsZero) {
        TPoolSlot slot;
        TWsConfig config = DefaultConfig();

        UNIT_ASSERT_VALUES_EQUAL(PollNextHint(slot, config), 0u);
    }

    // 10. DrainAtMost bounded — push > 256 items to injection queue →
    //     DrainAtMost(out, 256) returns 256, remaining items drainable
    //     on next call.
    Y_UNIT_TEST(DrainAtMostBounded) {
        TInjectionQueue queue;

        constexpr ui32 N = 300;
        auto nodes = std::make_unique<TInjectionNode[]>(N);
        for (ui32 i = 0; i < N; ++i) {
            nodes[i].Hint = i + 1;
            queue.Push(&nodes[i]);
        }

        // First drain: bounded to 256.
        std::vector<ui32> out1;
        ui32 count1 = queue.DrainAtMost(out1, 256);
        UNIT_ASSERT_VALUES_EQUAL(count1, 256u);
        UNIT_ASSERT_VALUES_EQUAL(out1.size(), 256u);

        // Verify FIFO order of first batch.
        for (ui32 i = 0; i < 256; ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(out1[i], i + 1,
                "FIFO order broken at index " << i);
        }

        // Second drain: remaining 44 items.
        std::vector<ui32> out2;
        ui32 count2 = queue.DrainAtMost(out2, 256);
        UNIT_ASSERT_VALUES_EQUAL(count2, N - 256);
        UNIT_ASSERT_VALUES_EQUAL(out2.size(), N - 256);

        for (ui32 i = 0; i < N - 256; ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(out2[i], 256 + i + 1,
                "FIFO order broken in second batch at index " << i);
        }

        // Queue should be empty now.
        UNIT_ASSERT(queue.IsEmpty());
    }

} // Y_UNIT_TEST_SUITE

} // namespace NActors
