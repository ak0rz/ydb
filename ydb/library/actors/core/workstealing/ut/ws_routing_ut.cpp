#include <ydb/library/actors/core/workstealing/ws_routing.h>

#include <library/cpp/testing/unittest/registar.h>

#include <memory>
#include <vector>

namespace NActors {

namespace {

    // Helper: transition slot to Active via the valid path.
    void MakeActive(TPoolSlot& slot) {
        UNIT_ASSERT(slot.TryTransition(ESlotState::Inactive, ESlotState::Initializing));
        UNIT_ASSERT(slot.TryTransition(ESlotState::Initializing, ESlotState::Active));
    }

} // anonymous namespace

Y_UNIT_TEST_SUITE(WsRouting) {

    // 1. Same-slot LIFO — lastSlotIdx matches mySlotIdx, LIFO empty →
    //    hint placed in LIFO, NodeConsumed=false.
    Y_UNIT_TEST(SameSlotLifo) {
        TPoolSlot mySlot;
        TPoolSlot* slots[] = {&mySlot};
        TInjectionNode node;
        ui64 prng = 12345;

        // lastSlotIdx=1 means slot 0 (1-based encoding).
        auto result = RouteActivation(42, 0, mySlot, slots, 1, 1, &node, prng);

        UNIT_ASSERT_VALUES_EQUAL(result.TargetSlotIdx, 0u);
        UNIT_ASSERT(!result.NeedsUnpark);
        UNIT_ASSERT(!result.NodeConsumed);
        UNIT_ASSERT_VALUES_EQUAL(mySlot.LifoSlot, 42u);
    }

    // 2. Same-slot deque — LIFO occupied → hint pushed to deque,
    //    NodeConsumed=false.
    Y_UNIT_TEST(SameSlotDeque) {
        TPoolSlot mySlot;
        mySlot.SetLifo(10); // occupy LIFO

        TPoolSlot* slots[] = {&mySlot};
        TInjectionNode node;
        ui64 prng = 12345;

        auto result = RouteActivation(42, 0, mySlot, slots, 1, 1, &node, prng);

        UNIT_ASSERT_VALUES_EQUAL(result.TargetSlotIdx, 0u);
        UNIT_ASSERT(!result.NeedsUnpark);
        UNIT_ASSERT(!result.NodeConsumed);

        // LIFO still holds 10, deque holds 42.
        UNIT_ASSERT_VALUES_EQUAL(mySlot.LifoSlot, 10u);
        UNIT_ASSERT_VALUES_EQUAL(mySlot.LocalDeque.GetSize(), 1);
        auto item = mySlot.LocalDeque.Pop();
        UNIT_ASSERT(item.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*item, 42u);
    }

    // 3. Same-slot deque full → random-2 — LIFO occupied + deque full
    //    (256 items) → NodeConsumed=true, injected to Active neighbor.
    Y_UNIT_TEST(SameSlotDequeFallsToRandom2) {
        TPoolSlot mySlot;
        TPoolSlot neighborSlot;
        MakeActive(neighborSlot);

        mySlot.SetLifo(10); // occupy LIFO
        for (ui32 i = 0; i < 256; ++i) {
            UNIT_ASSERT(mySlot.LocalDeque.Push(i + 1));
        }

        TPoolSlot* slots[] = {&mySlot, &neighborSlot};
        TInjectionNode node;
        ui64 prng = 12345;

        auto result = RouteActivation(42, 0, mySlot, slots, 2, 1, &node, prng);

        UNIT_ASSERT(result.NodeConsumed);
        UNIT_ASSERT_VALUES_EQUAL(result.TargetSlotIdx, 1u);
        UNIT_ASSERT_VALUES_EQUAL(node.Hint, 42u);
    }

    // 4. Cross-slot Active — target Active → NodeConsumed=true, hint
    //    in target's injection queue.
    Y_UNIT_TEST(CrossSlotActive) {
        TPoolSlot mySlot;
        TPoolSlot targetSlot;
        MakeActive(targetSlot);

        TPoolSlot* slots[] = {&mySlot, &targetSlot};
        TInjectionNode node;
        ui64 prng = 12345;

        // lastSlotIdx=2 means slot 1 (1-based encoding).
        auto result = RouteActivation(42, 0, mySlot, slots, 2, 2, &node, prng);

        UNIT_ASSERT_VALUES_EQUAL(result.TargetSlotIdx, 1u);
        UNIT_ASSERT(!result.NeedsUnpark);
        UNIT_ASSERT(result.NodeConsumed);
        UNIT_ASSERT_VALUES_EQUAL(node.Hint, 42u);

        // Verify hint arrived in target's injection queue.
        std::vector<ui32> drained;
        targetSlot.InjectionQueue.Drain(drained);
        UNIT_ASSERT_VALUES_EQUAL(drained.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(drained[0], 42u);
    }

    // 5. Cross-slot not Active → random-2 — target Inactive, redirect
    //    to an Active alternate slot.
    Y_UNIT_TEST(CrossSlotInactiveFallsToRandom2) {
        TPoolSlot mySlot;
        TPoolSlot targetSlot;     // stays Inactive
        TPoolSlot alternateSlot;
        MakeActive(alternateSlot);

        TPoolSlot* slots[] = {&mySlot, &targetSlot, &alternateSlot};
        TInjectionNode node;
        ui64 prng = 12345;

        // lastSlotIdx=2 means slot 1 (Inactive target).
        auto result = RouteActivation(42, 0, mySlot, slots, 3, 2, &node, prng);

        UNIT_ASSERT(result.NodeConsumed);
        UNIT_ASSERT_VALUES_EQUAL(result.TargetSlotIdx, 2u);
        UNIT_ASSERT_VALUES_EQUAL(node.Hint, 42u);
    }

    // 6. No affinity → random-2 — lastSlotIdx == 0 → random-2 injection.
    Y_UNIT_TEST(NoAffinityRandom2) {
        TPoolSlot mySlot;
        TPoolSlot targetSlot;
        MakeActive(targetSlot);

        TPoolSlot* slots[] = {&mySlot, &targetSlot};
        TInjectionNode node;
        ui64 prng = 12345;

        auto result = RouteActivation(42, 0, mySlot, slots, 2, 0, &node, prng);

        UNIT_ASSERT(result.NodeConsumed);
        UNIT_ASSERT_VALUES_EQUAL(result.TargetSlotIdx, 1u);
        UNIT_ASSERT_VALUES_EQUAL(node.Hint, 42u);

        // Verify hint in target's injection queue.
        std::vector<ui32> drained;
        targetSlot.InjectionQueue.Drain(drained);
        UNIT_ASSERT_VALUES_EQUAL(drained.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(drained[0], 42u);
    }

    // 7. Random-2 picks less loaded — two Active slots with different
    //    loads → less loaded chosen.
    Y_UNIT_TEST(Random2PicksLessLoaded) {
        TPoolSlot mySlot;
        TPoolSlot slotA, slotB;
        MakeActive(slotA);
        MakeActive(slotB);

        slotA.LoadEstimate.store(200, std::memory_order_relaxed);
        slotB.LoadEstimate.store(100, std::memory_order_relaxed);

        TPoolSlot* slots[] = {&mySlot, &slotA, &slotB};
        ui64 prng = 12345;

        // With exactly 2 eligible slots (excluding mySlot at idx 0),
        // PickRandom2Slot will find both and return the less loaded.
        ui32 result = PickRandom2Slot(slots, 3, 0, prng);
        UNIT_ASSERT_VALUES_EQUAL(result, 2u); // slotB (load 100)
    }

    // 8. NeedsUnpark — route to parked Active slot → NeedsUnpark=true.
    Y_UNIT_TEST(NeedsUnpark) {
        TPoolSlot mySlot;
        TPoolSlot targetSlot;
        MakeActive(targetSlot);
        targetSlot.Parked.store(true, std::memory_order_release);

        TPoolSlot* slots[] = {&mySlot, &targetSlot};
        TInjectionNode node;
        ui64 prng = 12345;

        // Cross-slot route: lastSlotIdx=2 → slot 1.
        auto result = RouteActivation(42, 0, mySlot, slots, 2, 2, &node, prng);

        UNIT_ASSERT_VALUES_EQUAL(result.TargetSlotIdx, 1u);
        UNIT_ASSERT(result.NeedsUnpark);
        UNIT_ASSERT(result.NodeConsumed);
    }

    // 9. RedistributeDeque — pop items from full deque into target
    //    injection queue via nodes.
    Y_UNIT_TEST(RedistributeDeque) {
        TPoolSlot mySlot;
        TPoolSlot targetSlot;
        MakeActive(targetSlot);

        // Fill deque.
        for (ui32 i = 0; i < 256; ++i) {
            UNIT_ASSERT(mySlot.LocalDeque.Push(i + 1));
        }

        TPoolSlot* slots[] = {&mySlot, &targetSlot};
        auto nodes = std::make_unique<TInjectionNode[]>(128);
        ui64 prng = 12345;

        auto result = NActors::RedistributeDeque(mySlot, 0, slots, 2, nodes.get(), 128, prng);

        UNIT_ASSERT_VALUES_EQUAL(result.Count, 128u);
        UNIT_ASSERT_VALUES_EQUAL(result.TargetSlotIdx, 1u);

        // Verify deque shrunk.
        UNIT_ASSERT_VALUES_EQUAL(mySlot.LocalDeque.GetSize(), 128);

        // Verify items in target's injection queue.
        std::vector<ui32> drained;
        targetSlot.InjectionQueue.Drain(drained);
        UNIT_ASSERT_VALUES_EQUAL(drained.size(), 128u);
    }

    // 10. PickRandom2Slot no eligible — no Active slots → returns
    //     slotCount sentinel.
    Y_UNIT_TEST(PickRandom2SlotNoEligible) {
        TPoolSlot mySlot;
        TPoolSlot inactiveSlot; // stays Inactive

        TPoolSlot* slots[] = {&mySlot, &inactiveSlot};
        ui64 prng = 12345;

        ui32 result = PickRandom2Slot(slots, 2, 0, prng);
        UNIT_ASSERT_VALUES_EQUAL(result, 2u); // slotCount sentinel
    }

} // Y_UNIT_TEST_SUITE

} // namespace NActors
