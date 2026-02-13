#include <ydb/library/actors/core/workstealing/ws_slot.h>
#include <ydb/library/actors/core/workstealing/ws_config.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NActors {

Y_UNIT_TEST_SUITE(PoolSlot) {

    // Verifies the full valid state transition sequence:
    //   Inactive → Initializing → Active → Draining → Inactive
    // Each transition should succeed. A failure means the CAS logic or
    // initial state is wrong.
    Y_UNIT_TEST(StateTransitionValidSequence) {
        TPoolSlot slot;

        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(slot.State.load()), static_cast<ui32>(ESlotState::Inactive));

        UNIT_ASSERT(slot.TryTransition(ESlotState::Inactive, ESlotState::Initializing));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(slot.State.load()), static_cast<ui32>(ESlotState::Initializing));

        UNIT_ASSERT(slot.TryTransition(ESlotState::Initializing, ESlotState::Active));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(slot.State.load()), static_cast<ui32>(ESlotState::Active));

        UNIT_ASSERT(slot.TryTransition(ESlotState::Active, ESlotState::Draining));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(slot.State.load()), static_cast<ui32>(ESlotState::Draining));

        UNIT_ASSERT(slot.TryTransition(ESlotState::Draining, ESlotState::Inactive));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(slot.State.load()), static_cast<ui32>(ESlotState::Inactive));
    }

    // Verifies that an invalid transition (Active → Initializing) is rejected
    // and the state remains unchanged.
    Y_UNIT_TEST(StateTransitionInvalid) {
        TPoolSlot slot;

        // Move to Active first.
        UNIT_ASSERT(slot.TryTransition(ESlotState::Inactive, ESlotState::Initializing));
        UNIT_ASSERT(slot.TryTransition(ESlotState::Initializing, ESlotState::Active));

        // Active → Initializing is not a valid transition.
        UNIT_ASSERT(!slot.TryTransition(ESlotState::Initializing, ESlotState::Active));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(slot.State.load()), static_cast<ui32>(ESlotState::Active));
    }

    // Exhaustive check: only the 4 valid transitions succeed.
    // All other (from, to) pairs must fail.
    Y_UNIT_TEST(StateTransitionAllInvalidPairs) {
        const ESlotState states[] = {
            ESlotState::Inactive,
            ESlotState::Initializing,
            ESlotState::Active,
            ESlotState::Draining,
        };

        // Valid transitions: (from → to)
        auto isValid = [](ESlotState from, ESlotState to) -> bool {
            return (from == ESlotState::Inactive && to == ESlotState::Initializing)
                || (from == ESlotState::Initializing && to == ESlotState::Active)
                || (from == ESlotState::Active && to == ESlotState::Draining)
                || (from == ESlotState::Draining && to == ESlotState::Inactive);
        };

        for (ESlotState from : states) {
            for (ESlotState to : states) {
                if (from == to) {
                    continue; // identity transition — CAS succeeds trivially, not interesting
                }

                TPoolSlot slot;

                // Move the slot to the `from` state via the valid path.
                if (from == ESlotState::Initializing || from == ESlotState::Active || from == ESlotState::Draining) {
                    UNIT_ASSERT(slot.TryTransition(ESlotState::Inactive, ESlotState::Initializing));
                }
                if (from == ESlotState::Active || from == ESlotState::Draining) {
                    UNIT_ASSERT(slot.TryTransition(ESlotState::Initializing, ESlotState::Active));
                }
                if (from == ESlotState::Draining) {
                    UNIT_ASSERT(slot.TryTransition(ESlotState::Active, ESlotState::Draining));
                }

                UNIT_ASSERT_VALUES_EQUAL_C(
                    static_cast<ui32>(slot.State.load()), static_cast<ui32>(from),
                    "setup failed for from=" << static_cast<ui32>(from));

                // Attempt the transition.
                bool result = slot.TryTransition(from, to);

                if (isValid(from, to)) {
                    UNIT_ASSERT_C(result,
                        "valid transition failed: " << static_cast<ui32>(from) << " → " << static_cast<ui32>(to));
                    UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(slot.State.load()), static_cast<ui32>(to));
                } else {
                    UNIT_ASSERT_C(!result,
                        "invalid transition succeeded: " << static_cast<ui32>(from) << " → " << static_cast<ui32>(to));
                    UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(slot.State.load()), static_cast<ui32>(from));
                }
            }
        }
    }

    // Verifies SetLifo stores a value and TakeLifo returns it and clears.
    Y_UNIT_TEST(LifoSlotSetAndTake) {
        TPoolSlot slot;

        slot.SetLifo(42);
        UNIT_ASSERT_VALUES_EQUAL(slot.TakeLifo(), 42u);

        // After taking, slot should be empty.
        UNIT_ASSERT_VALUES_EQUAL(slot.TakeLifo(), 0u);
    }

    // Verifies TakeLifo from an empty slot returns 0.
    Y_UNIT_TEST(LifoSlotTakeFromEmpty) {
        TPoolSlot slot;
        UNIT_ASSERT_VALUES_EQUAL(slot.TakeLifo(), 0u);
    }

    // Verifies that the starvation guard fires after `limit` consecutive
    // LIFO dispatches. With limit=3: 3 calls return false (count
    // increments 0→1→2→3), then the 4th call sees count >= limit and fires.
    Y_UNIT_TEST(StarvationGuardFiresAtLimit) {
        TPoolSlot slot;
        constexpr ui32 limit = 3;

        // First `limit` checks should return false (count: 0→1, 1→2, 2→3).
        for (ui32 i = 0; i < limit; ++i) {
            UNIT_ASSERT_C(!slot.CheckStarvationGuard(limit),
                "guard fired too early at i=" << i);
        }

        // The (limit+1)-th check should return true (starvation detected).
        UNIT_ASSERT(slot.CheckStarvationGuard(limit));

        // After firing, the counter resets — need `limit` more false calls.
        for (ui32 i = 0; i < limit; ++i) {
            UNIT_ASSERT_C(!slot.CheckStarvationGuard(limit),
                "guard fired too early after reset at i=" << i);
        }
        UNIT_ASSERT(slot.CheckStarvationGuard(limit));
    }

    // Verifies that ResetStarvationGuard resets the counter so the guard
    // starts over from zero.
    Y_UNIT_TEST(StarvationGuardReset) {
        TPoolSlot slot;
        constexpr ui32 limit = 3;

        // Increment twice.
        UNIT_ASSERT(!slot.CheckStarvationGuard(limit));
        UNIT_ASSERT(!slot.CheckStarvationGuard(limit));

        // Reset.
        slot.ResetStarvationGuard();

        // Should need full `limit` checks before firing again.
        for (ui32 i = 0; i < limit; ++i) {
            UNIT_ASSERT_C(!slot.CheckStarvationGuard(limit),
                "guard fired too early after explicit reset at i=" << i);
        }
        UNIT_ASSERT(slot.CheckStarvationGuard(limit));
    }

    // Verifies that UpdateLoadEstimate snapshots the delta after a full
    // window has elapsed, and that LoadEstimate holds the correct value.
    Y_UNIT_TEST(LoadEstimateUpdateAfterWindow) {
        TPoolSlot slot;
        constexpr ui64 windowNs = 1'000'000; // 1ms

        // Simulate: accumulate 500ns of exec time, but window hasn't elapsed.
        slot.ExecTimeAccum = 500;
        slot.UpdateLoadEstimate(500'000, windowNs); // 0.5ms since epoch
        UNIT_ASSERT_VALUES_EQUAL(slot.LoadEstimate.load(), 0u);

        // Now the window has elapsed (1ms since start).
        slot.ExecTimeAccum = 800;
        slot.UpdateLoadEstimate(1'000'000, windowNs);
        // Delta should be ExecTimeAccum - PrevAccum = 800 - 0 = 800.
        UNIT_ASSERT_VALUES_EQUAL(slot.LoadEstimate.load(), 800u);
        UNIT_ASSERT_VALUES_EQUAL(slot.PrevAccum, 800u);
        UNIT_ASSERT_VALUES_EQUAL(slot.LastSnapshotTs, 1'000'000u);

        // Accumulate more, snapshot again after another window.
        slot.ExecTimeAccum = 1200;
        slot.UpdateLoadEstimate(2'000'000, windowNs);
        // Delta = 1200 - 800 = 400.
        UNIT_ASSERT_VALUES_EQUAL(slot.LoadEstimate.load(), 400u);
        UNIT_ASSERT_VALUES_EQUAL(slot.PrevAccum, 1200u);
    }

} // Y_UNIT_TEST_SUITE

Y_UNIT_TEST_SUITE(WsConfig) {

    // Verifies that TWsConfig defaults match the design doc.
    Y_UNIT_TEST(Defaults) {
        TWsConfig config;
        UNIT_ASSERT_VALUES_EQUAL(config.MaxSlots, 64u);
        UNIT_ASSERT_VALUES_EQUAL(config.DequeSize, 256u);
        UNIT_ASSERT_VALUES_EQUAL(config.StarvationGuardLimit, 3u);
        UNIT_ASSERT_VALUES_EQUAL(config.DrainBudget, 512u);
        UNIT_ASSERT_VALUES_EQUAL(config.SpinThreshold, 100u);
        UNIT_ASSERT_VALUES_EQUAL(config.LoadWindowNs, 1'000'000u);
        UNIT_ASSERT_VALUES_EQUAL(config.TimePerMailboxTs, 0u);
        UNIT_ASSERT_VALUES_EQUAL(config.EventsPerMailbox, 100u);
    }

} // Y_UNIT_TEST_SUITE

} // namespace NActors
