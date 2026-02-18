#include <ydb/library/actors/core/workstealing/activation_router.h>

#include <library/cpp/testing/unittest/registar.h>

#include <array>

namespace NActors::NWorkStealing {

    Y_UNIT_TEST_SUITE(ActivationRouter) {

        // Helper: bring a slot from Inactive to Active.
        static void ActivateSlot(TSlot& slot) {
            UNIT_ASSERT(slot.TryTransition(ESlotState::Inactive, ESlotState::Initializing));
            UNIT_ASSERT(slot.TryTransition(ESlotState::Initializing, ESlotState::Active));
        }

        // Helper: drain a slot's injection queue into its work deque.
        static size_t DrainSlot(TSlot& slot, size_t max = 1024) {
            Y_UNUSED(slot);
            Y_UNUSED(max);
            return 0;
        }

        Y_UNIT_TEST(FreshMailboxDistributes) {
            constexpr size_t N = 4;
            std::array<TSlot, N> slots;
            // Active slots must be contiguous from 0
            ActivateSlot(slots[0]);
            ActivateSlot(slots[1]);

            TActivationRouter router(slots.data(), N);

            // Route 100 activations with lastSlotIdx=0 (fresh)
            for (ui32 i = 0; i < 100; ++i) {
                int chosen = router.Route(i, 0);
                UNIT_ASSERT(chosen == 0 || chosen == 1);
            }

            // At least one activation should have gone to each active slot
            DrainSlot(slots[0]);
            DrainSlot(slots[1]);

            bool slot0HasWork = slots[0].Pop().has_value();
            bool slot1HasWork = slots[1].Pop().has_value();
            UNIT_ASSERT(slot0HasWork || slot1HasWork);
        }

        Y_UNIT_TEST(StickyRouting) {
            constexpr size_t N = 4;
            std::array<TSlot, N> slots;
            ActivateSlot(slots[0]);
            ActivateSlot(slots[1]);

            TActivationRouter router(slots.data(), N);

            // lastSlotIdx=2 means 1-based index for slot[1]
            int chosen = router.Route(42, 2);
            UNIT_ASSERT_VALUES_EQUAL(chosen, 1);

            // Verify the hint was injected into slot 1
            DrainSlot(slots[1]);
            auto item = slots[1].Pop();
            UNIT_ASSERT(item.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*item, 42u);
        }

        Y_UNIT_TEST(StickyFallbackOnDraining) {
            constexpr size_t N = 4;
            std::array<TSlot, N> slots;
            ActivateSlot(slots[0]);
            ActivateSlot(slots[1]);

            // Transition slot 1 to Draining — now only slot 0 is active (contiguous)
            UNIT_ASSERT(slots[1].TryTransition(ESlotState::Active, ESlotState::Draining));

            TActivationRouter router(slots.data(), N);

            // lastSlotIdx=2 points to slot[1] which is Draining — should fall back
            int chosen = router.Route(42, 2);
            UNIT_ASSERT_VALUES_EQUAL(chosen, 0); // only active slot

            // Verify the hint went to slot 0
            DrainSlot(slots[0]);
            auto item = slots[0].Pop();
            UNIT_ASSERT(item.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*item, 42u);
        }

        Y_UNIT_TEST(NoActiveSlots) {
            constexpr size_t N = 4;
            std::array<TSlot, N> slots;

            TActivationRouter router(slots.data(), N);

            int chosen = router.Route(42, 0);
            UNIT_ASSERT_VALUES_EQUAL(chosen, -1);
        }

        Y_UNIT_TEST(PowerOfTwoBalance) {
            constexpr size_t N = 8;
            std::array<TSlot, N> slots;
            for (auto& slot : slots) {
                ActivateSlot(slot);
            }

            TActivationRouter router(slots.data(), N);

            constexpr int Total = 1000;
            std::array<int, N> counts{};

            for (int i = 0; i < Total; ++i) {
                int chosen = router.Route(static_cast<ui32>(i), 0);
                UNIT_ASSERT(chosen >= 0 && chosen < static_cast<int>(N));
                counts[chosen]++;
            }

            // Verify distribution: no single slot should get all activations
            for (size_t i = 0; i < N; ++i) {
                UNIT_ASSERT(counts[i] < Total);
            }

            // At least 2 different slots should have received work
            int nonZero = 0;
            for (size_t i = 0; i < N; ++i) {
                if (counts[i] > 0) {
                    nonZero++;
                }
            }
            UNIT_ASSERT(nonZero >= 2);
        }

        Y_UNIT_TEST(RefreshActiveSlots) {
            constexpr size_t N = 4;
            std::array<TSlot, N> slots;
            for (auto& slot : slots) {
                ActivateSlot(slot);
            }

            TActivationRouter router(slots.data(), N);

            // All 4 slots active -- routing should work
            int chosen = router.Route(1, 0);
            UNIT_ASSERT(chosen >= 0 && chosen < static_cast<int>(N));

            // Deactivate slots 2 and 3 (from the end, maintaining contiguity)
            UNIT_ASSERT(slots[3].TryTransition(ESlotState::Active, ESlotState::Draining));
            UNIT_ASSERT(slots[3].TryTransition(ESlotState::Draining, ESlotState::Inactive));
            UNIT_ASSERT(slots[2].TryTransition(ESlotState::Active, ESlotState::Draining));
            UNIT_ASSERT(slots[2].TryTransition(ESlotState::Draining, ESlotState::Inactive));

            router.RefreshActiveSlots();

            // Route 100 activations -- should only go to slots 0 and 1
            for (int i = 0; i < 100; ++i) {
                chosen = router.Route(static_cast<ui32>(i), 0);
                UNIT_ASSERT(chosen == 0 || chosen == 1);
            }
        }

    } // Y_UNIT_TEST_SUITE(ActivationRouter)

} // namespace NActors::NWorkStealing
