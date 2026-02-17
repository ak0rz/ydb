#include <ydb/library/actors/core/workstealing/ws_poll.h>

#include <library/cpp/testing/unittest/registar.h>

#include <vector>

namespace NActors::NWorkStealing {

    namespace {

        // Helper: bring a slot from Inactive to Active.
        void ActivateSlot(TSlot& slot) {
            UNIT_ASSERT(slot.TryTransition(ESlotState::Inactive, ESlotState::Initializing));
            UNIT_ASSERT(slot.TryTransition(ESlotState::Initializing, ESlotState::Active));
        }

        // Test steal iterator that yields slots from a provided list.
        class TTestStealIterator: public IStealIterator {
        public:
            explicit TTestStealIterator(std::vector<TSlot*> slots)
                : Slots_(std::move(slots))
                , Pos_(0)
            {
            }

            TSlot* Next() override {
                if (Pos_ < Slots_.size()) {
                    return Slots_[Pos_++];
                }
                return nullptr;
            }

            void Reset() override {
                Pos_ = 0;
            }

        private:
            std::vector<TSlot*> Slots_;
            size_t Pos_;
        };

    } // anonymous namespace

    Y_UNIT_TEST_SUITE(WsPoll) {

        Y_UNIT_TEST(PrePopulatedSlotReturnsBusy) {
            TSlot slot;
            ActivateSlot(slot);

            UNIT_ASSERT(slot.Inject(42));

            ui32 received = 0;
            bool callbackCalled = false;
            TExecuteCallback cb = [&](ui32 hint) -> bool {
                callbackCalled = true;
                received = hint;
                return false;
            };

            TWsConfig config;
            TPollState ps;
            EPollResult result = PollSlot(slot, nullptr, cb, config, ps);

            UNIT_ASSERT_EQUAL(result, EPollResult::Busy);
            UNIT_ASSERT(callbackCalled);
            UNIT_ASSERT_VALUES_EQUAL(received, 42u);
        }

        Y_UNIT_TEST(EmptySlotReturnsIdle) {
            TSlot slot;
            ActivateSlot(slot);

            bool callbackCalled = false;
            TExecuteCallback cb = [&](ui32) -> bool {
                callbackCalled = true;
                return false;
            };

            TWsConfig config;
            TPollState ps;
            EPollResult result = PollSlot(slot, nullptr, cb, config, ps);

            UNIT_ASSERT_EQUAL(result, EPollResult::Idle);
            UNIT_ASSERT(!callbackCalled);
        }

        Y_UNIT_TEST(ExecuteCallbackBudgetDepleted) {
            TSlot slot;
            ActivateSlot(slot);

            UNIT_ASSERT(slot.Inject(7));

            TExecuteCallback cb = [&](ui32) -> bool {
                return true; // budget depleted
            };

            TWsConfig config;
            TPollState ps;
            EPollResult result = PollSlot(slot, nullptr, cb, config, ps);

            UNIT_ASSERT_EQUAL(result, EPollResult::Busy);

            // The activation should have been reinjected.
            slot.DrainInjectionQueue(config.MaxDrainBatch);
            auto item = slot.PopActivation();
            UNIT_ASSERT(item.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*item, 7u);
        }

        Y_UNIT_TEST(ExecuteCallbackCompleted) {
            TSlot slot;
            ActivateSlot(slot);

            UNIT_ASSERT(slot.Inject(7));

            TExecuteCallback cb = [&](ui32) -> bool {
                return false; // mailbox empty, done
            };

            TWsConfig config;
            TPollState ps;
            EPollResult result = PollSlot(slot, nullptr, cb, config, ps);

            UNIT_ASSERT_EQUAL(result, EPollResult::Busy);

            // The activation should NOT have been reinjected.
            slot.DrainInjectionQueue(config.MaxDrainBatch);
            auto item = slot.PopActivation();
            UNIT_ASSERT(!item.has_value());
        }

        Y_UNIT_TEST(StealFromNeighbor) {
            TSlot slotA;
            ActivateSlot(slotA);

            TSlot slotB;
            ActivateSlot(slotB);

            // Put work into slotB
            for (ui32 i = 0; i < 10; ++i) {
                UNIT_ASSERT(slotB.Inject(100 + i));
            }
            slotB.DrainInjectionQueue(64);

            std::vector<TSlot*> neighbors = {&slotB};
            TTestStealIterator iter(neighbors);

            ui32 received = 0;
            TExecuteCallback cb = [&](ui32 hint) -> bool {
                received = hint;
                return false;
            };

            TWsConfig config;
            config.StarvationGuardLimit = 1; // steal on first idle
            TPollState ps;
            EPollResult result = PollSlot(slotA, &iter, cb, config, ps);

            UNIT_ASSERT_EQUAL(result, EPollResult::Busy);
            // Callback must have been called with one of the stolen hints
            UNIT_ASSERT(received >= 100 && received < 110);
        }

        Y_UNIT_TEST(StealIteratorExhausted) {
            TSlot slotA;
            ActivateSlot(slotA);

            // Iterator that yields nothing
            std::vector<TSlot*> empty;
            TTestStealIterator iter(empty);

            bool callbackCalled = false;
            TExecuteCallback cb = [&](ui32) -> bool {
                callbackCalled = true;
                return false;
            };

            TWsConfig config;
            config.StarvationGuardLimit = 1;
            TPollState ps;
            EPollResult result = PollSlot(slotA, &iter, cb, config, ps);

            UNIT_ASSERT_EQUAL(result, EPollResult::Idle);
            UNIT_ASSERT(!callbackCalled);
        }

        Y_UNIT_TEST(DrainAndPopCycle) {
            TSlot slot;
            ActivateSlot(slot);

            // Inject multiple items
            for (ui32 i = 0; i < 5; ++i) {
                UNIT_ASSERT(slot.Inject(i));
            }

            std::vector<ui32> executed;
            TExecuteCallback cb = [&](ui32 hint) -> bool {
                executed.push_back(hint);
                return false;
            };

            TWsConfig config;
            TPollState ps;

            // PollSlot processes all drained activations in a single call
            EPollResult result = PollSlot(slot, nullptr, cb, config, ps);
            UNIT_ASSERT_EQUAL(result, EPollResult::Busy);
            UNIT_ASSERT_VALUES_EQUAL(executed.size(), 5u);

            // All 5 hints should have been executed (order may vary)
            std::sort(executed.begin(), executed.end());
            for (ui32 i = 0; i < 5; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(executed[i], i);
            }

            // Subsequent call should be idle
            EPollResult idle = PollSlot(slot, nullptr, cb, config, ps);
            UNIT_ASSERT_EQUAL(idle, EPollResult::Idle);
        }

        Y_UNIT_TEST(StealPushesIntoDeque) {
            TSlot slotA;
            ActivateSlot(slotA);

            TSlot slotB;
            ActivateSlot(slotB);

            // Put multiple items into slotB
            for (ui32 i = 0; i < 6; ++i) {
                UNIT_ASSERT(slotB.Inject(200 + i));
            }
            slotB.DrainInjectionQueue(64);

            std::vector<TSlot*> neighbors = {&slotB};
            TTestStealIterator iter(neighbors);

            std::vector<ui32> executed;
            TExecuteCallback cb = [&](ui32 hint) -> bool {
                executed.push_back(hint);
                return false;
            };

            TWsConfig config;
            config.StarvationGuardLimit = 1; // steal on first idle
            TPollState ps;
            EPollResult result = PollSlot(slotA, &iter, cb, config, ps);
            UNIT_ASSERT_EQUAL(result, EPollResult::Busy);

            // Should have stolen half of 6 = 3 items and executed all of them
            UNIT_ASSERT_VALUES_EQUAL(executed.size(), 3u);

            // All executed hints should be from the stolen range
            for (ui32 hint : executed) {
                UNIT_ASSERT(hint >= 200 && hint < 206);
            }
        }

        Y_UNIT_TEST(NullStealIterator) {
            TSlot slot;
            ActivateSlot(slot);

            bool callbackCalled = false;
            TExecuteCallback cb = [&](ui32) -> bool {
                callbackCalled = true;
                return false;
            };

            TWsConfig config;
            TPollState ps;
            EPollResult result = PollSlot(slot, nullptr, cb, config, ps);

            UNIT_ASSERT_EQUAL(result, EPollResult::Idle);
            UNIT_ASSERT(!callbackCalled);
        }

        Y_UNIT_TEST(StealBackoff) {
            TSlot slotA;
            ActivateSlot(slotA);

            TSlot slotB;
            ActivateSlot(slotB);

            for (ui32 i = 0; i < 10; ++i) {
                UNIT_ASSERT(slotB.Inject(300 + i));
            }
            slotB.DrainInjectionQueue(64);

            std::vector<TSlot*> neighbors = {&slotB};
            TTestStealIterator iter(neighbors);

            TExecuteCallback cb = [&](ui32) -> bool { return false; };

            TWsConfig config;
            config.StarvationGuardLimit = 3; // default: steal every 3rd idle poll
            TPollState ps;

            // First 2 idle polls should NOT steal (backoff)
            EPollResult r1 = PollSlot(slotA, &iter, cb, config, ps);
            UNIT_ASSERT_EQUAL(r1, EPollResult::Idle);
            UNIT_ASSERT_VALUES_EQUAL(slotA.Counters.StealAttempts.load(std::memory_order_relaxed), 0u);

            EPollResult r2 = PollSlot(slotA, &iter, cb, config, ps);
            UNIT_ASSERT_EQUAL(r2, EPollResult::Idle);
            UNIT_ASSERT_VALUES_EQUAL(slotA.Counters.StealAttempts.load(std::memory_order_relaxed), 0u);

            // 3rd idle poll SHOULD steal
            EPollResult r3 = PollSlot(slotA, &iter, cb, config, ps);
            UNIT_ASSERT_EQUAL(r3, EPollResult::Busy);
            UNIT_ASSERT(slotA.Counters.StealAttempts.load(std::memory_order_relaxed) > 0);
        }

    } // Y_UNIT_TEST_SUITE(WsPoll)

} // namespace NActors::NWorkStealing
