#include <ydb/library/actors/core/workstealing/ws_poll.h>

#include <library/cpp/testing/unittest/registar.h>

#include <map>
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

        // Create a callback that simulates N events per hint.
        // First N calls per hint return true (event processed),
        // then false (mailbox empty/finalized).
        TExecuteCallback MakeCallback(
            std::vector<ui32>& executed,
            std::map<ui32, ui32>& remaining)
        {
            return [&](ui32 hint, NHPTimer::STime&) -> bool {
                auto it = remaining.find(hint);
                if (it == remaining.end() || it->second == 0) {
                    return false; // no events
                }
                executed.push_back(hint);
                --it->second;
                return true; // event processed
            };
        }

    } // anonymous namespace

    Y_UNIT_TEST_SUITE(WsPoll) {

        Y_UNIT_TEST(PrePopulatedSlotReturnsBusy) {
            TSlot slot;
            ActivateSlot(slot);

            slot.Push(42);

            // Simulate mailbox with 1 event.
            std::vector<ui32> executed;
            std::map<ui32, ui32> remaining = {{42, 1}};
            auto cb = MakeCallback(executed, remaining);

            TWsConfig config;
            TPollState ps;
            EPollResult result = PollSlot(slot, nullptr, cb, config, ps);

            UNIT_ASSERT_EQUAL(result, EPollResult::Busy);
            UNIT_ASSERT_VALUES_EQUAL(executed.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(executed[0], 42u);
        }

        Y_UNIT_TEST(EmptySlotReturnsIdle) {
            TSlot slot;
            ActivateSlot(slot);

            bool callbackCalled = false;
            TExecuteCallback cb = [&](ui32, NHPTimer::STime&) -> bool {
                callbackCalled = true;
                return false;
            };

            TWsConfig config;
            TPollState ps;
            EPollResult result = PollSlot(slot, nullptr, cb, config, ps);

            UNIT_ASSERT_EQUAL(result, EPollResult::Idle);
            UNIT_ASSERT(!callbackCalled);
        }

        Y_UNIT_TEST(SingleEventPushBack) {
            // When the callback returns true (more events), the activation
            // is pushed back into the queue for interleaved processing.
            TSlot slot;
            ActivateSlot(slot);

            slot.Push(7);

            ui32 callCount = 0;
            TExecuteCallback cb = [&](ui32, NHPTimer::STime&) -> bool {
                ++callCount;
                return callCount < 3; // 3 events total
            };

            TWsConfig config;
            TPollState ps;
            EPollResult result = PollSlot(slot, nullptr, cb, config, ps);

            UNIT_ASSERT_EQUAL(result, EPollResult::Busy);
            UNIT_ASSERT_VALUES_EQUAL(callCount, 3u);

            // After all events are processed, the slot queue should be empty
            auto item = slot.Pop();
            UNIT_ASSERT(!item.has_value());
        }

        Y_UNIT_TEST(BudgetExhaustedSavesHotContinuation) {
            // With MaxExecBatch=2 and unlimited events, PollSlot processes 2
            // and saves the activation as a hot continuation (not in queue).
            TSlot slot;
            ActivateSlot(slot);

            slot.Push(7);

            ui32 callCount = 0;
            TExecuteCallback cb = [&](ui32, NHPTimer::STime&) -> bool {
                ++callCount;
                return true; // always more events
            };

            TWsConfig config;
            config.MaxExecBatch = 2;
            TPollState ps;
            EPollResult result = PollSlot(slot, nullptr, cb, config, ps);

            UNIT_ASSERT_EQUAL(result, EPollResult::Busy);
            UNIT_ASSERT_VALUES_EQUAL(callCount, 2u);

            // The activation should be in HotContinuation, NOT the queue
            UNIT_ASSERT(ps.HotContinuation.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*ps.HotContinuation, 7u);
            auto item = slot.Pop();
            UNIT_ASSERT(!item.has_value());
        }

        Y_UNIT_TEST(InterleavedActivations) {
            // Two activations in the queue. With single-event processing
            // and pushback, they get interleaved.
            TSlot slot;
            ActivateSlot(slot);

            slot.Push(1);
            slot.Push(2);

            std::vector<ui32> order;
            std::map<ui32, ui32> remaining = {{1, 2}, {2, 2}};
            auto cb = MakeCallback(order, remaining);

            TWsConfig config;
            TPollState ps;
            EPollResult result = PollSlot(slot, nullptr, cb, config, ps);

            UNIT_ASSERT_EQUAL(result, EPollResult::Busy);
            // Both activations should be fully processed
            UNIT_ASSERT_VALUES_EQUAL(order.size(), 4u);
            // They should be interleaved (not all of 1 first, then all of 2)
            // The exact order depends on queue FIFO behavior, but both should appear
            ui32 count1 = 0, count2 = 0;
            for (ui32 h : order) {
                if (h == 1) ++count1;
                if (h == 2) ++count2;
            }
            UNIT_ASSERT_VALUES_EQUAL(count1, 2u);
            UNIT_ASSERT_VALUES_EQUAL(count2, 2u);
        }

        Y_UNIT_TEST(ExecuteCallbackCompleted) {
            TSlot slot;
            ActivateSlot(slot);

            slot.Push(7);

            // 1 event per activation
            std::vector<ui32> executed;
            std::map<ui32, ui32> remaining = {{7, 1}};
            auto cb = MakeCallback(executed, remaining);

            TWsConfig config;
            TPollState ps;
            EPollResult result = PollSlot(slot, nullptr, cb, config, ps);

            UNIT_ASSERT_EQUAL(result, EPollResult::Busy);

            // The activation should NOT have been reinjected.
            auto item = slot.Pop();
            UNIT_ASSERT(!item.has_value());
        }

        Y_UNIT_TEST(StealFromNeighbor) {
            TSlot slotA;
            ActivateSlot(slotA);

            TSlot slotB;
            ActivateSlot(slotB);

            // Put work into slotB and mark it as executing
            for (ui32 i = 0; i < 10; ++i) {
                slotB.Push(100 + i);
            }
            slotB.Executing.store(true, std::memory_order_relaxed);

            std::vector<TSlot*> neighbors = {&slotB};
            TTestStealIterator iter(neighbors);

            // Each stolen hint has 1 event
            std::vector<ui32> executed;
            std::map<ui32, ui32> remaining;
            for (ui32 i = 0; i < 10; ++i) {
                remaining[100 + i] = 1;
            }
            auto cb = MakeCallback(executed, remaining);

            TWsConfig config;
            config.StarvationGuardLimit = 1; // steal on first idle
            TPollState ps;
            EPollResult result = PollSlot(slotA, &iter, cb, config, ps);

            UNIT_ASSERT_EQUAL(result, EPollResult::Busy);
            // Callback must have been called with stolen hints
            UNIT_ASSERT(!executed.empty());
            for (ui32 hint : executed) {
                UNIT_ASSERT(hint >= 100 && hint < 110);
            }
        }

        Y_UNIT_TEST(StealIteratorExhausted) {
            TSlot slotA;
            ActivateSlot(slotA);

            // Iterator that yields nothing
            std::vector<TSlot*> empty;
            TTestStealIterator iter(empty);

            bool callbackCalled = false;
            TExecuteCallback cb = [&](ui32, NHPTimer::STime&) -> bool {
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

            // Inject 5 items — each represents a mailbox with 1 event
            for (ui32 i = 0; i < 5; ++i) {
                slot.Push(i + 1);
            }

            std::vector<ui32> executed;
            std::map<ui32, ui32> remaining;
            for (ui32 i = 0; i < 5; ++i) {
                remaining[i + 1] = 1;
            }
            auto cb = MakeCallback(executed, remaining);

            TWsConfig config;
            TPollState ps;

            // PollSlot processes all activations in a single call
            EPollResult result = PollSlot(slot, nullptr, cb, config, ps);
            UNIT_ASSERT_EQUAL(result, EPollResult::Busy);
            UNIT_ASSERT_VALUES_EQUAL(executed.size(), 5u);

            // All 5 hints should have been executed (order may vary)
            std::sort(executed.begin(), executed.end());
            for (ui32 i = 0; i < 5; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(executed[i], i + 1);
            }

            // Subsequent call should be idle
            EPollResult idle = PollSlot(slot, nullptr, cb, config, ps);
            UNIT_ASSERT_EQUAL(idle, EPollResult::Idle);
        }

        Y_UNIT_TEST(StealExecutesDirectly) {
            TSlot slotA;
            ActivateSlot(slotA);

            TSlot slotB;
            ActivateSlot(slotB);

            // Put multiple items into slotB and mark it as executing
            for (ui32 i = 0; i < 6; ++i) {
                slotB.Push(200 + i);
            }
            slotB.Executing.store(true, std::memory_order_relaxed);

            std::vector<TSlot*> neighbors = {&slotB};
            TTestStealIterator iter(neighbors);

            // Each stolen hint has 1 event
            std::vector<ui32> executed;
            std::map<ui32, ui32> remaining;
            for (ui32 i = 0; i < 6; ++i) {
                remaining[200 + i] = 1;
            }
            auto cb = MakeCallback(executed, remaining);

            TWsConfig config;
            config.StarvationGuardLimit = 1; // steal on first idle
            TPollState ps;
            EPollResult result = PollSlot(slotA, &iter, cb, config, ps);
            UNIT_ASSERT_EQUAL(result, EPollResult::Busy);

            // Without MailboxTable, steals up to maxCount and executes directly
            UNIT_ASSERT(executed.size() > 0 && executed.size() <= 6u);

            // All executed hints should be from the stolen range
            for (ui32 hint : executed) {
                UNIT_ASSERT(hint >= 200 && hint < 206);
            }
        }

        Y_UNIT_TEST(NullStealIterator) {
            TSlot slot;
            ActivateSlot(slot);

            bool callbackCalled = false;
            TExecuteCallback cb = [&](ui32, NHPTimer::STime&) -> bool {
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
                slotB.Push(300 + i);
            }
            slotB.Executing.store(true, std::memory_order_relaxed);

            std::vector<TSlot*> neighbors = {&slotB};
            TTestStealIterator iter(neighbors);

            // Each hint has 1 event
            std::map<ui32, ui32> remaining;
            for (ui32 i = 0; i < 10; ++i) {
                remaining[300 + i] = 1;
            }
            std::vector<ui32> executed;
            auto cb = MakeCallback(executed, remaining);

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

        Y_UNIT_TEST(HotContinuationPreservesActivation) {
            // When a hot mailbox (A with 200 events) exhausts execBudget,
            // it is saved as a continuation and served FIRST on the next
            // PollSlot call — before B which is waiting in the queue.
            TSlot slot;
            ActivateSlot(slot);

            std::vector<ui32> executed;
            std::map<ui32, ui32> remaining = {{10, 200}, {20, 1}};
            auto cb = MakeCallback(executed, remaining);

            slot.Push(10);
            slot.Push(20);

            TWsConfig config;
            config.MaxExecBatch = 10;
            TPollState ps;

            // First PollSlot: no continuation yet. Phase 2 pops mailbox 10,
            // processes 10 events, budget exhausted, saves as continuation.
            // Mailbox 20 stays in queue.
            EPollResult r1 = PollSlot(slot, nullptr, cb, config, ps);
            UNIT_ASSERT_EQUAL(r1, EPollResult::Busy);
            UNIT_ASSERT(ps.HotContinuation.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*ps.HotContinuation, 10u);
            // All executed so far should be from mailbox 10
            for (ui32 h : executed) {
                UNIT_ASSERT_VALUES_EQUAL(h, 10u);
            }
            size_t firstBatch = executed.size();

            // Second PollSlot: phase 1 serves continuation (mailbox 10)
            // with capped budget, then phase 2 serves mailbox 20 from queue.
            EPollResult r2 = PollSlot(slot, nullptr, cb, config, ps);
            UNIT_ASSERT_EQUAL(r2, EPollResult::Busy);
            // Verify mailbox 10 was served first in this batch (phase 1)
            UNIT_ASSERT_VALUES_EQUAL(executed[firstBatch], 10u);
            // Verify mailbox 20 was also served (phase 2 interleaving)
            bool saw20 = false;
            for (size_t i = firstBatch; i < executed.size(); ++i) {
                if (executed[i] == 20) saw20 = true;
            }
            UNIT_ASSERT(saw20);
        }

        Y_UNIT_TEST(ContinuationClearsWhenDrained) {
            // When a mailbox drains during a continuation, no continuation
            // is saved afterwards.
            TSlot slot;
            ActivateSlot(slot);

            std::vector<ui32> executed;
            std::map<ui32, ui32> remaining = {{10, 5}};
            auto cb = MakeCallback(executed, remaining);

            slot.Push(10);

            TWsConfig config;
            config.MaxExecBatch = 3;
            TPollState ps;

            // First PollSlot: processes 3, saves continuation (2 remain).
            EPollResult r1 = PollSlot(slot, nullptr, cb, config, ps);
            UNIT_ASSERT_EQUAL(r1, EPollResult::Busy);
            UNIT_ASSERT(ps.HotContinuation.has_value());
            UNIT_ASSERT_VALUES_EQUAL(executed.size(), 3u);

            // Second PollSlot: processes remaining 2, drains mailbox.
            EPollResult r2 = PollSlot(slot, nullptr, cb, config, ps);
            UNIT_ASSERT_EQUAL(r2, EPollResult::Busy);
            UNIT_ASSERT_VALUES_EQUAL(executed.size(), 5u);
            // No continuation should remain
            UNIT_ASSERT(!ps.HotContinuation.has_value());
        }

        Y_UNIT_TEST(ContinuationDoesNotStarveQueue) {
            // Even with a hot continuation, queue items must get processed.
            // This prevents system-wide stall when fan-in actors >= slots.
            TSlot slot;
            ActivateSlot(slot);

            std::vector<ui32> executed;
            std::map<ui32, ui32> remaining = {{10, 1000}, {20, 1}, {30, 1}};
            auto cb = MakeCallback(executed, remaining);

            // First PollSlot: establish continuation for mailbox 10
            slot.Push(10);
            TWsConfig config;
            config.MaxExecBatch = 10;
            TPollState ps;
            PollSlot(slot, nullptr, cb, config, ps);
            UNIT_ASSERT(ps.HotContinuation.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*ps.HotContinuation, 10u);

            // Now push queue items while continuation is active
            slot.Push(20);
            slot.Push(30);

            // Second PollSlot: continuation runs in phase 1 (capped),
            // then phase 2 must serve 20 and 30.
            executed.clear();
            PollSlot(slot, nullptr, cb, config, ps);

            // Both queue items must have been served
            bool saw20 = false, saw30 = false;
            for (ui32 h : executed) {
                if (h == 20) saw20 = true;
                if (h == 30) saw30 = true;
            }
            UNIT_ASSERT_C(saw20, "Queue item 20 was starved by continuation");
            UNIT_ASSERT_C(saw30, "Queue item 30 was starved by continuation");
            // Continuation should still be active
            UNIT_ASSERT(ps.HotContinuation.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*ps.HotContinuation, 10u);
        }

        Y_UNIT_TEST(ContinuationFlushedToQueue) {
            // When a continuation is set but we need to flush it (e.g.
            // before parking), pushing it to the queue makes it available.
            TSlot slot;
            ActivateSlot(slot);

            std::vector<ui32> executed;
            std::map<ui32, ui32> remaining = {{10, 100}};
            auto cb = MakeCallback(executed, remaining);

            slot.Push(10);

            TWsConfig config;
            config.MaxExecBatch = 3;
            TPollState ps;

            // Build up a continuation
            PollSlot(slot, nullptr, cb, config, ps);
            UNIT_ASSERT(ps.HotContinuation.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*ps.HotContinuation, 10u);

            // Simulate flush: push continuation to queue and clear
            slot.Push(*ps.HotContinuation);
            ps.HotContinuation.reset();

            // Verify the activation is now in the queue
            auto item = slot.Pop();
            UNIT_ASSERT(item.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*item, 10u);
            UNIT_ASSERT(!ps.HotContinuation.has_value());
        }

    } // Y_UNIT_TEST_SUITE(WsPoll)

} // namespace NActors::NWorkStealing
