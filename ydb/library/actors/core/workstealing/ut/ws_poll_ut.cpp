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
        // cyclesPerEvent: if non-zero, advance hpnow by this amount per event
        // to simulate realistic time progression for time-based interleaving.
        TExecuteCallback MakeCallback(
            std::vector<ui32>& executed,
            std::map<ui32, ui32>& remaining,
            uint64_t cyclesPerEvent = 0)
        {
            return [&, cyclesPerEvent](ui32 hint, NHPTimer::STime& hpnow) -> bool {
                auto it = remaining.find(hint);
                if (it == remaining.end() || it->second == 0) {
                    return false; // no events
                }
                executed.push_back(hint);
                --it->second;
                hpnow += cyclesPerEvent;
                return true; // event processed
            };
        }

        // No-op overflow callback for tests that don't test overflow.
        TOverflowCallback NoopOverflow() {
            return [](ui32) {};
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
            EPollResult result = PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);

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
            EPollResult result = PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);

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
            EPollResult result = PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);

            UNIT_ASSERT_EQUAL(result, EPollResult::Busy);
            UNIT_ASSERT_VALUES_EQUAL(callCount, 3u);

            // After all events are processed, the slot queue should be empty
            auto item = slot.Pop();
            UNIT_ASSERT(!item.has_value());
        }

        Y_UNIT_TEST(BudgetExhaustedSavesToRing) {
            // With MaxExecBatch=5 and 10 events, PollSlot processes 5
            // and saves the activation to the ring (not in queue).
            TSlot slot;
            ActivateSlot(slot);

            slot.Push(7);

            std::vector<ui32> executed;
            std::map<ui32, ui32> remaining = {{7, 10}};
            auto cb = MakeCallback(executed, remaining);

            TWsConfig config;
            config.MaxExecBatch = 5;
            TPollState ps;
            EPollResult result = PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);

            UNIT_ASSERT_EQUAL(result, EPollResult::Busy);
            UNIT_ASSERT_VALUES_EQUAL(executed.size(), 5u);

            // The activation should be in the ring, NOT the queue
            UNIT_ASSERT(!ps.Ring.Empty());
            auto ringItem = ps.Ring.Pop();
            UNIT_ASSERT(ringItem.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*ringItem, 7u);
            auto queueItem = slot.Pop();
            UNIT_ASSERT(!queueItem.has_value());
        }

        Y_UNIT_TEST(DrainedActivationRemoved) {
            // 3 events, budget=10 → all drained, ring should be empty.
            TSlot slot;
            ActivateSlot(slot);

            slot.Push(7);

            std::vector<ui32> executed;
            std::map<ui32, ui32> remaining = {{7, 3}};
            auto cb = MakeCallback(executed, remaining);

            TWsConfig config;
            config.MaxExecBatch = 10;
            TPollState ps;
            EPollResult result = PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);

            UNIT_ASSERT_EQUAL(result, EPollResult::Busy);
            UNIT_ASSERT_VALUES_EQUAL(executed.size(), 3u);
            UNIT_ASSERT(ps.Ring.Empty());
            UNIT_ASSERT(!slot.Pop().has_value());
        }

        Y_UNIT_TEST(RingPreservesMultipleActivations) {
            // 3 activations with tiny budget → all saved to ring.
            TSlot slot;
            ActivateSlot(slot);

            slot.Push(1);
            slot.Push(2);
            slot.Push(3);

            std::vector<ui32> executed;
            std::map<ui32, ui32> remaining = {{1, 100}, {2, 100}, {3, 100}};
            auto cb = MakeCallback(executed, remaining);

            TWsConfig config;
            config.MaxExecBatch = 3;  // tight budget
            TPollState ps;
            EPollResult result = PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);

            UNIT_ASSERT_EQUAL(result, EPollResult::Busy);
            // All 3 budget used (1 event each or some interleaving)
            UNIT_ASSERT_VALUES_EQUAL(executed.size(), 3u);
            // Queue should be empty (items are in ring or being processed)
            // Ring should have at least some items preserved
            // The exact count depends on interleaving, but the ring should not be empty
            // since all 3 activations have remaining events
            uint8_t ringCount = ps.Ring.Size();
            // We processed 3 events from potentially 3 activations. Budget exhausted.
            // The last activation that had events is saved to ring.
            // Previous activations that were swapped out are also in ring.
            UNIT_ASSERT(ringCount >= 1);
        }

        Y_UNIT_TEST(InterleavingAlternatesRingAndQueue) {
            // Ring has activation A, queue has activation B.
            // The interleaving strategy should alternate between them.
            TSlot slot;
            ActivateSlot(slot);

            // Pre-populate ring with activation 10
            TPollState ps;
            ps.Ring.Push(10);

            // Queue has activation 20
            slot.Push(20);

            std::vector<ui32> executed;
            std::map<ui32, ui32> remaining = {{10, 1}, {20, 1}};
            auto cb = MakeCallback(executed, remaining);

            TWsConfig config;
            EPollResult result = PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);

            UNIT_ASSERT_EQUAL(result, EPollResult::Busy);
            UNIT_ASSERT_VALUES_EQUAL(executed.size(), 2u);

            // Both should be executed
            bool saw10 = false, saw20 = false;
            for (ui32 h : executed) {
                if (h == 10) saw10 = true;
                if (h == 20) saw20 = true;
            }
            UNIT_ASSERT(saw10);
            UNIT_ASSERT(saw20);

            // Ring starts non-empty → preferRing=true → ring item (10) first
            UNIT_ASSERT_VALUES_EQUAL(executed[0], 10u);
        }

        Y_UNIT_TEST(SwapSavesToRing) {
            // When time expires with other work available, the current
            // activation is saved to ring and the next is executed.
            TSlot slot;
            ActivateSlot(slot);

            // Use two activations with lots of events
            slot.Push(1);
            slot.Push(2);

            std::vector<ui32> executed;
            std::map<ui32, ui32> remaining = {{1, 1000}, {2, 1000}};
            // Advance hpnow by 10 cycles per event so deadline (1 cycle) triggers
            auto cb = MakeCallback(executed, remaining, 10);

            TWsConfig config;
            config.MaxExecBatch = 20;
            config.MailboxBatchCycles = 1;  // expire immediately → force swap
            TPollState ps;
            EPollResult result = PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);

            UNIT_ASSERT_EQUAL(result, EPollResult::Busy);
            UNIT_ASSERT_VALUES_EQUAL(executed.size(), 20u);

            // Both activations should have been executed (interleaved via swap)
            bool saw1 = false, saw2 = false;
            for (ui32 h : executed) {
                if (h == 1) saw1 = true;
                if (h == 2) saw2 = true;
            }
            UNIT_ASSERT(saw1);
            UNIT_ASSERT(saw2);

            // The leftover should be in the ring
            UNIT_ASSERT(!ps.Ring.Empty());
        }

        Y_UNIT_TEST(ContinueSameWhenNothingElse) {
            // When time expires but nothing else is in ring/queue,
            // continue same activation with reset deadline.
            TSlot slot;
            ActivateSlot(slot);

            slot.Push(42);

            ui32 callCount = 0;
            TExecuteCallback cb = [&](ui32, NHPTimer::STime&) -> bool {
                ++callCount;
                return callCount < 10;  // 10 events total
            };

            TWsConfig config;
            config.MaxExecBatch = 64;
            config.MailboxBatchCycles = 1;  // expire immediately
            TPollState ps;
            EPollResult result = PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);

            UNIT_ASSERT_EQUAL(result, EPollResult::Busy);
            // All 10 events should be processed (no swap partner → continue)
            UNIT_ASSERT_VALUES_EQUAL(callCount, 10u);
            // Drained → ring should be empty
            UNIT_ASSERT(ps.Ring.Empty());
        }

        Y_UNIT_TEST(InnerSwapGoesToQueue) {
            // Time-based inner swaps push the current activation to queue
            // (not ring). Only budget-exhaustion leftovers go to ring.
            TSlot slot;
            ActivateSlot(slot);

            slot.Push(100);
            slot.Push(200);

            std::vector<ui32> executed;
            std::map<ui32, ui32> remaining = {{100, 1000}, {200, 1000}};
            // cyclesPerEvent=10 to trigger swap after 1 event (deadline=1)
            auto cb = MakeCallback(executed, remaining, 10);

            TWsConfig config;
            config.MaxExecBatch = 4;
            config.MailboxBatchCycles = 1;  // force swaps after each event
            TPollState ps;
            EPollResult result = PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);

            UNIT_ASSERT_EQUAL(result, EPollResult::Busy);
            UNIT_ASSERT_VALUES_EQUAL(executed.size(), 4u);
            // No ring overflows — inner swaps go to queue, not ring
            UNIT_ASSERT_VALUES_EQUAL(
                slot.Counters.RingOverflows.load(std::memory_order_relaxed), 0u);
            // Both activations interleaved (swapped via queue, not ring)
            bool saw100 = false, saw200 = false;
            for (ui32 h : executed) {
                if (h == 100) saw100 = true;
                if (h == 200) saw200 = true;
            }
            UNIT_ASSERT(saw100);
            UNIT_ASSERT(saw200);
        }

        Y_UNIT_TEST(ContinuationCountReflectsRing) {
            // After PollSlot, slot.ContinuationCount should match the ring size.
            TSlot slot;
            ActivateSlot(slot);

            slot.Push(7);

            std::vector<ui32> executed;
            std::map<ui32, ui32> remaining = {{7, 100}};
            auto cb = MakeCallback(executed, remaining);

            TWsConfig config;
            config.MaxExecBatch = 5;
            TPollState ps;
            PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);

            uint8_t ringSize = ps.Ring.Size();
            uint8_t contCount = slot.ContinuationCount.load(std::memory_order_relaxed);
            UNIT_ASSERT_VALUES_EQUAL(contCount, ringSize);
        }

        Y_UNIT_TEST(RingFlushPushesAll) {
            // FlushTo pushes all ring items to the queue.
            TSlot slot;
            ActivateSlot(slot);

            TContinuationRing ring;
            ring.Push(10);
            ring.Push(20);
            ring.Push(30);

            UNIT_ASSERT_VALUES_EQUAL(ring.Size(), 3u);

            ring.FlushTo(slot);

            UNIT_ASSERT(ring.Empty());
            UNIT_ASSERT_VALUES_EQUAL(ring.Size(), 0u);

            // All 3 items should be in the queue
            auto a = slot.Pop();
            UNIT_ASSERT(a.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*a, 10u);
            auto b = slot.Pop();
            UNIT_ASSERT(b.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*b, 20u);
            auto c = slot.Pop();
            UNIT_ASSERT(c.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*c, 30u);
            UNIT_ASSERT(!slot.Pop().has_value());
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
            EPollResult result = PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);

            UNIT_ASSERT_EQUAL(result, EPollResult::Busy);
            // Both activations should be fully processed
            UNIT_ASSERT_VALUES_EQUAL(order.size(), 4u);
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
            EPollResult result = PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);

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
            EPollResult result = PollSlot(slotA, &iter, cb, NoopOverflow(), config, ps);

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
            EPollResult result = PollSlot(slotA, &iter, cb, NoopOverflow(), config, ps);

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
            EPollResult result = PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);
            UNIT_ASSERT_EQUAL(result, EPollResult::Busy);
            UNIT_ASSERT_VALUES_EQUAL(executed.size(), 5u);

            // All 5 hints should have been executed (order may vary)
            std::sort(executed.begin(), executed.end());
            for (ui32 i = 0; i < 5; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(executed[i], i + 1);
            }

            // Subsequent call should be idle
            EPollResult idle = PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);
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
            EPollResult result = PollSlot(slotA, &iter, cb, NoopOverflow(), config, ps);
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
            EPollResult result = PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);

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
            EPollResult r1 = PollSlot(slotA, &iter, cb, NoopOverflow(), config, ps);
            UNIT_ASSERT_EQUAL(r1, EPollResult::Idle);
            UNIT_ASSERT_VALUES_EQUAL(slotA.Counters.StealAttempts.load(std::memory_order_relaxed), 0u);

            EPollResult r2 = PollSlot(slotA, &iter, cb, NoopOverflow(), config, ps);
            UNIT_ASSERT_EQUAL(r2, EPollResult::Idle);
            UNIT_ASSERT_VALUES_EQUAL(slotA.Counters.StealAttempts.load(std::memory_order_relaxed), 0u);

            // 3rd idle poll SHOULD steal
            EPollResult r3 = PollSlot(slotA, &iter, cb, NoopOverflow(), config, ps);
            UNIT_ASSERT_EQUAL(r3, EPollResult::Busy);
            UNIT_ASSERT(slotA.Counters.StealAttempts.load(std::memory_order_relaxed) > 0);
        }

        Y_UNIT_TEST(RingContinuationPreservesActivation) {
            // When a hot mailbox (A with 200 events) exhausts execBudget,
            // it is saved to the ring and served on the next PollSlot call
            // — interleaved with B which is waiting in the queue.
            TSlot slot;
            ActivateSlot(slot);

            std::vector<ui32> executed;
            std::map<ui32, ui32> remaining = {{10, 200}, {20, 1}};
            // Advance hpnow so time-based swap can trigger
            auto cb = MakeCallback(executed, remaining, 10000);

            slot.Push(10);
            slot.Push(20);

            TWsConfig config;
            config.MaxExecBatch = 10;
            TPollState ps;

            // First PollSlot: pops mailbox 10 first. With time advancement,
            // deadline triggers after 5 events. Swap check finds 20 in queue.
            // Both 10 and 20 get served in a single PollSlot call.
            EPollResult r1 = PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);
            UNIT_ASSERT_EQUAL(r1, EPollResult::Busy);

            // Verify mailbox 10 was served first (queue FIFO)
            UNIT_ASSERT(!executed.empty());
            UNIT_ASSERT_VALUES_EQUAL(executed[0], 10u);
            // Verify mailbox 20 was also served (time-based interleaving)
            bool saw20 = false;
            for (ui32 h : executed) {
                if (h == 20) saw20 = true;
            }
            UNIT_ASSERT(saw20);
            // Mailbox 10 should still be in ring (has many remaining events)
            UNIT_ASSERT(!ps.Ring.Empty());
        }

        Y_UNIT_TEST(RingContinuationClearsWhenDrained) {
            // When a mailbox drains during execution, it is removed
            // and the ring should be empty afterwards.
            TSlot slot;
            ActivateSlot(slot);

            std::vector<ui32> executed;
            std::map<ui32, ui32> remaining = {{10, 5}};
            auto cb = MakeCallback(executed, remaining);

            slot.Push(10);

            TWsConfig config;
            config.MaxExecBatch = 3;
            TPollState ps;

            // First PollSlot: processes 3, saves to ring (2 remain).
            EPollResult r1 = PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);
            UNIT_ASSERT_EQUAL(r1, EPollResult::Busy);
            UNIT_ASSERT(!ps.Ring.Empty());
            UNIT_ASSERT_VALUES_EQUAL(executed.size(), 3u);

            // Second PollSlot: processes remaining 2, drains mailbox.
            EPollResult r2 = PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);
            UNIT_ASSERT_EQUAL(r2, EPollResult::Busy);
            UNIT_ASSERT_VALUES_EQUAL(executed.size(), 5u);
            // Ring should be empty (activation was drained)
            UNIT_ASSERT(ps.Ring.Empty());
        }

        Y_UNIT_TEST(RingContinuationDoesNotStarveQueue) {
            // Even with ring items, queue items must get processed.
            TSlot slot;
            ActivateSlot(slot);

            std::vector<ui32> executed;
            std::map<ui32, ui32> remaining = {{10, 1000}, {20, 1}, {30, 1}};
            // Advance hpnow so time-based swap triggers (5 events per deadline)
            auto cb = MakeCallback(executed, remaining, 10000);

            // First PollSlot: establish ring item for mailbox 10
            slot.Push(10);
            TWsConfig config;
            config.MaxExecBatch = 20;  // enough budget for all 3 activations
            TPollState ps;
            PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);
            UNIT_ASSERT(!ps.Ring.Empty());

            // Now push queue items while ring has continuation
            slot.Push(20);
            slot.Push(30);

            // Second PollSlot: ring serves 10, queue serves 20/30.
            // Interleaving ensures all get processed.
            executed.clear();
            PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);

            // Both queue items must have been served
            bool saw20 = false, saw30 = false;
            for (ui32 h : executed) {
                if (h == 20) saw20 = true;
                if (h == 30) saw30 = true;
            }
            UNIT_ASSERT_C(saw20, "Queue item 20 was starved by ring continuation");
            UNIT_ASSERT_C(saw30, "Queue item 30 was starved by ring continuation");
        }

        Y_UNIT_TEST(RingFlushedToQueue) {
            // When a ring item is flushed (e.g., before parking),
            // pushing it to the queue makes it available.
            TSlot slot;
            ActivateSlot(slot);

            std::vector<ui32> executed;
            std::map<ui32, ui32> remaining = {{10, 100}};
            auto cb = MakeCallback(executed, remaining);

            slot.Push(10);

            TWsConfig config;
            config.MaxExecBatch = 3;
            TPollState ps;

            // Build up a ring item
            PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);
            UNIT_ASSERT(!ps.Ring.Empty());

            // Simulate flush: push ring items to queue and clear
            ps.Ring.FlushTo(slot);

            // Verify the activation is now in the queue
            auto item = slot.Pop();
            UNIT_ASSERT(item.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*item, 10u);
            UNIT_ASSERT(ps.Ring.Empty());
        }

    } // Y_UNIT_TEST_SUITE(WsPoll)

} // namespace NActors::NWorkStealing
