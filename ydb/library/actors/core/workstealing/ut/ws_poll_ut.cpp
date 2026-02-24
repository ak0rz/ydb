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
            // is pushed back into the ring for subsequent processing.
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

        Y_UNIT_TEST(TwoQueueItemsProcessedAcrossCalls) {
            // PollSlot pops one queue item per call (when ring is empty).
            // Two queue items need two PollSlot calls.
            TSlot slot;
            ActivateSlot(slot);

            slot.Push(1);
            slot.Push(2);

            std::vector<ui32> executed;
            std::map<ui32, ui32> remaining = {{1, 100}, {2, 100}};
            auto cb = MakeCallback(executed, remaining);

            TWsConfig config;
            config.MaxExecBatch = 3;  // tight budget
            TPollState ps;

            // First call: pops item from queue, processes up to budget
            EPollResult r1 = PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);
            UNIT_ASSERT_EQUAL(r1, EPollResult::Busy);
            UNIT_ASSERT_VALUES_EQUAL(executed.size(), 3u);
            // Survivor in ring
            UNIT_ASSERT(!ps.Ring.Empty());

            // Second call: ring item from first call + second queue item
            EPollResult r2 = PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);
            UNIT_ASSERT_EQUAL(r2, EPollResult::Busy);
            // Both activations served across the two calls
            bool saw1 = false, saw2 = false;
            for (ui32 h : executed) {
                if (h == 1) saw1 = true;
                if (h == 2) saw2 = true;
            }
            UNIT_ASSERT(saw1);
            UNIT_ASSERT(saw2);
        }

        Y_UNIT_TEST(InterleavingAlternatesRingAndQueue) {
            // Ring has activation A, queue has activation B.
            // Both are served in a single PollSlot call.
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

            // Ring item (10) is executed first
            UNIT_ASSERT_VALUES_EQUAL(executed[0], 10u);
        }

        Y_UNIT_TEST(BothSurvivorsGoToRing) {
            // When both ring and queue items have remaining events,
            // both survivors are saved to ring.
            TSlot slot;
            ActivateSlot(slot);

            TPollState ps;
            ps.Ring.Push(1);
            slot.Push(2);

            std::vector<ui32> executed;
            std::map<ui32, ui32> remaining = {{1, 1000}, {2, 1000}};
            auto cb = MakeCallback(executed, remaining);

            TWsConfig config;
            config.MaxExecBatch = 5;
            EPollResult result = PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);

            UNIT_ASSERT_EQUAL(result, EPollResult::Busy);
            // Each activation gets its own budget of 5
            UNIT_ASSERT_VALUES_EQUAL(executed.size(), 10u);

            // Both should be saved to ring
            UNIT_ASSERT_VALUES_EQUAL(ps.Ring.Size(), 2u);
        }

        Y_UNIT_TEST(EachActivationGetsOwnBudget) {
            // Ring item and queue item each get MaxExecBatch events.
            TSlot slot;
            ActivateSlot(slot);

            TPollState ps;
            ps.Ring.Push(10);
            slot.Push(20);

            std::vector<ui32> executed;
            std::map<ui32, ui32> remaining = {{10, 100}, {20, 100}};
            auto cb = MakeCallback(executed, remaining);

            TWsConfig config;
            config.MaxExecBatch = 7;
            EPollResult result = PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);

            UNIT_ASSERT_EQUAL(result, EPollResult::Busy);
            // 7 events from ring item + 7 from queue item = 14 total
            UNIT_ASSERT_VALUES_EQUAL(executed.size(), 14u);

            // Count per activation
            ui32 count10 = 0, count20 = 0;
            for (ui32 h : executed) {
                if (h == 10) ++count10;
                if (h == 20) ++count20;
            }
            UNIT_ASSERT_VALUES_EQUAL(count10, 7u);
            UNIT_ASSERT_VALUES_EQUAL(count20, 7u);
        }

        Y_UNIT_TEST(ContinueSameWhenNothingElse) {
            // When time expires but nothing else is in ring/queue,
            // the activation runs to budget exhaustion.
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
            // Time budget expires after first event but budget still available.
            // Since there's only one activation, it processes just 1 event
            // (deadline check fires after the first callback).
            // The activation is saved to ring with remaining events.
            UNIT_ASSERT(callCount >= 1);
        }

        Y_UNIT_TEST(TimeBudgetLimitsExecution) {
            // With MailboxBatchCycles=1 and cyclesPerEvent=10,
            // time budget fires after the first event.
            TSlot slot;
            ActivateSlot(slot);

            slot.Push(100);

            std::vector<ui32> executed;
            std::map<ui32, ui32> remaining = {{100, 1000}};
            auto cb = MakeCallback(executed, remaining, 10);

            TWsConfig config;
            config.MaxExecBatch = 20;
            config.MailboxBatchCycles = 1;  // expire after first event
            TPollState ps;
            EPollResult result = PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);

            UNIT_ASSERT_EQUAL(result, EPollResult::Busy);
            // Only 1 event processed (time budget expired after first)
            UNIT_ASSERT_VALUES_EQUAL(executed.size(), 1u);
            // Activation saved to ring
            UNIT_ASSERT(!ps.Ring.Empty());
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

        Y_UNIT_TEST(DrainAndPopCycle) {
            // 5 items in queue, each with 1 event. PollSlot processes
            // one queue item per call (when ring is empty). Multiple
            // calls drain all items.
            TSlot slot;
            ActivateSlot(slot);

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

            // Each PollSlot call processes one queue item (ring is empty).
            // Drained items don't go to ring. Keep calling until idle.
            for (int i = 0; i < 10; ++i) {
                EPollResult result = PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);
                if (result == EPollResult::Idle) {
                    break;
                }
            }

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
            // A hot mailbox (10 with many events) exhausts budget and is
            // saved to ring. On the next PollSlot call, it is served from
            // ring while a new queue item (20) is served from queue.
            TSlot slot;
            ActivateSlot(slot);

            std::vector<ui32> executed;
            std::map<ui32, ui32> remaining = {{10, 200}, {20, 1}};
            auto cb = MakeCallback(executed, remaining);

            slot.Push(10);

            TWsConfig config;
            config.MaxExecBatch = 10;
            TPollState ps;

            // First PollSlot: queue item 10, processes 10 events, saves to ring.
            EPollResult r1 = PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);
            UNIT_ASSERT_EQUAL(r1, EPollResult::Busy);
            UNIT_ASSERT(!ps.Ring.Empty());

            // Push item 20 to queue
            slot.Push(20);

            // Second PollSlot: ring item=10, queue item=20. Both served.
            EPollResult r2 = PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);
            UNIT_ASSERT_EQUAL(r2, EPollResult::Busy);

            // Verify mailbox 20 was served
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
            // Even with ring items, queue items get processed each PollSlot call.
            TSlot slot;
            ActivateSlot(slot);

            std::vector<ui32> executed;
            std::map<ui32, ui32> remaining = {{10, 1000}, {20, 1}, {30, 1}};
            auto cb = MakeCallback(executed, remaining);

            // First PollSlot: establish ring item for mailbox 10
            slot.Push(10);
            TWsConfig config;
            config.MaxExecBatch = 20;
            TPollState ps;
            PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);
            UNIT_ASSERT(!ps.Ring.Empty());

            // Push first queue item
            slot.Push(20);

            // Second PollSlot: ring serves 10, queue serves 20.
            executed.clear();
            PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);

            bool saw20 = false;
            for (ui32 h : executed) {
                if (h == 20) saw20 = true;
            }
            UNIT_ASSERT_C(saw20, "Queue item 20 was starved by ring continuation");

            // Push second queue item
            slot.Push(30);

            // Third PollSlot: ring serves 10, queue serves 30.
            executed.clear();
            PollSlot(slot, nullptr, cb, NoopOverflow(), config, ps);

            bool saw30 = false;
            for (ui32 h : executed) {
                if (h == 30) saw30 = true;
            }
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
