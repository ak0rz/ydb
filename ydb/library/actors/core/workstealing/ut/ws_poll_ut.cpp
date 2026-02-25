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

        // --- TContinuationRing unit tests ---

        Y_UNIT_TEST(RingPushPop) {
            TContinuationRing ring;
            UNIT_ASSERT(ring.Empty());
            UNIT_ASSERT_VALUES_EQUAL(ring.Size(), 0u);

            UNIT_ASSERT(ring.Push(10));
            UNIT_ASSERT(ring.Push(20));
            UNIT_ASSERT(ring.Push(30));

            UNIT_ASSERT(!ring.Empty());
            UNIT_ASSERT_VALUES_EQUAL(ring.Size(), 3u);

            auto a = ring.Pop();
            UNIT_ASSERT(a.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*a, 10u);

            auto b = ring.Pop();
            UNIT_ASSERT(b.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*b, 20u);

            auto c = ring.Pop();
            UNIT_ASSERT(c.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*c, 30u);

            UNIT_ASSERT(ring.Empty());
            UNIT_ASSERT(!ring.Pop().has_value());
        }

        Y_UNIT_TEST(RingCapacityLimit) {
            TContinuationRing ring;
            // Default capacity is 4
            UNIT_ASSERT(ring.Push(1));
            UNIT_ASSERT(ring.Push(2));
            UNIT_ASSERT(ring.Push(3));
            UNIT_ASSERT(ring.Push(4));
            UNIT_ASSERT(!ring.Push(5)); // overflow

            UNIT_ASSERT(ring.Full());
            UNIT_ASSERT_VALUES_EQUAL(ring.Size(), 4u);
        }

        Y_UNIT_TEST(RingFlushPushesAll) {
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

        Y_UNIT_TEST(RingSnapshotToSlot) {
            TSlot slot;
            ActivateSlot(slot);

            TContinuationRing ring;
            ring.Push(100);
            ring.Push(200);

            ring.SnapshotTo(slot);

            UNIT_ASSERT_VALUES_EQUAL(
                slot.RingSnapshot[0].load(std::memory_order_relaxed), 100u);
            UNIT_ASSERT_VALUES_EQUAL(
                slot.RingSnapshot[1].load(std::memory_order_relaxed), 200u);
            // Remaining slots zeroed
            UNIT_ASSERT_VALUES_EQUAL(
                slot.RingSnapshot[2].load(std::memory_order_relaxed), 0u);
        }

        // --- PollSlot with null deps (no execution, tests scheduling mechanics) ---
        // With null wsTable/ctx, PollSlot pops items but execution is a no-op
        // (returns {HasMore=false, EventsProcessed=0}).

        Y_UNIT_TEST(EmptySlotReturnsIdle) {
            TSlot slot;
            ActivateSlot(slot);
            slot.AcquireForWorker();

            EPollResult result = PollSlot(slot);

            UNIT_ASSERT_EQUAL(result, EPollResult::Idle);
        }

        Y_UNIT_TEST(NullStealIteratorDoesNotCrash) {
            TSlot slot;
            ActivateSlot(slot);
            slot.AcquireForWorker();

            EPollResult result = PollSlot(slot);

            UNIT_ASSERT_EQUAL(result, EPollResult::Idle);
        }

        Y_UNIT_TEST(QueueItemConsumedWithNullDeps) {
            // Push an item. With null deps, PollSlot pops it (no execution).
            // Item is consumed and not re-queued (HasMore=false).
            TSlot slot;
            ActivateSlot(slot);
            slot.AcquireForWorker();

            slot.Push(42);
            UNIT_ASSERT_VALUES_EQUAL(slot.SizeEstimate(), 1u);

            PollSlot(slot);

            // Item was popped from queue
            UNIT_ASSERT(!slot.Pop().has_value());
            // Ring is empty (no execution = no continuation)
            UNIT_ASSERT(slot.GetPollState().Ring.Empty());
        }

        Y_UNIT_TEST(RingItemConsumedWithNullDeps) {
            TSlot slot;
            ActivateSlot(slot);
            slot.AcquireForWorker();

            slot.GetPollState().Ring.Push(7);

            PollSlot(slot);

            // Ring item was popped and consumed
            UNIT_ASSERT(slot.GetPollState().Ring.Empty());
            UNIT_ASSERT(!slot.Pop().has_value());
        }

        Y_UNIT_TEST(ConsecutiveIdleIncrementsOnEmptySlot) {
            TSlot slot;
            ActivateSlot(slot);
            slot.AcquireForWorker();

            PollSlot(slot);
            UNIT_ASSERT_VALUES_EQUAL(slot.GetPollState().ConsecutiveIdle, 1u);

            PollSlot(slot);
            UNIT_ASSERT_VALUES_EQUAL(slot.GetPollState().ConsecutiveIdle, 2u);

            PollSlot(slot);
            UNIT_ASSERT_VALUES_EQUAL(slot.GetPollState().ConsecutiveIdle, 3u);
        }

        Y_UNIT_TEST(StealBackoffRespectsStarvationGuardLimit) {
            TSlot slotA;
            ActivateSlot(slotA);

            TSlot slotB;
            ActivateSlot(slotB);

            // Put work into slotB and mark it as executing
            for (ui32 i = 0; i < 5; ++i) {
                slotB.Push(100 + i);
            }
            slotB.Executing.store(true, std::memory_order_relaxed);

            std::vector<TSlot*> neighbors = {&slotB};
            TTestStealIterator iter(neighbors);

            TWsConfig config;
            config.StarvationGuardLimit = 3;
            slotA.Config = &config;
            slotA.AcquireForWorker();

            // First 2 idle polls should NOT steal
            PollSlot(slotA, &iter);
            UNIT_ASSERT_VALUES_EQUAL(
                slotA.Counters.StealAttempts.load(std::memory_order_relaxed), 0u);

            PollSlot(slotA, &iter);
            UNIT_ASSERT_VALUES_EQUAL(
                slotA.Counters.StealAttempts.load(std::memory_order_relaxed), 0u);

            // 3rd idle poll SHOULD attempt to steal
            PollSlot(slotA, &iter);
            UNIT_ASSERT(
                slotA.Counters.StealAttempts.load(std::memory_order_relaxed) > 0);
        }

        Y_UNIT_TEST(StealIteratorExhausted) {
            TSlot slotA;
            ActivateSlot(slotA);

            // Iterator that yields nothing
            std::vector<TSlot*> empty;
            TTestStealIterator iter(empty);

            TWsConfig config;
            config.StarvationGuardLimit = 1;
            slotA.Config = &config;
            slotA.AcquireForWorker();
            EPollResult result = PollSlot(slotA, &iter);

            UNIT_ASSERT_EQUAL(result, EPollResult::Idle);
        }

        Y_UNIT_TEST(IdlePollCounterIncrements) {
            TSlot slot;
            ActivateSlot(slot);
            slot.AcquireForWorker();

            PollSlot(slot);
            UNIT_ASSERT_VALUES_EQUAL(
                slot.Counters.IdlePolls.load(std::memory_order_relaxed), 1u);

            PollSlot(slot);
            UNIT_ASSERT_VALUES_EQUAL(
                slot.Counters.IdlePolls.load(std::memory_order_relaxed), 2u);
        }

        Y_UNIT_TEST(ContinuationCountUpdatedAfterPoll) {
            TSlot slot;
            ActivateSlot(slot);
            slot.AcquireForWorker();

            slot.GetPollState().Ring.Push(1);
            slot.GetPollState().Ring.Push(2);

            // After PollSlot with null deps, ring items are consumed (no continuation)
            PollSlot(slot);

            // ContinuationCount should reflect post-execution ring state
            uint8_t contCount = slot.ContinuationCount.load(std::memory_order_relaxed);
            UNIT_ASSERT_VALUES_EQUAL(contCount, slot.GetPollState().Ring.Size());
        }

    } // Y_UNIT_TEST_SUITE(WsPoll)

} // namespace NActors::NWorkStealing
