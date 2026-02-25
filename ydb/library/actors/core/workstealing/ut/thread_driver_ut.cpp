#include <ydb/library/actors/core/workstealing/thread_driver.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>

#include <atomic>
#include <vector>

namespace NActors::NWorkStealing {

    Y_UNIT_TEST_SUITE(ThreadDriver) {

        Y_UNIT_TEST(CreateAndDestroy) {
            TWsConfig config;
            TThreadDriver driver(config);
            auto topology = TCpuTopology::MakeFlat(4);
            driver.Prepare(topology);
            // Shutdown without starting -- should not crash.
            driver.Shutdown();
        }

        Y_UNIT_TEST(RegisterSlotAndActivate) {
            TWsConfig config;
            TThreadDriver driver(config);
            auto topology = TCpuTopology::MakeFlat(4);
            driver.Prepare(topology);

            TSlot slot;
            driver.RegisterSlots(&slot, 1);

            driver.Start();

            driver.ActivateSlot(&slot);
            Sleep(TDuration::MilliSeconds(50));

            UNIT_ASSERT_EQUAL(slot.GetState(), ESlotState::Active);

            driver.PrepareStop();
            driver.Shutdown();
        }

        Y_UNIT_TEST(WorkerProcessesActivation) {
            TWsConfig config;
            config.SpinThresholdCycles = 1000000; // high threshold to keep worker spinning
            TThreadDriver driver(config);
            auto topology = TCpuTopology::MakeFlat(4);
            driver.Prepare(topology);

            TSlot slot;
            driver.RegisterSlots(&slot, 1);

            driver.Start();
            driver.ActivateSlot(&slot);

            // Give the worker time to start polling
            Sleep(TDuration::MilliSeconds(50));

            // Inject an activation and wake the worker (it may have parked).
            // With null ctx, PollSlot pops the hint but skips execution.
            // The item is consumed (not re-queued), verifying the poll loop runs.
            slot.Push(42);
            driver.WakeSlot(&slot);

            // Wait for worker to consume it
            for (int i = 0; i < 100; ++i) {
                if (slot.SizeEstimate() == 0) {
                    break;
                }
                Sleep(TDuration::MilliSeconds(10));
            }

            UNIT_ASSERT_EQUAL(slot.SizeEstimate(), 0);

            driver.PrepareStop();
            driver.Shutdown();
        }

        Y_UNIT_TEST(DeactivateSlot) {
            TWsConfig config;
            TThreadDriver driver(config);
            auto topology = TCpuTopology::MakeFlat(4);
            driver.Prepare(topology);

            TSlot slot;
            driver.RegisterSlots(&slot, 1);

            driver.Start();

            driver.ActivateSlot(&slot);
            Sleep(TDuration::MilliSeconds(50));

            UNIT_ASSERT_EQUAL(slot.GetState(), ESlotState::Active);

            driver.DeactivateSlot(&slot);
            UNIT_ASSERT_EQUAL(slot.GetState(), ESlotState::Draining);

            driver.PrepareStop();
            driver.Shutdown();
        }

        Y_UNIT_TEST(StealIterator) {
            TSlot slots[3];
            std::vector<TSlot*> slotPtrs = {&slots[0], &slots[1], &slots[2]};

            // Exclude slot 1, maxProbe=10 (larger than neighbor count)
            // Circular from slot 1: starts at slot 2, then slot 0
            TTopologyStealIterator iter(slotPtrs, &slots[1], 10);

            TSlot* first = iter.Next();
            TSlot* second = iter.Next();
            TSlot* third = iter.Next();

            UNIT_ASSERT_EQUAL(first, &slots[2]);
            UNIT_ASSERT_EQUAL(second, &slots[0]);
            UNIT_ASSERT_EQUAL(third, nullptr);

            // After reset, rotates by probed count (2), which wraps on size 2 → same order
            iter.Reset();
            UNIT_ASSERT_EQUAL(iter.Next(), &slots[2]);
            UNIT_ASSERT_EQUAL(iter.Next(), &slots[0]);
            UNIT_ASSERT_EQUAL(iter.Next(), nullptr);
        }

        Y_UNIT_TEST(StealIteratorMaxProbe) {
            TSlot slots[5];
            std::vector<TSlot*> slotPtrs;
            for (auto& s : slots) {
                slotPtrs.push_back(&s);
            }

            // Exclude slot 0, maxProbe=2: only scan 2 of 4 neighbors
            TTopologyStealIterator iter(slotPtrs, &slots[0], 2);

            UNIT_ASSERT(iter.Next() != nullptr);
            UNIT_ASSERT(iter.Next() != nullptr);
            UNIT_ASSERT_EQUAL(iter.Next(), nullptr); // stopped at maxProbe

            // After reset, rotates starting position so we scan different neighbors
            iter.Reset();
            TSlot* a = iter.Next();
            TSlot* b = iter.Next();
            UNIT_ASSERT(a != nullptr);
            UNIT_ASSERT(b != nullptr);
            UNIT_ASSERT_EQUAL(iter.Next(), nullptr);
        }

        Y_UNIT_TEST(GracefulShutdown) {
            TWsConfig config;
            TThreadDriver driver(config);
            auto topology = TCpuTopology::MakeFlat(4);
            driver.Prepare(topology);

            TSlot slots[2];
            driver.RegisterSlots(slots, 2);

            driver.Start();

            driver.ActivateSlot(&slots[0]);
            driver.ActivateSlot(&slots[1]);
            Sleep(TDuration::MilliSeconds(50));

            driver.PrepareStop();
            driver.Shutdown();
            // If we reach here without hanging, threads joined cleanly.
        }

        Y_UNIT_TEST(WakeUnparksWorker) {
            TWsConfig config;
            config.SpinThresholdCycles = 100; // low threshold so worker parks quickly
            TThreadDriver driver(config);
            auto topology = TCpuTopology::MakeFlat(4);
            driver.Prepare(topology);

            TSlot slot;
            driver.RegisterSlots(&slot, 1);

            driver.Start();
            driver.ActivateSlot(&slot);

            // Wait for worker to park (it should park quickly with low spin threshold)
            Sleep(TDuration::MilliSeconds(100));

            // Inject work and wake the driver
            slot.Push(99);
            driver.WakeSlot(&slot);

            // Wait for the worker to consume the activation
            for (int i = 0; i < 100; ++i) {
                if (slot.SizeEstimate() == 0) {
                    break;
                }
                Sleep(TDuration::MilliSeconds(10));
            }

            UNIT_ASSERT_EQUAL(slot.SizeEstimate(), 0);

            driver.PrepareStop();
            driver.Shutdown();
        }

    } // Y_UNIT_TEST_SUITE(ThreadDriver)

} // namespace NActors::NWorkStealing
