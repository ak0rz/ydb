#include <ydb/library/actors/core/workstealing/executor_pool_ws.h>
#include <ydb/library/actors/core/workstealing/ws_slot.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NActors::NWorkStealing {

    Y_UNIT_TEST_SUITE(ExecutorPoolWs) {

        // --- Helpers ---

        static TWSExecutorPoolConfig MakeConfig(
            ui32 poolId = 0,
            const TString& name = "ws-test",
            i16 minSlots = 1,
            i16 maxSlots = 8,
            i16 defaultSlots = 4)
        {
            TWSExecutorPoolConfig cfg;
            cfg.PoolId = poolId;
            cfg.PoolName = name;
            cfg.MinSlotCount = minSlots;
            cfg.MaxSlotCount = maxSlots;
            cfg.DefaultSlotCount = defaultSlots;
            cfg.TimePerMailbox = TDuration::MilliSeconds(10);
            cfg.EventsPerMailbox = 100;
            cfg.Priority = 0;
            return cfg;
        }

        // Minimal IDriver stub for testing (does nothing)
        class TStubDriver: public IDriver {
        public:
            void Prepare(const TCpuTopology& /*topology*/) override {
            }
            void Start() override {
            }
            void PrepareStop() override {
            }
            void Shutdown() override {
            }

            void RegisterSlots(TSlot* /*slots*/, size_t count) override {
                RegisteredSlots += static_cast<int>(count);
            }
            void ActivateSlot(TSlot* slot) override {
                slot->TryTransition(ESlotState::Inactive, ESlotState::Initializing);
                slot->TryTransition(ESlotState::Initializing, ESlotState::Active);
                ActivatedSlots++;
            }
            void DeactivateSlot(TSlot* slot) override {
                slot->TryTransition(ESlotState::Active, ESlotState::Draining);
                slot->TryTransition(ESlotState::Draining, ESlotState::Inactive);
                DeactivatedSlots++;
            }
            void WakeSlot(TSlot* /*slot*/) override {
                WakeCount++;
            }
            void SetWorkerCallbacks(TSlot* /*slot*/, TWorkerCallbacks /*callbacks*/) override {
            }
            std::unique_ptr<IStealIterator> MakeStealIterator(TSlot* /*exclude*/) override {
                return nullptr;
            }

            int RegisteredSlots = 0;
            int ActivatedSlots = 0;
            int DeactivatedSlots = 0;
            int WakeCount = 0;
        };

        // --- Tests ---

        Y_UNIT_TEST(PoolCreation) {
            auto cfg = MakeConfig(5, "my-pool", 2, 16, 8);
            TWSExecutorPool pool(cfg);

            UNIT_ASSERT_VALUES_EQUAL(pool.PoolId, 5u);
            UNIT_ASSERT_VALUES_EQUAL(pool.GetName(), "my-pool");
            UNIT_ASSERT_VALUES_EQUAL(pool.GetSlotCount(), 16u);
            UNIT_ASSERT_VALUES_EQUAL(pool.GetMinFullThreadCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(pool.GetMaxFullThreadCount(), 16);
            UNIT_ASSERT_VALUES_EQUAL(pool.GetDefaultFullThreadCount(), 8);
            UNIT_ASSERT_VALUES_EQUAL(pool.GetPriority(), 0);
        }

        Y_UNIT_TEST(GetReadyActivationAlwaysNull) {
            auto cfg = MakeConfig();
            TWSExecutorPool pool(cfg);

            UNIT_ASSERT(pool.GetReadyActivation(0) == nullptr);
            UNIT_ASSERT(pool.GetReadyActivation(42) == nullptr);
            UNIT_ASSERT(pool.GetReadyActivation(UINT64_MAX) == nullptr);
        }

        Y_UNIT_TEST(ThreadCountDefaults) {
            auto cfg = MakeConfig(0, "tc", 2, 32, 8);
            TWSExecutorPool pool(cfg);

            UNIT_ASSERT_DOUBLES_EQUAL(pool.GetMinThreadCount(), 2.0f, 0.01f);
            UNIT_ASSERT_DOUBLES_EQUAL(pool.GetMaxThreadCount(), 32.0f, 0.01f);
            UNIT_ASSERT_DOUBLES_EQUAL(pool.GetDefaultThreadCount(), 8.0f, 0.01f);
        }

        Y_UNIT_TEST(TimePerMailbox) {
            auto cfg = MakeConfig();
            cfg.TimePerMailbox = TDuration::MilliSeconds(5);
            cfg.EventsPerMailbox = 200;
            TWSExecutorPool pool(cfg);

            UNIT_ASSERT(pool.TimePerMailboxTs() > 0);
            UNIT_ASSERT_VALUES_EQUAL(pool.EventsPerMailbox(), 200u);
        }

        Y_UNIT_TEST(AffinityIsNull) {
            auto cfg = MakeConfig();
            TWSExecutorPool pool(cfg);

            UNIT_ASSERT(pool.Affinity() == nullptr);
        }

        Y_UNIT_TEST(SetDriverAndPrepare) {
            auto cfg = MakeConfig(0, "prep", 1, 8, 4);
            TWSExecutorPool pool(cfg);
            TStubDriver driver;
            pool.SetDriver(&driver);

            NSchedulerQueue::TReader* readers = nullptr;
            ui32 readerCount = 0;
            pool.Prepare(nullptr, &readers, &readerCount);

            // Should have allocated schedule readers
            UNIT_ASSERT(readers != nullptr);
            UNIT_ASSERT_VALUES_EQUAL(readerCount, 8u);

            // Driver should have registered all 8 slots
            UNIT_ASSERT_VALUES_EQUAL(driver.RegisteredSlots, 8);

            // Driver should have activated DefaultSlotCount=4 slots
            UNIT_ASSERT_VALUES_EQUAL(driver.ActivatedSlots, 4);

            // Active slot count should be default
            UNIT_ASSERT_VALUES_EQUAL(pool.GetFullThreadCount(), 4);
        }

        Y_UNIT_TEST(SetFullThreadCountClamps) {
            auto cfg = MakeConfig(0, "clamp", 2, 8, 4);
            TWSExecutorPool pool(cfg);
            TStubDriver driver;
            pool.SetDriver(&driver);

            NSchedulerQueue::TReader* readers = nullptr;
            ui32 readerCount = 0;
            pool.Prepare(nullptr, &readers, &readerCount);

            // Try to set below min
            pool.SetFullThreadCount(0);
            UNIT_ASSERT_VALUES_EQUAL(pool.GetFullThreadCount(), 2);

            // Try to set above max
            pool.SetFullThreadCount(100);
            UNIT_ASSERT_VALUES_EQUAL(pool.GetFullThreadCount(), 8);

            // Set to a valid value
            pool.SetFullThreadCount(6);
            UNIT_ASSERT_VALUES_EQUAL(pool.GetFullThreadCount(), 6);
        }

        Y_UNIT_TEST(ScheduleActivationRoutes) {
            auto cfg = MakeConfig(0, "route", 1, 4, 2);
            TWSExecutorPool pool(cfg);
            TStubDriver driver;
            pool.SetDriver(&driver);

            NSchedulerQueue::TReader* readers = nullptr;
            ui32 readerCount = 0;
            pool.Prepare(nullptr, &readers, &readerCount);

            // Create a mailbox-like object on the stack for testing.
            // We only need Hint and LastPoolSlotIdx fields.
            TMailbox mailbox;
            mailbox.Hint = 42;
            mailbox.LastPoolSlotIdx = 0; // fresh

            // Schedule activation -- should route to one of the active slots
            pool.ScheduleActivation(&mailbox);

            // Verify the activation landed in one of the active slots
            TSlot* slots = pool.GetSlots();
            bool found = false;
            for (size_t i = 0; i < pool.GetSlotCount(); ++i) {
                if (slots[i].GetState() == ESlotState::Active) {
                    size_t drained = slots[i].DrainInjectionQueue(64);
                    if (drained > 0) {
                        auto item = slots[i].PopActivation();
                        if (item.has_value() && *item == 42u) {
                            found = true;
                            break;
                        }
                    }
                }
            }
            UNIT_ASSERT(found);
        }

        Y_UNIT_TEST(ScheduleActivationStickyRouting) {
            auto cfg = MakeConfig(0, "sticky", 1, 4, 4);
            TWSExecutorPool pool(cfg);
            TStubDriver driver;
            pool.SetDriver(&driver);

            NSchedulerQueue::TReader* readers = nullptr;
            ui32 readerCount = 0;
            pool.Prepare(nullptr, &readers, &readerCount);

            TMailbox mailbox;
            mailbox.Hint = 99;
            mailbox.LastPoolSlotIdx = 3; // 1-based => slot index 2

            pool.ScheduleActivation(&mailbox);

            // Should route to slot 2 (sticky)
            TSlot* slots = pool.GetSlots();
            size_t drained = slots[2].DrainInjectionQueue(64);
            UNIT_ASSERT(drained > 0);
            auto item = slots[2].PopActivation();
            UNIT_ASSERT(item.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*item, 99u);
        }

        Y_UNIT_TEST(WakeOnSlotZero) {
            auto cfg = MakeConfig(0, "wake", 1, 4, 4);
            TWSExecutorPool pool(cfg);
            TStubDriver driver;
            pool.SetDriver(&driver);

            NSchedulerQueue::TReader* readers = nullptr;
            ui32 readerCount = 0;
            pool.Prepare(nullptr, &readers, &readerCount);

            int wakesBefore = driver.WakeCount;

            // Force routing to slot 0: set LastPoolSlotIdx=1 (1-based => slot 0)
            TMailbox mailbox;
            mailbox.Hint = 7;
            mailbox.LastPoolSlotIdx = 1; // sticky to slot 0

            pool.ScheduleActivation(&mailbox);

            // Should have woken the driver
            UNIT_ASSERT(driver.WakeCount > wakesBefore);
        }

        Y_UNIT_TEST(LifecycleNoDriver) {
            // Verify lifecycle works without a driver (no crash)
            auto cfg = MakeConfig(0, "nodriver", 1, 4, 2);
            TWSExecutorPool pool(cfg);

            NSchedulerQueue::TReader* readers = nullptr;
            ui32 readerCount = 0;
            pool.Prepare(nullptr, &readers, &readerCount);

            pool.Start();
            pool.PrepareStop();
            pool.Shutdown();

            // Should not crash
            UNIT_ASSERT(true);
        }

        Y_UNIT_TEST(LifecycleWithDriver) {
            auto cfg = MakeConfig(0, "driver", 1, 4, 2);
            TWSExecutorPool pool(cfg);
            TStubDriver driver;
            pool.SetDriver(&driver);

            NSchedulerQueue::TReader* readers = nullptr;
            ui32 readerCount = 0;
            pool.Prepare(nullptr, &readers, &readerCount);

            pool.Start();
            pool.PrepareStop();
            pool.Shutdown();

            // PrepareStop should have deactivated the 2 active slots
            UNIT_ASSERT(driver.DeactivatedSlots >= 2);
            UNIT_ASSERT_VALUES_EQUAL(pool.GetFullThreadCount(), 0);
        }

        Y_UNIT_TEST(GetThreadCpuConsumption) {
            auto cfg = MakeConfig(0, "cpu", 1, 4, 4);
            TWSExecutorPool pool(cfg);
            TStubDriver driver;
            pool.SetDriver(&driver);

            NSchedulerQueue::TReader* readers = nullptr;
            ui32 readerCount = 0;
            pool.Prepare(nullptr, &readers, &readerCount);

            // Set load estimate on slot 1
            TSlot* slots = pool.GetSlots();
            slots[1].LoadEstimate.store(0.75, std::memory_order_relaxed);

            auto consumption = pool.GetThreadCpuConsumption(1);
            UNIT_ASSERT_DOUBLES_EQUAL(consumption.CpuUs, 0.75, 0.01);
            UNIT_ASSERT_DOUBLES_EQUAL(consumption.ElapsedUs, 0.75, 0.01);

            // Out-of-range index returns zero
            auto zero = pool.GetThreadCpuConsumption(-1);
            UNIT_ASSERT_DOUBLES_EQUAL(zero.CpuUs, 0.0, 0.01);
            auto zero2 = pool.GetThreadCpuConsumption(100);
            UNIT_ASSERT_DOUBLES_EQUAL(zero2.CpuUs, 0.0, 0.01);
        }

        Y_UNIT_TEST(SpecificScheduleActivationRoutes) {
            auto cfg = MakeConfig(0, "specific", 1, 4, 2);
            TWSExecutorPool pool(cfg);
            TStubDriver driver;
            pool.SetDriver(&driver);

            NSchedulerQueue::TReader* readers = nullptr;
            ui32 readerCount = 0;
            pool.Prepare(nullptr, &readers, &readerCount);

            TMailbox mailbox;
            mailbox.Hint = 55;
            mailbox.LastPoolSlotIdx = 0;

            pool.SpecificScheduleActivation(&mailbox);

            // Verify the activation landed somewhere
            TSlot* slots = pool.GetSlots();
            bool found = false;
            for (size_t i = 0; i < pool.GetSlotCount(); ++i) {
                if (slots[i].GetState() == ESlotState::Active) {
                    size_t drained = slots[i].DrainInjectionQueue(64);
                    if (drained > 0) {
                        auto item = slots[i].PopActivation();
                        if (item.has_value() && *item == 55u) {
                            found = true;
                            break;
                        }
                    }
                }
            }
            UNIT_ASSERT(found);
        }

        Y_UNIT_TEST(ScheduleActivationExRoutes) {
            auto cfg = MakeConfig(0, "ex", 1, 4, 2);
            TWSExecutorPool pool(cfg);
            TStubDriver driver;
            pool.SetDriver(&driver);

            NSchedulerQueue::TReader* readers = nullptr;
            ui32 readerCount = 0;
            pool.Prepare(nullptr, &readers, &readerCount);

            TMailbox mailbox;
            mailbox.Hint = 77;
            mailbox.LastPoolSlotIdx = 0;

            pool.ScheduleActivationEx(&mailbox, 123);

            // Verify the activation landed somewhere
            TSlot* slots = pool.GetSlots();
            bool found = false;
            for (size_t i = 0; i < pool.GetSlotCount(); ++i) {
                if (slots[i].GetState() == ESlotState::Active) {
                    size_t drained = slots[i].DrainInjectionQueue(64);
                    if (drained > 0) {
                        auto item = slots[i].PopActivation();
                        if (item.has_value() && *item == 77u) {
                            found = true;
                            break;
                        }
                    }
                }
            }
            UNIT_ASSERT(found);
        }

        Y_UNIT_TEST(SetFullThreadCountActivatesDeactivates) {
            auto cfg = MakeConfig(0, "resize", 1, 8, 2);
            TWSExecutorPool pool(cfg);
            TStubDriver driver;
            pool.SetDriver(&driver);

            NSchedulerQueue::TReader* readers = nullptr;
            ui32 readerCount = 0;
            pool.Prepare(nullptr, &readers, &readerCount);

            // Initially 2 activated
            UNIT_ASSERT_VALUES_EQUAL(driver.ActivatedSlots, 2);
            UNIT_ASSERT_VALUES_EQUAL(pool.GetFullThreadCount(), 2);
            UNIT_ASSERT_DOUBLES_EQUAL(pool.GetThreadCount(), 2.0f, 0.01f);
            UNIT_ASSERT_VALUES_EQUAL(pool.GetThreads(), 2u);

            // Scale up to 6
            pool.SetFullThreadCount(6);
            UNIT_ASSERT_VALUES_EQUAL(pool.GetFullThreadCount(), 6);
            // 2 initial + 4 new = 6 total activations
            UNIT_ASSERT_VALUES_EQUAL(driver.ActivatedSlots, 6);

            // Scale down to 3
            pool.SetFullThreadCount(3);
            UNIT_ASSERT_VALUES_EQUAL(pool.GetFullThreadCount(), 3);
            UNIT_ASSERT(driver.DeactivatedSlots >= 3);
        }

    } // Y_UNIT_TEST_SUITE(ExecutorPoolWs)

} // namespace NActors::NWorkStealing
