#include <ydb/library/actors/core/workstealing/harmonizer_adapter.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NActors::NWorkStealing {

    Y_UNIT_TEST_SUITE(HarmonizerAdapter) {

        Y_UNIT_TEST(GetSlotCpuConsumptionDefault) {
            TSlot slots[2];
            THarmonizerAdapter adapter(slots, 2);

            auto c = adapter.GetSlotCpuConsumption(0);
            UNIT_ASSERT_DOUBLES_EQUAL(c.CpuUs, 0.0, 1e-9);
            UNIT_ASSERT_DOUBLES_EQUAL(c.ElapsedUs, 0.0, 1e-9);

            c = adapter.GetSlotCpuConsumption(1);
            UNIT_ASSERT_DOUBLES_EQUAL(c.CpuUs, 0.0, 1e-9);
            UNIT_ASSERT_DOUBLES_EQUAL(c.ElapsedUs, 0.0, 1e-9);
        }

        Y_UNIT_TEST(GetSlotCpuConsumptionWithLoad) {
            TSlot slots[1];
            slots[0].Stats.BusyCycles.store(500, std::memory_order_relaxed);
            slots[0].Stats.IdleCycles.store(500, std::memory_order_relaxed);
            THarmonizerAdapter adapter(slots, 1);

            auto c = adapter.GetSlotCpuConsumption(0);
            // CpuUs derived from BusyCycles, ElapsedUs from BusyCycles + IdleCycles
            UNIT_ASSERT(c.CpuUs > 0.0);
            UNIT_ASSERT(c.ElapsedUs > 0.0);
            // Elapsed should be approximately twice the CPU time
            UNIT_ASSERT_DOUBLES_EQUAL(c.ElapsedUs, c.CpuUs * 2.0, c.CpuUs * 0.01);
        }

        Y_UNIT_TEST(GetSlotCpuConsumptionOutOfRange) {
            TSlot slots[2];
            slots[0].Stats.BusyCycles.store(100, std::memory_order_relaxed);
            THarmonizerAdapter adapter(slots, 2);

            // Negative index
            auto c = adapter.GetSlotCpuConsumption(-1);
            UNIT_ASSERT_DOUBLES_EQUAL(c.CpuUs, 0.0, 1e-9);
            UNIT_ASSERT_DOUBLES_EQUAL(c.ElapsedUs, 0.0, 1e-9);

            // Beyond max
            c = adapter.GetSlotCpuConsumption(2);
            UNIT_ASSERT_DOUBLES_EQUAL(c.CpuUs, 0.0, 1e-9);
            UNIT_ASSERT_DOUBLES_EQUAL(c.ElapsedUs, 0.0, 1e-9);
        }

        Y_UNIT_TEST(CollectPoolStats) {
            TSlot slots[3];
            slots[0].Stats.ActivationsExecuted = 10;
            slots[0].Stats.BusyCycles.store(100, std::memory_order_relaxed);
            slots[0].Stats.IdleCycles.store(200, std::memory_order_relaxed);
            slots[0].Stats.ExecTimeAccumNs = 1000;

            slots[1].Stats.ActivationsExecuted = 20;
            slots[1].Stats.BusyCycles.store(300, std::memory_order_relaxed);
            slots[1].Stats.IdleCycles.store(400, std::memory_order_relaxed);
            slots[1].Stats.ExecTimeAccumNs = 2000;

            slots[2].Stats.ActivationsExecuted = 50;
            slots[2].Stats.BusyCycles.store(999, std::memory_order_relaxed);
            slots[2].Stats.IdleCycles.store(999, std::memory_order_relaxed);
            slots[2].Stats.ExecTimeAccumNs = 9999;

            THarmonizerAdapter adapter(slots, 3);

            // Only aggregate first 2 active slots
            TSlotStats agg;
            adapter.CollectPoolStats(agg, 2);

            UNIT_ASSERT_EQUAL(agg.ActivationsExecuted, 30u);
            UNIT_ASSERT_EQUAL(agg.BusyCycles.load(std::memory_order_relaxed), 400u);
            UNIT_ASSERT_EQUAL(agg.IdleCycles.load(std::memory_order_relaxed), 600u);
            UNIT_ASSERT_EQUAL(agg.ExecTimeAccumNs, 3000u);
        }

        Y_UNIT_TEST(CollectPoolStatsAllSlots) {
            TSlot slots[2];
            slots[0].Stats.ActivationsExecuted = 5;
            slots[0].Stats.BusyCycles.store(50, std::memory_order_relaxed);
            slots[0].Stats.IdleCycles.store(50, std::memory_order_relaxed);
            slots[0].Stats.ExecTimeAccumNs = 500;

            slots[1].Stats.ActivationsExecuted = 15;
            slots[1].Stats.BusyCycles.store(150, std::memory_order_relaxed);
            slots[1].Stats.IdleCycles.store(150, std::memory_order_relaxed);
            slots[1].Stats.ExecTimeAccumNs = 1500;

            THarmonizerAdapter adapter(slots, 2);

            TSlotStats agg;
            adapter.CollectPoolStats(agg, 2);

            UNIT_ASSERT_EQUAL(agg.ActivationsExecuted, 20u);
            UNIT_ASSERT_EQUAL(agg.BusyCycles.load(std::memory_order_relaxed), 200u);
            UNIT_ASSERT_EQUAL(agg.IdleCycles.load(std::memory_order_relaxed), 200u);
            UNIT_ASSERT_EQUAL(agg.ExecTimeAccumNs, 2000u);
        }

        Y_UNIT_TEST(UpdateLoadEstimates) {
            TSlot slots[2];
            slots[0].Stats.BusyCycles.store(750, std::memory_order_relaxed);
            slots[0].Stats.IdleCycles.store(250, std::memory_order_relaxed);

            slots[1].Stats.BusyCycles.store(200, std::memory_order_relaxed);
            slots[1].Stats.IdleCycles.store(800, std::memory_order_relaxed);

            THarmonizerAdapter adapter(slots, 2);
            adapter.UpdateLoadEstimates(2);

            double load0 = slots[0].LoadEstimate.load(std::memory_order_relaxed);
            UNIT_ASSERT_DOUBLES_EQUAL(load0, 0.75, 1e-9);

            double load1 = slots[1].LoadEstimate.load(std::memory_order_relaxed);
            UNIT_ASSERT_DOUBLES_EQUAL(load1, 0.2, 1e-9);
        }

        Y_UNIT_TEST(UpdateLoadEstimatesZeroCycles) {
            TSlot slots[1];
            // Both cycles zero — load should be 0.0

            THarmonizerAdapter adapter(slots, 1);
            adapter.UpdateLoadEstimates(1);

            double load = slots[0].LoadEstimate.load(std::memory_order_relaxed);
            UNIT_ASSERT_DOUBLES_EQUAL(load, 0.0, 1e-9);
        }

        Y_UNIT_TEST(UpdateLoadEstimatesFullLoad) {
            TSlot slots[1];
            slots[0].Stats.BusyCycles.store(1000, std::memory_order_relaxed);

            THarmonizerAdapter adapter(slots, 1);
            adapter.UpdateLoadEstimates(1);

            double load = slots[0].LoadEstimate.load(std::memory_order_relaxed);
            UNIT_ASSERT_DOUBLES_EQUAL(load, 1.0, 1e-9);
        }

        Y_UNIT_TEST(ResetCounters) {
            TSlot slots[2];
            slots[0].Stats.BusyCycles.store(100, std::memory_order_relaxed);
            slots[0].Stats.IdleCycles.store(200, std::memory_order_relaxed);
            slots[0].Stats.ActivationsExecuted = 50;
            slots[0].Stats.ExecTimeAccumNs = 999;

            slots[1].Stats.BusyCycles.store(300, std::memory_order_relaxed);
            slots[1].Stats.IdleCycles.store(400, std::memory_order_relaxed);
            slots[1].Stats.ActivationsExecuted = 70;
            slots[1].Stats.ExecTimeAccumNs = 777;

            THarmonizerAdapter adapter(slots, 2);
            adapter.ResetCounters(2);

            // BusyCycles and IdleCycles should be zeroed
            UNIT_ASSERT_EQUAL(slots[0].Stats.BusyCycles.load(std::memory_order_relaxed), 0u);
            UNIT_ASSERT_EQUAL(slots[0].Stats.IdleCycles.load(std::memory_order_relaxed), 0u);
            UNIT_ASSERT_EQUAL(slots[1].Stats.BusyCycles.load(std::memory_order_relaxed), 0u);
            UNIT_ASSERT_EQUAL(slots[1].Stats.IdleCycles.load(std::memory_order_relaxed), 0u);

            // ActivationsExecuted and ExecTimeAccumNs are not reset
            UNIT_ASSERT_EQUAL(slots[0].Stats.ActivationsExecuted, 50u);
            UNIT_ASSERT_EQUAL(slots[0].Stats.ExecTimeAccumNs, 999u);
            UNIT_ASSERT_EQUAL(slots[1].Stats.ActivationsExecuted, 70u);
            UNIT_ASSERT_EQUAL(slots[1].Stats.ExecTimeAccumNs, 777u);
        }

        Y_UNIT_TEST(NullSlots) {
            THarmonizerAdapter adapter(nullptr, 0);

            auto c = adapter.GetSlotCpuConsumption(0);
            UNIT_ASSERT_DOUBLES_EQUAL(c.CpuUs, 0.0, 1e-9);
            UNIT_ASSERT_DOUBLES_EQUAL(c.ElapsedUs, 0.0, 1e-9);

            TSlotStats agg;
            adapter.CollectPoolStats(agg, 0);
            UNIT_ASSERT_EQUAL(agg.ActivationsExecuted, 0u);

            // Should not crash
            adapter.UpdateLoadEstimates(0);
            adapter.ResetCounters(0);
        }

    } // Y_UNIT_TEST_SUITE(HarmonizerAdapter)

} // namespace NActors::NWorkStealing
