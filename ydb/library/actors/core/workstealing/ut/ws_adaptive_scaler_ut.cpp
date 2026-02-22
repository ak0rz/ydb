#include <ydb/library/actors/core/workstealing/ws_adaptive_scaler.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NActors::NWorkStealing {

    namespace {

        // Test helper: creates a scaler with controllable active count and
        // records SetFullThreadCount calls.
        struct TTestHarness {
            TSlot Slots[512];
            TWsConfig Config;
            i16 ActiveCount = 8;
            i16 LastSetCount = -1;
            int SetCountCalls = 0;

            TTestHarness() {
                Config.AdaptiveScaling = true;
                Config.AdaptiveEvalCycles = 0;       // disable timing guard
                Config.AdaptiveCooldownCycles = 0;    // disable cooldown
            }

            std::unique_ptr<TAdaptiveScaler> MakeScaler() {
                return std::make_unique<TAdaptiveScaler>(
                    [this](i16 count) {
                        LastSetCount = count;
                        ActiveCount = count;
                        ++SetCountCalls;
                    },
                    [this]() -> i16 { return ActiveCount; },
                    Slots,
                    static_cast<i16>(sizeof(Slots) / sizeof(Slots[0])),
                    Config);
            }

            // Simulate slot utilization: set busy/idle so that util = busyFraction
            void SetSlotUtil(i16 slotIdx, double busyFraction, uint64_t totalCycles = 10000) {
                uint64_t busy = static_cast<uint64_t>(totalCycles * busyFraction);
                uint64_t idle = totalCycles - busy;
                Slots[slotIdx].Stats.BusyCycles.store(busy, std::memory_order_relaxed);
                Slots[slotIdx].Stats.IdleCycles.store(idle, std::memory_order_relaxed);
            }

            // Set all active slots to the same utilization
            void SetAllUtil(double busyFraction, uint64_t totalCycles = 10000) {
                for (i16 i = 0; i < ActiveCount; ++i) {
                    SetSlotUtil(i, busyFraction, totalCycles);
                }
            }
        };

    } // namespace

    Y_UNIT_TEST_SUITE(AdaptiveScaler) {

        Y_UNIT_TEST(NoScalingWhenDisabled) {
            TTestHarness h;
            h.Config.AdaptiveScaling = false;
            h.SetAllUtil(0.0);
            auto scaler = h.MakeScaler();

            scaler->Evaluate();

            UNIT_ASSERT_EQUAL(h.SetCountCalls, 0);
            UNIT_ASSERT_EQUAL(scaler->InflateEvents(), 0u);
            UNIT_ASSERT_EQUAL(scaler->DeflateEvents(), 0u);
        }

        Y_UNIT_TEST(NoScalingInDeadband) {
            TTestHarness h;
            h.ActiveCount = 8;
            // 50% utilization — between deflate (0.3) and inflate (0.8) thresholds
            // Need 4 out of 8 slots busy (each slot >10% util)
            for (i16 i = 0; i < 4; ++i) {
                h.SetSlotUtil(i, 0.5);  // busy
            }
            for (i16 i = 4; i < 8; ++i) {
                h.SetSlotUtil(i, 0.0);  // idle
            }
            auto scaler = h.MakeScaler();

            scaler->Evaluate();

            UNIT_ASSERT_EQUAL(h.SetCountCalls, 0);
        }

        Y_UNIT_TEST(DeflateOnLowUtil) {
            TTestHarness h;
            h.ActiveCount = 8;
            // 1 busy slot out of 8 = 12.5% < 30% deflate threshold
            h.SetSlotUtil(0, 0.5);  // busy (>10%)
            for (i16 i = 1; i < 8; ++i) {
                h.SetSlotUtil(i, 0.0);  // idle
            }
            auto scaler = h.MakeScaler();

            scaler->Evaluate();

            UNIT_ASSERT_EQUAL(h.SetCountCalls, 1);
            // target = busySlots(1) + max(1, 1/4=0) = 1+1 = 2
            // halfCurrent = 8/2 = 4
            // newCount = max(2, 4) = 4
            UNIT_ASSERT_EQUAL(h.LastSetCount, 4);
            UNIT_ASSERT_EQUAL(scaler->DeflateEvents(), 1u);
            UNIT_ASSERT_EQUAL(scaler->InflateEvents(), 0u);
        }

        Y_UNIT_TEST(DeflateGeometric) {
            TTestHarness h;
            h.ActiveCount = 384;
            // 10 busy slots, rest idle
            for (i16 i = 0; i < 10; ++i) {
                h.SetSlotUtil(i, 0.5);
            }
            auto scaler = h.MakeScaler();

            // Step 1: 384 → should halve to 192
            scaler->Evaluate();
            UNIT_ASSERT_EQUAL(h.ActiveCount, 192);

            // Reset stats for next eval
            for (i16 i = 0; i < 10; ++i) {
                h.SetSlotUtil(i, 0.5);
            }

            // Step 2: 192 → 96
            scaler->Evaluate();
            UNIT_ASSERT_EQUAL(h.ActiveCount, 96);

            for (i16 i = 0; i < 10; ++i) {
                h.SetSlotUtil(i, 0.5);
            }

            // Step 3: 96 → 48
            scaler->Evaluate();
            UNIT_ASSERT_EQUAL(h.ActiveCount, 48);

            for (i16 i = 0; i < 10; ++i) {
                h.SetSlotUtil(i, 0.5);
            }

            // Step 4: 48 → 24
            scaler->Evaluate();
            UNIT_ASSERT_EQUAL(h.ActiveCount, 24);

            // Step 5: 24 active, 10 busy → fraction=10/24=0.417 > 0.3 → deadband, no change
            for (i16 i = 0; i < 10; ++i) {
                h.SetSlotUtil(i, 0.5);
            }
            scaler->Evaluate();
            UNIT_ASSERT_EQUAL(h.ActiveCount, 24);
        }

        Y_UNIT_TEST(InflateOnHighUtil) {
            TTestHarness h;
            h.ActiveCount = 4;
            // All 4 slots busy = 100% >= 80%
            h.SetAllUtil(0.9);
            auto scaler = h.MakeScaler();

            scaler->Evaluate();

            UNIT_ASSERT_EQUAL(h.SetCountCalls, 1);
            // growth = max(1, 4/4=1) = 1, newCount = 4+1 = 5
            UNIT_ASSERT_EQUAL(h.LastSetCount, 5);
            UNIT_ASSERT_EQUAL(scaler->InflateEvents(), 1u);
        }

        Y_UNIT_TEST(InflateOnQueuePressure) {
            TTestHarness h;
            h.ActiveCount = 4;
            // Low utilization but high queue depth
            h.SetAllUtil(0.05);
            // Simulate queue pressure by pushing items into a slot's queue
            for (uint32_t i = 0; i < 20; ++i) {
                h.Slots[0].Push(i);
            }
            auto scaler = h.MakeScaler();

            scaler->Evaluate();

            UNIT_ASSERT_EQUAL(h.SetCountCalls, 1);
            UNIT_ASSERT(h.LastSetCount > 4);
            UNIT_ASSERT_EQUAL(scaler->InflateEvents(), 1u);
        }

        Y_UNIT_TEST(CooldownPreventsRapidChange) {
            TTestHarness h;
            h.Config.AdaptiveCooldownCycles = 1000000000ULL;  // very long cooldown
            h.ActiveCount = 8;
            // All idle → should deflate
            h.SetAllUtil(0.0);
            auto scaler = h.MakeScaler();

            scaler->Evaluate();
            UNIT_ASSERT_EQUAL(h.SetCountCalls, 1);

            // Second call within cooldown — should be no-op
            h.SetAllUtil(0.0);
            scaler->Evaluate();
            UNIT_ASSERT_EQUAL(h.SetCountCalls, 1);  // still 1
        }

        Y_UNIT_TEST(StatsResetAfterEval) {
            TTestHarness h;
            h.ActiveCount = 4;
            // Set stats that would trigger deflation
            h.SetAllUtil(0.0);
            h.Slots[0].Stats.BusyCycles.store(5000, std::memory_order_relaxed);
            h.Slots[0].Stats.IdleCycles.store(5000, std::memory_order_relaxed);
            auto scaler = h.MakeScaler();

            scaler->Evaluate();

            // After Evaluate, BusyCycles and IdleCycles should be zeroed (exchanged to 0)
            for (i16 i = 0; i < 4; ++i) {
                UNIT_ASSERT_EQUAL(h.Slots[i].Stats.BusyCycles.load(std::memory_order_relaxed), 0u);
                UNIT_ASSERT_EQUAL(h.Slots[i].Stats.IdleCycles.load(std::memory_order_relaxed), 0u);
            }
        }

        Y_UNIT_TEST(MinSlotRespected) {
            TTestHarness h;
            h.ActiveCount = 2;
            h.SetAllUtil(0.0);
            auto scaler = h.MakeScaler();

            scaler->Evaluate();

            // newCount = max(1, target) — at least 1
            UNIT_ASSERT(h.LastSetCount >= 1);
        }

        Y_UNIT_TEST(MaxSlotRespected) {
            TTestHarness h;
            h.ActiveCount = 512;  // at max
            h.SetAllUtil(0.95);
            auto scaler = h.MakeScaler();

            scaler->Evaluate();

            // Already at max — no inflate beyond
            UNIT_ASSERT_EQUAL(h.SetCountCalls, 0);
        }

    } // Y_UNIT_TEST_SUITE(AdaptiveScaler)

} // namespace NActors::NWorkStealing
