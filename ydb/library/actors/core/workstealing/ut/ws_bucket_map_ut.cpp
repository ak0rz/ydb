#include <ydb/library/actors/core/workstealing/ws_bucket_map.h>
#include <ydb/library/actors/core/workstealing/activation_router.h>
#include <ydb/library/actors/core/mailbox_lockfree.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NActors::NWorkStealing {

    namespace {

        struct TTestMailboxTable {
            std::unique_ptr<TMailboxTable> Table;

            TTestMailboxTable() {
                Table.reset(TMailboxTable::Create());
            }

            ~TTestMailboxTable() {
                TMailboxTable::Destroy(Table.release());
            }

            // Allocate a mailbox and return its hint
            ui32 AllocateHint() {
                TMailbox* mbox = Table->Allocate();
                return mbox ? mbox->Hint : 0;
            }
        };

    } // namespace

    Y_UNIT_TEST_SUITE(BucketMap) {

        Y_UNIT_TEST(DefaultBucketIsFast) {
            TTestMailboxTable t;
            TBucketConfig config;
            TBucketMap bm(t.Table.get(), config);

            ui32 hint = t.AllocateHint();
            UNIT_ASSERT(hint > 0);

            UNIT_ASSERT_EQUAL(bm.GetBucket(hint), 0u);
        }

        Y_UNIT_TEST(InlineEvictionOnHighCost) {
            TTestMailboxTable t;
            TBucketConfig config;
            config.CostThresholdCycles = 100000;
            TBucketMap bm(t.Table.get(), config);

            ui32 hint = t.AllocateHint();

            // Initially fast
            UNIT_ASSERT_EQUAL(bm.GetBucket(hint), 0u);

            // Execute a costly event
            bm.UpdateAfterExecution(hint, 200000);

            // Should be evicted to heavy bucket
            UNIT_ASSERT_EQUAL(bm.GetBucket(hint), 1u);
        }

        Y_UNIT_TEST(NoCheapEviction) {
            TTestMailboxTable t;
            TBucketConfig config;
            config.CostThresholdCycles = 100000;
            TBucketMap bm(t.Table.get(), config);

            ui32 hint = t.AllocateHint();

            // Execute a cheap event — should stay fast
            bm.UpdateAfterExecution(hint, 5000);
            UNIT_ASSERT_EQUAL(bm.GetBucket(hint), 0u);
        }

        Y_UNIT_TEST(PredictFromClassCost) {
            TTestMailboxTable t;
            TBucketConfig config;
            config.CostThresholdCycles = 100000;
            TBucketMap bm(t.Table.get(), config);

            UNIT_ASSERT_EQUAL(bm.PredictFromClassCost(50000), 0u);   // fast
            UNIT_ASSERT_EQUAL(bm.PredictFromClassCost(200000), 1u);  // heavy
        }

        Y_UNIT_TEST(SetBucketExplicit) {
            TTestMailboxTable t;
            TBucketConfig config;
            TBucketMap bm(t.Table.get(), config);

            ui32 hint = t.AllocateHint();

            bm.SetBucket(hint, 1);
            UNIT_ASSERT_EQUAL(bm.GetBucket(hint), 1u);

            bm.SetBucket(hint, 0);
            UNIT_ASSERT_EQUAL(bm.GetBucket(hint), 0u);
        }

        Y_UNIT_TEST(ResetBucketOnReclaim) {
            TTestMailboxTable t;
            TBucketConfig config;
            config.CostThresholdCycles = 100000;
            TBucketMap bm(t.Table.get(), config);

            ui32 hint = t.AllocateHint();

            // Classify as heavy
            bm.UpdateAfterExecution(hint, 200000);
            UNIT_ASSERT_EQUAL(bm.GetBucket(hint), 1u);
            UNIT_ASSERT(bm.IsClassified(hint));

            // Reset (simulates mailbox reclamation)
            bm.ResetBucket(hint);
            UNIT_ASSERT_EQUAL(bm.GetBucket(hint), 0u);
            UNIT_ASSERT(!bm.IsClassified(hint));
        }

        Y_UNIT_TEST(InitialBoundaryIsZero) {
            TTestMailboxTable t;
            TBucketConfig config;
            TBucketMap bm(t.Table.get(), config);

            bm.SetActiveCount(8);

            // Initially: no partitioning (all fast)
            UNIT_ASSERT_EQUAL(bm.GetBucketBoundary(), 0u);
        }

        Y_UNIT_TEST(DemandDrivenBoundary) {
            TTestMailboxTable t;
            TBucketConfig config;
            config.CostThresholdCycles = 100000;
            config.MinActiveForBucketing = 4;
            TBucketMap bm(t.Table.get(), config);
            bm.SetActiveCount(16);

            // Create 3 heavy actors
            ui32 hints[3];
            for (int i = 0; i < 3; ++i) {
                hints[i] = t.AllocateHint();
                bm.UpdateAfterExecution(hints[i], 200000); // inline eviction → heavy
                bm.TrackActive(hints[i]);
            }

            // Reclassify should set boundary = 3 (3 heavy slots at [0,3))
            bm.Reclassify();
            UNIT_ASSERT_EQUAL(bm.GetBucketBoundary(), 3u);
        }

        Y_UNIT_TEST(BucketingDisabledBelowMinActive) {
            TTestMailboxTable t;
            TBucketConfig config;
            config.CostThresholdCycles = 100000;
            config.MinActiveForBucketing = 4;
            TBucketMap bm(t.Table.get(), config);
            bm.SetActiveCount(3); // below minimum

            ui32 hint = t.AllocateHint();
            bm.UpdateAfterExecution(hint, 200000);
            bm.TrackActive(hint);

            bm.Reclassify();

            // Should remain 0 — bucketing disabled
            UNIT_ASSERT_EQUAL(bm.GetBucketBoundary(), 0u);
        }

        Y_UNIT_TEST(BoundaryScalesWithActiveCount) {
            TTestMailboxTable t;
            TBucketConfig config;
            config.CostThresholdCycles = 100000;
            config.MinActiveForBucketing = 4;
            TBucketMap bm(t.Table.get(), config);
            bm.SetActiveCount(16);

            // Create 4 heavy actors
            for (int i = 0; i < 4; ++i) {
                ui32 h = t.AllocateHint();
                bm.UpdateAfterExecution(h, 200000);
                bm.TrackActive(h);
            }
            bm.Reclassify();
            UNIT_ASSERT_EQUAL(bm.GetBucketBoundary(), 4u);

            // Deflate to 8 active — heavy target stays 4
            bm.SetActiveCount(8);
            UNIT_ASSERT_EQUAL(bm.GetBucketBoundary(), 4u);

            // Deflate to 3 — below min, bucketing disabled
            bm.SetActiveCount(3);
            UNIT_ASSERT_EQUAL(bm.GetBucketBoundary(), 0u);
        }

        Y_UNIT_TEST(GenerationIncrements) {
            TTestMailboxTable t;
            TBucketConfig config;
            config.CostThresholdCycles = 100000;
            config.MinActiveForBucketing = 2;
            TBucketMap bm(t.Table.get(), config);
            bm.SetActiveCount(8);

            uint32_t gen0 = bm.GetGeneration();

            // Reclassify with a heavy actor should change boundary → bump generation
            ui32 hint = t.AllocateHint();
            bm.UpdateAfterExecution(hint, 200000);
            bm.TrackActive(hint);
            bm.Reclassify();

            uint32_t gen1 = bm.GetGeneration();
            UNIT_ASSERT(gen1 > gen0);

            // Reclassify with same actors, no change → no generation bump
            bm.TrackActive(hint);
            bm.UpdateAfterExecution(hint, 200000);
            bm.Reclassify();
            uint32_t gen2 = bm.GetGeneration();
            UNIT_ASSERT_EQUAL(gen2, gen1);
        }

        Y_UNIT_TEST(ActiveHintSetBasic) {
            TActiveHintSet set;
            set.Clear();

            set.Insert(42);
            set.Insert(100);
            UNIT_ASSERT(set.Size() > 0);

            std::vector<uint32_t> collected;
            set.DrainAndClear([&](uint32_t h) {
                collected.push_back(h);
            });

            UNIT_ASSERT(collected.size() >= 2);
            UNIT_ASSERT(std::find(collected.begin(), collected.end(), 42) != collected.end());
            UNIT_ASSERT(std::find(collected.begin(), collected.end(), 100) != collected.end());

            // After drain, size should be 0
            UNIT_ASSERT_EQUAL(set.Size(), 0u);
        }

        Y_UNIT_TEST(ReclassifyDowngrade) {
            TTestMailboxTable t;
            TBucketConfig config;
            config.CostThresholdCycles = 100000;
            config.DowngradeThresholdCycles = 50000;
            config.MinSamplesForClassification = 16;
            TBucketMap bm(t.Table.get(), config);

            ui32 hint = t.AllocateHint();

            // Evict to heavy
            bm.UpdateAfterExecution(hint, 200000);
            UNIT_ASSERT_EQUAL(bm.GetBucket(hint), 1u);

            // Need enough events to satisfy MinSamples check in Reclassify.
            // Bump EventsProcessed via the stats counter.
            if (auto* stats = t.Table->GetStats(hint)) {
                stats->EventsProcessed.store(100, std::memory_order_relaxed);
            }

            // Now simulate many cheap events to bring EMA down
            for (int i = 0; i < 100; ++i) {
                bm.UpdateAfterExecution(hint, 1000);
                bm.TrackActive(hint);
            }

            // Reclassify should downgrade
            bm.Reclassify();
            UNIT_ASSERT_EQUAL(bm.GetBucket(hint), 0u);
        }

        Y_UNIT_TEST(BucketAwareRouting) {
            // Layout: heavy=[0,boundary), fast=[boundary,active)
            constexpr size_t N = 8;
            TSlot slots[N];
            for (auto& slot : slots) {
                UNIT_ASSERT(slot.TryTransition(ESlotState::Inactive, ESlotState::Initializing));
                UNIT_ASSERT(slot.TryTransition(ESlotState::Initializing, ESlotState::Active));
            }

            TTestMailboxTable t;
            TBucketConfig config;
            config.CostThresholdCycles = 100000;
            config.MinActiveForBucketing = 2;
            TBucketMap bm(t.Table.get(), config);
            bm.SetActiveCount(N);

            // Create 3 heavy actors to set boundary=3
            for (int i = 0; i < 3; ++i) {
                ui32 h = t.AllocateHint();
                bm.UpdateAfterExecution(h, 200000);
                bm.TrackActive(h);
            }
            bm.Reclassify();
            UNIT_ASSERT_EQUAL(bm.GetBucketBoundary(), 3u);

            TActivationRouter router(slots, N);
            router.SetBucketMap(&bm);

            ui32 fastHint = t.AllocateHint();
            ui32 heavyHint = t.AllocateHint();
            bm.SetBucket(heavyHint, 1); // heavy

            // Heavy hint should route to [0,3)
            int heavySlot = router.Route(heavyHint, 0);
            UNIT_ASSERT(heavySlot >= 0 && heavySlot < 3);

            // Fast hint should route to [3,8)
            int fastSlot = router.Route(fastHint, 0);
            UNIT_ASSERT(fastSlot >= 3 && fastSlot < 8);
        }

    } // Y_UNIT_TEST_SUITE(BucketMap)

} // namespace NActors::NWorkStealing
