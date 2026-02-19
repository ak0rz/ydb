#include "actor_class_id.h"
#include "actor_class_stats.h"

#include <library/cpp/testing/unittest/registar.h>

#include <thread>
#include <vector>

using namespace NActors;

namespace {
    // Dummy actor types for testing
    struct TActorA {};
    struct TActorB {};
    struct TActorC {};

    // Generate many types via template parameter
    template <int N>
    struct TActorN {};
}

Y_UNIT_TEST_SUITE(ActorClassId) {

    Y_UNIT_TEST(UniqueSequentialIds) {
        ui32 idA = TActorClassInfo<TActorA>::Instance().ClassId;
        ui32 idB = TActorClassInfo<TActorB>::Instance().ClassId;
        ui32 idC = TActorClassInfo<TActorC>::Instance().ClassId;

        // All different
        UNIT_ASSERT_UNEQUAL(idA, idB);
        UNIT_ASSERT_UNEQUAL(idA, idC);
        UNIT_ASSERT_UNEQUAL(idB, idC);
    }

    Y_UNIT_TEST(StableIds) {
        ui32 id1 = TActorClassInfo<TActorA>::Instance().ClassId;
        ui32 id2 = TActorClassInfo<TActorA>::Instance().ClassId;
        UNIT_ASSERT_EQUAL(id1, id2);
    }

    Y_UNIT_TEST(StableStatsPointer) {
        TActorClassStats* p1 = TActorClassInfo<TActorA>::Instance().Stats;
        TActorClassStats* p2 = TActorClassInfo<TActorA>::Instance().Stats;
        UNIT_ASSERT_EQUAL(p1, p2);
        UNIT_ASSERT(p1 != nullptr);
    }

    Y_UNIT_TEST(CachedLookup) {
        // Calling Instance() twice returns the same object
        const auto& info1 = TActorClassInfo<TActorB>::Instance();
        const auto& info2 = TActorClassInfo<TActorB>::Instance();
        UNIT_ASSERT_EQUAL(&info1, &info2);
    }
}

Y_UNIT_TEST_SUITE(ActorClassStatsTable) {

    Y_UNIT_TEST(GetReturnsStablePointers) {
        auto& table = GetActorClassStatsTable();

        // Allocate entries across slab boundaries
        std::vector<TActorClassStats*> ptrs;
        for (ui32 i = 0; i < 200; ++i) {
            ptrs.push_back(&table.Get(i));
        }

        // Re-fetch and verify pointer stability
        for (ui32 i = 0; i < 200; ++i) {
            UNIT_ASSERT_EQUAL(&table.Get(i), ptrs[i]);
        }
    }

    Y_UNIT_TEST(SlabGrowth) {
        auto& table = GetActorClassStatsTable();

        // Access entries beyond first slab
        TActorClassStats& entry0 = table.Get(0);
        TActorClassStats& entry63 = table.Get(63);  // last in first slab
        TActorClassStats& entry64 = table.Get(64);  // first in second slab
        TActorClassStats& entry127 = table.Get(127); // last in second slab
        TActorClassStats& entry128 = table.Get(128); // first in third slab

        // All should be different addresses
        UNIT_ASSERT(&entry0 != &entry63);
        UNIT_ASSERT(&entry63 != &entry64);
        UNIT_ASSERT(&entry64 != &entry127);
        UNIT_ASSERT(&entry127 != &entry128);

        // Same entry accessed twice should be same pointer
        UNIT_ASSERT_EQUAL(&table.Get(64), &entry64);
    }

    Y_UNIT_TEST(AtomicCounters) {
        auto& table = GetActorClassStatsTable();
        TActorClassStats& stats = table.Get(0);

        ui64 before = stats.MessagesProcessed.load(std::memory_order_relaxed);
        stats.MessagesProcessed.fetch_add(1, std::memory_order_relaxed);
        ui64 after = stats.MessagesProcessed.load(std::memory_order_relaxed);
        UNIT_ASSERT_EQUAL(after, before + 1);
    }

    Y_UNIT_TEST(ConcurrentSlabGrowth) {
        TActorClassStatsTable table;
        constexpr int NumThreads = 8;
        constexpr int EntriesPerThread = 256;

        std::vector<std::vector<TActorClassStats*>> results(NumThreads);
        std::vector<std::thread> threads;

        for (int t = 0; t < NumThreads; ++t) {
            threads.emplace_back([&table, &results, t]() {
                for (int i = 0; i < EntriesPerThread; ++i) {
                    results[t].push_back(&table.Get(i));
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        // All threads should see the same pointer for the same ID
        for (int i = 0; i < EntriesPerThread; ++i) {
            for (int t = 1; t < NumThreads; ++t) {
                UNIT_ASSERT_EQUAL(results[0][i], results[t][i]);
            }
        }
    }

    Y_UNIT_TEST(ForEach) {
        // Register some class IDs to ensure Count() > 0
        TActorClassInfo<TActorA>::Instance();
        TActorClassInfo<TActorB>::Instance();
        TActorClassInfo<TActorC>::Instance();

        auto& table = GetActorClassStatsTable();
        ui32 count = TActorClassCounter::Count();
        UNIT_ASSERT(count >= 3);

        ui32 visited = 0;
        table.ForEach([&visited](ui32 classId, const TActorClassStats&) {
            UNIT_ASSERT(classId == visited);
            ++visited;
        });
        UNIT_ASSERT_EQUAL(visited, count);
    }

    Y_UNIT_TEST(ConcurrentIncrements) {
        TActorClassStatsTable table;
        TActorClassStats& stats = table.Get(0);
        constexpr int NumThreads = 8;
        constexpr int IncrementsPerThread = 100000;

        std::vector<std::thread> threads;
        for (int t = 0; t < NumThreads; ++t) {
            threads.emplace_back([&stats]() {
                for (int i = 0; i < IncrementsPerThread; ++i) {
                    stats.MessagesProcessed.fetch_add(1, std::memory_order_relaxed);
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        UNIT_ASSERT_EQUAL(
            stats.MessagesProcessed.load(std::memory_order_relaxed),
            static_cast<ui64>(NumThreads) * IncrementsPerThread);
    }
}
