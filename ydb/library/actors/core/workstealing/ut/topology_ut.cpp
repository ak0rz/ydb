#include <ydb/library/actors/core/workstealing/topology.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NActors::NWorkStealing {

    Y_UNIT_TEST_SUITE(Topology) {

        Y_UNIT_TEST(ParseCpuListSimple) {
            auto cpus = TCpuTopology::ParseCpuList("0,1,2,3");
            UNIT_ASSERT_VALUES_EQUAL(cpus.size(), 4u);
            UNIT_ASSERT_VALUES_EQUAL(cpus[0], 0u);
            UNIT_ASSERT_VALUES_EQUAL(cpus[1], 1u);
            UNIT_ASSERT_VALUES_EQUAL(cpus[2], 2u);
            UNIT_ASSERT_VALUES_EQUAL(cpus[3], 3u);
        }

        Y_UNIT_TEST(ParseCpuListRanges) {
            auto cpus = TCpuTopology::ParseCpuList("0-3,8-11");
            UNIT_ASSERT_VALUES_EQUAL(cpus.size(), 8u);
            UNIT_ASSERT_VALUES_EQUAL(cpus[0], 0u);
            UNIT_ASSERT_VALUES_EQUAL(cpus[1], 1u);
            UNIT_ASSERT_VALUES_EQUAL(cpus[2], 2u);
            UNIT_ASSERT_VALUES_EQUAL(cpus[3], 3u);
            UNIT_ASSERT_VALUES_EQUAL(cpus[4], 8u);
            UNIT_ASSERT_VALUES_EQUAL(cpus[5], 9u);
            UNIT_ASSERT_VALUES_EQUAL(cpus[6], 10u);
            UNIT_ASSERT_VALUES_EQUAL(cpus[7], 11u);
        }

        Y_UNIT_TEST(ParseCpuListMixed) {
            auto cpus = TCpuTopology::ParseCpuList("0,2-4,7");
            UNIT_ASSERT_VALUES_EQUAL(cpus.size(), 5u);
            UNIT_ASSERT_VALUES_EQUAL(cpus[0], 0u);
            UNIT_ASSERT_VALUES_EQUAL(cpus[1], 2u);
            UNIT_ASSERT_VALUES_EQUAL(cpus[2], 3u);
            UNIT_ASSERT_VALUES_EQUAL(cpus[3], 4u);
            UNIT_ASSERT_VALUES_EQUAL(cpus[4], 7u);
        }

        Y_UNIT_TEST(ParseCpuListSingle) {
            auto cpus = TCpuTopology::ParseCpuList("5");
            UNIT_ASSERT_VALUES_EQUAL(cpus.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(cpus[0], 5u);
        }

        Y_UNIT_TEST(MakeFlatTopology) {
            auto topo = TCpuTopology::MakeFlat(4);
            UNIT_ASSERT_VALUES_EQUAL(topo.GetCpuCount(), 4u);

            const auto& cpus = topo.GetCpus();
            for (ui32 i = 0; i < 4; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(cpus[i].CpuId, i);
                UNIT_ASSERT_VALUES_EQUAL(cpus[i].L3GroupId, 0u);
                UNIT_ASSERT_VALUES_EQUAL(cpus[i].NumaNodeId, 0u);
                UNIT_ASSERT_VALUES_EQUAL(cpus[i].PackageId, 0u);
            }

            // All neighbors should be returned for each CPU
            auto neighbors = topo.GetNeighborsOrdered(0);
            UNIT_ASSERT_VALUES_EQUAL(neighbors.size(), 3u);
        }

        Y_UNIT_TEST(MakeFlatNeighborOrder) {
            auto topo = TCpuTopology::MakeFlat(8);
            auto neighbors = topo.GetNeighborsOrdered(0);

            // All CPUs equidistant (same L3, same NUMA), sorted by CpuId
            UNIT_ASSERT_VALUES_EQUAL(neighbors.size(), 7u);
            for (ui32 i = 0; i < 7; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(neighbors[i], i + 1);
            }
        }

        Y_UNIT_TEST(DiscoverDoesNotCrash) {
            auto topo = TCpuTopology::Discover();
            UNIT_ASSERT(topo.GetCpuCount() >= 1u);
            UNIT_ASSERT(!topo.GetCpus().empty());
        }

        Y_UNIT_TEST(NeighborOrderRespectsTiers) {
            // Construct a topology with 2 sockets, 2 L3 groups each, 2 cores each with SMT (= 2 threads per core)
            //
            // Socket 0 (NUMA node 0):
            //   L3 group 0: core 0 (cpu 0, cpu 8), core 1 (cpu 1, cpu 9)
            //   L3 group 1: core 2 (cpu 2, cpu 10), core 3 (cpu 3, cpu 11)
            // Socket 1 (NUMA node 1):
            //   L3 group 2: core 4 (cpu 4, cpu 12), core 5 (cpu 5, cpu 13)
            //   L3 group 3: core 6 (cpu 6, cpu 14), core 7 (cpu 7, cpu 15)

            TVector<TCpuInfo> cpus;
            // Socket 0, L3 group 0
            cpus.push_back({0, 0, 0, 0, 0});
            cpus.push_back({8, 0, 0, 0, 0}); // SMT sibling of cpu 0
            cpus.push_back({1, 1, 0, 0, 0});
            cpus.push_back({9, 1, 0, 0, 0});
            // Socket 0, L3 group 1
            cpus.push_back({2, 2, 0, 1, 0});
            cpus.push_back({10, 2, 0, 1, 0});
            cpus.push_back({3, 3, 0, 1, 0});
            cpus.push_back({11, 3, 0, 1, 0});
            // Socket 1, L3 group 2
            cpus.push_back({4, 4, 1, 2, 1});
            cpus.push_back({12, 4, 1, 2, 1});
            cpus.push_back({5, 5, 1, 2, 1});
            cpus.push_back({13, 5, 1, 2, 1});
            // Socket 1, L3 group 3
            cpus.push_back({6, 6, 1, 3, 1});
            cpus.push_back({14, 6, 1, 3, 1});
            cpus.push_back({7, 7, 1, 3, 1});
            cpus.push_back({15, 7, 1, 3, 1});

            // NUMA distance matrix: node 0 <-> node 1
            TVector<TVector<ui32>> numaDistances = {
                {10, 21}, // node 0 -> node 0 = 10, node 0 -> node 1 = 21
                {21, 10}, // node 1 -> node 0 = 21, node 1 -> node 1 = 10
            };

            TCpuTopology topo(std::move(cpus), std::move(numaDistances));

            // Get neighbors for CPU 0 (core 0, L3 group 0, NUMA node 0, package 0)
            auto neighbors = topo.GetNeighborsOrdered(0);
            UNIT_ASSERT_VALUES_EQUAL(neighbors.size(), 15u);

            // Tier 0: SMT sibling (same CoreId=0, same PackageId=0) -> cpu 8
            UNIT_ASSERT_VALUES_EQUAL(neighbors[0], 8u);

            // Tier 1: same L3 group 0, different core -> cpus 1, 9 (sorted by CpuId)
            UNIT_ASSERT_VALUES_EQUAL(neighbors[1], 1u);
            UNIT_ASSERT_VALUES_EQUAL(neighbors[2], 9u);

            // Tier 2: same NUMA node 0, different L3 group -> cpus 2, 3, 10, 11 (sorted by CpuId)
            UNIT_ASSERT_VALUES_EQUAL(neighbors[3], 2u);
            UNIT_ASSERT_VALUES_EQUAL(neighbors[4], 3u);
            UNIT_ASSERT_VALUES_EQUAL(neighbors[5], 10u);
            UNIT_ASSERT_VALUES_EQUAL(neighbors[6], 11u);

            // Tier 3: cross-NUMA (node 1, distance 21) -> cpus 4, 5, 6, 7, 12, 13, 14, 15 (sorted by CpuId)
            UNIT_ASSERT_VALUES_EQUAL(neighbors[7], 4u);
            UNIT_ASSERT_VALUES_EQUAL(neighbors[8], 5u);
            UNIT_ASSERT_VALUES_EQUAL(neighbors[9], 6u);
            UNIT_ASSERT_VALUES_EQUAL(neighbors[10], 7u);
            UNIT_ASSERT_VALUES_EQUAL(neighbors[11], 12u);
            UNIT_ASSERT_VALUES_EQUAL(neighbors[12], 13u);
            UNIT_ASSERT_VALUES_EQUAL(neighbors[13], 14u);
            UNIT_ASSERT_VALUES_EQUAL(neighbors[14], 15u);
        }

        Y_UNIT_TEST(NumaDistanceSameNode) {
            auto topo = TCpuTopology::MakeFlat(4);
            UNIT_ASSERT_VALUES_EQUAL(topo.GetNumaDistance(0, 0), 10u);
        }

    } // Y_UNIT_TEST_SUITE(Topology)

} // namespace NActors::NWorkStealing
