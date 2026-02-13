#include <ydb/library/actors/core/workstealing/topology.h>

#ifdef _linux_

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/string/cast.h>

#include <util/generic/hash_set.h>

#include <numeric>

namespace NActors {

namespace {

    // Writes a small text file, creating parent directories as needed.
    void WriteMockFile(const TFsPath& path, const TString& content) {
        path.Parent().MkDirs();
        TUnbufferedFileOutput(path.GetPath()).Write(content);
    }

    // Sets up mock sysfs for a single CPU: SMT siblings and cache hierarchy.
    void AddMockCpu(const TFsPath& root, ui32 cpuId,
                    const TString& threadSiblingsList,
                    const TVector<std::pair<ui32, TString>>& caches)
    {
        TFsPath cpuDir = root / "cpu" / ("cpu" + ToString(cpuId));
        WriteMockFile(
            cpuDir / "topology" / "thread_siblings_list",
            threadSiblingsList + "\n");

        for (const auto& [level, sharedCpuList] : caches) {
            TString idx = "index" + ToString(level);
            WriteMockFile(cpuDir / "cache" / idx / "level", ToString(level) + "\n");
            WriteMockFile(cpuDir / "cache" / idx / "shared_cpu_list",
                          sharedCpuList + "\n");
        }
    }

    // Adds a NUMA node with the given CPU list.
    void AddMockNumaNode(const TFsPath& root, ui32 nodeId,
                         const TString& cpulist)
    {
        WriteMockFile(
            root / "node" / ("node" + ToString(nodeId)) / "cpulist",
            cpulist + "\n");
    }

} // namespace

Y_UNIT_TEST_SUITE(Topology) {

    // Verifies CPU list parsing: single IDs, ranges, mixed, edge cases.
    Y_UNIT_TEST(ParseCpuList) {
        UNIT_ASSERT_VALUES_EQUAL(
            TTopologyMap::ParseCpuList("0"), TVector<ui32>({0}));
        UNIT_ASSERT_VALUES_EQUAL(
            TTopologyMap::ParseCpuList("0-3"), TVector<ui32>({0, 1, 2, 3}));
        UNIT_ASSERT_VALUES_EQUAL(
            TTopologyMap::ParseCpuList("0-3,5,7-9"),
            TVector<ui32>({0, 1, 2, 3, 5, 7, 8, 9}));
        UNIT_ASSERT_VALUES_EQUAL(
            TTopologyMap::ParseCpuList("0,4"),
            TVector<ui32>({0, 4}));
        // Single element range.
        UNIT_ASSERT_VALUES_EQUAL(
            TTopologyMap::ParseCpuList("5-5"), TVector<ui32>({5}));
        // Empty string.
        UNIT_ASSERT(TTopologyMap::ParseCpuList("").empty());
        // Whitespace tolerance.
        UNIT_ASSERT_VALUES_EQUAL(
            TTopologyMap::ParseCpuList(" 1 , 3 "), TVector<ui32>({1, 3}));
    }

    // Single-socket, 4 cores / 8 threads.
    // SMT pairs: (0,4), (1,5), (2,6), (3,7).
    // All CPUs share one L3 cache. Single NUMA node.
    Y_UNIT_TEST(SingleSocket4C8T) {
        TTempDir tmp;
        TFsPath root = tmp.Path();

        for (ui32 cpu = 0; cpu < 8; ++cpu) {
            ui32 lo = cpu < 4 ? cpu : cpu - 4;
            ui32 hi = lo + 4;
            TString smtList = ToString(lo) + "," + ToString(hi);
            AddMockCpu(root, cpu, smtList, {
                {1, smtList},
                {2, smtList},
                {3, "0-7"},
            });
        }
        AddMockNumaNode(root, 0, "0-7");

        TTopologyMap topo(root.GetPath());

        UNIT_ASSERT_VALUES_EQUAL(topo.GetCpuCount(), 8u);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetPhysicalCores().size(), 4u);

        // SMT siblings.
        UNIT_ASSERT_VALUES_EQUAL(topo.GetSmtSiblings(0), TVector<ui32>({0, 4}));
        UNIT_ASSERT_VALUES_EQUAL(topo.GetSmtSiblings(4), TVector<ui32>({0, 4}));
        UNIT_ASSERT_VALUES_EQUAL(topo.GetSmtSiblings(3), TVector<ui32>({3, 7}));

        // LLC: all 8 CPUs share L3.
        TVector<ui32> allCpus = {0, 1, 2, 3, 4, 5, 6, 7};
        UNIT_ASSERT_VALUES_EQUAL(topo.GetLlcNeighbors(0), allCpus);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetLlcNeighbors(5), allCpus);

        // All CPUs share one LLC ID.
        for (const auto& core : topo.GetPhysicalCores()) {
            UNIT_ASSERT_VALUES_EQUAL(core.LlcId, 0u);
        }

        // NUMA: single node.
        UNIT_ASSERT_VALUES_EQUAL(topo.GetNumaNode(0), 0u);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetNumaNeighbors(0), allCpus);

        // Physical cores sorted by CoreId (first sibling).
        const auto& cores = topo.GetPhysicalCores();
        for (ui32 i = 0; i < 4; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(cores[i].CoreId, i);
            UNIT_ASSERT_VALUES_EQUAL(cores[i].Cpus, TVector<ui32>({i, i + 4}));
            UNIT_ASSERT_VALUES_EQUAL(cores[i].NumaNode, 0u);
        }
    }

    // Dual-socket, 2×4 cores / 16 threads.
    // Socket 0: cores 0-3, CPUs 0-3 + 8-11, LLC 0, NUMA 0.
    // Socket 1: cores 4-7, CPUs 4-7 + 12-15, LLC 1, NUMA 1.
    Y_UNIT_TEST(DualSocket2x4C8T) {
        TTempDir tmp;
        TFsPath root = tmp.Path();

        for (ui32 cpu = 0; cpu < 16; ++cpu) {
            ui32 lo = cpu < 8 ? cpu : cpu - 8;
            ui32 hi = lo + 8;
            TString smtList = ToString(lo) + "," + ToString(hi);

            // L3 shared within socket.
            TString l3List;
            if (lo < 4) {
                l3List = "0-3,8-11";
            } else {
                l3List = "4-7,12-15";
            }

            AddMockCpu(root, cpu, smtList, {
                {1, smtList},
                {2, smtList},
                {3, l3List},
            });
        }

        AddMockNumaNode(root, 0, "0-3,8-11");
        AddMockNumaNode(root, 1, "4-7,12-15");

        TTopologyMap topo(root.GetPath());

        UNIT_ASSERT_VALUES_EQUAL(topo.GetCpuCount(), 16u);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetPhysicalCores().size(), 8u);

        // SMT siblings.
        UNIT_ASSERT_VALUES_EQUAL(topo.GetSmtSiblings(0), TVector<ui32>({0, 8}));
        UNIT_ASSERT_VALUES_EQUAL(topo.GetSmtSiblings(12), TVector<ui32>({4, 12}));

        // LLC groups: socket 0 vs socket 1.
        TVector<ui32> socket0 = {0, 1, 2, 3, 8, 9, 10, 11};
        TVector<ui32> socket1 = {4, 5, 6, 7, 12, 13, 14, 15};
        UNIT_ASSERT_VALUES_EQUAL(topo.GetLlcNeighbors(0), socket0);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetLlcNeighbors(9), socket0);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetLlcNeighbors(4), socket1);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetLlcNeighbors(15), socket1);

        // Distinct LLC IDs per socket.
        UNIT_ASSERT(topo.GetPhysicalCores()[0].LlcId !=
                    topo.GetPhysicalCores()[4].LlcId);

        // NUMA nodes.
        UNIT_ASSERT_VALUES_EQUAL(topo.GetNumaNode(0), 0u);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetNumaNode(4), 1u);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetNumaNode(8), 0u);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetNumaNode(12), 1u);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetNumaNeighbors(0), socket0);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetNumaNeighbors(5), socket1);
    }

    // Minimal topology: single core with 2 hyperthreads.
    Y_UNIT_TEST(SingleCore1C2T) {
        TTempDir tmp;
        TFsPath root = tmp.Path();

        AddMockCpu(root, 0, "0,1", {{2, "0,1"}});
        AddMockCpu(root, 1, "0,1", {{2, "0,1"}});
        AddMockNumaNode(root, 0, "0-1");

        TTopologyMap topo(root.GetPath());

        UNIT_ASSERT_VALUES_EQUAL(topo.GetCpuCount(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetPhysicalCores().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetSmtSiblings(0), TVector<ui32>({0, 1}));
        UNIT_ASSERT_VALUES_EQUAL(topo.GetSmtSiblings(1), TVector<ui32>({0, 1}));
        UNIT_ASSERT_VALUES_EQUAL(topo.GetPhysicalCores()[0].CoreId, 0u);
    }

    // 4 cores, no SMT (1 thread per core), single NUMA node.
    // Verifies that the absence of SMT is handled gracefully:
    // each CPU is its own physical core.
    Y_UNIT_TEST(NoSmtSingleNuma) {
        TTempDir tmp;
        TFsPath root = tmp.Path();

        for (ui32 cpu = 0; cpu < 4; ++cpu) {
            AddMockCpu(root, cpu, ToString(cpu), {
                {1, ToString(cpu)},
                {3, "0-3"},
            });
        }
        AddMockNumaNode(root, 0, "0-3");

        TTopologyMap topo(root.GetPath());

        UNIT_ASSERT_VALUES_EQUAL(topo.GetCpuCount(), 4u);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetPhysicalCores().size(), 4u);

        // Each CPU is its own core (no sibling).
        for (ui32 cpu = 0; cpu < 4; ++cpu) {
            UNIT_ASSERT_VALUES_EQUAL(
                topo.GetSmtSiblings(cpu), TVector<ui32>({cpu}));
        }

        // All share one LLC.
        TVector<ui32> allCpus = {0, 1, 2, 3};
        for (ui32 cpu = 0; cpu < 4; ++cpu) {
            UNIT_ASSERT_VALUES_EQUAL(topo.GetLlcNeighbors(cpu), allCpus);
        }

        // Single NUMA node.
        for (ui32 cpu = 0; cpu < 4; ++cpu) {
            UNIT_ASSERT_VALUES_EQUAL(topo.GetNumaNode(cpu), 0u);
            UNIT_ASSERT_VALUES_EQUAL(topo.GetNumaNeighbors(cpu), allCpus);
        }
    }

    // AMD EPYC 9654 (Genoa), dual-socket, NPS=1.
    // 2 sockets × 96 cores × 2 SMT = 384 logical CPUs.
    //
    // Linux enumerates physical threads first, then HT siblings:
    //   Socket 0 physical: 0-95,   HT: 192-287
    //   Socket 1 physical: 96-191, HT: 288-383
    //   SMT pair: CPU N and CPU N+192  (for N < 192)
    //
    // 12 CCDs per socket, 8 cores per CCD sharing L3 (32 MB per CCD):
    //   CCD 0:  cores 0-7,   CPUs {0-7, 192-199}
    //   CCD 1:  cores 8-15,  CPUs {8-15, 200-207}
    //   ...
    //   CCD 11: cores 88-95, CPUs {88-95, 280-287}
    //   CCD 12: cores 96-103, CPUs {96-103, 288-295}  (socket 1)
    //   ...
    //   CCD 23: cores 184-191, CPUs {184-191, 376-383}
    //
    // NPS=1: one NUMA node per socket.
    //   NUMA 0: CPUs 0-95, 192-287
    //   NUMA 1: CPUs 96-191, 288-383
    Y_UNIT_TEST(DualSocketAmdEpyc9654Nps1) {
        TTempDir tmp;
        TFsPath root = tmp.Path();

        constexpr ui32 CoresPerSocket = 96;
        constexpr ui32 CoresPerCcd = 8;
        constexpr ui32 TotalPhysical = 2 * CoresPerSocket; // 192
        constexpr ui32 HtOffset = TotalPhysical;            // 192
        for (ui32 phys = 0; phys < TotalPhysical; ++phys) {
            ui32 ht = phys + HtOffset;

            // SMT siblings: "phys,ht"
            TString smtList = ToString(phys) + "," + ToString(ht);

            // L3 shared within CCD (8 physical + 8 HT = 16 CPUs).
            ui32 ccdBase = (phys / CoresPerCcd) * CoresPerCcd;
            TString l3List = ToString(ccdBase) + "-" + ToString(ccdBase + CoresPerCcd - 1)
                + "," + ToString(ccdBase + HtOffset) + "-" + ToString(ccdBase + HtOffset + CoresPerCcd - 1);

            AddMockCpu(root, phys, smtList, {{3, l3List}});
            AddMockCpu(root, ht,   smtList, {{3, l3List}});
        }

        AddMockNumaNode(root, 0, "0-95,192-287");
        AddMockNumaNode(root, 1, "96-191,288-383");

        TTopologyMap topo(root.GetPath());

        // 384 CPUs, 192 physical cores.
        UNIT_ASSERT_VALUES_EQUAL(topo.GetCpuCount(), 384u);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetPhysicalCores().size(), 192u);

        // 24 distinct LLC domains (12 per socket).
        THashSet<ui32> llcIds;
        for (const auto& core : topo.GetPhysicalCores()) {
            llcIds.insert(core.LlcId);
        }
        UNIT_ASSERT_VALUES_EQUAL(llcIds.size(), 24u);

        // Spot-check SMT siblings.
        UNIT_ASSERT_VALUES_EQUAL(topo.GetSmtSiblings(0), TVector<ui32>({0, 192}));
        UNIT_ASSERT_VALUES_EQUAL(topo.GetSmtSiblings(192), TVector<ui32>({0, 192}));
        UNIT_ASSERT_VALUES_EQUAL(topo.GetSmtSiblings(95), TVector<ui32>({95, 287}));
        UNIT_ASSERT_VALUES_EQUAL(topo.GetSmtSiblings(96), TVector<ui32>({96, 288}));
        UNIT_ASSERT_VALUES_EQUAL(topo.GetSmtSiblings(383), TVector<ui32>({191, 383}));

        // Spot-check LLC groups.
        // CCD 0 (socket 0): CPUs 0-7, 192-199.
        TVector<ui32> ccd0 = {0, 1, 2, 3, 4, 5, 6, 7, 192, 193, 194, 195, 196, 197, 198, 199};
        UNIT_ASSERT_VALUES_EQUAL(topo.GetLlcNeighbors(0), ccd0);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetLlcNeighbors(5), ccd0);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetLlcNeighbors(195), ccd0);

        // CCD 12 (first CCD of socket 1): CPUs 96-103, 288-295.
        TVector<ui32> ccd12 = {96, 97, 98, 99, 100, 101, 102, 103,
                               288, 289, 290, 291, 292, 293, 294, 295};
        UNIT_ASSERT_VALUES_EQUAL(topo.GetLlcNeighbors(96), ccd12);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetLlcNeighbors(290), ccd12);

        // Cross-CCD: CPU 0 (CCD 0) and CPU 8 (CCD 1) have different LLC IDs.
        UNIT_ASSERT(topo.GetPhysicalCores()[0].LlcId !=
                    topo.GetPhysicalCores()[CoresPerCcd].LlcId);

        // Same CCD: CPU 0 and CPU 7 share LLC ID.
        UNIT_ASSERT_VALUES_EQUAL(
            topo.GetPhysicalCores()[0].LlcId,
            topo.GetPhysicalCores()[CoresPerCcd - 1].LlcId);

        // NUMA: socket 0 vs socket 1.
        UNIT_ASSERT_VALUES_EQUAL(topo.GetNumaNode(0), 0u);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetNumaNode(192), 0u);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetNumaNode(96), 1u);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetNumaNode(288), 1u);

        // NUMA group sizes: 192 CPUs per node.
        UNIT_ASSERT_VALUES_EQUAL(topo.GetNumaNeighbors(0).size(), 192u);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetNumaNeighbors(100).size(), 192u);

        // LLC groups are supersets of SMT groups.
        for (const auto& core : topo.GetPhysicalCores()) {
            auto llc = topo.GetLlcNeighbors(core.Cpus[0]);
            THashSet<ui32> llcSet(llc.begin(), llc.end());
            for (ui32 cpu : core.Cpus) {
                UNIT_ASSERT_C(llcSet.contains(cpu),
                    "SMT sibling " << cpu << " not in LLC group");
            }
        }
    }

    // NVIDIA Grace dual-socket (ARM Neoverse V2).
    // 2 sockets × 72 cores, NO SMT = 144 logical CPUs.
    //
    // No hyperthreading — each CPU is its own physical core.
    //
    // The L3 (SLC, ~117 MB) is shared across ALL 72 cores of the
    // package — one LLC domain per socket, not per-cluster.
    // Real sysfs: shared_cpu_list:0-71 for all CPUs on socket 0.
    //
    // 2 NUMA nodes (one per socket):
    //   NUMA 0: CPUs 0-71
    //   NUMA 1: CPUs 72-143
    //
    // This topology is the opposite extreme from EPYC 9654: many
    // small L3 domains (EPYC) vs. one huge L3 per socket (Grace).
    // The scheduler must handle both — steal order within a Grace
    // socket is flat (no LLC locality tier), while EPYC has a
    // meaningful LLC tier between SMT and NUMA.
    Y_UNIT_TEST(DualSocketNvidiaGrace) {
        TTempDir tmp;
        TFsPath root = tmp.Path();

        constexpr ui32 CoresPerSocket = 72;
        constexpr ui32 TotalCpus = 2 * CoresPerSocket; // 144

        for (ui32 cpu = 0; cpu < TotalCpus; ++cpu) {
            // No SMT: each CPU is its own sibling.
            TString smtList = ToString(cpu);

            // L3 shared across all cores on the same socket.
            TString l3List;
            if (cpu < CoresPerSocket) {
                l3List = "0-71";
            } else {
                l3List = "72-143";
            }

            AddMockCpu(root, cpu, smtList, {{3, l3List}});
        }

        AddMockNumaNode(root, 0, "0-71");
        AddMockNumaNode(root, 1, "72-143");

        TTopologyMap topo(root.GetPath());

        // 144 CPUs, all are physical cores (no SMT).
        UNIT_ASSERT_VALUES_EQUAL(topo.GetCpuCount(), 144u);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetPhysicalCores().size(), 144u);

        // 2 LLC domains — one per socket (all 72 cores share L3).
        THashSet<ui32> llcIds;
        for (const auto& core : topo.GetPhysicalCores()) {
            llcIds.insert(core.LlcId);
        }
        UNIT_ASSERT_VALUES_EQUAL(llcIds.size(), 2u);

        // No SMT: each CPU is its own sibling.
        for (ui32 cpu = 0; cpu < TotalCpus; ++cpu) {
            UNIT_ASSERT_VALUES_EQUAL(topo.GetSmtSiblings(cpu), TVector<ui32>({cpu}));
        }

        // Each physical core has exactly 1 CPU.
        for (const auto& core : topo.GetPhysicalCores()) {
            UNIT_ASSERT_VALUES_EQUAL(core.Cpus.size(), 1u);
        }

        // LLC group = entire socket.
        TVector<ui32> socket0(CoresPerSocket);
        std::iota(socket0.begin(), socket0.end(), 0);
        TVector<ui32> socket1(CoresPerSocket);
        std::iota(socket1.begin(), socket1.end(), CoresPerSocket);

        UNIT_ASSERT_VALUES_EQUAL(topo.GetLlcNeighbors(0), socket0);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetLlcNeighbors(35), socket0);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetLlcNeighbors(71), socket0);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetLlcNeighbors(72), socket1);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetLlcNeighbors(100), socket1);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetLlcNeighbors(143), socket1);

        // Cross-socket: different LLC IDs.
        UNIT_ASSERT(topo.GetPhysicalCores()[0].LlcId !=
                    topo.GetPhysicalCores()[CoresPerSocket].LlcId);

        // Same socket: same LLC ID.
        UNIT_ASSERT_VALUES_EQUAL(
            topo.GetPhysicalCores()[0].LlcId,
            topo.GetPhysicalCores()[CoresPerSocket - 1].LlcId);

        // NUMA nodes.
        UNIT_ASSERT_VALUES_EQUAL(topo.GetNumaNode(0), 0u);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetNumaNode(71), 0u);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetNumaNode(72), 1u);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetNumaNode(143), 1u);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetNumaNeighbors(0).size(), 72u);
        UNIT_ASSERT_VALUES_EQUAL(topo.GetNumaNeighbors(100).size(), 72u);

        // On Grace, LLC == NUMA (both cover the full socket).
        // This means the steal order collapses: no distinct LLC tier.
        UNIT_ASSERT_VALUES_EQUAL(topo.GetLlcNeighbors(0).size(),
                                 topo.GetNumaNeighbors(0).size());
    }

    // Tests against real sysfs (when available). Verifies structural
    // invariants that must hold on any Linux machine:
    //   - Non-zero CPU count
    //   - SMT siblings are symmetric (if A is sibling of B, then B is sibling of A)
    //   - LLC groups are supersets of SMT groups
    //   - Every CPU belongs to exactly one physical core
    Y_UNIT_TEST(RealSysfs) {
        TFsPath sysfs("/sys/devices/system");
        if (!sysfs.Exists()) {
            Cerr << "Skipping RealSysfs: /sys/devices/system not available" << Endl;
            return;
        }

        TTopologyMap topo;
        UNIT_ASSERT(topo.GetCpuCount() > 0);
        UNIT_ASSERT(!topo.GetPhysicalCores().empty());

        // SMT symmetry: if cpu A lists B as a sibling, B must list A.
        for (const auto& core : topo.GetPhysicalCores()) {
            for (ui32 cpu : core.Cpus) {
                auto siblings = topo.GetSmtSiblings(cpu);
                UNIT_ASSERT_VALUES_EQUAL_C(siblings, core.Cpus,
                    "SMT asymmetry at cpu " << cpu);
            }
        }

        // LLC groups are supersets of SMT groups.
        for (const auto& core : topo.GetPhysicalCores()) {
            if (core.Cpus.empty()) continue;
            auto llcGroup = topo.GetLlcNeighbors(core.Cpus[0]);
            for (ui32 cpu : core.Cpus) {
                bool found = false;
                for (ui32 n : llcGroup) {
                    if (n == cpu) { found = true; break; }
                }
                UNIT_ASSERT_C(found,
                    "SMT sibling " << cpu << " not in its LLC group");
            }
        }

        // Every CPU appears in exactly one physical core.
        THashSet<ui32> seen;
        for (const auto& core : topo.GetPhysicalCores()) {
            for (ui32 cpu : core.Cpus) {
                UNIT_ASSERT_C(seen.insert(cpu).second,
                    "cpu " << cpu << " appears in multiple physical cores");
            }
        }
    }

} // Y_UNIT_TEST_SUITE

} // namespace NActors

#endif // _linux_
