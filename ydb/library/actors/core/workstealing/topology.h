#pragma once

#include <util/generic/vector.h>
#include <util/generic/string.h>

namespace NActors::NWorkStealing {

    using TCpuId = ui32;

    struct TCpuInfo {
        TCpuId CpuId;
        ui32 CoreId;     // physical core (SMT siblings share this)
        ui32 PackageId;  // physical socket/package
        ui32 L3GroupId;  // CPUs sharing L3 cache (CCD/CCX)
        ui32 NumaNodeId; // NUMA node
    };

    class TCpuTopology {
    public:
        // Discover topology from Linux sysfs.
        // On non-Linux: returns flat topology (all CPUs equidistant).
        static TCpuTopology Discover();

        // Create flat (non-hierarchical) topology for testing.
        // All CPUs treated as equidistant.
        static TCpuTopology MakeFlat(ui32 cpuCount);

        // Construct from explicit data (for testing).
        TCpuTopology(TVector<TCpuInfo> cpus, TVector<TVector<ui32>> numaDistances);

        // Get info for all discovered CPUs.
        const TVector<TCpuInfo>& GetCpus() const;

        // Get all CPUs sorted by topology proximity to the given CPU.
        // Order: SMT siblings -> same L3 group -> same NUMA node -> other NUMA nodes (by distance)
        TVector<TCpuId> GetNeighborsOrdered(TCpuId cpuId) const;

        // Get NUMA distance between two nodes (from sysfs node/*/distance).
        // Returns 10 for same-node (Linux convention), higher for cross-node.
        ui32 GetNumaDistance(ui32 numaNodeA, ui32 numaNodeB) const;

        // Number of CPUs discovered.
        ui32 GetCpuCount() const;

        // Parse a CPU list string like "0-3,8-11" into a vector of CPU IDs.
        static TVector<TCpuId> ParseCpuList(const TString& cpuList);

    private:
        TCpuTopology() = default;

        TVector<TCpuInfo> Cpus_;
        // NUMA distance matrix [fromNode][toNode]
        TVector<TVector<ui32>> NumaDistances_;
    };

} // namespace NActors::NWorkStealing
