#include "topology.h"

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/string/strip.h>
#include <util/system/info.h>

#ifdef _linux_
    #include <util/stream/file.h>
    #include <util/system/fs.h>
    #include <util/folder/dirut.h>
    #include <util/folder/iterator.h>
#endif

namespace NActors::NWorkStealing {

    TCpuTopology::TCpuTopology(TVector<TCpuInfo> cpus, TVector<TVector<ui32>> numaDistances)
        : Cpus_(std::move(cpus))
        , NumaDistances_(std::move(numaDistances))
    {
    }

    const TVector<TCpuInfo>& TCpuTopology::GetCpus() const {
        return Cpus_;
    }

    ui32 TCpuTopology::GetCpuCount() const {
        return static_cast<ui32>(Cpus_.size());
    }

    ui32 TCpuTopology::GetNumaDistance(ui32 numaNodeA, ui32 numaNodeB) const {
        if (numaNodeA < NumaDistances_.size() && numaNodeB < NumaDistances_[numaNodeA].size()) {
            return NumaDistances_[numaNodeA][numaNodeB];
        }
        // Same node or unknown: use Linux convention (10 = same node)
        return (numaNodeA == numaNodeB) ? 10 : 20;
    }

    TVector<TCpuId> TCpuTopology::GetNeighborsOrdered(TCpuId cpuId) const {
        // Find the reference CPU info
        const TCpuInfo* self = nullptr;
        for (const auto& cpu : Cpus_) {
            if (cpu.CpuId == cpuId) {
                self = &cpu;
                break;
            }
        }
        if (!self) {
            return {};
        }

        // Assign proximity tiers:
        // 0 = SMT sibling (same CoreId + PackageId)
        // 1 = same L3 group (same L3GroupId, different core)
        // 2 = same NUMA node (same NumaNodeId, different L3 group)
        // 3+ = other NUMA nodes, sub-sorted by NUMA distance
        struct TNeighborKey {
            ui32 Tier;
            ui32 NumaDistance;
            TCpuId CpuId;

            bool operator<(const TNeighborKey& other) const {
                if (Tier != other.Tier) {
                    return Tier < other.Tier;
                }
                if (NumaDistance != other.NumaDistance) {
                    return NumaDistance < other.NumaDistance;
                }
                return CpuId < other.CpuId;
            }
        };

        TVector<TNeighborKey> neighbors;
        neighbors.reserve(Cpus_.size() - 1);

        for (const auto& cpu : Cpus_) {
            if (cpu.CpuId == cpuId) {
                continue;
            }

            ui32 tier;
            ui32 numaDist = GetNumaDistance(self->NumaNodeId, cpu.NumaNodeId);

            if (cpu.CoreId == self->CoreId && cpu.PackageId == self->PackageId) {
                // SMT sibling
                tier = 0;
            } else if (cpu.L3GroupId == self->L3GroupId) {
                // Same L3 cache group
                tier = 1;
            } else if (cpu.NumaNodeId == self->NumaNodeId) {
                // Same NUMA node, different L3 group
                tier = 2;
            } else {
                // Cross-NUMA
                tier = 3;
            }

            neighbors.push_back({tier, numaDist, cpu.CpuId});
        }

        Sort(neighbors);

        TVector<TCpuId> result;
        result.reserve(neighbors.size());
        for (const auto& n : neighbors) {
            result.push_back(n.CpuId);
        }
        return result;
    }

    TVector<TCpuId> TCpuTopology::ParseCpuList(const TString& cpuList) {
        TVector<TCpuId> result;
        TString stripped = Strip(cpuList);
        if (stripped.empty()) {
            return result;
        }

        TVector<TString> parts;
        StringSplitter(stripped).Split(',').SkipEmpty().Collect(&parts);

        for (const auto& part : parts) {
            TString trimmed = Strip(part);
            size_t dashPos = trimmed.find('-');
            if (dashPos != TString::npos) {
                ui32 lo = FromString<ui32>(trimmed.substr(0, dashPos));
                ui32 hi = FromString<ui32>(trimmed.substr(dashPos + 1));
                for (ui32 i = lo; i <= hi; ++i) {
                    result.push_back(i);
                }
            } else {
                result.push_back(FromString<ui32>(trimmed));
            }
        }
        return result;
    }

    TCpuTopology TCpuTopology::MakeFlat(ui32 cpuCount) {
        TCpuTopology topo;
        topo.Cpus_.reserve(cpuCount);
        for (ui32 i = 0; i < cpuCount; ++i) {
            topo.Cpus_.push_back(TCpuInfo{
                .CpuId = i,
                .CoreId = i, // each CPU is its own core (no SMT)
                .PackageId = 0,
                .L3GroupId = 0,
                .NumaNodeId = 0,
            });
        }
        // Single NUMA node, distance to self = 10
        topo.NumaDistances_ = {{10}};
        return topo;
    }

#ifdef _linux_

    namespace {

        // Read a single-line sysfs file, return stripped content.
        // Returns empty string if file does not exist or cannot be read.
        TString ReadSysfsFile(const TString& path) {
            try {
                TFileInput f(path);
                TString line;
                f.ReadLine(line);
                return Strip(line);
            } catch (...) {
                return {};
            }
        }

    } // namespace

    TCpuTopology TCpuTopology::Discover() {
        TCpuTopology topo;

        // 1. Read online CPU list: /sys/devices/system/cpu/online
        TString onlineStr = ReadSysfsFile("/sys/devices/system/cpu/online");
        if (onlineStr.empty()) {
            return MakeFlat(NSystemInfo::NumberOfCpus());
        }
        TVector<TCpuId> onlineCpus = ParseCpuList(onlineStr);
        if (onlineCpus.empty()) {
            return MakeFlat(NSystemInfo::NumberOfCpus());
        }

        // 2. Discover NUMA nodes and build CPU-to-node mapping
        THashMap<TCpuId, ui32> cpuToNumaNode;
        ui32 maxNumaNode = 0;
        bool hasNuma = false;

        // Enumerate /sys/devices/system/node/node*
        for (ui32 node = 0; node < 256; ++node) {
            TString nodePath = "/sys/devices/system/node/node" + ToString(node);
            TString cpuListStr = ReadSysfsFile(nodePath + "/cpulist");
            if (cpuListStr.empty()) {
                if (node > 0) {
                    // No more nodes after a gap - stop scanning
                    // (but allow node 0 to be empty and keep trying)
                    break;
                }
                continue;
            }
            hasNuma = true;
            maxNumaNode = node;
            TVector<TCpuId> nodeCpus = ParseCpuList(cpuListStr);
            for (TCpuId cpu : nodeCpus) {
                cpuToNumaNode[cpu] = node;
            }
        }

        // 3. Read NUMA distance matrix
        if (hasNuma) {
            topo.NumaDistances_.resize(maxNumaNode + 1);
            for (ui32 node = 0; node <= maxNumaNode; ++node) {
                TString distStr = ReadSysfsFile(
                    "/sys/devices/system/node/node" + ToString(node) + "/distance");
                if (!distStr.empty()) {
                    TVector<TString> fields;
                    StringSplitter(distStr).Split(' ').SkipEmpty().Collect(&fields);
                    topo.NumaDistances_[node].reserve(fields.size());
                    for (const auto& f : fields) {
                        topo.NumaDistances_[node].push_back(FromString<ui32>(Strip(f)));
                    }
                }
            }
        } else {
            // No NUMA info: single node
            topo.NumaDistances_ = {{10}};
        }

        // 4. Read per-CPU topology and L3 cache groups
        THashMap<TString, ui32> l3GroupMap; // shared_cpu_list -> L3GroupId
        ui32 nextL3GroupId = 0;

        topo.Cpus_.reserve(onlineCpus.size());
        for (TCpuId cpuId : onlineCpus) {
            TString cpuBase = "/sys/devices/system/cpu/cpu" + ToString(cpuId);

            TCpuInfo info;
            info.CpuId = cpuId;

            // /sys/devices/system/cpu/cpu{N}/topology/core_id
            TString coreIdStr = ReadSysfsFile(cpuBase + "/topology/core_id");
            info.CoreId = coreIdStr.empty() ? cpuId : FromString<ui32>(coreIdStr);

            // /sys/devices/system/cpu/cpu{N}/topology/physical_package_id
            TString pkgIdStr = ReadSysfsFile(cpuBase + "/topology/physical_package_id");
            info.PackageId = pkgIdStr.empty() ? 0 : FromString<ui32>(pkgIdStr);

            // /sys/devices/system/cpu/cpu{N}/cache/index3/shared_cpu_list
            TString sharedCpuList = ReadSysfsFile(cpuBase + "/cache/index3/shared_cpu_list");
            if (sharedCpuList.empty()) {
                // No L3 info: group by package
                sharedCpuList = "pkg" + ToString(info.PackageId);
            }
            auto it = l3GroupMap.find(sharedCpuList);
            if (it == l3GroupMap.end()) {
                l3GroupMap[sharedCpuList] = nextL3GroupId;
                info.L3GroupId = nextL3GroupId;
                ++nextL3GroupId;
            } else {
                info.L3GroupId = it->second;
            }

            // NUMA node
            auto numaIt = cpuToNumaNode.find(cpuId);
            info.NumaNodeId = (numaIt != cpuToNumaNode.end()) ? numaIt->second : 0;

            topo.Cpus_.push_back(info);
        }

        return topo;
    }

#else // !_linux_

    TCpuTopology TCpuTopology::Discover() {
        return MakeFlat(NSystemInfo::NumberOfCpus());
    }

#endif // _linux_

} // namespace NActors::NWorkStealing
