#include "topology.h"

#ifdef _linux_

#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/string/strip.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NActors {

    TVector<ui32> TTopologyMap::ParseCpuList(const TString& s) {
        TVector<ui32> result;
        for (const auto& part : StringSplitter(s).Split(',').SkipEmpty()) {
            TStringBuf token = StripString(part.Token());
            size_t dash = token.find('-');
            if (dash != TStringBuf::npos) {
                ui32 lo = FromString<ui32>(token.SubStr(0, dash));
                ui32 hi = FromString<ui32>(token.SubStr(dash + 1));
                for (ui32 i = lo; i <= hi; ++i) {
                    result.push_back(i);
                }
            } else {
                result.push_back(FromString<ui32>(token));
            }
        }
        Sort(result);
        return result;
    }

    TString TTopologyMap::ReadSmallFile(const TString& path) {
        try {
            return StripString(TFileInput(path).ReadAll());
        } catch (...) {
            return {};
        }
    }

    void TTopologyMap::Discover(const TString& sysfsRoot) {
        TFsPath cpuBase = TFsPath(sysfsRoot) / "cpu";
        TFsPath nodeBase = TFsPath(sysfsRoot) / "node";

        // 1. Enumerate CPUs by scanning for cpu<N> directories.
        TVector<ui32> cpuIds;
        if (cpuBase.Exists() && cpuBase.IsDirectory()) {
            TVector<TFsPath> children;
            cpuBase.List(children);
            for (const auto& child : children) {
                TString name = child.GetName();
                if (name.StartsWith("cpu") && name.size() > 3) {
                    ui32 id;
                    if (TryFromString(name.substr(3), id)) {
                        cpuIds.push_back(id);
                    }
                }
            }
        }
        Sort(cpuIds);

        if (cpuIds.empty()) {
            return;
        }

        CpuCount_ = cpuIds.size();
        ui32 maxCpu = cpuIds.back();
        SmtSiblings_.resize(maxCpu + 1);
        LlcNeighbors_.resize(maxCpu + 1);
        NumaNeighbors_.resize(maxCpu + 1);
        NumaNode_.resize(maxCpu + 1, 0);
        LlcId_.resize(maxCpu + 1, 0);

        // 2. For each CPU, read SMT siblings and LLC group.
        //
        // LLC identification: scan all cache index directories, find the
        // highest level (typically L3), and use its shared_cpu_list.
        // Canonical string form of the CPU list serves as the LLC group key.
        THashMap<TString, ui32> llcGroupToId;
        ui32 nextLlcId = 0;

        for (ui32 cpu : cpuIds) {
            TFsPath cpuDir = cpuBase / ("cpu" + ToString(cpu));

            // SMT siblings.
            TString siblings = ReadSmallFile(
                (cpuDir / "topology" / "thread_siblings_list").GetPath());
            if (siblings) {
                SmtSiblings_[cpu] = ParseCpuList(siblings);
            } else {
                SmtSiblings_[cpu] = {cpu};
            }

            // LLC: find highest-level cache.
            TFsPath cacheDir = cpuDir / "cache";
            ui32 highestLevel = 0;
            TVector<ui32> llcCpus;
            if (cacheDir.Exists() && cacheDir.IsDirectory()) {
                TVector<TFsPath> cacheEntries;
                cacheDir.List(cacheEntries);
                for (const auto& entry : cacheEntries) {
                    TString entryName = entry.GetName();
                    if (!entryName.StartsWith("index")) {
                        continue;
                    }
                    TString levelStr = ReadSmallFile(
                        (entry / "level").GetPath());
                    if (!levelStr) {
                        continue;
                    }
                    ui32 level;
                    if (!TryFromString(levelStr, level)) {
                        continue;
                    }
                    if (level > highestLevel) {
                        highestLevel = level;
                        TString sharedStr = ReadSmallFile(
                            (entry / "shared_cpu_list").GetPath());
                        if (sharedStr) {
                            llcCpus = ParseCpuList(sharedStr);
                        }
                    }
                }
            }
            if (llcCpus.empty()) {
                // Fallback: LLC group = SMT group.
                llcCpus = SmtSiblings_[cpu];
            }
            LlcNeighbors_[cpu] = llcCpus;

            // Assign a stable LLC ID by the canonical CPU list.
            TString llcKey;
            for (size_t i = 0; i < llcCpus.size(); ++i) {
                if (i > 0) llcKey += ',';
                llcKey += ToString(llcCpus[i]);
            }
            auto [it, inserted] = llcGroupToId.try_emplace(llcKey, nextLlcId);
            if (inserted) {
                ++nextLlcId;
            }
            LlcId_[cpu] = it->second;
        }

        // 3. NUMA nodes: scan /sys/devices/system/node/node<N>/cpulist.
        if (nodeBase.Exists() && nodeBase.IsDirectory()) {
            TVector<TFsPath> nodeEntries;
            nodeBase.List(nodeEntries);
            for (const auto& entry : nodeEntries) {
                TString name = entry.GetName();
                if (!name.StartsWith("node") || name.size() <= 4) {
                    continue;
                }
                ui32 nodeId;
                if (!TryFromString(name.substr(4), nodeId)) {
                    continue;
                }
                TString cpulist = ReadSmallFile(
                    (entry / "cpulist").GetPath());
                if (!cpulist) {
                    continue;
                }
                TVector<ui32> cpus = ParseCpuList(cpulist);
                for (ui32 cpu : cpus) {
                    if (cpu <= maxCpu) {
                        NumaNode_[cpu] = nodeId;
                        NumaNeighbors_[cpu] = cpus;
                    }
                }
            }
        }

        // Fallback: if any CPU has no NUMA info, assign all CPUs to node 0.
        for (ui32 cpu : cpuIds) {
            if (NumaNeighbors_[cpu].empty()) {
                NumaNeighbors_[cpu] = cpuIds;
            }
        }

        // 4. Build physical cores by grouping SMT siblings.
        THashSet<ui32> assigned;
        for (ui32 cpu : cpuIds) {
            if (assigned.contains(cpu)) {
                continue;
            }
            const auto& sibs = SmtSiblings_[cpu];
            for (ui32 s : sibs) {
                assigned.insert(s);
            }

            TPhysicalCore core;
            core.CoreId = sibs.empty() ? cpu : sibs[0];
            core.Cpus = sibs;
            core.NumaNode = NumaNode_[cpu];
            core.LlcId = LlcId_[cpu];
            PhysicalCores_.push_back(std::move(core));
        }

        SortBy(PhysicalCores_, [](const TPhysicalCore& c) { return c.CoreId; });
    }

} // namespace NActors

#endif // _linux_
