#pragma once

// Hardware topology discovery from Linux sysfs.
//
// Reads SMT (hyperthread), LLC (last-level cache), and NUMA groupings
// from /sys/devices/system/ at construction time. Query methods return
// precomputed neighbor lists for each CPU.
//
// The constructor accepts a custom sysfs root for testability (mock
// sysfs trees). If sysfs is unavailable or a CPU's topology files are
// missing, fallback behavior treats each CPU as its own core/LLC/NUMA.
//
// Non-Linux: Discover() is compiled out, so the map stays empty —
// CpuCount_ == 0, all getters return their fallback values ({cpu} for
// neighbors, 0 for NUMA node). The scheduler should fall back to
// std::thread::hardware_concurrency() for thread count and use a flat
// steal order (no topology tiers).
//
// Used by the work-stealing scheduler to build per-slot steal order:
//   SMT sibling → same LLC → same NUMA → cross-NUMA

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NActors {

    struct TPhysicalCore {
        ui32 CoreId = 0;               // first CPU in the SMT group
        TVector<ui32> Cpus;            // SMT siblings (logical CPUs)
        ui32 NumaNode = 0;
        ui32 LlcId = 0;
    };

    class TTopologyMap {
    public:
        // Discovers topology from sysfs rooted at `sysfsRoot`.
        // Default is /sys/devices/system for production use.
        // Pass a temp directory root for testing with mock sysfs.
        // On non-Linux, construction is a no-op (empty topology).
        explicit TTopologyMap([[maybe_unused]] const TString& sysfsRoot = "/sys/devices/system") {
#ifdef _linux_
            Discover(sysfsRoot);
#endif
        }

        // Returns SMT siblings (hyperthreads on the same physical core).
        // Includes `cpu` itself. Returns {cpu} if unknown.
        TVector<ui32> GetSmtSiblings(ui32 cpu) const {
            if (cpu < SmtSiblings_.size() && !SmtSiblings_[cpu].empty()) {
                return SmtSiblings_[cpu];
            }
            return {cpu};
        }

        // Returns CPUs sharing the last-level cache (typically L3).
        // Includes `cpu` itself. Returns {cpu} if unknown.
        TVector<ui32> GetLlcNeighbors(ui32 cpu) const {
            if (cpu < LlcNeighbors_.size() && !LlcNeighbors_[cpu].empty()) {
                return LlcNeighbors_[cpu];
            }
            return {cpu};
        }

        // Returns CPUs on the same NUMA node.
        // Includes `cpu` itself. Returns {cpu} if unknown.
        TVector<ui32> GetNumaNeighbors(ui32 cpu) const {
            if (cpu < NumaNeighbors_.size() && !NumaNeighbors_[cpu].empty()) {
                return NumaNeighbors_[cpu];
            }
            return {cpu};
        }

        // Returns the NUMA node ID for `cpu`. Returns 0 if unknown.
        ui32 GetNumaNode(ui32 cpu) const {
            if (cpu < NumaNode_.size()) {
                return NumaNode_[cpu];
            }
            return 0;
        }

        // Returns all physical cores, sorted by CoreId.
        const TVector<TPhysicalCore>& GetPhysicalCores() const {
            return PhysicalCores_;
        }

        // Returns the number of discovered CPUs.
        ui32 GetCpuCount() const {
            return CpuCount_;
        }

#ifdef _linux_
        // Parses a Linux CPU list string (e.g. "0-3,5,7-9") into a
        // sorted vector of CPU IDs. Public for testability.
        static TVector<ui32> ParseCpuList(const TString& s);
#endif

    private:
#ifdef _linux_
        static TString ReadSmallFile(const TString& path);
        void Discover(const TString& sysfsRoot);
#endif

        TVector<TPhysicalCore> PhysicalCores_;

        // Per-CPU lookup tables, indexed by CPU ID.
        // Entries for undiscovered CPUs are empty / 0.
        TVector<TVector<ui32>> SmtSiblings_;
        TVector<TVector<ui32>> LlcNeighbors_;
        TVector<TVector<ui32>> NumaNeighbors_;
        TVector<ui32> NumaNode_;
        TVector<ui32> LlcId_;
        ui32 CpuCount_ = 0;
    };

} // namespace NActors
