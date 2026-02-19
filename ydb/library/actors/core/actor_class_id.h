#pragma once

#include <atomic>

#include <util/system/types.h>

namespace NActors {

    struct TActorClassCounter {
        static inline std::atomic<ui32> Next{0};

        static ui32 Count() {
            return Next.load(std::memory_order_relaxed);
        }
    };

    struct TActorClassStats;
    struct TActorClassStatsTable;
    TActorClassStatsTable& GetActorClassStatsTable();

    // Per-type info: sequential class ID + cached stats pointer.
    // Static local → thread-safe one-time init (C++11), O(1) after first use.
    template <typename TActorType>
    struct TActorClassInfo {
        ui32 ClassId;
        TActorClassStats* Stats;

        static const TActorClassInfo& Instance();
    };

} // namespace NActors
