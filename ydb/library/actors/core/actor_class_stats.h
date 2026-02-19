#pragma once

#include "actor_class_id.h"

#include <atomic>
#include <algorithm>

#include <util/system/types.h>

namespace NActors {

    struct alignas(64) TActorClassStats {
        std::atomic<ui64> ActorsCreated{0};
        std::atomic<ui64> ActorsDestroyed{0};
        std::atomic<ui64> MessagesProcessed{0};
        std::atomic<ui64> ExecutionCycles{0};
        std::atomic<ui64> LifetimeCyclesSum{0};
    };

    struct TActorClassStatsSlab {
        static constexpr size_t SlabSize = 64;

        TActorClassStats Entries[SlabSize];
        std::atomic<TActorClassStatsSlab*> Next{nullptr};
    };

    struct TActorClassStatsTable {
        TActorClassStatsSlab Head;

        // Get reference by class ID. Grows slabs as needed (lock-free via CAS).
        TActorClassStats& Get(ui32 classId) {
            ui32 slabIdx = classId / TActorClassStatsSlab::SlabSize;
            ui32 entryIdx = classId % TActorClassStatsSlab::SlabSize;

            TActorClassStatsSlab* slab = &Head;
            for (ui32 i = 0; i < slabIdx; ++i) {
                TActorClassStatsSlab* next = slab->Next.load(std::memory_order_acquire);
                if (!next) {
                    auto* fresh = new TActorClassStatsSlab();
                    if (slab->Next.compare_exchange_strong(next, fresh,
                            std::memory_order_release, std::memory_order_acquire)) {
                        next = fresh;
                    } else {
                        delete fresh;
                    }
                }
                slab = next;
            }
            return slab->Entries[entryIdx];
        }

        // Iterate all entries up to TActorClassCounter::Count()
        template <typename TFn>
        void ForEach(TFn&& fn) const {
            const ui32 count = TActorClassCounter::Count();
            const TActorClassStatsSlab* slab = &Head;
            ui32 id = 0;
            while (slab && id < count) {
                ui32 limit = std::min(count - id, static_cast<ui32>(TActorClassStatsSlab::SlabSize));
                for (ui32 i = 0; i < limit; ++i) {
                    fn(id + i, slab->Entries[i]);
                }
                id += TActorClassStatsSlab::SlabSize;
                slab = slab->Next.load(std::memory_order_acquire);
            }
        }

        ~TActorClassStatsTable() {
            TActorClassStatsSlab* slab = Head.Next.load(std::memory_order_relaxed);
            while (slab) {
                TActorClassStatsSlab* next = slab->Next.load(std::memory_order_relaxed);
                delete slab;
                slab = next;
            }
        }
    };

    // Defined here so the full TActorClassStatsTable definition is visible.
    template <typename TActorType>
    const TActorClassInfo<TActorType>& TActorClassInfo<TActorType>::Instance() {
        static const TActorClassInfo info = [] {
            TActorClassInfo result;
            result.ClassId = TActorClassCounter::Next.fetch_add(1, std::memory_order_relaxed);
            result.Stats = &GetActorClassStatsTable().Get(result.ClassId);
            return result;
        }();
        return info;
    }

} // namespace NActors
