#include <ydb/library/actors/core/workstealing/ws_slot.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/sanitizers.h>

#include <thread>
#include <vector>
#include <atomic>
#include <algorithm>
#include <latch>

namespace NActors::NWorkStealing {

    Y_UNIT_TEST_SUITE(WsSlot) {

        // Helper: bring a slot from Inactive to Active.
        static void ActivateSlot(TSlot& slot) {
            UNIT_ASSERT(slot.TryTransition(ESlotState::Inactive, ESlotState::Initializing));
            UNIT_ASSERT(slot.TryTransition(ESlotState::Initializing, ESlotState::Active));
        }

        Y_UNIT_TEST(ValidStateTransitions) {
            TSlot slot;

            UNIT_ASSERT_EQUAL(slot.GetState(), ESlotState::Inactive);

            // Inactive -> Initializing
            UNIT_ASSERT(slot.TryTransition(ESlotState::Inactive, ESlotState::Initializing));
            UNIT_ASSERT_EQUAL(slot.GetState(), ESlotState::Initializing);

            // Initializing -> Active
            UNIT_ASSERT(slot.TryTransition(ESlotState::Initializing, ESlotState::Active));
            UNIT_ASSERT_EQUAL(slot.GetState(), ESlotState::Active);

            // Active -> Draining
            UNIT_ASSERT(slot.TryTransition(ESlotState::Active, ESlotState::Draining));
            UNIT_ASSERT_EQUAL(slot.GetState(), ESlotState::Draining);

            // Draining -> Inactive
            UNIT_ASSERT(slot.TryTransition(ESlotState::Draining, ESlotState::Inactive));
            UNIT_ASSERT_EQUAL(slot.GetState(), ESlotState::Inactive);
        }

        Y_UNIT_TEST(InvalidStateTransitions) {
            // Inactive -> Active (skip Initializing)
            {
                TSlot slot;
                UNIT_ASSERT(!slot.TryTransition(ESlotState::Inactive, ESlotState::Active));
                UNIT_ASSERT_EQUAL(slot.GetState(), ESlotState::Inactive);
            }

            // Inactive -> Draining
            {
                TSlot slot;
                UNIT_ASSERT(!slot.TryTransition(ESlotState::Inactive, ESlotState::Draining));
            }

            // Active -> Inactive (skip Draining)
            {
                TSlot slot;
                ActivateSlot(slot);
                UNIT_ASSERT(!slot.TryTransition(ESlotState::Active, ESlotState::Inactive));
                UNIT_ASSERT_EQUAL(slot.GetState(), ESlotState::Active);
            }

            // Active -> Initializing
            {
                TSlot slot;
                ActivateSlot(slot);
                UNIT_ASSERT(!slot.TryTransition(ESlotState::Active, ESlotState::Initializing));
            }

            // Draining -> Active
            {
                TSlot slot;
                ActivateSlot(slot);
                UNIT_ASSERT(slot.TryTransition(ESlotState::Active, ESlotState::Draining));
                UNIT_ASSERT(!slot.TryTransition(ESlotState::Draining, ESlotState::Active));
                UNIT_ASSERT_EQUAL(slot.GetState(), ESlotState::Draining);
            }

            // Draining -> Initializing
            {
                TSlot slot;
                ActivateSlot(slot);
                UNIT_ASSERT(slot.TryTransition(ESlotState::Active, ESlotState::Draining));
                UNIT_ASSERT(!slot.TryTransition(ESlotState::Draining, ESlotState::Initializing));
            }

            // Initializing -> Inactive
            {
                TSlot slot;
                UNIT_ASSERT(slot.TryTransition(ESlotState::Inactive, ESlotState::Initializing));
                UNIT_ASSERT(!slot.TryTransition(ESlotState::Initializing, ESlotState::Inactive));
            }

            // Initializing -> Draining
            {
                TSlot slot;
                UNIT_ASSERT(slot.TryTransition(ESlotState::Inactive, ESlotState::Initializing));
                UNIT_ASSERT(!slot.TryTransition(ESlotState::Initializing, ESlotState::Draining));
            }
        }

        Y_UNIT_TEST(PushAndPop) {
            TSlot slot;
            ActivateSlot(slot);

            slot.Push(42);
            auto item = slot.Pop();
            UNIT_ASSERT(item.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*item, 42u);
        }

        Y_UNIT_TEST(PushMultipleAndPop) {
            TSlot slot;
            ActivateSlot(slot);

            constexpr ui32 N = 20;
            for (ui32 i = 0; i < N; ++i) {
                slot.Push(i + 1);
            }

            std::vector<ui32> popped;
            while (auto item = slot.Pop()) {
                popped.push_back(*item);
            }
            UNIT_ASSERT_VALUES_EQUAL(popped.size(), N);

            std::sort(popped.begin(), popped.end());
            for (ui32 i = 0; i < N; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(popped[i], i + 1);
            }
        }

        Y_UNIT_TEST(PopEmpty) {
            TSlot slot;
            ActivateSlot(slot);

            UNIT_ASSERT(!slot.Pop().has_value());
        }

        Y_UNIT_TEST(StealHalfWhenActive) {
            TSlot slot;
            ActivateSlot(slot);

            constexpr ui32 N = 10;
            for (ui32 i = 0; i < N; ++i) {
                slot.Push(i + 1);
            }

            ui32 buffer[N];
            size_t stolen = 0;

            std::thread stealer([&] {
                stolen = slot.StealHalf(buffer, N);
            });
            stealer.join();

            UNIT_ASSERT_VALUES_EQUAL(stolen, N / 2);

            // Verify stolen items are valid (1..N range, no duplicates)
            std::vector<ui32> stolenVec(buffer, buffer + stolen);
            std::sort(stolenVec.begin(), stolenVec.end());
            for (size_t i = 1; i < stolenVec.size(); ++i) {
                UNIT_ASSERT(stolenVec[i] != stolenVec[i - 1]);
                UNIT_ASSERT(stolenVec[i] >= 1 && stolenVec[i] <= N);
            }
        }

        Y_UNIT_TEST(StealHalfWhenDraining) {
            TSlot slot;
            ActivateSlot(slot);

            constexpr ui32 N = 10;
            for (ui32 i = 0; i < N; ++i) {
                slot.Push(i + 1);
            }

            UNIT_ASSERT(slot.TryTransition(ESlotState::Active, ESlotState::Draining));

            ui32 buffer[N];
            size_t stolen = 0;

            std::thread stealer([&] {
                stolen = slot.StealHalf(buffer, N);
            });
            stealer.join();

            UNIT_ASSERT(stolen > 0);
        }

        Y_UNIT_TEST(StealHalfWhenInactive) {
            TSlot slot;

            ui32 buffer[8];
            size_t stolen = 0;

            std::thread stealer([&] {
                stolen = slot.StealHalf(buffer, 8);
            });
            stealer.join();

            UNIT_ASSERT_VALUES_EQUAL(stolen, 0u);
        }

        // ---- Stress tests for the push → pop/steal pipeline ----

        // Simulates the PollSlot pattern: multiple pushers push into MPMC,
        // one owner pops, multiple stealers steal.
        // Every pushed value must be consumed exactly once.
        Y_UNIT_TEST(StressPushPopSteal) {
            constexpr size_t NumInjectors = 8;
            constexpr size_t NumStealers = 4;
            constexpr size_t ItemsPerInjector = NSan::PlainOrUnderSanitizer(50000u, 5000u);

            TSlot slot;
            ActivateSlot(slot);

            std::atomic<bool> stop{false};
            std::atomic<size_t> injected{0};
            std::latch startBarrier(NumInjectors + NumStealers + 1);

            // Injector threads: push unique values into MPSC
            std::vector<std::thread> injectors;
            injectors.reserve(NumInjectors);
            for (size_t t = 0; t < NumInjectors; ++t) {
                injectors.emplace_back([&, t] {
                    startBarrier.arrive_and_wait();
                    for (size_t i = 0; i < ItemsPerInjector; ++i) {
                        ui32 val = static_cast<ui32>(t * ItemsPerInjector + i + 1); // 1-based to avoid 0
                        slot.Push(val);
                        injected.fetch_add(1, std::memory_order_relaxed);
                    }
                });
            }

            // Stealer threads: steal from Chase-Lev
            std::vector<std::vector<ui32>> stealerResults(NumStealers);
            std::vector<std::thread> stealers;
            stealers.reserve(NumStealers);
            for (size_t s = 0; s < NumStealers; ++s) {
                stealers.emplace_back([&, s] {
                    startBarrier.arrive_and_wait();
                    ui32 buf[128];
                    while (!stop.load(std::memory_order_acquire)) {
                        size_t n = slot.StealHalf(buf, 128);
                        for (size_t i = 0; i < n; ++i) {
                            stealerResults[s].push_back(buf[i]);
                        }
                    }
                    // Final drain after stop
                    for (int pass = 0; pass < 3; ++pass) {
                        size_t n = slot.StealHalf(buf, 128);
                        for (size_t i = 0; i < n; ++i) {
                            stealerResults[s].push_back(buf[i]);
                        }
                    }
                });
            }

            // Owner thread: drain and pop
            std::vector<ui32> ownerResults;
            ownerResults.reserve(NumInjectors * ItemsPerInjector);
            startBarrier.arrive_and_wait();

            const size_t totalExpected = NumInjectors * ItemsPerInjector;
            while (ownerResults.size() < totalExpected || !stop.load(std::memory_order_relaxed)) {
                while (auto item = slot.Pop()) {
                    ownerResults.push_back(*item);
                }
                if (injected.load(std::memory_order_relaxed) >= totalExpected) {
                    // All injected, do one more drain+pop then stop
                    while (auto item = slot.Pop()) {
                        ownerResults.push_back(*item);
                    }
                    stop.store(true, std::memory_order_release);
                    break;
                }
            }

            for (auto& t : injectors) t.join();
            for (auto& t : stealers) t.join();

            // Final cleanup: pop remaining items
            while (auto item = slot.Pop()) {
                ownerResults.push_back(*item);
            }

            // Collect all results
            std::vector<ui32> all;
            all.insert(all.end(), ownerResults.begin(), ownerResults.end());
            for (size_t s = 0; s < NumStealers; ++s) {
                all.insert(all.end(), stealerResults[s].begin(), stealerResults[s].end());
            }

            std::sort(all.begin(), all.end());

            // Check for duplicates
            size_t duplicates = 0;
            for (size_t i = 1; i < all.size(); ++i) {
                if (all[i] == all[i - 1]) {
                    ++duplicates;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL_C(duplicates, 0u,
                "Found " << duplicates << " duplicate values");
            UNIT_ASSERT_VALUES_EQUAL_C(all.size(), totalExpected,
                "Lost items: expected " << totalExpected << " got " << all.size());

            Cerr << "  PushPopSteal: owner=" << ownerResults.size()
                 << " stolen=";
            size_t totalStolen = 0;
            for (size_t s = 0; s < NumStealers; ++s) {
                totalStolen += stealerResults[s].size();
            }
            Cerr << totalStolen << Endl;
        }

        // Multi-slot pipeline: inject into one slot, steal to another, verify
        // no duplication across the entire pipeline.
        Y_UNIT_TEST(StressMultiSlotSteal) {
            constexpr size_t NumSlots = 4;
            constexpr size_t ItemsPerSlot = NSan::PlainOrUnderSanitizer(50000u, 5000u);

            TSlot slots[NumSlots];
            for (size_t i = 0; i < NumSlots; ++i) {
                ActivateSlot(slots[i]);
            }

            std::atomic<bool> stop{false};
            std::atomic<size_t> totalInjected{0};
            std::latch startBarrier(NumSlots * 2 + 1); // injectors + owner/stealers + main

            // Results per slot (owner-popped items)
            std::vector<std::vector<ui32>> ownerResults(NumSlots);
            // Results from cross-slot stealing
            std::vector<std::vector<ui32>> stealResults(NumSlots);

            // For each slot: one injector thread
            std::vector<std::thread> threads;
            for (size_t s = 0; s < NumSlots; ++s) {
                threads.emplace_back([&, s] {
                    startBarrier.arrive_and_wait();
                    for (size_t i = 0; i < ItemsPerSlot; ++i) {
                        ui32 val = static_cast<ui32>(s * ItemsPerSlot + i + 1);
                        slots[s].Push(val);
                        totalInjected.fetch_add(1, std::memory_order_relaxed);
                    }
                });
            }

            // For each slot: one owner+stealer thread
            // Owner drains and pops its own slot, steals from the next slot
            for (size_t s = 0; s < NumSlots; ++s) {
                threads.emplace_back([&, s] {
                    startBarrier.arrive_and_wait();
                    size_t nextSlot = (s + 1) % NumSlots;
                    ui32 stealBuf[128];
                    while (!stop.load(std::memory_order_acquire)) {
                        while (auto item = slots[s].Pop()) {
                            ownerResults[s].push_back(*item);
                        }
                        // Steal from neighbor
                        size_t n = slots[nextSlot].StealHalf(stealBuf, 128);
                        for (size_t i = 0; i < n; ++i) {
                            stealResults[s].push_back(stealBuf[i]);
                        }
                    }
                    // Final cleanup
                    while (auto item = slots[s].Pop()) {
                        ownerResults[s].push_back(*item);
                    }
                });
            }

            const size_t totalExpectedItems = NumSlots * ItemsPerSlot;
            startBarrier.arrive_and_wait();
            // Wait for all injections to complete
            while (totalInjected.load(std::memory_order_relaxed) < totalExpectedItems) {
                // spin
            }
            // Let workers drain a bit more
            for (int i = 0; i < 100; ++i) {
                // brief spin
                std::atomic_signal_fence(std::memory_order_seq_cst);
            }
            stop.store(true, std::memory_order_release);

            for (auto& t : threads) t.join();

            // Final drain of all slots
            for (size_t s = 0; s < NumSlots; ++s) {
                while (auto item = slots[s].Pop()) {
                    ownerResults[s].push_back(*item);
                }
            }

            // Collect all results
            std::vector<ui32> all;
            for (size_t s = 0; s < NumSlots; ++s) {
                all.insert(all.end(), ownerResults[s].begin(), ownerResults[s].end());
                all.insert(all.end(), stealResults[s].begin(), stealResults[s].end());
            }
            std::sort(all.begin(), all.end());

            const size_t totalExpected = NumSlots * ItemsPerSlot;
            size_t duplicates = 0;
            for (size_t i = 1; i < all.size(); ++i) {
                if (all[i] == all[i - 1]) {
                    if (duplicates < 10) {
                        Cerr << "  DUPLICATE: " << all[i] << Endl;
                    }
                    ++duplicates;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL_C(duplicates, 0u,
                "Found " << duplicates << " duplicate values");
            UNIT_ASSERT_VALUES_EQUAL_C(all.size(), totalExpected,
                "Lost items: expected " << totalExpected << " got " << all.size());

            size_t totalOwner = 0, totalSteal = 0;
            for (size_t s = 0; s < NumSlots; ++s) {
                totalOwner += ownerResults[s].size();
                totalSteal += stealResults[s].size();
            }
            Cerr << "  MultiSlotSteal: owner=" << totalOwner
                 << " stolen=" << totalSteal << Endl;
        }

        // Stress the exact PollSlot pop-steal loop with re-injection,
        // simulating what happens when executeCallback returns true.
        Y_UNIT_TEST(StressPopReinjectSteal) {
            constexpr size_t NumStealers = 4;
            constexpr size_t NumItems = NSan::PlainOrUnderSanitizer(100000u, 10000u);

            TSlot slot;
            ActivateSlot(slot);

            // Pre-inject all items
            for (size_t i = 0; i < NumItems; ++i) {
                slot.Push(static_cast<ui32>(i + 1));
            }

            std::atomic<bool> done{false};
            std::latch start(NumStealers + 1);

            std::vector<std::vector<ui32>> stealerResults(NumStealers);
            std::vector<std::atomic<size_t>> stealerCounts(NumStealers);
            for (size_t s = 0; s < NumStealers; ++s) {
                stealerCounts[s].store(0, std::memory_order_relaxed);
            }
            std::vector<std::thread> stealers;
            stealers.reserve(NumStealers);
            for (size_t s = 0; s < NumStealers; ++s) {
                stealers.emplace_back([&, s] {
                    start.arrive_and_wait();
                    ui32 buf[64];
                    while (!done.load(std::memory_order_acquire)) {
                        size_t n = slot.StealHalf(buf, 64);
                        for (size_t i = 0; i < n; ++i) {
                            stealerResults[s].push_back(buf[i]);
                        }
                        stealerCounts[s].store(stealerResults[s].size(), std::memory_order_release);
                    }
                    // Final passes
                    for (int pass = 0; pass < 5; ++pass) {
                        size_t n = slot.StealHalf(buf, 64);
                        for (size_t i = 0; i < n; ++i) {
                            stealerResults[s].push_back(buf[i]);
                        }
                    }
                });
            }

            // Owner: drain, pop, sometimes reinject (simulating budget exhaustion)
            std::vector<ui32> ownerResults;
            ownerResults.reserve(NumItems);
            start.arrive_and_wait();

            size_t reinjectCount = 0;
            while (ownerResults.size() < NumItems) {
                size_t budget = 8;
                while (budget > 0) {
                    auto item = slot.Pop();
                    if (!item) break;
                    --budget;
                    // Every 5th item, reinject instead of consuming
                    if (ownerResults.size() % 5 == 4) {
                        slot.Push(*item);
                        ++reinjectCount;
                    } else {
                        ownerResults.push_back(*item);
                    }
                }
                size_t totalStolen = 0;
                for (size_t s = 0; s < NumStealers; ++s) {
                    totalStolen += stealerCounts[s].load(std::memory_order_acquire);
                }
                if (ownerResults.size() + totalStolen >= NumItems) {
                    break;
                }
            }

            // Final pop
            while (auto item = slot.Pop()) {
                ownerResults.push_back(*item);
            }
            done.store(true, std::memory_order_release);

            for (auto& t : stealers) t.join();

            // Collect and verify
            std::vector<ui32> all;
            all.insert(all.end(), ownerResults.begin(), ownerResults.end());
            for (size_t s = 0; s < NumStealers; ++s) {
                all.insert(all.end(), stealerResults[s].begin(), stealerResults[s].end());
            }
            std::sort(all.begin(), all.end());

            size_t duplicates = 0;
            for (size_t i = 1; i < all.size(); ++i) {
                if (all[i] == all[i - 1]) {
                    if (duplicates < 10) {
                        Cerr << "  DUPLICATE: " << all[i] << Endl;
                    }
                    ++duplicates;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL_C(duplicates, 0u,
                "Found " << duplicates << " duplicate values (reinjects=" << reinjectCount << ")");
            UNIT_ASSERT_VALUES_EQUAL_C(all.size(), NumItems,
                "Lost items: expected " << NumItems << " got " << all.size());

            size_t totalStolen = 0;
            for (size_t s = 0; s < NumStealers; ++s) {
                totalStolen += stealerResults[s].size();
            }
            Cerr << "  PopReinjectSteal: owner=" << ownerResults.size()
                 << " stolen=" << totalStolen
                 << " reinjects=" << reinjectCount << Endl;
        }

    } // Y_UNIT_TEST_SUITE(WsSlot)

} // namespace NActors::NWorkStealing
