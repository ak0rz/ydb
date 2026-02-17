#include <ydb/library/actors/core/workstealing/chase_lev_deque.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/sanitizers.h>

#include <thread>
#include <vector>
#include <set>
#include <algorithm>
#include <latch>

namespace NActors::NWorkStealing {

    Y_UNIT_TEST_SUITE(ChaseLevDeque) {

        Y_UNIT_TEST(PushPopSingleThread) {
            TChaseLevDeque<int, 64> deque;

            constexpr int N = 32;
            for (int i = 0; i < N; ++i) {
                UNIT_ASSERT(deque.Push(i));
            }

            // PopOwner should return items in LIFO order
            for (int i = N - 1; i >= 0; --i) {
                auto item = deque.PopOwner();
                UNIT_ASSERT(item.has_value());
                UNIT_ASSERT_VALUES_EQUAL(*item, i);
            }

            // Deque should be empty now
            UNIT_ASSERT(!deque.PopOwner().has_value());
        }

        Y_UNIT_TEST(PushFullDeque) {
            TChaseLevDeque<int, 16> deque;

            // Fill to capacity
            for (int i = 0; i < 16; ++i) {
                UNIT_ASSERT(deque.Push(i));
            }

            // One more should fail
            UNIT_ASSERT(!deque.Push(999));

            // Pop one, then push should succeed again
            auto item = deque.PopOwner();
            UNIT_ASSERT(item.has_value());
            UNIT_ASSERT(deque.Push(999));
        }

        Y_UNIT_TEST(PopEmptyDeque) {
            TChaseLevDeque<int, 16> deque;

            UNIT_ASSERT(!deque.PopOwner().has_value());
            UNIT_ASSERT(!deque.PopOwner().has_value());

            // Push and pop one, then try again
            deque.Push(42);
            auto item = deque.PopOwner();
            UNIT_ASSERT(item.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*item, 42);
            UNIT_ASSERT(!deque.PopOwner().has_value());
        }

        Y_UNIT_TEST(StealOneBasic) {
            TChaseLevDeque<int, 64> deque;

            for (int i = 0; i < 10; ++i) {
                deque.Push(i);
            }

            // Steal one from another thread
            std::optional<int> stolen;
            std::thread stealer([&] {
                stolen = deque.StealOne();
            });
            stealer.join();

            UNIT_ASSERT(stolen.has_value());
            // StealOne takes from the top (FIFO), so it should be 0
            UNIT_ASSERT_VALUES_EQUAL(*stolen, 0);

            // Remaining items should be 1..9
            UNIT_ASSERT_VALUES_EQUAL(deque.SizeEstimate(), 9u);
        }

        Y_UNIT_TEST(StealHalfBasic) {
            TChaseLevDeque<int, 64> deque;

            constexpr int N = 10;
            for (int i = 0; i < N; ++i) {
                deque.Push(i);
            }

            int buffer[N];
            size_t count = 0;

            std::thread stealer([&] {
                count = deque.StealHalf(buffer, N);
            });
            stealer.join();

            // Should steal half = 5 items
            UNIT_ASSERT_VALUES_EQUAL(count, 5u);

            // Stolen items should be from the top: 0, 1, 2, 3, 4
            for (size_t i = 0; i < count; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(buffer[i], static_cast<int>(i));
            }

            // Remaining: 5..9
            UNIT_ASSERT_VALUES_EQUAL(deque.SizeEstimate(), 5u);
        }

        Y_UNIT_TEST(StealHalfBatchSize) {
            TChaseLevDeque<int, 64> deque;

            // Push 20 items
            for (int i = 0; i < 20; ++i) {
                deque.Push(i);
            }

            int buffer[20];
            size_t count = 0;

            std::thread stealer([&] {
                count = deque.StealHalf(buffer, 20);
            });
            stealer.join();

            // Should steal at most half = 10
            UNIT_ASSERT(count <= 10u);
            UNIT_ASSERT(count > 0u);

            // With maxOutput = 3, should steal at most 3
            TChaseLevDeque<int, 64> deque2;
            for (int i = 0; i < 20; ++i) {
                deque2.Push(i);
            }

            size_t count2 = 0;
            std::thread stealer2([&] {
                count2 = deque2.StealHalf(buffer, 3);
            });
            stealer2.join();

            UNIT_ASSERT(count2 <= 3u);
            UNIT_ASSERT(count2 > 0u);
        }

        Y_UNIT_TEST(SizeEstimate) {
            TChaseLevDeque<int, 64> deque;

            UNIT_ASSERT_VALUES_EQUAL(deque.SizeEstimate(), 0u);
            UNIT_ASSERT(deque.Empty());

            for (int i = 0; i < 10; ++i) {
                deque.Push(i);
            }

            UNIT_ASSERT_VALUES_EQUAL(deque.SizeEstimate(), 10u);
            UNIT_ASSERT(!deque.Empty());

            deque.PopOwner();
            UNIT_ASSERT_VALUES_EQUAL(deque.SizeEstimate(), 9u);
        }

        Y_UNIT_TEST(EmptyAfterStealAll) {
            TChaseLevDeque<int, 64> deque;

            for (int i = 0; i < 8; ++i) {
                deque.Push(i);
            }

            UNIT_ASSERT(!deque.Empty());

            // Steal all items via repeated StealHalf
            std::thread stealer([&] {
                int buffer[64];
                while (!deque.Empty()) {
                    deque.StealHalf(buffer, 64);
                }
            });
            stealer.join();

            UNIT_ASSERT(deque.Empty());
            UNIT_ASSERT(!deque.PopOwner().has_value());
        }

        Y_UNIT_TEST(OwnerAndStealersConcurrent) {
            constexpr size_t NumStealers = 4;
            constexpr size_t NumItems = NSan::PlainOrUnderSanitizer(100000, 10000);

            TChaseLevDeque<size_t, 1024> deque;

            std::atomic<size_t> totalOwnerPopped{0};
            std::atomic<size_t> totalStolen{0};

            // Each thread collects the items it got
            std::vector<size_t> ownerItems;
            std::vector<std::vector<size_t>> stealerItems(NumStealers);

            std::atomic<bool> done{false};
            std::latch start(NumStealers + 1);

            // Stealer threads
            std::vector<std::thread> stealers;
            stealers.reserve(NumStealers);
            for (size_t s = 0; s < NumStealers; ++s) {
                stealers.emplace_back([&, s] {
                    start.arrive_and_wait();
                    size_t buffer[512];
                    while (!done.load(std::memory_order_relaxed) || !deque.Empty()) {
                        size_t n = deque.StealHalf(buffer, 512);
                        for (size_t i = 0; i < n; ++i) {
                            stealerItems[s].push_back(buffer[i]);
                        }
                        totalStolen.fetch_add(n, std::memory_order_relaxed);
                    }
                });
            }

            // Owner thread
            std::thread owner([&] {
                start.arrive_and_wait();
                for (size_t i = 0; i < NumItems; ++i) {
                    // Push, retrying if full
                    while (!deque.Push(i)) {
                        // Deque is full, pop some items ourselves
                        auto item = deque.PopOwner();
                        if (item.has_value()) {
                            ownerItems.push_back(*item);
                            totalOwnerPopped.fetch_add(1, std::memory_order_relaxed);
                        }
                    }
                }
                // Drain remaining items from the bottom
                while (auto item = deque.PopOwner()) {
                    ownerItems.push_back(*item);
                    totalOwnerPopped.fetch_add(1, std::memory_order_relaxed);
                }
                done.store(true, std::memory_order_release);
            });

            owner.join();
            for (auto& t : stealers) {
                t.join();
            }

            // Collect all items
            std::vector<size_t> allItems;
            allItems.insert(allItems.end(), ownerItems.begin(), ownerItems.end());
            for (size_t s = 0; s < NumStealers; ++s) {
                allItems.insert(allItems.end(), stealerItems[s].begin(), stealerItems[s].end());
            }

            // Every item must appear exactly once
            std::sort(allItems.begin(), allItems.end());
            UNIT_ASSERT_VALUES_EQUAL(allItems.size(), NumItems);
            for (size_t i = 0; i < allItems.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(allItems[i], i);
            }

            Cerr << "... owner popped " << ownerItems.size()
                 << ", stealers took " << totalStolen.load() << Endl;
        }

        // Stress: owner pushes small batches, pops, while stealers steal.
        // Uses a small deque to maximize wrap-around and contention on the
        // last element (Top_ == Bottom_ race).
        Y_UNIT_TEST(StressPushPopStealSmallDeque) {
            constexpr size_t NumStealers = 8;
            constexpr size_t NumRounds = NSan::PlainOrUnderSanitizer(500000u, 50000u);

            // Small capacity = more wrap-around, more last-element races
            TChaseLevDeque<ui32, 8> deque;
            std::atomic<bool> done{false};
            std::latch start(NumStealers + 1);

            std::vector<std::vector<ui32>> stealerResults(NumStealers);
            std::vector<std::thread> stealers;
            stealers.reserve(NumStealers);
            for (size_t s = 0; s < NumStealers; ++s) {
                stealers.emplace_back([&, s] {
                    start.arrive_and_wait();
                    ui32 buf[4]; // small buffer
                    while (!done.load(std::memory_order_acquire)) {
                        size_t n = deque.StealHalf(buf, 4);
                        for (size_t i = 0; i < n; ++i) {
                            stealerResults[s].push_back(buf[i]);
                        }
                    }
                    // Final passes
                    for (int pass = 0; pass < 10; ++pass) {
                        size_t n = deque.StealHalf(buf, 4);
                        for (size_t i = 0; i < n; ++i) {
                            stealerResults[s].push_back(buf[i]);
                        }
                    }
                });
            }

            std::vector<ui32> ownerResults;
            ownerResults.reserve(NumRounds);
            start.arrive_and_wait();

            for (size_t i = 0; i < NumRounds; ++i) {
                ui32 val = static_cast<ui32>(i + 1);
                while (!deque.Push(val)) {
                    // Deque full, pop to make room
                    if (auto item = deque.PopOwner()) {
                        ownerResults.push_back(*item);
                    }
                }
                // Pop roughly every other push to keep deque small
                if (i % 2 == 0) {
                    if (auto item = deque.PopOwner()) {
                        ownerResults.push_back(*item);
                    }
                }
            }

            while (auto item = deque.PopOwner()) {
                ownerResults.push_back(*item);
            }
            done.store(true, std::memory_order_release);

            for (auto& t : stealers) t.join();

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
                "Found " << duplicates << " duplicate values");
            UNIT_ASSERT_VALUES_EQUAL_C(all.size(), NumRounds,
                "Lost items: expected " << NumRounds << " got " << all.size());

            size_t totalStolen = 0;
            for (size_t s = 0; s < NumStealers; ++s) {
                totalStolen += stealerResults[s].size();
            }
            Cerr << "  SmallDequeStress: owner=" << ownerResults.size()
                 << " stolen=" << totalStolen << Endl;
        }

        // Stress: concurrent StealHalf from multiple threads.
        // Owner pushes a batch, then ALL stealers race to steal.
        // Repeat. No item should be stolen by two threads.
        Y_UNIT_TEST(StressConcurrentStealHalf) {
            constexpr size_t NumStealers = 16;
            constexpr size_t NumBatches = NSan::PlainOrUnderSanitizer(10000u, 1000u);
            constexpr size_t BatchSize = 16;

            TChaseLevDeque<ui32, 256> deque;
            std::atomic<bool> done{false};
            std::latch start(NumStealers + 1);

            std::vector<std::vector<ui32>> stealerResults(NumStealers);
            std::vector<std::thread> stealers;
            stealers.reserve(NumStealers);
            for (size_t s = 0; s < NumStealers; ++s) {
                stealers.emplace_back([&, s] {
                    start.arrive_and_wait();
                    ui32 buf[256];
                    while (!done.load(std::memory_order_acquire)) {
                        size_t n = deque.StealHalf(buf, 256);
                        for (size_t i = 0; i < n; ++i) {
                            stealerResults[s].push_back(buf[i]);
                        }
                    }
                });
            }

            std::vector<ui32> ownerResults;
            ownerResults.reserve(NumBatches * BatchSize);
            start.arrive_and_wait();

            for (size_t batch = 0; batch < NumBatches; ++batch) {
                for (size_t i = 0; i < BatchSize; ++i) {
                    ui32 val = static_cast<ui32>(batch * BatchSize + i + 1);
                    deque.Push(val);
                }
                // Let stealers race, then pop remaining
                for (size_t spin = 0; spin < 10; ++spin) {
                    // Give stealers a chance
                }
                while (auto item = deque.PopOwner()) {
                    ownerResults.push_back(*item);
                }
            }
            done.store(true, std::memory_order_release);

            for (auto& t : stealers) t.join();

            std::vector<ui32> all;
            all.insert(all.end(), ownerResults.begin(), ownerResults.end());
            for (size_t s = 0; s < NumStealers; ++s) {
                all.insert(all.end(), stealerResults[s].begin(), stealerResults[s].end());
            }
            std::sort(all.begin(), all.end());

            const size_t totalExpected = NumBatches * BatchSize;
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

            Cerr << "  ConcurrentStealHalf: owner=" << ownerResults.size()
                 << " stolen=" << (all.size() - ownerResults.size()) << Endl;
        }

    } // Y_UNIT_TEST_SUITE(ChaseLevDeque)

} // namespace NActors::NWorkStealing
