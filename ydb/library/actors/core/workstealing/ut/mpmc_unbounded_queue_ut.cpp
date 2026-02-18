#include <ydb/library/actors/core/workstealing/mpmc_unbounded_queue.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/sanitizers.h>

#include <thread>
#include <vector>
#include <atomic>
#include <algorithm>
#include <latch>

namespace NActors::NWorkStealing {

    Y_UNIT_TEST_SUITE(MPMCUnboundedQueue) {

        using TQueue = TMPMCUnboundedQueue<4096, ui32>;

        Y_UNIT_TEST(SingleThreadPushPop) {
            TQueue q;
            constexpr ui32 N = 100;
            for (ui32 i = 0; i < N; ++i) {
                q.Push(i + 1);
            }
            for (ui32 i = 0; i < N; ++i) {
                auto val = q.TryPop();
                UNIT_ASSERT(val.has_value());
                UNIT_ASSERT_VALUES_EQUAL(*val, i + 1);
            }
        }


        Y_UNIT_TEST(PushBeyondOneRing) {
            TQueue q;
            constexpr ui32 N = 2000; // well beyond 502-cell ring capacity
            for (ui32 i = 0; i < N; ++i) {
                q.Push(i + 1);
            }
            for (ui32 i = 0; i < N; ++i) {
                auto val = q.TryPop();
                UNIT_ASSERT(val.has_value());
                UNIT_ASSERT_VALUES_EQUAL(*val, i + 1);
            }
        }

        Y_UNIT_TEST(PopFromEmpty) {
            TQueue q;
            UNIT_ASSERT(!q.TryPop().has_value());
        }

        Y_UNIT_TEST(MPMCStress) {
            constexpr size_t NumProducers = 4;
            constexpr size_t NumConsumers = 4;
            constexpr size_t ItemsPerProducer = NSan::PlainOrUnderSanitizer(100000u, 10000u);

            TQueue q;
            std::atomic<bool> done{false};
            std::latch start(NumProducers + NumConsumers);

            std::vector<std::vector<ui32>> results(NumConsumers);

            std::vector<std::thread> producers;
            producers.reserve(NumProducers);
            for (size_t p = 0; p < NumProducers; ++p) {
                producers.emplace_back([&, p] {
                    start.arrive_and_wait();
                    for (size_t i = 0; i < ItemsPerProducer; ++i) {
                        ui32 val = static_cast<ui32>(p * ItemsPerProducer + i + 1);
                        q.Push(val);
                    }
                });
            }

            std::vector<std::thread> consumers;
            consumers.reserve(NumConsumers);
            for (size_t c = 0; c < NumConsumers; ++c) {
                consumers.emplace_back([&, c] {
                    start.arrive_and_wait();
                    while (!done.load(std::memory_order_acquire)) {
                        if (auto val = q.TryPop()) {
                            results[c].push_back(*val);
                        }
                    }
                    // Final drain
                    while (auto val = q.TryPop()) {
                        results[c].push_back(*val);
                    }
                });
            }

            for (auto& t : producers) t.join();
            done.store(true, std::memory_order_release);
            for (auto& t : consumers) t.join();

            std::vector<ui32> all;
            for (size_t c = 0; c < NumConsumers; ++c) {
                all.insert(all.end(), results[c].begin(), results[c].end());
            }
            std::sort(all.begin(), all.end());

            const size_t totalExpected = NumProducers * ItemsPerProducer;
            UNIT_ASSERT_VALUES_EQUAL(all.size(), totalExpected);

            size_t duplicates = 0;
            for (size_t i = 1; i < all.size(); ++i) {
                if (all[i] == all[i - 1]) {
                    ++duplicates;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL_C(duplicates, 0u,
                "Found " << duplicates << " duplicate values");
        }

        Y_UNIT_TEST(BurstPattern) {
            TQueue q;
            constexpr ui32 N = 100000;
            for (ui32 i = 0; i < N; ++i) {
                q.Push(i + 1);
            }
            std::vector<ui32> popped;
            popped.reserve(N);
            while (auto val = q.TryPop()) {
                popped.push_back(*val);
            }
            UNIT_ASSERT_VALUES_EQUAL(popped.size(), N);
            for (ui32 i = 0; i < N; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(popped[i], i + 1);
            }
        }

        Y_UNIT_TEST(SingleProducerMultipleConsumers) {
            constexpr size_t NumConsumers = 4;
            constexpr size_t TotalItems = NSan::PlainOrUnderSanitizer(200000u, 20000u);

            TQueue q;
            std::atomic<bool> done{false};
            std::latch start(NumConsumers + 1);

            std::vector<std::vector<ui32>> results(NumConsumers);

            std::thread producer([&] {
                start.arrive_and_wait();
                for (size_t i = 0; i < TotalItems; ++i) {
                    q.Push(static_cast<ui32>(i + 1));
                }
            });

            std::vector<std::thread> consumers;
            consumers.reserve(NumConsumers);
            for (size_t c = 0; c < NumConsumers; ++c) {
                consumers.emplace_back([&, c] {
                    start.arrive_and_wait();
                    while (!done.load(std::memory_order_acquire)) {
                        if (auto val = q.TryPop()) {
                            results[c].push_back(*val);
                        }
                    }
                    while (auto val = q.TryPop()) {
                        results[c].push_back(*val);
                    }
                });
            }

            producer.join();
            done.store(true, std::memory_order_release);
            for (auto& t : consumers) t.join();

            std::vector<ui32> all;
            for (size_t c = 0; c < NumConsumers; ++c) {
                all.insert(all.end(), results[c].begin(), results[c].end());
            }
            std::sort(all.begin(), all.end());

            UNIT_ASSERT_VALUES_EQUAL(all.size(), TotalItems);
            size_t duplicates = 0;
            for (size_t i = 1; i < all.size(); ++i) {
                if (all[i] == all[i - 1]) {
                    ++duplicates;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL_C(duplicates, 0u,
                "Found " << duplicates << " duplicate values");
        }

        Y_UNIT_TEST(MultipleProducersSingleConsumer) {
            constexpr size_t NumProducers = 4;
            constexpr size_t ItemsPerProducer = NSan::PlainOrUnderSanitizer(50000u, 5000u);

            TQueue q;
            std::atomic<bool> done{false};
            std::latch start(NumProducers + 1);

            std::vector<ui32> results;

            std::vector<std::thread> producers;
            producers.reserve(NumProducers);
            for (size_t p = 0; p < NumProducers; ++p) {
                producers.emplace_back([&, p] {
                    start.arrive_and_wait();
                    for (size_t i = 0; i < ItemsPerProducer; ++i) {
                        q.Push(static_cast<ui32>(p * ItemsPerProducer + i + 1));
                    }
                });
            }

            std::thread consumer([&] {
                start.arrive_and_wait();
                while (!done.load(std::memory_order_acquire)) {
                    if (auto val = q.TryPop()) {
                        results.push_back(*val);
                    }
                }
                while (auto val = q.TryPop()) {
                    results.push_back(*val);
                }
            });

            for (auto& t : producers) t.join();
            done.store(true, std::memory_order_release);
            consumer.join();

            std::sort(results.begin(), results.end());

            const size_t totalExpected = NumProducers * ItemsPerProducer;
            UNIT_ASSERT_VALUES_EQUAL(results.size(), totalExpected);
            size_t duplicates = 0;
            for (size_t i = 1; i < results.size(); ++i) {
                if (results[i] == results[i - 1]) {
                    ++duplicates;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL_C(duplicates, 0u,
                "Found " << duplicates << " duplicate values");
        }

        // Regression: producer loads TailPage_ pointing to seg, gets preempted;
        // meanwhile seg fills, tail advances, consumers exhaust and free seg;
        // producer resumes and calls TryPush on freed memory.
        // Slow producers (yield every 32 pushes) let consumers catch up and
        // reclaim segments that producers still reference.
        Y_UNIT_TEST(ProducerSegmentRace) {
            constexpr size_t NumProducers = 8;
            constexpr size_t NumConsumers = 8;
            constexpr size_t ItemsPerProducer = NSan::PlainOrUnderSanitizer(100000u, 10000u);

            TQueue q;
            std::atomic<bool> done{false};
            std::latch start(NumProducers + NumConsumers);

            std::vector<std::vector<ui32>> results(NumConsumers);

            std::vector<std::thread> producers;
            producers.reserve(NumProducers);
            for (size_t p = 0; p < NumProducers; ++p) {
                producers.emplace_back([&, p] {
                    start.arrive_and_wait();
                    for (size_t i = 0; i < ItemsPerProducer; ++i) {
                        q.Push(static_cast<ui32>(p * ItemsPerProducer + i + 1));
                        // Yield periodically so consumers can catch up and
                        // reclaim segments, exposing the tail-side race.
                        if (i % 32 == 0) {
                            std::this_thread::yield();
                        }
                    }
                });
            }

            std::vector<std::thread> consumers;
            consumers.reserve(NumConsumers);
            for (size_t c = 0; c < NumConsumers; ++c) {
                consumers.emplace_back([&, c] {
                    start.arrive_and_wait();
                    while (!done.load(std::memory_order_acquire)) {
                        if (auto val = q.TryPop()) {
                            results[c].push_back(*val);
                        }
                    }
                    while (auto val = q.TryPop()) {
                        results[c].push_back(*val);
                    }
                });
            }

            for (auto& t : producers) t.join();
            done.store(true, std::memory_order_release);
            for (auto& t : consumers) t.join();

            std::vector<ui32> all;
            for (size_t c = 0; c < NumConsumers; ++c) {
                all.insert(all.end(), results[c].begin(), results[c].end());
            }
            std::sort(all.begin(), all.end());

            const size_t totalExpected = NumProducers * ItemsPerProducer;
            UNIT_ASSERT_VALUES_EQUAL(all.size(), totalExpected);

            size_t duplicates = 0;
            for (size_t i = 1; i < all.size(); ++i) {
                if (all[i] == all[i - 1]) {
                    ++duplicates;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL_C(duplicates, 0u,
                "Found " << duplicates << " duplicate values");
        }

        Y_UNIT_TEST(MultiSegmentCorrectness) {
            TQueue q;
            constexpr ui32 SegCap = TQueue::SegmentCapacity();
            constexpr ui32 N = SegCap * 5;
            for (ui32 i = 0; i < N; ++i) {
                q.Push(i + 1);
            }
            for (ui32 i = 0; i < N; ++i) {
                auto val = q.TryPop();
                UNIT_ASSERT(val.has_value());
                UNIT_ASSERT_VALUES_EQUAL(*val, i + 1);
            }
            UNIT_ASSERT(!q.TryPop().has_value());
        }

        Y_UNIT_TEST(ConcurrentHighVolume) {
            constexpr size_t NumProducers = 4;
            constexpr size_t NumConsumers = 4;
            constexpr size_t ItemsPerProducer = NSan::PlainOrUnderSanitizer(200000u, 20000u);

            TQueue q;
            std::atomic<bool> done{false};
            std::latch start(NumProducers + NumConsumers);

            std::vector<std::vector<ui32>> results(NumConsumers);

            std::vector<std::thread> producers;
            producers.reserve(NumProducers);
            for (size_t p = 0; p < NumProducers; ++p) {
                producers.emplace_back([&, p] {
                    start.arrive_and_wait();
                    for (size_t i = 0; i < ItemsPerProducer; ++i) {
                        ui32 val = static_cast<ui32>(p * ItemsPerProducer + i + 1);
                        q.Push(val);
                    }
                });
            }

            std::vector<std::thread> consumers;
            consumers.reserve(NumConsumers);
            for (size_t c = 0; c < NumConsumers; ++c) {
                consumers.emplace_back([&, c] {
                    start.arrive_and_wait();
                    while (!done.load(std::memory_order_acquire)) {
                        if (auto val = q.TryPop()) {
                            results[c].push_back(*val);
                        }
                    }
                    while (auto val = q.TryPop()) {
                        results[c].push_back(*val);
                    }
                });
            }

            for (auto& t : producers) t.join();
            done.store(true, std::memory_order_release);
            for (auto& t : consumers) t.join();

            std::vector<ui32> all;
            for (size_t c = 0; c < NumConsumers; ++c) {
                all.insert(all.end(), results[c].begin(), results[c].end());
            }
            std::sort(all.begin(), all.end());

            const size_t totalExpected = NumProducers * ItemsPerProducer;
            UNIT_ASSERT_VALUES_EQUAL(all.size(), totalExpected);

            size_t duplicates = 0;
            for (size_t i = 1; i < all.size(); ++i) {
                if (all[i] == all[i - 1]) {
                    ++duplicates;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL_C(duplicates, 0u,
                "Found " << duplicates << " duplicate values");
        }

        // Pointer type: verify tagged pointer packing works correctly.
        Y_UNIT_TEST(PointerTypePushPop) {
            struct TItem { ui32 Id; };
            TMPMCUnboundedQueue<4096, TItem*> q;

            constexpr size_t N = 200;
            TItem items[N];
            for (size_t i = 0; i < N; ++i) {
                items[i].Id = static_cast<ui32>(i);
                q.Push(&items[i]);
            }
            for (size_t i = 0; i < N; ++i) {
                auto val = q.TryPop();
                UNIT_ASSERT(val.has_value());
                UNIT_ASSERT_VALUES_EQUAL((*val)->Id, static_cast<ui32>(i));
            }
            UNIT_ASSERT(!q.TryPop().has_value());
        }

        Y_UNIT_TEST(NullptrPushPop) {
            TMPMCUnboundedQueue<4096, int*> q;
            int x = 42;
            q.Push(nullptr);
            q.Push(&x);
            q.Push(nullptr);

            auto v1 = q.TryPop();
            UNIT_ASSERT(v1.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*v1, nullptr);

            auto v2 = q.TryPop();
            UNIT_ASSERT(v2.has_value());
            UNIT_ASSERT_VALUES_EQUAL(**v2, 42);

            auto v3 = q.TryPop();
            UNIT_ASSERT(v3.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*v3, nullptr);

            UNIT_ASSERT(!q.TryPop().has_value());
        }

        // Pointer type: concurrent producers and consumers.
        Y_UNIT_TEST(PointerMPMCStress) {
            struct TItem { ui32 Id; };
            constexpr size_t NumProducers = 4;
            constexpr size_t NumConsumers = 4;
            constexpr size_t ItemsPerProducer = NSan::PlainOrUnderSanitizer(50000u, 5000u);

            // Pre-allocate items so pointers remain stable.
            std::vector<TItem> items(NumProducers * ItemsPerProducer);
            for (size_t i = 0; i < items.size(); ++i) {
                items[i].Id = static_cast<ui32>(i + 1);
            }

            TMPMCUnboundedQueue<4096, TItem*> q;
            std::atomic<bool> done{false};
            std::latch start(NumProducers + NumConsumers);

            std::vector<std::vector<ui32>> results(NumConsumers);

            std::vector<std::thread> producers;
            producers.reserve(NumProducers);
            for (size_t p = 0; p < NumProducers; ++p) {
                producers.emplace_back([&, p] {
                    start.arrive_and_wait();
                    for (size_t i = 0; i < ItemsPerProducer; ++i) {
                        q.Push(&items[p * ItemsPerProducer + i]);
                    }
                });
            }

            std::vector<std::thread> consumers;
            consumers.reserve(NumConsumers);
            for (size_t c = 0; c < NumConsumers; ++c) {
                consumers.emplace_back([&, c] {
                    start.arrive_and_wait();
                    while (!done.load(std::memory_order_acquire)) {
                        if (auto val = q.TryPop()) {
                            results[c].push_back((*val)->Id);
                        }
                    }
                    while (auto val = q.TryPop()) {
                        results[c].push_back((*val)->Id);
                    }
                });
            }

            for (auto& t : producers) t.join();
            done.store(true, std::memory_order_release);
            for (auto& t : consumers) t.join();

            std::vector<ui32> all;
            for (size_t c = 0; c < NumConsumers; ++c) {
                all.insert(all.end(), results[c].begin(), results[c].end());
            }
            std::sort(all.begin(), all.end());

            const size_t totalExpected = NumProducers * ItemsPerProducer;
            UNIT_ASSERT_VALUES_EQUAL(all.size(), totalExpected);

            size_t duplicates = 0;
            for (size_t i = 1; i < all.size(); ++i) {
                if (all[i] == all[i - 1]) {
                    ++duplicates;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL_C(duplicates, 0u,
                "Found " << duplicates << " duplicate values");
        }

        // DEBRA reclamation: push/pop spanning many segments,
        // verify FreeCount > 0 before destructor (online reclamation works).
        Y_UNIT_TEST(SingleThreadReclamation) {
            TQueue q;
            TQueue::ResetSegmentCounters();

            constexpr ui32 SegCap = TQueue::SegmentCapacity();
            // Push/pop enough to exhaust several segments.
            constexpr ui32 Rounds = 10;
            for (ui32 r = 0; r < Rounds; ++r) {
                for (ui32 i = 0; i < SegCap; ++i) {
                    q.Push(i + 1);
                }
                for (ui32 i = 0; i < SegCap; ++i) {
                    auto val = q.TryPop();
                    UNIT_ASSERT(val.has_value());
                    UNIT_ASSERT_VALUES_EQUAL(*val, i + 1);
                }
            }

            // Online reclamation should have freed some segments.
            size_t freed = TQueue::SegmentFreeCount();
            UNIT_ASSERT_C(freed > 0,
                "Expected online reclamation to free segments, but FreeCount=" << freed);
        }

        // Concurrent reclamation stress: 8 producers + 8 consumers,
        // producers yield periodically to expose reclamation races.
        Y_UNIT_TEST(ConcurrentReclamationStress) {
            constexpr size_t NumProducers = 8;
            constexpr size_t NumConsumers = 8;
            constexpr size_t ItemsPerProducer = NSan::PlainOrUnderSanitizer(100000u, 10000u);

            TQueue q;
            TQueue::ResetSegmentCounters();
            std::atomic<bool> done{false};
            std::latch start(NumProducers + NumConsumers);

            std::vector<std::vector<ui32>> results(NumConsumers);

            std::vector<std::thread> producers;
            producers.reserve(NumProducers);
            for (size_t p = 0; p < NumProducers; ++p) {
                producers.emplace_back([&, p] {
                    start.arrive_and_wait();
                    for (size_t i = 0; i < ItemsPerProducer; ++i) {
                        q.Push(static_cast<ui32>(p * ItemsPerProducer + i + 1));
                        if (i % 32 == 0) {
                            std::this_thread::yield();
                        }
                    }
                });
            }

            std::vector<std::thread> consumers;
            consumers.reserve(NumConsumers);
            for (size_t c = 0; c < NumConsumers; ++c) {
                consumers.emplace_back([&, c] {
                    start.arrive_and_wait();
                    while (!done.load(std::memory_order_acquire)) {
                        if (auto val = q.TryPop()) {
                            results[c].push_back(*val);
                        }
                    }
                    while (auto val = q.TryPop()) {
                        results[c].push_back(*val);
                    }
                });
            }

            for (auto& t : producers) t.join();
            done.store(true, std::memory_order_release);
            for (auto& t : consumers) t.join();

            // Verify correctness.
            std::vector<ui32> all;
            for (size_t c = 0; c < NumConsumers; ++c) {
                all.insert(all.end(), results[c].begin(), results[c].end());
            }
            std::sort(all.begin(), all.end());

            const size_t totalExpected = NumProducers * ItemsPerProducer;
            UNIT_ASSERT_VALUES_EQUAL(all.size(), totalExpected);

            size_t duplicates = 0;
            for (size_t i = 1; i < all.size(); ++i) {
                if (all[i] == all[i - 1]) {
                    ++duplicates;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL_C(duplicates, 0u,
                "Found " << duplicates << " duplicate values");

            // Verify reclamation occurred.
            size_t freed = TQueue::SegmentFreeCount();
            UNIT_ASSERT_C(freed > 0,
                "Expected online reclamation to free segments, but FreeCount=" << freed);
        }

    } // Y_UNIT_TEST_SUITE(MPMCUnboundedQueue)

} // namespace NActors::NWorkStealing
