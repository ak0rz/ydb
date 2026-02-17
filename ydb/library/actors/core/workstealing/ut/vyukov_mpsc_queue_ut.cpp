#include <ydb/library/actors/core/workstealing/vyukov_mpsc_queue.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/sanitizers.h>

#include <algorithm>
#include <atomic>
#include <thread>
#include <vector>

using namespace NActors::NWorkStealing;

Y_UNIT_TEST_SUITE(VyukovMpscQueue) {

    Y_UNIT_TEST(PushPopSingleThread) {
        TVyukovMPSCQueue<ui32> queue;

        constexpr ui32 N = 1000;
        for (ui32 i = 0; i < N; ++i) {
            queue.Push(i);
        }

        for (ui32 i = 0; i < N; ++i) {
            auto val = queue.TryPop();
            UNIT_ASSERT(val.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*val, i);
        }

        UNIT_ASSERT(!queue.TryPop().has_value());
    }

    Y_UNIT_TEST(PopEmptyQueue) {
        TVyukovMPSCQueue<ui32> queue;

        UNIT_ASSERT(!queue.TryPop().has_value());
        UNIT_ASSERT(!queue.TryPop().has_value());
        UNIT_ASSERT(!queue.NonEmpty());
    }

    Y_UNIT_TEST(DrainToBasic) {
        TVyukovMPSCQueue<ui32> queue;

        constexpr ui32 N = 100;
        for (ui32 i = 0; i < N; ++i) {
            queue.Push(i);
        }

        std::vector<ui32> output(N);
        size_t count = queue.DrainTo(output.data(), N);

        UNIT_ASSERT_VALUES_EQUAL(count, N);
        for (ui32 i = 0; i < N; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(output[i], i);
        }

        UNIT_ASSERT(!queue.TryPop().has_value());
    }

    Y_UNIT_TEST(DrainToPartial) {
        TVyukovMPSCQueue<ui32> queue;

        constexpr ui32 N = 100;
        constexpr ui32 MaxDrain = 30;

        for (ui32 i = 0; i < N; ++i) {
            queue.Push(i);
        }

        std::vector<ui32> output(MaxDrain);
        size_t count = queue.DrainTo(output.data(), MaxDrain);

        UNIT_ASSERT_VALUES_EQUAL(count, MaxDrain);
        for (ui32 i = 0; i < MaxDrain; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(output[i], i);
        }

        // Remaining items should still be in the queue
        auto val = queue.TryPop();
        UNIT_ASSERT(val.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*val, MaxDrain);
    }

    Y_UNIT_TEST(MultiProducerSingleConsumer) {
        constexpr size_t NumThreads = 4;
        constexpr size_t ItemsPerThread = NSan::PlainOrUnderSanitizer(100000u, 10000u);

        TVyukovMPSCQueue<ui64> queue;
        std::atomic<bool> start{false};

        // Each producer pushes values encoded as (threadId * ItemsPerThread + seqNo)
        std::vector<std::thread> producers;
        producers.reserve(NumThreads);

        for (size_t t = 0; t < NumThreads; ++t) {
            producers.emplace_back([&queue, &start, t] {
                while (!start.load(std::memory_order_acquire)) {
                    // spin until all threads are ready
                }
                for (size_t i = 0; i < ItemsPerThread; ++i) {
                    queue.Push(static_cast<ui64>(t * ItemsPerThread + i));
                }
            });
        }

        start.store(true, std::memory_order_release);

        // Consumer: drain all items
        std::vector<ui64> observed;
        observed.reserve(NumThreads * ItemsPerThread);
        const size_t totalExpected = NumThreads * ItemsPerThread;

        while (observed.size() < totalExpected) {
            auto val = queue.TryPop();
            if (val) {
                observed.push_back(*val);
            }
        }

        for (auto& thread : producers) {
            thread.join();
        }

        UNIT_ASSERT_VALUES_EQUAL(observed.size(), totalExpected);

        // Verify every item was received exactly once
        std::sort(observed.begin(), observed.end());
        for (size_t i = 0; i < totalExpected; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(observed[i], i);
        }
    }

    Y_UNIT_TEST(NonEmptyCheck) {
        TVyukovMPSCQueue<ui32> queue;

        UNIT_ASSERT(!queue.NonEmpty());

        queue.Push(42);
        UNIT_ASSERT(queue.NonEmpty());

        auto val = queue.TryPop();
        UNIT_ASSERT(val.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*val, 42u);

        UNIT_ASSERT(!queue.NonEmpty());
    }

    Y_UNIT_TEST(NodePoolRecycling) {
        // Use a pooled queue: PoolThreshold=64
        TVyukovMPSCQueue<ui32, 64> queue;

        // Push and pop many items to exercise pool recycling.
        // After the first batch, subsequent pushes should reuse pooled nodes.
        constexpr ui32 Rounds = 10;
        constexpr ui32 BatchSize = 50;

        for (ui32 round = 0; round < Rounds; ++round) {
            for (ui32 i = 0; i < BatchSize; ++i) {
                queue.Push(round * BatchSize + i);
            }
            for (ui32 i = 0; i < BatchSize; ++i) {
                auto val = queue.TryPop();
                UNIT_ASSERT(val.has_value());
                UNIT_ASSERT_VALUES_EQUAL(*val, round * BatchSize + i);
            }
            UNIT_ASSERT(!queue.TryPop().has_value());
        }

        // If ASAN is enabled, it will detect any double-frees or leaks from
        // incorrect pool management.
    }

    Y_UNIT_TEST(NodePoolDisabled) {
        // Use a queue with pool disabled: PoolThreshold=0
        TVyukovMPSCQueue<ui32, 0> queue;

        constexpr ui32 N = 200;
        for (ui32 i = 0; i < N; ++i) {
            queue.Push(i);
        }
        for (ui32 i = 0; i < N; ++i) {
            auto val = queue.TryPop();
            UNIT_ASSERT(val.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*val, i);
        }
        UNIT_ASSERT(!queue.TryPop().has_value());
    }

    Y_UNIT_TEST(DestructorCleansUp) {
        // Push items then destroy without popping.
        // ASAN will detect leaks if the destructor fails to free nodes.
        {
            TVyukovMPSCQueue<ui32> queue;
            for (ui32 i = 0; i < 100; ++i) {
                queue.Push(i);
            }
            // queue goes out of scope without popping
        }

        // Also test with pool enabled
        {
            TVyukovMPSCQueue<ui32, 32> queue;
            for (ui32 i = 0; i < 100; ++i) {
                queue.Push(i);
            }
            // Pop half, then destroy
            for (ui32 i = 0; i < 50; ++i) {
                queue.TryPop();
            }
        }
    }

    Y_UNIT_TEST(MultiProducerWithPool) {
        // Concurrent test with pool enabled to exercise pool steal path
        constexpr size_t NumThreads = 4;
        constexpr size_t ItemsPerThread = NSan::PlainOrUnderSanitizer(50000u, 5000u);

        TVyukovMPSCQueue<ui64, 128> queue;
        std::atomic<bool> start{false};

        std::vector<std::thread> producers;
        producers.reserve(NumThreads);

        for (size_t t = 0; t < NumThreads; ++t) {
            producers.emplace_back([&queue, &start, t] {
                while (!start.load(std::memory_order_acquire)) {
                    // spin
                }
                for (size_t i = 0; i < ItemsPerThread; ++i) {
                    queue.Push(static_cast<ui64>(t * ItemsPerThread + i));
                }
            });
        }

        start.store(true, std::memory_order_release);

        std::vector<ui64> observed;
        observed.reserve(NumThreads * ItemsPerThread);
        const size_t totalExpected = NumThreads * ItemsPerThread;

        while (observed.size() < totalExpected) {
            auto val = queue.TryPop();
            if (val) {
                observed.push_back(*val);
            }
        }

        for (auto& thread : producers) {
            thread.join();
        }

        UNIT_ASSERT_VALUES_EQUAL(observed.size(), totalExpected);

        std::sort(observed.begin(), observed.end());
        for (size_t i = 0; i < totalExpected; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(observed[i], i);
        }
    }

    Y_UNIT_TEST(InterleavedPushPop) {
        // Interleave pushes and pops to exercise varied queue states
        TVyukovMPSCQueue<ui32> queue;

        ui32 pushCount = 0;
        ui32 popCount = 0;

        for (ui32 round = 0; round < 100; ++round) {
            // Push a variable number
            ui32 toPush = (round % 7) + 1;
            for (ui32 i = 0; i < toPush; ++i) {
                queue.Push(pushCount++);
            }

            // Pop a variable number
            ui32 toPop = (round % 5) + 1;
            for (ui32 i = 0; i < toPop; ++i) {
                auto val = queue.TryPop();
                if (!val) {
                    break;
                }
                UNIT_ASSERT_VALUES_EQUAL(*val, popCount);
                popCount++;
            }
        }

        // Drain remaining
        while (auto val = queue.TryPop()) {
            UNIT_ASSERT_VALUES_EQUAL(*val, popCount);
            popCount++;
        }

        UNIT_ASSERT_VALUES_EQUAL(popCount, pushCount);
    }

} // Y_UNIT_TEST_SUITE(VyukovMpscQueue)
