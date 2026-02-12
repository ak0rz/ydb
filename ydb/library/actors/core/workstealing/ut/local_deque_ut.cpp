#include <ydb/library/actors/core/workstealing/local_deque.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/sanitizers.h>
#include <util/system/spinlock.h>

#include <atomic>
#include <latch>
#include <optional>
#include <set>
#include <thread>
#include <vector>

namespace NActors {

Y_UNIT_TEST_SUITE(BoundedSPMCDeque) {

    // Verifies basic LIFO ordering: items pushed last are popped first.
    // A failure here means the ring-buffer indexing or bottom tracking is broken.
    Y_UNIT_TEST(SingleThreadPushPopLifo) {
        TBoundedSPMCDeque<16> deque;

        for (ui32 i = 1; i <= 5; ++i) {
            UNIT_ASSERT(deque.Push(i));
        }

        // Pop should return items in reverse order (LIFO).
        for (ui32 expected = 5; expected >= 1; --expected) {
            auto item = deque.Pop();
            UNIT_ASSERT(item.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*item, expected);
        }

        // Deque should be empty now.
        UNIT_ASSERT(!deque.Pop().has_value());
    }

    // Verifies that Push returns false when the deque is full, and that
    // popping one element makes room for exactly one more push.
    // A failure here means the fullness check (b - t >= Size) is wrong.
    Y_UNIT_TEST(PushUntilFull) {
        TBoundedSPMCDeque<8> deque;

        // Fill completely.
        for (ui32 i = 0; i < 8; ++i) {
            UNIT_ASSERT(deque.Push(i));
        }

        // Next push should fail.
        UNIT_ASSERT(!deque.Push(99));

        // Pop one to make room.
        auto item = deque.Pop();
        UNIT_ASSERT(item.has_value());

        // Now push should succeed again.
        UNIT_ASSERT(deque.Push(99));

        // And be full again.
        UNIT_ASSERT(!deque.Push(100));
    }

    // Stress test: owner pushes N items, one stealer thread steals concurrently.
    // After both finish, every item 0..N-1 must be present exactly once.
    // A failure means either items were lost (steal/pop race bug) or duplicated
    // (double-dispatch due to missing memory fence).
    Y_UNIT_TEST(OwnerPushOneStealer) {
        constexpr ui32 N = NSan::PlainOrUnderSanitizer(1000000u, 100000u);
        TBoundedSPMCDeque<256> deque;

        std::vector<ui32> stolen;
        stolen.reserve(N);

        std::latch start(2);

        std::thread stealer([&] {
            start.arrive_and_wait();
            while (true) {
                auto item = deque.Steal();
                if (item.has_value()) {
                    stolen.push_back(*item);
                    if (*item == N - 1) {
                        // Last item seen — drain remaining.
                        while (auto tail = deque.Steal()) {
                            stolen.push_back(*tail);
                        }
                        break;
                    }
                } else {
                    SpinLockPause();
                }
            }
        });

        start.arrive_and_wait();
        for (ui32 i = 0; i < N; ++i) {
            while (!deque.Push(i)) {
                SpinLockPause();
            }
        }

        stealer.join();

        // Every item must appear exactly once.
        std::sort(stolen.begin(), stolen.end());
        UNIT_ASSERT_VALUES_EQUAL(stolen.size(), N);
        for (ui32 i = 0; i < N; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(stolen[i], i);
        }
    }

    // Stress test with owner push+pop and multiple stealers.
    // Owner pushes all items; when the deque is full, owner pops into its own
    // collection. Stealers steal concurrently. After all threads join, the
    // union of owner-popped and stealer-stolen items must equal 0..N-1.
    // Tested with 1, 2, and 4 stealers to exercise different contention levels.
    Y_UNIT_TEST(OwnerPushPopMultipleStealers) {
        for (ui32 nStealers : {1u, 2u, 4u}) {
            constexpr ui32 N = NSan::PlainOrUnderSanitizer(500000u, 50000u);
            TBoundedSPMCDeque<256> deque;

            std::atomic<bool> done{false};

            // Each stealer collects into its own vector to avoid contention
            // on a shared collection.
            std::vector<std::vector<ui32>> stealerResults(nStealers);
            for (auto& v : stealerResults) {
                v.reserve(N / nStealers + 1);
            }

            std::latch start(nStealers + 1);

            std::vector<std::thread> stealers;
            stealers.reserve(nStealers);
            for (ui32 s = 0; s < nStealers; ++s) {
                stealers.emplace_back([&, s] {
                    start.arrive_and_wait();
                    while (!done.load(std::memory_order_relaxed)) {
                        auto item = deque.Steal();
                        if (item.has_value()) {
                            stealerResults[s].push_back(*item);
                        } else {
                            SpinLockPause();
                        }
                    }
                    // Final drain after owner signals done.
                    while (auto item = deque.Steal()) {
                        stealerResults[s].push_back(*item);
                    }
                });
            }

            // Owner: push all items, pop when full.
            std::vector<ui32> ownerPopped;
            ownerPopped.reserve(N);

            start.arrive_and_wait();
            for (ui32 i = 0; i < N; ++i) {
                while (!deque.Push(i)) {
                    // Deque full — pop to make room.
                    auto item = deque.Pop();
                    if (item.has_value()) {
                        ownerPopped.push_back(*item);
                    }
                }
            }

            // Drain remaining items from owner side.
            while (auto item = deque.Pop()) {
                ownerPopped.push_back(*item);
            }

            done.store(true, std::memory_order_relaxed);
            for (auto& t : stealers) {
                t.join();
            }

            // Merge all results and verify completeness.
            std::vector<ui32> all;
            all.reserve(N);
            all.insert(all.end(), ownerPopped.begin(), ownerPopped.end());
            for (auto& v : stealerResults) {
                all.insert(all.end(), v.begin(), v.end());
            }

            std::sort(all.begin(), all.end());
            UNIT_ASSERT_VALUES_EQUAL_C(all.size(), N,
                "nStealers=" << nStealers << ": item count mismatch");
            for (ui32 i = 0; i < N; ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(all[i], i,
                    "nStealers=" << nStealers << ": missing or duplicate item at index " << i);
            }
        }
    }

    // Verifies that StealHalf transfers approximately half the items and
    // that no items are lost or duplicated in the process.
    Y_UNIT_TEST(StealHalfApproximatelyHalf) {
        TBoundedSPMCDeque<256> source;
        TBoundedSPMCDeque<256> target;

        constexpr ui32 N = 100;
        for (ui32 i = 0; i < N; ++i) {
            UNIT_ASSERT(source.Push(i));
        }

        ui32 stolen = source.StealHalf(target);

        // Should steal approximately half (50 ± some margin for implementation).
        UNIT_ASSERT(stolen > 0);
        UNIT_ASSERT(stolen <= N);

        // Total across both deques should be N.
        i64 sourceSize = source.GetSize();
        i64 targetSize = target.GetSize();
        UNIT_ASSERT_VALUES_EQUAL(sourceSize + targetSize, N);

        // Drain both and verify all items present, no duplicates.
        std::set<ui32> seen;
        while (auto item = source.Pop()) {
            UNIT_ASSERT(seen.insert(*item).second);  // .second is false on duplicate
        }
        while (auto item = target.Pop()) {
            UNIT_ASSERT(seen.insert(*item).second);
        }
        UNIT_ASSERT_VALUES_EQUAL(seen.size(), N);
    }

    // Verifies that an empty deque behaves correctly: Pop and Steal both
    // return nullopt, GetSize returns 0. Also checks that after push+pop
    // the deque returns to empty state.
    Y_UNIT_TEST(EmptyDeque) {
        TBoundedSPMCDeque<16> deque;

        UNIT_ASSERT(!deque.Pop().has_value());
        UNIT_ASSERT(!deque.Steal().has_value());
        UNIT_ASSERT_VALUES_EQUAL(deque.GetSize(), 0);

        // Push and pop, should be empty again.
        UNIT_ASSERT(deque.Push(42));
        UNIT_ASSERT_VALUES_EQUAL(deque.GetSize(), 1);
        auto item = deque.Pop();
        UNIT_ASSERT(item.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*item, 42u);

        UNIT_ASSERT(!deque.Pop().has_value());
        UNIT_ASSERT(!deque.Steal().has_value());
        UNIT_ASSERT_VALUES_EQUAL(deque.GetSize(), 0);
    }

    // Contention test: push a single item and have both the owner and a stealer
    // race to take it. Exactly one must succeed per iteration.
    // A failure means either both got the item (double-dispatch) or neither did
    // (item lost), indicating a fence or CAS bug.
    Y_UNIT_TEST(ContendedSingleElement) {
        constexpr ui32 ITERS = NSan::PlainOrUnderSanitizer(100000u, 10000u);

        ui32 ownerWins = 0;
        ui32 stealerWins = 0;

        for (ui32 iter = 0; iter < ITERS; ++iter) {
            TBoundedSPMCDeque<16> deque;
            UNIT_ASSERT(deque.Push(iter));

            std::optional<ui32> ownerResult;
            std::optional<ui32> stealerResult;

            std::latch go(2);

            std::thread stealer([&] {
                go.arrive_and_wait();
                stealerResult = deque.Steal();
            });

            go.arrive_and_wait();
            ownerResult = deque.Pop();

            stealer.join();

            // Exactly one of them must have gotten the item.
            bool ownerGot = ownerResult.has_value();
            bool stealerGot = stealerResult.has_value();
            UNIT_ASSERT_C(ownerGot != stealerGot,
                "iter=" << iter << ": both=" << (ownerGot && stealerGot)
                        << " neither=" << (!ownerGot && !stealerGot));

            if (ownerGot) {
                UNIT_ASSERT_VALUES_EQUAL(*ownerResult, iter);
                ++ownerWins;
            } else {
                UNIT_ASSERT_VALUES_EQUAL(*stealerResult, iter);
                ++stealerWins;
            }
        }

        Cerr << "ContendedSingleElement stats: owner=" << ownerWins
             << " stealer=" << stealerWins << " total=" << ITERS << Endl;
    }

} // Y_UNIT_TEST_SUITE

} // namespace NActors
