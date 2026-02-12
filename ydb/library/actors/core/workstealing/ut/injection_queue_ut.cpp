#include <ydb/library/actors/core/workstealing/injection_queue.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/sanitizers.h>
#include <util/system/spinlock.h>

#include <atomic>
#include <latch>
#include <memory>
#include <thread>
#include <vector>

namespace NActors {

Y_UNIT_TEST_SUITE(InjectionQueue) {

    // Verifies FIFO ordering: items pushed first are drained first.
    // The Vyukov MPSC queue preserves insertion order from a single producer
    // because each push links to the predecessor's Next pointer, building a
    // forward chain from tail to head.
    // A failure here means the linking protocol (X1/S1) or drain traversal
    // is broken.
    Y_UNIT_TEST(SingleThreadPushDrainFifo) {
        TInjectionQueue queue;

        constexpr ui32 N = 5;
        TInjectionNode nodes[N];
        for (ui32 i = 0; i < N; ++i) {
            nodes[i].Hint = i + 1;
            queue.Push(&nodes[i]);
        }

        std::vector<ui32> out;
        ui32 count = queue.Drain(out);

        UNIT_ASSERT_VALUES_EQUAL(count, N);
        UNIT_ASSERT_VALUES_EQUAL(out.size(), N);
        for (ui32 i = 0; i < N; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(out[i], i + 1);
        }

        // Queue should be empty now.
        UNIT_ASSERT(queue.IsEmpty());
    }

    // Stress test: N producer threads push concurrently, then a single
    // consumer drains everything after all producers have joined.
    // Verifies that all items are present (no lost items) and unique
    // (no duplicates). Tested with 1, 2, 4, 8, and 16 producers.
    // A failure means the XCHG-based push protocol lost a node (broken
    // linking) or the drain missed a chain segment.
    Y_UNIT_TEST(MultiProducerSingleConsumer) {
        for (ui32 nProducers : {1u, 2u, 4u, 8u, 16u}) {
            constexpr ui32 PerProducer = NSan::PlainOrUnderSanitizer(200000u, 20000u);
            const ui32 total = nProducers * PerProducer;

            TInjectionQueue queue;

            // Pre-allocate nodes per producer. Each producer gets its own
            // contiguous array so there's no false sharing between threads.
            std::vector<std::unique_ptr<TInjectionNode[]>> allNodes(nProducers);
            for (ui32 p = 0; p < nProducers; ++p) {
                allNodes[p] = std::make_unique<TInjectionNode[]>(PerProducer);
                for (ui32 i = 0; i < PerProducer; ++i) {
                    allNodes[p][i].Hint = p * PerProducer + i;
                }
            }

            std::latch start(nProducers);
            std::vector<std::thread> producers;
            producers.reserve(nProducers);
            for (ui32 p = 0; p < nProducers; ++p) {
                producers.emplace_back([&, p] {
                    start.arrive_and_wait();
                    for (ui32 i = 0; i < PerProducer; ++i) {
                        queue.Push(&allNodes[p][i]);
                    }
                });
            }

            for (auto& t : producers) {
                t.join();
            }

            // All pushes complete (both X1 and S1). Drain on main thread.
            std::vector<ui32> drained;
            drained.reserve(total);
            queue.Drain(drained);

            std::sort(drained.begin(), drained.end());
            UNIT_ASSERT_VALUES_EQUAL_C(drained.size(), total,
                "nProducers=" << nProducers << ": item count mismatch");
            for (ui32 i = 0; i < total; ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(drained[i], i,
                    "nProducers=" << nProducers
                    << ": missing or duplicate at index " << i);
            }
        }
    }

    // Verifies correct behavior on an empty queue: Drain returns 0 items
    // and IsEmpty returns true. Also checks that after push+drain the
    // queue returns to the empty state.
    Y_UNIT_TEST(DrainEmptyQueue) {
        TInjectionQueue queue;

        std::vector<ui32> out;
        UNIT_ASSERT_VALUES_EQUAL(queue.Drain(out), 0u);
        UNIT_ASSERT(out.empty());
        UNIT_ASSERT(queue.IsEmpty());

        // Push one item and drain it.
        TInjectionNode node;
        node.Hint = 42;
        queue.Push(&node);
        UNIT_ASSERT(!queue.IsEmpty());

        queue.Drain(out);
        UNIT_ASSERT_VALUES_EQUAL(out.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(out[0], 42u);
        UNIT_ASSERT(queue.IsEmpty());

        // Drain again — should be empty.
        out.clear();
        UNIT_ASSERT_VALUES_EQUAL(queue.Drain(out), 0u);
        UNIT_ASSERT(out.empty());
    }

    // Exercises the incomplete-push window: between X1 (head.exchange) and
    // S1 (prev->next.store) there is a brief period where the push is "in
    // progress" — head has advanced but the chain link isn't complete yet.
    // During this window, Drain must return empty (not garbage).
    //
    // We can't inject a deterministic delay into Push, so this test relies
    // on concurrent execution + TSAN to exercise the window. Multiple
    // producers push one item each while the consumer races to drain.
    // Each iteration: push one item from a producer thread while the
    // consumer thread drains concurrently. The consumer may get 0 items
    // (caught the window or ran first) or 1 item (push completed first).
    // A final drain on the main thread picks up any remainder. We verify
    // that exactly 1 item is collected total with the correct hint value.
    //
    // A failure means drain returned garbage during the window (data race
    // on the buffer) or lost the item entirely.
    Y_UNIT_TEST(IncompletePushWindow) {
        constexpr ui32 ITERS = NSan::PlainOrUnderSanitizer(50000u, 5000u);

        for (ui32 iter = 0; iter < ITERS; ++iter) {
            TInjectionQueue queue;
            TInjectionNode node;
            node.Hint = iter;

            std::vector<ui32> consumerResult;
            std::latch go(2);

            std::thread consumer([&] {
                go.arrive_and_wait();
                queue.Drain(consumerResult);
            });

            go.arrive_and_wait();
            queue.Push(&node);

            consumer.join();

            // Consumer may have gotten 0 or 1 items. Drain remainder.
            std::vector<ui32> remainder;
            queue.Drain(remainder);

            ui32 total = consumerResult.size() + remainder.size();
            UNIT_ASSERT_VALUES_EQUAL_C(total, 1u,
                "iter=" << iter << ": expected exactly 1 item, got "
                << consumerResult.size() << "+" << remainder.size());

            ui32 item = consumerResult.empty() ? remainder[0] : consumerResult[0];
            UNIT_ASSERT_VALUES_EQUAL_C(item, iter,
                "iter=" << iter << ": wrong hint value");
        }
    }

    // One producer pushes continuously while one consumer drains
    // periodically. After the producer finishes, the consumer does final
    // drains. Verifies all items are eventually collected with no
    // duplicates and no losses.
    // This tests the steady-state interaction pattern: the occupant
    // periodically draining its injection queue while external threads
    // push activations.
    Y_UNIT_TEST(InterleavedPushDrain) {
        constexpr ui32 N = NSan::PlainOrUnderSanitizer(500000u, 50000u);

        TInjectionQueue queue;
        auto nodes = std::make_unique<TInjectionNode[]>(N);
        for (ui32 i = 0; i < N; ++i) {
            nodes[i].Hint = i;
        }

        std::atomic<bool> done{false};
        std::vector<ui32> drained;
        drained.reserve(N);

        std::latch start(2);

        // Consumer: drain repeatedly with brief pauses.
        std::thread consumer([&] {
            start.arrive_and_wait();
            while (!done.load(std::memory_order_acquire)) {
                queue.Drain(drained);
                SpinLockPause();
            }
            // Final drain. The acquire on done synchronizes with the
            // producer's release store, which is sequenced after all
            // pushes. So all push effects (both X1 and S1) are visible
            // and this drain will collect any remaining items.
            queue.Drain(drained);
        });

        // Producer: push all items, then signal done.
        start.arrive_and_wait();
        for (ui32 i = 0; i < N; ++i) {
            queue.Push(&nodes[i]);
        }
        done.store(true, std::memory_order_release);

        consumer.join();

        std::sort(drained.begin(), drained.end());
        UNIT_ASSERT_VALUES_EQUAL(drained.size(), N);
        for (ui32 i = 0; i < N; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(drained[i], i);
        }
    }

} // Y_UNIT_TEST_SUITE

} // namespace NActors
