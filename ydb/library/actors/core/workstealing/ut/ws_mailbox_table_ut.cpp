#include <ydb/library/actors/core/workstealing/ws_mailbox_table.h>

#include <library/cpp/testing/unittest/registar.h>

#include <thread>
#include <vector>
#include <set>
#include <algorithm>
#include <latch>

namespace NActors::NWorkStealing {

    Y_UNIT_TEST_SUITE(WsMailboxTable) {

        Y_UNIT_TEST(GetReturnsNullForUnallocatedSegment) {
            TWsMailboxTable table;
            // No segments allocated yet — hint 1 should return nullptr
            UNIT_ASSERT_EQUAL(table.Get(1), nullptr);
            UNIT_ASSERT_EQUAL(table.GetStats(1), nullptr);
        }

        Y_UNIT_TEST(GetReturnsNullForHintZero) {
            TWsMailboxTable table;
            // Hint 0 is reserved; segment 0 not allocated yet
            UNIT_ASSERT_EQUAL(table.Get(0), nullptr);
        }

        Y_UNIT_TEST(AllocateBatchAllocatesSegment) {
            TWsMailboxTable table;
            ui32 hints[8];
            size_t got = table.AllocateBatch(hints, 8);
            UNIT_ASSERT(got > 0);
            UNIT_ASSERT(got <= 8);

            // All hints should be non-zero
            for (size_t i = 0; i < got; ++i) {
                UNIT_ASSERT(hints[i] != 0);
            }

            // All hints should resolve to valid mailboxes
            for (size_t i = 0; i < got; ++i) {
                TMailbox* mbox = table.Get(hints[i]);
                UNIT_ASSERT(mbox != nullptr);
                UNIT_ASSERT_EQUAL(mbox->Hint, hints[i]);
            }

            // Stats should also resolve
            for (size_t i = 0; i < got; ++i) {
                TMailboxExecStats* stats = table.GetStats(hints[i]);
                UNIT_ASSERT(stats != nullptr);
            }
        }

        Y_UNIT_TEST(AllocatedHintsAreUnique) {
            TWsMailboxTable table;
            constexpr size_t BatchSize = 1024;
            ui32 hints[BatchSize];
            size_t got = table.AllocateBatch(hints, BatchSize);
            UNIT_ASSERT_EQUAL(got, BatchSize);

            std::set<ui32> unique(hints, hints + got);
            UNIT_ASSERT_EQUAL(unique.size(), got);

            // No hint should be zero
            UNIT_ASSERT(unique.find(0) == unique.end());
        }

        Y_UNIT_TEST(FreeBatchAndReallocate) {
            TWsMailboxTable table;

            // Drain the entire initial segment so the pool is empty
            constexpr size_t SegHints = TWsMailboxTable::MailboxesPerSegment - 1; // skip hint 0
            std::vector<ui32> drain(SegHints);
            size_t drained = table.AllocateBatch(drain.data(), SegHints);
            UNIT_ASSERT_EQUAL(drained, SegHints);

            // Pick 16 hints from the drained set and free them
            ui32 hints[16];
            for (size_t i = 0; i < 16; ++i) {
                hints[i] = drain[i];
            }
            table.FreeBatch(hints, 16);

            // Reallocate — only the 16 freed hints are in the pool
            ui32 hints2[16];
            size_t got2 = table.AllocateBatch(hints2, 16);
            UNIT_ASSERT_EQUAL(got2, 16);

            // The reallocated hints should be exactly the freed ones
            std::set<ui32> freed(hints, hints + 16);
            std::set<ui32> got(hints2, hints2 + 16);
            UNIT_ASSERT_EQUAL(freed, got);

            // Return everything for cleanup
            table.FreeBatch(drain.data() + 16, SegHints - 16);
            table.FreeBatch(hints2, 16);
        }

        Y_UNIT_TEST(HintEncodingIsCorrect) {
            TWsMailboxTable table;
            ui32 hints[4];
            size_t got = table.AllocateBatch(hints, 4);
            UNIT_ASSERT(got > 0);

            for (size_t i = 0; i < got; ++i) {
                ui32 segIdx = hints[i] >> TWsMailboxTable::SegmentShift;
                ui32 offset = hints[i] & TWsMailboxTable::OffsetMask;
                UNIT_ASSERT(segIdx < TWsMailboxTable::MaxSegments);
                UNIT_ASSERT(offset < TWsMailboxTable::MailboxesPerSegment);
            }
        }

        Y_UNIT_TEST(MailboxHintMatchesGet) {
            TWsMailboxTable table;
            ui32 hints[64];
            size_t got = table.AllocateBatch(hints, 64);
            UNIT_ASSERT_EQUAL(got, 64);

            for (size_t i = 0; i < got; ++i) {
                TMailbox* mbox = table.Get(hints[i]);
                UNIT_ASSERT(mbox != nullptr);
                // The Hint field should be set during segment allocation
                UNIT_ASSERT_EQUAL(mbox->Hint, hints[i]);
            }
        }

        Y_UNIT_TEST(StatsInitializedToZero) {
            TWsMailboxTable table;
            ui32 hints[4];
            size_t got = table.AllocateBatch(hints, 4);
            UNIT_ASSERT(got > 0);

            for (size_t i = 0; i < got; ++i) {
                auto* stats = table.GetStats(hints[i]);
                UNIT_ASSERT(stats != nullptr);
                UNIT_ASSERT_EQUAL(stats->EventsProcessed.load(), 0u);
                UNIT_ASSERT_EQUAL(stats->TotalExecutionCycles.load(), 0u);
                UNIT_ASSERT_EQUAL(stats->ActivationCount.load(), 0u);
            }
        }

        Y_UNIT_TEST(CleanupOnEmptyTable) {
            TWsMailboxTable table;
            UNIT_ASSERT(table.Cleanup());
        }

        Y_UNIT_TEST(CleanupWithAllocatedSegment) {
            TWsMailboxTable table;
            ui32 hints[4];
            table.AllocateBatch(hints, 4);
            UNIT_ASSERT(table.Cleanup());
        }

        Y_UNIT_TEST(LargeAllocation) {
            TWsMailboxTable table;
            // Allocate more than one segment worth
            constexpr size_t N = TWsMailboxTable::MailboxesPerSegment + 100;
            std::vector<ui32> hints(N);
            size_t total = 0;
            while (total < N) {
                size_t got = table.AllocateBatch(hints.data() + total, N - total);
                UNIT_ASSERT(got > 0);
                total += got;
            }
            UNIT_ASSERT_EQUAL(total, N);

            // All unique
            std::set<ui32> unique(hints.begin(), hints.end());
            UNIT_ASSERT_EQUAL(unique.size(), N);

            // Hints from at least two segments
            std::set<ui32> segments;
            for (ui32 h : hints) {
                segments.insert(h >> TWsMailboxTable::SegmentShift);
            }
            UNIT_ASSERT(segments.size() >= 2);
        }
    }

    Y_UNIT_TEST_SUITE(WsSlotAllocator) {

        Y_UNIT_TEST(BasicAllocateFree) {
            TWsMailboxTable table;
            TWsSlotAllocator alloc;
            alloc.Init(&table);

            ui32 h1 = alloc.Allocate();
            UNIT_ASSERT(h1 != 0);

            ui32 h2 = alloc.Allocate();
            UNIT_ASSERT(h2 != 0);
            UNIT_ASSERT(h2 != h1);

            alloc.Free(h1);
            alloc.Free(h2);
        }

        Y_UNIT_TEST(LifoOrder) {
            TWsMailboxTable table;
            TWsSlotAllocator alloc;
            alloc.Init(&table);

            // Allocate two
            ui32 h1 = alloc.Allocate();
            ui32 h2 = alloc.Allocate();

            // Free in order: h1, h2
            alloc.Free(h1);
            alloc.Free(h2);

            // LIFO: next allocate should return h2 (last freed)
            ui32 got = alloc.Allocate();
            UNIT_ASSERT_EQUAL(got, h2);

            // Then h1
            got = alloc.Allocate();
            UNIT_ASSERT_EQUAL(got, h1);
        }

        Y_UNIT_TEST(ResolvedMailboxIsValid) {
            TWsMailboxTable table;
            TWsSlotAllocator alloc;
            alloc.Init(&table);

            ui32 hint = alloc.Allocate();
            TMailbox* mbox = table.Get(hint);
            UNIT_ASSERT(mbox != nullptr);
            UNIT_ASSERT_EQUAL(mbox->Hint, hint);

            alloc.Free(hint);
        }

        Y_UNIT_TEST(DrainReturnsAllToGlobal) {
            TWsMailboxTable table;
            TWsSlotAllocator alloc;
            alloc.Init(&table);

            // Allocate several
            std::vector<ui32> hints;
            for (int i = 0; i < 100; ++i) {
                hints.push_back(alloc.Allocate());
            }

            // Free them all into the allocator
            for (ui32 h : hints) {
                alloc.Free(h);
            }

            // Drain returns them to global pool
            alloc.Drain();

            // Now a fresh allocator on the same table should be able
            // to get those hints
            TWsSlotAllocator alloc2;
            alloc2.Init(&table);
            for (int i = 0; i < 100; ++i) {
                ui32 h = alloc2.Allocate();
                UNIT_ASSERT(h != 0);
            }
            alloc2.Drain();
        }

        Y_UNIT_TEST(RefillOnEmpty) {
            TWsMailboxTable table;
            TWsSlotAllocator alloc;
            alloc.Init(&table);

            // Allocating from an empty allocator triggers refill
            ui32 h = alloc.Allocate();
            UNIT_ASSERT(h != 0);

            alloc.Free(h);
            alloc.Drain();
        }

        Y_UNIT_TEST(ReturnExcessOnFull) {
            TWsMailboxTable table;
            TWsSlotAllocator alloc;
            alloc.Init(&table);

            // Fill the allocator to capacity by freeing many hints
            std::vector<ui32> allocated;
            for (size_t i = 0; i < TWsSlotAllocator::MaxHints + 100; ++i) {
                allocated.push_back(alloc.Allocate());
            }
            // Free all of them — triggers ReturnExcess when full
            for (ui32 h : allocated) {
                alloc.Free(h);
            }

            alloc.Drain();
        }

        Y_UNIT_TEST(MultipleAllocatorsOnSameTable) {
            TWsMailboxTable table;
            TWsSlotAllocator alloc1, alloc2;
            alloc1.Init(&table);
            alloc2.Init(&table);

            std::set<ui32> hints1, hints2;
            for (int i = 0; i < 50; ++i) {
                hints1.insert(alloc1.Allocate());
                hints2.insert(alloc2.Allocate());
            }

            // No overlap between the two allocators
            for (ui32 h : hints1) {
                UNIT_ASSERT(hints2.find(h) == hints2.end());
            }

            // Clean up
            for (ui32 h : hints1) alloc1.Free(h);
            for (ui32 h : hints2) alloc2.Free(h);
            alloc1.Drain();
            alloc2.Drain();
        }

        Y_UNIT_TEST(ConcurrentAllocatorsOnSameTable) {
            TWsMailboxTable table;
            constexpr int NumThreads = 4;
            constexpr int HintsPerThread = 500;

            std::vector<std::vector<ui32>> threadHints(NumThreads);
            std::latch startLatch(NumThreads);

            auto worker = [&](int idx) {
                TWsSlotAllocator alloc;
                alloc.Init(&table);
                startLatch.arrive_and_wait();

                for (int i = 0; i < HintsPerThread; ++i) {
                    threadHints[idx].push_back(alloc.Allocate());
                }
                for (ui32 h : threadHints[idx]) {
                    alloc.Free(h);
                }
                alloc.Drain();
            };

            std::vector<std::thread> threads;
            for (int i = 0; i < NumThreads; ++i) {
                threads.emplace_back(worker, i);
            }
            for (auto& t : threads) {
                t.join();
            }

            // Verify all allocated hints were unique across all threads
            std::set<ui32> all;
            for (auto& th : threadHints) {
                for (ui32 h : th) {
                    UNIT_ASSERT(all.insert(h).second);
                }
            }
            UNIT_ASSERT_EQUAL(all.size(), static_cast<size_t>(NumThreads * HintsPerThread));
        }
    }

} // namespace NActors::NWorkStealing
