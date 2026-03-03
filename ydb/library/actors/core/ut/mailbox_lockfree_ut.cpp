#include "mailbox_lockfree.h"
#include "events.h"
#include "actor.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/sanitizers.h>

#include <latch>
#include <thread>

namespace NActors {

Y_UNIT_TEST_SUITE(LockFreeMailbox) {

    // Helper: pop one event via template Pop and return it with the result.
    std::pair<std::unique_ptr<IEventHandle>, EPopResult> PopOne(TMailbox& m) {
        IEventHandle* captured = nullptr;
        auto result = m.Pop([&](IActor*, IEventHandle* ev) {
            captured = ev;
        });
        return {std::unique_ptr<IEventHandle>(captured), result};
    }

    Y_UNIT_TEST(Basics) {
        TMailbox m;

        UNIT_ASSERT(m.IsEmpty());

        // Fresh mailbox is Idle with initialized sentinels — push transitions to Locked
        std::unique_ptr<IEventHandle> ev1 = std::make_unique<IEventHandle>(
            TActorId(), TActorId(), new TEvents::TEvPing, 0, 1);
        UNIT_ASSERT(m.Push(ev1) == EMailboxPush::Locked);
        UNIT_ASSERT(!ev1);

        // Push to a non-idle mailbox returns Pushed
        ev1 = std::make_unique<IEventHandle>(
            TActorId(), TActorId(), new TEvents::TEvPing, 0, 2);
        UNIT_ASSERT(m.Push(ev1) == EMailboxPush::Pushed);
        UNIT_ASSERT(!ev1);

        // Pop the first event (FIFO), second event still exists
        auto [ev2, r2] = PopOne(m);
        UNIT_ASSERT(ev2);
        UNIT_ASSERT_VALUES_EQUAL(ev2->Cookie, (ui64)1);
        UNIT_ASSERT(r2 == EPopResult::Processed);

        // Pop the second event (last — queue goes idle)
        auto [ev3, r3] = PopOne(m);
        UNIT_ASSERT(ev3);
        UNIT_ASSERT_VALUES_EQUAL(ev3->Cookie, (ui64)2);
        UNIT_ASSERT(r3 == EPopResult::Idle);

        // Now push to an idle mailbox — first push returns Locked
        ev1 = std::make_unique<IEventHandle>(
            TActorId(), TActorId(), new TEvents::TEvPing, 0, 10);
        ev2 = std::make_unique<IEventHandle>(
            TActorId(), TActorId(), new TEvents::TEvPing, 0, 20);
        ev3 = std::make_unique<IEventHandle>(
            TActorId(), TActorId(), new TEvents::TEvPing, 0, 30);
        UNIT_ASSERT(m.Push(ev1) == EMailboxPush::Locked);
        UNIT_ASSERT(!ev1);
        UNIT_ASSERT(m.Push(ev2) == EMailboxPush::Pushed);
        UNIT_ASSERT(!ev2);
        UNIT_ASSERT(m.Push(ev3) == EMailboxPush::Pushed);
        UNIT_ASSERT(!ev3);

        // Pop all three events in FIFO order
        auto [p1, rr1] = PopOne(m);
        UNIT_ASSERT_VALUES_EQUAL(p1->Cookie, (ui64)10);
        UNIT_ASSERT(rr1 == EPopResult::Processed);
        auto [p2, rr2] = PopOne(m);
        UNIT_ASSERT_VALUES_EQUAL(p2->Cookie, (ui64)20);
        UNIT_ASSERT(rr2 == EPopResult::Processed);
        auto [p3, rr3] = PopOne(m);
        UNIT_ASSERT_VALUES_EQUAL(p3->Cookie, (ui64)30);
        UNIT_ASSERT(rr3 == EPopResult::Idle);

        // Push to idle mailbox returns Locked, Pop returns Idle for single event
        {
            std::unique_ptr<IEventHandle> lockEv = std::make_unique<IEventHandle>(
                TActorId(), TActorId(), new TEvents::TEvPing, 0, 99);
            UNIT_ASSERT(m.Push(lockEv) == EMailboxPush::Locked);
            auto [popped, result] = PopOne(m);
            UNIT_ASSERT(popped);
            UNIT_ASSERT_VALUES_EQUAL(popped->Cookie, (ui64)99);
            UNIT_ASSERT(result == EPopResult::Idle);
        }

        UNIT_ASSERT(m.IsEmpty());
    }

    Y_UNIT_TEST(FIFOOrder) {
        TMailbox m;

        // Push multiple events to an idle mailbox
        constexpr size_t N = 10;
        for (size_t i = 0; i < N; ++i) {
            std::unique_ptr<IEventHandle> ev = std::make_unique<IEventHandle>(
                TActorId(), TActorId(), new TEvents::TEvPing, 0, i);
            auto result = m.Push(ev);
            UNIT_ASSERT(!ev);
            if (i == 0) {
                UNIT_ASSERT(result == EMailboxPush::Locked);
            } else {
                UNIT_ASSERT(result == EMailboxPush::Pushed);
            }
        }

        // Pop should return events in FIFO order
        for (size_t i = 0; i < N; ++i) {
            auto [ev, result] = PopOne(m);
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, i);
            if (i < N - 1) {
                UNIT_ASSERT(result == EPopResult::Processed);
            } else {
                UNIT_ASSERT(result == EPopResult::Idle);
            }
        }
    }

    Y_UNIT_TEST(SnapshotBudgetExhausted) {
        TMailbox m;

        // Push to idle mailbox and pop to get back to idle
        {
            std::unique_ptr<IEventHandle> ev = std::make_unique<IEventHandle>(
                TActorId(), TActorId(), new TEvents::TEvPing, 0, 99);
            UNIT_ASSERT(m.Push(ev) == EMailboxPush::Locked);
            auto [popped, result] = PopOne(m);
            UNIT_ASSERT(popped);
            UNIT_ASSERT(result == EPopResult::Idle);
        }

        // Push 5 events
        for (size_t i = 0; i < 5; ++i) {
            std::unique_ptr<IEventHandle> ev = std::make_unique<IEventHandle>(
                TActorId(), TActorId(), new TEvents::TEvPing, 0, i);
            m.Push(ev);
        }

        // Use snapshot to pop with a budget of 3 — doesn't reach SnapshotTail
        ESnapshotResult result;
        {
            TMailboxSnapshot snapshot(&m, result);
            for (ui32 i = 0; i < 3; ++i) {
                auto ev = snapshot.Pop();
                if (!ev) break;
                UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, i);
            }
        }
        UNIT_ASSERT(result == ESnapshotResult::NeedsReschedule);

        // Remaining events should be accessible via template Pop
        auto [ev3, r3] = PopOne(m);
        UNIT_ASSERT(ev3);
        UNIT_ASSERT_VALUES_EQUAL(ev3->Cookie, (ui64)3);
        UNIT_ASSERT(r3 == EPopResult::Processed);

        auto [ev4, r4] = PopOne(m);
        UNIT_ASSERT(ev4);
        UNIT_ASSERT_VALUES_EQUAL(ev4->Cookie, (ui64)4);
        UNIT_ASSERT(r4 == EPopResult::Idle);
    }

    Y_UNIT_TEST(SnapshotIdle) {
        TMailbox m;

        // Push 3 events to idle mailbox (first push returns Locked)
        for (size_t i = 0; i < 3; ++i) {
            std::unique_ptr<IEventHandle> ev = std::make_unique<IEventHandle>(
                TActorId(), TActorId(), new TEvents::TEvPing, 0, i);
            m.Push(ev);
        }

        // Snapshot consumes all 3 events. The last one (SnapshotTail) triggers
        // CAS-based pop. Since no new events arrive, CAS succeeds → Idle.
        ESnapshotResult result;
        {
            TMailboxSnapshot snapshot(&m, result);
            for (ui32 i = 0; i < 10; ++i) {
                auto ev = snapshot.Pop();
                if (!ev) break;
                UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, i);
            }
        }
        UNIT_ASSERT(result == ESnapshotResult::Idle);

        // Queue should be empty. TryUnlock succeeds (Tail/Head/Stub at stub/0).
        UNIT_ASSERT(m.TryUnlock());
    }

    Y_UNIT_TEST(SnapshotNeedsReschedule) {
        TMailbox m;

        // Push 3 events
        for (size_t i = 0; i < 3; ++i) {
            std::unique_ptr<IEventHandle> ev = std::make_unique<IEventHandle>(
                TActorId(), TActorId(), new TEvents::TEvPing, 0, i);
            m.Push(ev);
        }

        // Snapshot captures Tail at event 2.
        // Push 2 more events AFTER the snapshot is created but before
        // consuming SnapshotTail — simulates concurrent producers.
        ESnapshotResult result;
        {
            TMailboxSnapshot snapshot(&m, result);

            // Pop events 0 and 1 (normal walk)
            for (ui32 i = 0; i < 2; ++i) {
                auto ev = snapshot.Pop();
                UNIT_ASSERT(ev);
                UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, i);
            }

            // Push 2 more events (concurrent producers)
            for (size_t i = 3; i < 5; ++i) {
                std::unique_ptr<IEventHandle> ev = std::make_unique<IEventHandle>(
                    TActorId(), TActorId(), new TEvents::TEvPing, 0, i);
                m.Push(ev);
            }

            // Pop event 2 (SnapshotTail). CAS-based pop detects events
            // beyond boundary → NeedsReschedule.
            auto ev = snapshot.Pop();
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, (ui64)2);
        }
        UNIT_ASSERT(result == ESnapshotResult::NeedsReschedule);

        // Post-snapshot events are accessible via template Pop
        auto [ev3, r3] = PopOne(m);
        UNIT_ASSERT(ev3);
        UNIT_ASSERT_VALUES_EQUAL(ev3->Cookie, (ui64)3);
        UNIT_ASSERT(r3 == EPopResult::Processed);

        auto [ev4, r4] = PopOne(m);
        UNIT_ASSERT(ev4);
        UNIT_ASSERT_VALUES_EQUAL(ev4->Cookie, (ui64)4);
        UNIT_ASSERT(r4 == EPopResult::Idle);
    }

    Y_UNIT_TEST(PushFront) {
        TMailbox m;

        // Push ev1 to the idle mailbox (returns Locked)
        std::unique_ptr<IEventHandle> ev1 = std::make_unique<IEventHandle>(
            TActorId(), TActorId(), new TEvents::TEvPing, 0, 1);
        UNIT_ASSERT(m.Push(ev1) == EMailboxPush::Locked);
        UNIT_ASSERT(!ev1);

        // PushFront should insert at the head (before ev1)
        std::unique_ptr<IEventHandle> ev0 = std::make_unique<IEventHandle>(
            TActorId(), TActorId(), new TEvents::TEvPing, 0, 0);
        m.PushFront(std::move(ev0));

        // Pop should return ev0 first, then ev1
        auto [popped0, r0] = PopOne(m);
        UNIT_ASSERT(popped0);
        UNIT_ASSERT_VALUES_EQUAL(popped0->Cookie, (ui64)0);
        UNIT_ASSERT(r0 == EPopResult::Processed);

        auto [popped1, r1] = PopOne(m);
        UNIT_ASSERT(popped1);
        UNIT_ASSERT_VALUES_EQUAL(popped1->Cookie, (ui64)1);
        UNIT_ASSERT(r1 == EPopResult::Idle);
    }

    class TSimpleActor : public TActor<TSimpleActor> {
    public:
        TSimpleActor()
            : TActor(&TThis::StateFunc)
        {}

    private:
        void StateFunc(TAutoPtr<IEventHandle>&) {
            // nothing
        }
    };

    Y_UNIT_TEST(RegisterActors) {
        for (int count = 1; count < 16; ++count) {
            TMailbox m;
            std::vector<std::unique_ptr<IActor>> actors;
            for (int i = 1; i <= count; ++i) {
                actors.emplace_back(new TSimpleActor);
                m.AttachActor(i, actors.back().get());
                UNIT_ASSERT(!m.IsEmpty());
                UNIT_ASSERT(m.FindActor(i) == actors.back().get());
            }
            for (int i = 1; i <= count; ++i) {
                UNIT_ASSERT(m.FindActor(i) == actors[i-1].get());
                UNIT_ASSERT(m.DetachActor(i) == actors[i-1].get());
                UNIT_ASSERT(m.FindActor(i) == nullptr);
            }
            UNIT_ASSERT(m.IsEmpty());
        }
    }

    Y_UNIT_TEST(RegisterAliases) {
        TMailbox m;
        std::vector<std::unique_ptr<IActor>> actors;
        for (int i = 1; i <= 4; ++i) {
            actors.emplace_back(new TSimpleActor);
            IActor* actor = actors.back().get();
            m.AttachActor(i * 100, actor);
            UNIT_ASSERT(m.FindActor(i * 100) == actor);
            for (int j = 1; j <= 16; ++j) {
                m.AttachAlias(i * 100 + j, actor);
                UNIT_ASSERT(m.FindActor(i * 100 + j) == nullptr);
                UNIT_ASSERT(m.FindAlias(i * 100 + j) == actor);
            }
        }
        for (int i = 1; i <= 4; ++i) {
            IActor* actor = actors[i - 1].get();
            UNIT_ASSERT(m.FindActor(i * 100) == actor);
            // Verify every alias can be removed individually
            for (int j = 1; j <= 8; ++j) {
                UNIT_ASSERT(m.FindAlias(i * 100 + j) == actor);
                IActor* detached = m.DetachAlias(i * 100 + j);
                UNIT_ASSERT(detached == actor);
                UNIT_ASSERT(m.FindAlias(i * 100 + j) == nullptr);
            }
            UNIT_ASSERT(m.DetachActor(i * 100) == actor);
            // Verify all aliases are detached with the actor
            for (int j = 9; j <= 16; ++j) {
                UNIT_ASSERT(m.FindAlias(i * 100 + j) == nullptr);
            }
        }
        UNIT_ASSERT(m.IsEmpty());
    }

    Y_UNIT_TEST(MultiThreadedPushPop) {
        constexpr size_t nThreads = 3;
        constexpr size_t nEvents = NSan::PlainOrUnderSanitizer(1000000, 100000);

        TMailbox mailbox;

        std::atomic<size_t> eventIndex{ 0 };
        std::atomic<size_t> switches{ 0 };

        std::vector<std::thread> threads;
        threads.reserve(nThreads);
        std::latch start(nThreads);

        std::vector<size_t> observed;

        for (size_t i = 0; i < nThreads; ++i) {
            threads.emplace_back([&]{
                start.arrive_and_wait();

                bool is_producer = true;

                for (;;) {
                    if (is_producer) {
                        // Work as a producer
                        size_t index = eventIndex.fetch_add(1, std::memory_order_relaxed);
                        if (index >= nEvents) {
                            // All events have been produced
                            break;
                        }
                        std::unique_ptr<IEventHandle> ev = std::make_unique<IEventHandle>(TActorId(), TActorId(), new TEvents::TEvPing, 0, index);
                        switch (mailbox.Push(ev)) {
                            case EMailboxPush::Locked:
                                // This thread is now a consumer
                                switches.fetch_add(1, std::memory_order_relaxed);
                                is_producer = false;
                                break;
                            case EMailboxPush::Pushed:
                                // This thread is still a producer
                                break;
                        }
                    } else {
                        // Work as a consumer using template Pop.
                        // Template Pop processes the event inside the callback
                        // (single-consumer guarantee) and returns Idle when the
                        // queue becomes empty — the consumer exits immediately,
                        // no locked-empty window.
                        auto consume = [&](IActor*, IEventHandle* ev) {
                            TAutoPtr<IEventHandle> evAuto(ev);
                            observed.push_back(ev->Cookie);
                        };

                        EPopResult popResult = mailbox.Pop(consume);
                        if (popResult == EPopResult::Idle) {
                            is_producer = true;
                            continue;
                        }
                        if (popResult == EPopResult::Empty) {
                            if (mailbox.TryUnlock()) {
                                is_producer = true;
                                continue;
                            }
                            // TryUnlock failed — events exist. Retry.
                            for (int spin = 0; spin < 1000; ++spin) {
                                popResult = mailbox.Pop(consume);
                                if (popResult != EPopResult::Empty) break;
                            }
                            Y_ABORT_UNLESS(popResult != EPopResult::Empty, "Pop stuck after TryUnlock failure");
                            if (popResult == EPopResult::Idle) {
                                is_producer = true;
                                continue;
                            }
                        }
                    }
                }
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }

        UNIT_ASSERT_VALUES_EQUAL(observed.size(), nEvents);

        // We must observe every event exactly once
        std::sort(observed.begin(), observed.end());
        for (size_t i = 0; i < observed.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(observed[i], i);
        }

        Cerr << "... there have been " << switches.load() << " switches to consumer mode" << Endl;
    }

} // Y_UNIT_TEST_SUITE(LockFreeMailbox)

} // namespace NActors
