#include "mailbox_lockfree.h"
#include "events.h"
#include "actor.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/sanitizers.h>

#include <latch>
#include <thread>

namespace NActors {

Y_UNIT_TEST_SUITE(LockFreeMailbox) {

    Y_UNIT_TEST(Basics) {
        TMailbox m;

        UNIT_ASSERT(m.IsFree());
        UNIT_ASSERT(m.IsEmpty());

        // Check that we cannot push events to free mailboxes
        std::unique_ptr<IEventHandle> ev1 = std::make_unique<IEventHandle>(TActorId(), TActorId(), new TEvents::TEvPing);
        IEventHandle* ev1raw = ev1.get();
        UNIT_ASSERT(m.Push(ev1) == EMailboxPush::Free);
        UNIT_ASSERT(ev1);

        // Check that we can push events after mailbox is locked from free
        // LockFromFree puts mailbox in Locked state (consumer holds it)
        m.LockFromFree();
        UNIT_ASSERT(!m.IsFree());

        // Push to a locked mailbox returns Pushed (consumer already running)
        UNIT_ASSERT(m.Push(ev1) == EMailboxPush::Pushed);
        UNIT_ASSERT(!ev1);

        // Check that we pop the event we just pushed
        std::unique_ptr<IEventHandle> ev2 = m.Pop();
        UNIT_ASSERT(ev2.get() == ev1raw);

        // Check that the mailbox is now empty
        UNIT_ASSERT(!m.Pop());

        // Unlocking should succeed (queue is empty)
        UNIT_ASSERT(m.TryUnlock());

        // Now push to an idle (unlocked) mailbox — first push returns Locked
        ev1 = std::move(ev2);
        ev2 = std::make_unique<IEventHandle>(TActorId(), TActorId(), new TEvents::TEvPing);
        IEventHandle* ev2raw = ev2.get();
        std::unique_ptr<IEventHandle> ev3 = std::make_unique<IEventHandle>(TActorId(), TActorId(), new TEvents::TEvPing);
        IEventHandle* ev3raw = ev3.get();
        UNIT_ASSERT(m.Push(ev1) == EMailboxPush::Locked);
        UNIT_ASSERT(!ev1);
        UNIT_ASSERT(m.Push(ev2) == EMailboxPush::Pushed);
        UNIT_ASSERT(!ev2);
        UNIT_ASSERT(m.Push(ev3) == EMailboxPush::Pushed);
        UNIT_ASSERT(!ev3);

        // Pop all three events in FIFO order
        ev1.reset(m.Pop().release());
        UNIT_ASSERT(ev1.get() == ev1raw);
        ev2.reset(m.Pop().release());
        UNIT_ASSERT(ev2.get() == ev2raw);
        ev3.reset(m.Pop().release());
        UNIT_ASSERT(ev3.get() == ev3raw);
        UNIT_ASSERT(!m.Pop());

        // Unlock and then transition to free
        UNIT_ASSERT(m.TryUnlock());

        // TryLock should succeed on an idle mailbox
        UNIT_ASSERT(m.TryLock());

        // LockToFree drains remaining events
        m.LockToFree();

        std::unique_ptr<IEventHandle> ev4 = std::make_unique<IEventHandle>(TActorId(), TActorId(), new TEvents::TEvPing);
        UNIT_ASSERT(m.Push(ev4) == EMailboxPush::Free);

        // Mailbox is back in initial state
        UNIT_ASSERT(m.IsFree());
        UNIT_ASSERT(m.IsEmpty());
    }

    Y_UNIT_TEST(FIFOOrder) {
        TMailbox m;
        m.LockFromFree();
        UNIT_ASSERT(m.TryUnlock());

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
            auto ev = m.Pop();
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, i);
        }
        UNIT_ASSERT(!m.Pop());
        UNIT_ASSERT(m.TryUnlock());

        m.LockToFree();
    }


    Y_UNIT_TEST(PushFront) {
        TMailbox m;
        m.LockFromFree();

        // Push event via normal Push (returns Pushed since State=Locked)
        std::unique_ptr<IEventHandle> ev1 = std::make_unique<IEventHandle>(
            TActorId(), TActorId(), new TEvents::TEvPing, 0, 1);
        UNIT_ASSERT(m.Push(ev1) == EMailboxPush::Pushed);

        // PushFront should insert at the head
        std::unique_ptr<IEventHandle> ev0 = std::make_unique<IEventHandle>(
            TActorId(), TActorId(), new TEvents::TEvPing, 0, 0);
        m.PushFront(std::move(ev0));

        // Pop should return ev0 first, then ev1
        auto popped = m.Pop();
        UNIT_ASSERT(popped);
        UNIT_ASSERT_VALUES_EQUAL(popped->Cookie, (ui64)0);

        popped = m.Pop();
        UNIT_ASSERT(popped);
        UNIT_ASSERT_VALUES_EQUAL(popped->Cookie, (ui64)1);

        UNIT_ASSERT(!m.Pop());
        UNIT_ASSERT(m.TryUnlock());
        m.LockToFree();
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
        mailbox.LockFromFree();
        UNIT_ASSERT(mailbox.TryUnlock());

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
                            case EMailboxPush::Free:
                                // Cannot happen during this test
                                Y_ABORT();
                        }
                    } else {
                        // Work as a consumer
                        auto ev = mailbox.Pop();
                        if (!ev) {
                            if (mailbox.TryUnlock()) {
                                // This thread is now a producer
                                is_producer = true;
                                continue;
                            }
                            // We must have one more event — retry
                            ev = mailbox.Pop();
                            if (!ev) {
                                // Incomplete push: producer linked Tail but
                                // hasn't stored next yet. Spin briefly.
                                for (int spin = 0; !ev && spin < 1000; ++spin) {
                                    ev = mailbox.Pop();
                                }
                                Y_ABORT_UNLESS(ev, "Pop stuck after TryUnlock failure");
                            }
                        }
                        // Only one thread is supposed to be a consumer
                        observed.push_back(ev->Cookie);
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
