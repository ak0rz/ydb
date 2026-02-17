#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/config.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/core/workstealing/executor_pool_ws.h>

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>

using namespace NActors;

// ---------------------------------------------------------------------------
// Event types (unique IDs to avoid collisions with other test files)
// ---------------------------------------------------------------------------

struct TEvIntPing: public TEventLocal<TEvIntPing, 10500> {};
struct TEvIntPong: public TEventLocal<TEvIntPong, 10501> {};

// ---------------------------------------------------------------------------
// Helpers: actor-system setup
// ---------------------------------------------------------------------------

// Create a setup with a Basic pool (pool 0) + WS pool (pool 1).
// A Basic pool is always needed so the scheduler has a traditional executor.
static THolder<TActorSystemSetup> MakeWSSetup(i16 wsSlots = 4) {
    auto setup = MakeHolder<TActorSystemSetup>();
    setup->NodeId = 1;

    TBasicExecutorPoolConfig basicCfg;
    basicCfg.PoolId = 0;
    basicCfg.PoolName = "basic";
    basicCfg.Threads = 2;
    basicCfg.MinThreadCount = 2;
    basicCfg.MaxThreadCount = 2;
    basicCfg.DefaultThreadCount = 2;
    setup->CpuManager.Basic.push_back(basicCfg);

    TWorkStealingPoolConfig wsCfg;
    wsCfg.PoolId = 1;
    wsCfg.PoolName = "ws";
    wsCfg.MinSlotCount = 1;
    wsCfg.MaxSlotCount = wsSlots;
    wsCfg.DefaultSlotCount = wsSlots;
    setup->CpuManager.WorkStealing = TWorkStealingConfig{
        .Enabled = true,
        .Pools = {wsCfg},
    };

    setup->Scheduler.Reset(new TBasicSchedulerThread(TSchedulerConfig()));
    return setup;
}

// ---------------------------------------------------------------------------
// Test actors
// ---------------------------------------------------------------------------

class TCountingActor: public TActor<TCountingActor> {
    std::atomic<i64>& Count_;

public:
    explicit TCountingActor(std::atomic<i64>& count)
        : TActor(&TCountingActor::StateFunc)
        , Count_(count)
    {
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvIntPing, Handle);
            default:
                break;
        }
    }

    void Handle(TEvIntPing::TPtr&) {
        Count_.fetch_add(1, std::memory_order_relaxed);
    }
};

// Sends one TEvIntPing to Target on bootstrap, then PassAway.
class TFireAndForgetActor: public TActorBootstrapped<TFireAndForgetActor> {
    TActorId Target_;

public:
    explicit TFireAndForgetActor(TActorId target)
        : Target_(target)
    {
    }

    void Bootstrap() {
        Send(Target_, new TEvIntPing());
        PassAway();
    }
};

// ---------------------------------------------------------------------------

Y_UNIT_TEST_SUITE(ExecutorPoolWsIntegration) {

    // =======================================================================
    // 1. Actor system lifecycle with WS pool
    // =======================================================================

    Y_UNIT_TEST(SystemStartStop) {
        auto setup = MakeWSSetup(4);
        TActorSystem system(setup);
        system.Start();
        Sleep(TDuration::MilliSeconds(50));
        system.Stop();
    }

    Y_UNIT_TEST(SystemStartStopMinimalSlots) {
        auto setup = MakeWSSetup(1);
        TActorSystem system(setup);
        system.Start();
        Sleep(TDuration::MilliSeconds(50));
        system.Stop();
    }

    Y_UNIT_TEST(SystemStartStopImmediate) {
        auto setup = MakeWSSetup(2);
        TActorSystem system(setup);
        system.Start();
        system.Stop();
    }

    Y_UNIT_TEST(SystemStartStopEmpty) {
        // No actors registered, just lifecycle.
        auto setup = MakeWSSetup(4);
        TActorSystem system(setup);
        system.Start();
        Sleep(TDuration::MilliSeconds(100));
        system.Stop();
    }

    Y_UNIT_TEST(SystemStartStopMultipleWsPools) {
        auto setup = MakeHolder<TActorSystemSetup>();
        setup->NodeId = 1;

        TBasicExecutorPoolConfig basicCfg;
        basicCfg.PoolId = 0;
        basicCfg.PoolName = "basic";
        basicCfg.Threads = 1;
        basicCfg.MinThreadCount = 1;
        basicCfg.MaxThreadCount = 1;
        basicCfg.DefaultThreadCount = 1;
        setup->CpuManager.Basic.push_back(basicCfg);

        TWorkStealingPoolConfig ws1;
        ws1.PoolId = 1;
        ws1.PoolName = "ws1";
        ws1.MinSlotCount = 1;
        ws1.MaxSlotCount = 2;
        ws1.DefaultSlotCount = 2;

        TWorkStealingPoolConfig ws2;
        ws2.PoolId = 2;
        ws2.PoolName = "ws2";
        ws2.MinSlotCount = 1;
        ws2.MaxSlotCount = 4;
        ws2.DefaultSlotCount = 4;

        setup->CpuManager.WorkStealing = TWorkStealingConfig{
            .Enabled = true,
            .Pools = {ws1, ws2},
        };

        setup->Scheduler.Reset(new TBasicSchedulerThread(TSchedulerConfig()));

        TActorSystem system(setup);
        system.Start();
        Sleep(TDuration::MilliSeconds(50));
        system.Stop();
    }

    // =======================================================================
    // 2. Pool metadata queries
    // =======================================================================

    Y_UNIT_TEST(PoolThreadCountsReflected) {
        auto setup = MakeWSSetup(4);
        TActorSystem system(setup);
        system.Start();

        // Basic pool (pool 0): max = 2
        UNIT_ASSERT_DOUBLES_EQUAL(system.GetPoolMaxThreadsCount(0), 2.0f, 0.01f);

        // WS pool (pool 1): max = 4
        UNIT_ASSERT_DOUBLES_EQUAL(system.GetPoolMaxThreadsCount(1), 4.0f, 0.01f);

        auto wsThreads = system.GetPoolThreadsCount(1);
        UNIT_ASSERT(wsThreads.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*wsThreads, 4u);

        system.Stop();
    }

    Y_UNIT_TEST(PoolNamesReflectedInConfig) {
        auto setup = MakeWSSetup(2);
        UNIT_ASSERT_VALUES_EQUAL(setup->CpuManager.GetPoolName(0), "basic");
        UNIT_ASSERT_VALUES_EQUAL(setup->CpuManager.GetPoolName(1), "ws");
    }

    // =======================================================================
    // 3. Actor registration on Basic pool (with WS pool present)
    //    Validates that the WS pool does not interfere with normal operation.
    // =======================================================================

    Y_UNIT_TEST(RegisterActorOnBasicPoolWithWSPresent) {
        auto setup = MakeWSSetup(2);
        TActorSystem system(setup);
        system.Start();

        std::atomic<i64> count{0};
        TActorId actorId = system.Register(new TCountingActor(count), TMailboxType::HTSwap, 0);

        UNIT_ASSERT(actorId);
        UNIT_ASSERT_VALUES_EQUAL(actorId.NodeId(), 1u);

        system.Send(actorId, new TEvIntPing());

        for (int i = 0; i < 200; ++i) {
            if (count.load(std::memory_order_relaxed) > 0) {
                break;
            }
            Sleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT_VALUES_EQUAL(count.load(std::memory_order_relaxed), 1);

        system.Stop();
    }

    Y_UNIT_TEST(MultipleActorsOnBasicPoolWithWSPresent) {
        auto setup = MakeWSSetup(4);
        TActorSystem system(setup);
        system.Start();

        constexpr int kActors = 8;
        constexpr int kMsgs = 10;

        std::atomic<i64> counts[kActors];
        TActorId ids[kActors];
        for (int i = 0; i < kActors; ++i) {
            counts[i].store(0, std::memory_order_relaxed);
            ids[i] = system.Register(new TCountingActor(counts[i]), TMailboxType::HTSwap, 0);
        }

        for (int i = 0; i < kActors; ++i) {
            for (int j = 0; j < kMsgs; ++j) {
                system.Send(ids[i], new TEvIntPing());
            }
        }

        for (int attempt = 0; attempt < 500; ++attempt) {
            bool done = true;
            for (int i = 0; i < kActors; ++i) {
                if (counts[i].load(std::memory_order_relaxed) < kMsgs) {
                    done = false;
                    break;
                }
            }
            if (done) {
                break;
            }
            Sleep(TDuration::MilliSeconds(10));
        }

        for (int i = 0; i < kActors; ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                counts[i].load(std::memory_order_relaxed), kMsgs,
                TStringBuilder() << "Actor " << i << " got "
                                 << counts[i].load(std::memory_order_relaxed));
        }

        system.Stop();
    }

    // =======================================================================
    // 4. Actor registration and message delivery on WS pool
    // =======================================================================

    Y_UNIT_TEST(RegisterActorOnWSPoolReturnsValidId) {
        auto setup = MakeWSSetup(2);
        TActorSystem system(setup);
        system.Start();

        std::atomic<i64> count{0};
        TActorId actorId = system.Register(new TCountingActor(count), TMailboxType::HTSwap, 1);

        UNIT_ASSERT(actorId);
        UNIT_ASSERT_VALUES_EQUAL(actorId.NodeId(), 1u);

        system.Stop();
    }

    Y_UNIT_TEST(RegisterMultipleActorsOnWSPoolCleanShutdown) {
        auto setup = MakeWSSetup(4);
        TActorSystem system(setup);
        system.Start();

        std::atomic<i64> dummies[16];
        for (int i = 0; i < 16; ++i) {
            dummies[i].store(0, std::memory_order_relaxed);
            system.Register(new TCountingActor(dummies[i]), TMailboxType::HTSwap, 1);
        }

        Sleep(TDuration::MilliSeconds(50));
        system.Stop();
    }

    // =======================================================================
    // 5. End-to-end message delivery on WS pool
    // =======================================================================

    Y_UNIT_TEST(SendToWSPoolActorDelivered) {
        auto setup = MakeWSSetup(2);
        TActorSystem system(setup);
        system.Start();

        std::atomic<i64> count{0};
        TActorId actorId = system.Register(new TCountingActor(count), TMailboxType::HTSwap, 1);

        system.Send(actorId, new TEvIntPing());

        for (int i = 0; i < 200; ++i) {
            if (count.load(std::memory_order_relaxed) > 0) {
                break;
            }
            Sleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT_VALUES_EQUAL(count.load(std::memory_order_relaxed), 1);

        system.Stop();
    }

    Y_UNIT_TEST(MultipleSendsToWSPoolActorDelivered) {
        auto setup = MakeWSSetup(4);
        TActorSystem system(setup);
        system.Start();

        std::atomic<i64> count{0};
        TActorId actorId = system.Register(new TCountingActor(count), TMailboxType::HTSwap, 1);

        constexpr int kMsgs = 100;
        for (int i = 0; i < kMsgs; ++i) {
            system.Send(actorId, new TEvIntPing());
        }

        for (int i = 0; i < 500; ++i) {
            if (count.load(std::memory_order_relaxed) >= kMsgs) {
                break;
            }
            Sleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT_VALUES_EQUAL(count.load(std::memory_order_relaxed), kMsgs);

        system.Stop();
    }

    // =======================================================================
    // 6. Scheduler integration (on Basic pool, with WS present)
    // =======================================================================

    Y_UNIT_TEST(ScheduleDelayedOnBasicPoolWithWSPresent) {
        auto setup = MakeWSSetup(2);
        TActorSystem system(setup);
        system.Start();

        std::atomic<i64> count{0};
        TActorId actorId = system.Register(new TCountingActor(count), TMailboxType::HTSwap, 0);

        system.Schedule(
            TDuration::MilliSeconds(100),
            new IEventHandle(actorId, TActorId(), new TEvIntPing()));

        for (int i = 0; i < 300; ++i) {
            if (count.load(std::memory_order_relaxed) > 0) {
                break;
            }
            Sleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT_VALUES_EQUAL(count.load(std::memory_order_relaxed), 1);

        system.Stop();
    }

    Y_UNIT_TEST(ScheduleImmediateOnBasicPoolWithWSPresent) {
        auto setup = MakeWSSetup(2);
        TActorSystem system(setup);
        system.Start();

        std::atomic<i64> count{0};
        TActorId actorId = system.Register(new TCountingActor(count), TMailboxType::HTSwap, 0);

        system.Schedule(
            TDuration::Zero(),
            new IEventHandle(actorId, TActorId(), new TEvIntPing()));

        for (int i = 0; i < 200; ++i) {
            if (count.load(std::memory_order_relaxed) > 0) {
                break;
            }
            Sleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT_VALUES_EQUAL(count.load(std::memory_order_relaxed), 1);

        system.Stop();
    }

    // =======================================================================
    // 7. Basic pool ping-pong with WS pool present
    // =======================================================================

    Y_UNIT_TEST(PingPongOnBasicPoolWithWSPresent) {
        auto setup = MakeWSSetup(4);
        TActorSystem system(setup);
        system.Start();

        // Both counters track how many messages each actor processed.
        std::atomic<i64> countA{0};
        std::atomic<i64> countB{0};

        class TPingPongActor: public TActor<TPingPongActor> {
            std::atomic<i64>& Count_;
            TActorId Peer_;
            int Remaining_;

        public:
            TPingPongActor(std::atomic<i64>& count, int rounds)
                : TActor(&TPingPongActor::StateFunc)
                , Count_(count)
                , Remaining_(rounds)
            {
            }

            void SetPeer(TActorId peer) {
                Peer_ = peer;
            }

            STFUNC(StateFunc) {
                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvIntPing, HandlePing);
                    hFunc(TEvIntPong, HandlePong);
                    default:
                        break;
                }
            }

            void HandlePing(TEvIntPing::TPtr&) {
                Count_.fetch_add(1, std::memory_order_relaxed);
                if (Remaining_-- > 0) {
                    Send(Peer_, new TEvIntPong());
                }
            }

            void HandlePong(TEvIntPong::TPtr&) {
                Count_.fetch_add(1, std::memory_order_relaxed);
                if (Remaining_-- > 0) {
                    Send(Peer_, new TEvIntPing());
                }
            }
        };

        constexpr int kRounds = 50;
        auto* actorA = new TPingPongActor(countA, kRounds);
        auto* actorB = new TPingPongActor(countB, kRounds);

        TActorId idA = system.Register(actorA, TMailboxType::HTSwap, 0);
        TActorId idB = system.Register(actorB, TMailboxType::HTSwap, 0);

        actorA->SetPeer(idB);
        actorB->SetPeer(idA);

        system.Send(idA, new TEvIntPing());

        for (int i = 0; i < 500; ++i) {
            i64 total = countA.load(std::memory_order_relaxed) + countB.load(std::memory_order_relaxed);
            if (total >= kRounds) {
                break;
            }
            Sleep(TDuration::MilliSeconds(10));
        }

        i64 total = countA.load(std::memory_order_relaxed) + countB.load(std::memory_order_relaxed);
        UNIT_ASSERT_GE(total, kRounds);

        system.Stop();
    }

    // =======================================================================
    // 8. PassAway / actor lifecycle
    // =======================================================================

    Y_UNIT_TEST(PassAwayOnBasicPoolWithWSPresent) {
        auto setup = MakeWSSetup(2);
        TActorSystem system(setup);
        system.Start();

        std::atomic<i64> count{0};
        TActorId targetId = system.Register(new TCountingActor(count), TMailboxType::HTSwap, 0);

        system.Register(new TFireAndForgetActor(targetId), TMailboxType::HTSwap, 0);

        for (int i = 0; i < 200; ++i) {
            if (count.load(std::memory_order_relaxed) > 0) {
                break;
            }
            Sleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT_VALUES_EQUAL(count.load(std::memory_order_relaxed), 1);

        system.Stop();
    }

    // =======================================================================
    // 9. Repeated start/stop cycles
    // =======================================================================

    Y_UNIT_TEST(RepeatedStartStop) {
        for (int cycle = 0; cycle < 3; ++cycle) {
            auto setup = MakeWSSetup(2);
            TActorSystem system(setup);
            system.Start();
            Sleep(TDuration::MilliSeconds(20));
            system.Stop();
        }
    }
} // Y_UNIT_TEST_SUITE(ExecutorPoolWsIntegration)
