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
#include <vector>

using namespace NActors;

// ---------------------------------------------------------------------------
// Event types (10700+ range to avoid collisions)
// ---------------------------------------------------------------------------

struct TEvStressPing: TEventLocal<TEvStressPing, 10701> {};
struct TEvStressPong: TEventLocal<TEvStressPong, 10702> {};
struct TEvStressMsg: TEventLocal<TEvStressMsg, 10703> {};

// ---------------------------------------------------------------------------
// Setup helper
// ---------------------------------------------------------------------------

static THolder<TActorSystemSetup> MakeWSStressSetup(i16 wsSlots) {
    auto setup = MakeHolder<TActorSystemSetup>();
    setup->NodeId = 1;

    TBasicExecutorPoolConfig basicCfg;
    basicCfg.PoolId = 0;
    basicCfg.PoolName = "scheduler";
    basicCfg.Threads = 1;
    basicCfg.MinThreadCount = 1;
    basicCfg.MaxThreadCount = 1;
    basicCfg.DefaultThreadCount = 1;
    setup->CpuManager.Basic.push_back(basicCfg);

    TWorkStealingPoolConfig wsCfg;
    wsCfg.PoolId = 1;
    wsCfg.PoolName = "ws-stress";
    wsCfg.MinSlotCount = wsSlots;
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
// Ping-Pong actors (unbounded — keeps going until Stop flag)
// ---------------------------------------------------------------------------

class TStressPingActor: public TActor<TStressPingActor> {
    TActorId Peer_;
    std::atomic<ui64>& Counter_;
    std::atomic<bool>& Stop_;

public:
    TStressPingActor(TActorId peer, std::atomic<ui64>& counter, std::atomic<bool>& stop)
        : TActor(&TStressPingActor::StateFunc)
        , Peer_(peer)
        , Counter_(counter)
        , Stop_(stop)
    {
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStressPing, HandlePing);
            hFunc(TEvStressPong, HandlePong);
        }
    }

    void HandlePing(TEvStressPing::TPtr&) {
        Counter_.fetch_add(1, std::memory_order_relaxed);
        if (!Stop_.load(std::memory_order_relaxed)) {
            Send(Peer_, new TEvStressPong());
        }
    }

    void HandlePong(TEvStressPong::TPtr&) {
        Counter_.fetch_add(1, std::memory_order_relaxed);
        if (!Stop_.load(std::memory_order_relaxed)) {
            Send(Peer_, new TEvStressPing());
        }
    }
};

class TStressPongActor: public TActor<TStressPongActor> {
    std::atomic<ui64>& Counter_;
    std::atomic<bool>& Stop_;
    TActorId Peer_;

public:
    TStressPongActor(std::atomic<ui64>& counter, std::atomic<bool>& stop)
        : TActor(&TStressPongActor::StateFunc)
        , Counter_(counter)
        , Stop_(stop)
    {
    }

    void SetPeer(TActorId peer) {
        Peer_ = peer;
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStressPing, HandlePing);
            hFunc(TEvStressPong, HandlePong);
        }
    }

    void HandlePing(TEvStressPing::TPtr& ev) {
        Counter_.fetch_add(1, std::memory_order_relaxed);
        if (!Stop_.load(std::memory_order_relaxed)) {
            Send(ev->Sender, new TEvStressPong());
        }
    }

    void HandlePong(TEvStressPong::TPtr&) {
        Counter_.fetch_add(1, std::memory_order_relaxed);
        if (!Stop_.load(std::memory_order_relaxed)) {
            Send(Peer_, new TEvStressPing());
        }
    }
};

// Self-sending actor: sends to itself as fast as possible (hot mailbox)
class TSelfSender: public TActor<TSelfSender> {
    std::atomic<ui64>& Counter_;
    std::atomic<bool>& Stop_;

public:
    TSelfSender(std::atomic<ui64>& counter, std::atomic<bool>& stop)
        : TActor(&TSelfSender::StateFunc)
        , Counter_(counter)
        , Stop_(stop)
    {
    }

    STFUNC(StateFunc) {
        Y_UNUSED(ev);
        Counter_.fetch_add(1, std::memory_order_relaxed);
        if (!Stop_.load(std::memory_order_relaxed)) {
            Send(SelfId(), new TEvStressMsg());
        }
    }
};

// Star receiver: many senders target one receiver
class TStarReceiver: public TActor<TStarReceiver> {
    std::atomic<ui64>& Counter_;

public:
    TStarReceiver(std::atomic<ui64>& counter)
        : TActor(&TStarReceiver::StateFunc)
        , Counter_(counter)
    {
    }

    STFUNC(StateFunc) {
        Y_UNUSED(ev);
        Counter_.fetch_add(1, std::memory_order_relaxed);
    }
};

class TStarSender: public TActor<TStarSender> {
    TActorId Receiver_;
    std::atomic<bool>& Stop_;

public:
    TStarSender(TActorId receiver, std::atomic<bool>& stop)
        : TActor(&TStarSender::StateFunc)
        , Receiver_(receiver)
        , Stop_(stop)
    {
    }

    STFUNC(StateFunc) {
        Y_UNUSED(ev);
        if (!Stop_.load(std::memory_order_relaxed)) {
            Send(Receiver_, new TEvStressMsg());
            Send(SelfId(), new TEvStressMsg());
        }
    }
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static void RunPingPongStress(i16 slots, ui32 pairs, TDuration duration) {
    auto setup = MakeWSStressSetup(slots);
    TActorSystem sys(setup);
    sys.Start();

    std::atomic<ui64> counter{0};
    std::atomic<bool> stop{false};

    for (ui32 i = 0; i < pairs; ++i) {
        auto* pong = new TStressPongActor(counter, stop);
        TActorId pongId = sys.Register(pong, TMailboxType::HTSwap, 1);

        auto* ping = new TStressPingActor(pongId, counter, stop);
        TActorId pingId = sys.Register(ping, TMailboxType::HTSwap, 1);

        pong->SetPeer(pingId);
        sys.Send(pingId, new TEvStressPing());
    }

    Sleep(duration);
    stop.store(true, std::memory_order_relaxed);

    ui64 ops = counter.load(std::memory_order_relaxed);
    Cerr << "  PingPong slots=" << slots << " pairs=" << pairs
         << " ops=" << ops << Endl;

    Sleep(TDuration::MilliSeconds(200));
    sys.Stop();

    UNIT_ASSERT(ops > 0);
}

static void RunSelfSendStress(i16 slots, ui32 actors, TDuration duration) {
    auto setup = MakeWSStressSetup(slots);
    TActorSystem sys(setup);
    sys.Start();

    std::atomic<ui64> counter{0};
    std::atomic<bool> stop{false};

    for (ui32 i = 0; i < actors; ++i) {
        auto* actor = new TSelfSender(counter, stop);
        TActorId id = sys.Register(actor, TMailboxType::HTSwap, 1);
        sys.Send(id, new TEvStressMsg());
    }

    Sleep(duration);
    stop.store(true, std::memory_order_relaxed);

    ui64 ops = counter.load(std::memory_order_relaxed);
    Cerr << "  SelfSend slots=" << slots << " actors=" << actors
         << " ops=" << ops << Endl;

    Sleep(TDuration::MilliSeconds(200));
    sys.Stop();

    UNIT_ASSERT(ops > 0);
}

static void RunStarStress(i16 slots, ui32 senders, TDuration duration) {
    auto setup = MakeWSStressSetup(slots);
    TActorSystem sys(setup);
    sys.Start();

    std::atomic<ui64> counter{0};
    std::atomic<bool> stop{false};

    auto* receiver = new TStarReceiver(counter);
    TActorId receiverId = sys.Register(receiver, TMailboxType::HTSwap, 1);

    for (ui32 i = 0; i < senders; ++i) {
        auto* sender = new TStarSender(receiverId, stop);
        TActorId senderId = sys.Register(sender, TMailboxType::HTSwap, 1);
        sys.Send(senderId, new TEvStressMsg());
    }

    Sleep(duration);
    stop.store(true, std::memory_order_relaxed);

    ui64 ops = counter.load(std::memory_order_relaxed);
    Cerr << "  Star slots=" << slots << " senders=" << senders
         << " ops=" << ops << Endl;

    Sleep(TDuration::MilliSeconds(200));
    sys.Stop();

    UNIT_ASSERT(ops > 0);
}

// ---------------------------------------------------------------------------
// Test suite
// ---------------------------------------------------------------------------

Y_UNIT_TEST_SUITE(WsStress) {

    // Ping-pong at escalating thread counts.
    // This exercises the steal path heavily with many concurrent mailbox
    // activations, which is where the duplicate-hint / concurrent-processing
    // bug manifests.

    Y_UNIT_TEST(PingPong_4Slots_50Pairs) {
        RunPingPongStress(4, 50, TDuration::Seconds(3));
    }

    Y_UNIT_TEST(PingPong_8Slots_100Pairs) {
        RunPingPongStress(8, 100, TDuration::Seconds(3));
    }

    Y_UNIT_TEST(PingPong_16Slots_100Pairs) {
        RunPingPongStress(16, 100, TDuration::Seconds(5));
    }

    Y_UNIT_TEST(PingPong_32Slots_200Pairs) {
        RunPingPongStress(32, 200, TDuration::Seconds(5));
    }

    // Self-send: single hot mailbox per actor, exercises drain+pop path.
    // Few actors relative to slots forces stealing.

    Y_UNIT_TEST(SelfSend_8Slots_4Actors) {
        RunSelfSendStress(8, 4, TDuration::Seconds(3));
    }

    Y_UNIT_TEST(SelfSend_16Slots_8Actors) {
        RunSelfSendStress(16, 8, TDuration::Seconds(3));
    }

    Y_UNIT_TEST(SelfSend_16Slots_64Actors) {
        RunSelfSendStress(16, 64, TDuration::Seconds(3));
    }

    // Star: many senders → one receiver. Exercises high-contention
    // injection into a single mailbox from many threads.

    Y_UNIT_TEST(Star_8Slots_32Senders) {
        RunStarStress(8, 32, TDuration::Seconds(3));
    }

    Y_UNIT_TEST(Star_16Slots_64Senders) {
        RunStarStress(16, 64, TDuration::Seconds(5));
    }

    // Mixed: combine all patterns to maximize scheduling pressure.

    Y_UNIT_TEST(Mixed_16Slots) {
        auto setup = MakeWSStressSetup(16);
        TActorSystem sys(setup);
        sys.Start();

        std::atomic<ui64> ppCounter{0};
        std::atomic<ui64> ssCounter{0};
        std::atomic<ui64> starCounter{0};
        std::atomic<bool> stop{false};

        // 20 ping-pong pairs
        for (ui32 i = 0; i < 20; ++i) {
            auto* pong = new TStressPongActor(ppCounter, stop);
            TActorId pongId = sys.Register(pong, TMailboxType::HTSwap, 1);
            auto* ping = new TStressPingActor(pongId, ppCounter, stop);
            TActorId pingId = sys.Register(ping, TMailboxType::HTSwap, 1);
            pong->SetPeer(pingId);
            sys.Send(pingId, new TEvStressPing());
        }

        // 10 self-senders
        for (ui32 i = 0; i < 10; ++i) {
            auto* actor = new TSelfSender(ssCounter, stop);
            TActorId id = sys.Register(actor, TMailboxType::HTSwap, 1);
            sys.Send(id, new TEvStressMsg());
        }

        // Star with 20 senders
        auto* receiver = new TStarReceiver(starCounter);
        TActorId receiverId = sys.Register(receiver, TMailboxType::HTSwap, 1);
        for (ui32 i = 0; i < 20; ++i) {
            auto* sender = new TStarSender(receiverId, stop);
            TActorId senderId = sys.Register(sender, TMailboxType::HTSwap, 1);
            sys.Send(senderId, new TEvStressMsg());
        }

        Sleep(TDuration::Seconds(5));
        stop.store(true, std::memory_order_relaxed);

        Cerr << "  Mixed: pp=" << ppCounter.load()
             << " ss=" << ssCounter.load()
             << " star=" << starCounter.load() << Endl;

        Sleep(TDuration::MilliSeconds(200));
        sys.Stop();

        UNIT_ASSERT(ppCounter.load() > 0);
        UNIT_ASSERT(ssCounter.load() > 0);
        UNIT_ASSERT(starCounter.load() > 0);
    }

    // Stealing disabled: isolate whether the bug is in steal path or inject/drain/pop.
    // If this test passes but PingPong_16Slots_100Pairs crashes, the bug is in stealing.

    Y_UNIT_TEST(PingPong_16Slots_NoSteal) {
        auto setup = MakeWSStressSetup(16);
        // Override WS config to disable stealing
        auto& pools = setup->CpuManager.WorkStealing->Pools;
        pools[0].WsConfig.MaxStealNeighbors = 0;
        pools[0].WsConfig.StarvationGuardLimit = 999999999;

        TActorSystem sys(setup);
        sys.Start();

        std::atomic<ui64> counter{0};
        std::atomic<bool> stop{false};

        for (ui32 i = 0; i < 100; ++i) {
            auto* pong = new TStressPongActor(counter, stop);
            TActorId pongId = sys.Register(pong, TMailboxType::HTSwap, 1);
            auto* ping = new TStressPingActor(pongId, counter, stop);
            TActorId pingId = sys.Register(ping, TMailboxType::HTSwap, 1);
            pong->SetPeer(pingId);
            sys.Send(pingId, new TEvStressPing());
        }

        Sleep(TDuration::Seconds(5));
        stop.store(true, std::memory_order_relaxed);

        Cerr << "  PingPong_NoSteal slots=16 pairs=100 ops="
             << counter.load() << Endl;

        Sleep(TDuration::MilliSeconds(200));
        sys.Stop();

        UNIT_ASSERT(counter.load() > 0);
    }

    Y_UNIT_TEST(PingPong_32Slots_NoSteal) {
        auto setup = MakeWSStressSetup(32);
        auto& pools = setup->CpuManager.WorkStealing->Pools;
        pools[0].WsConfig.MaxStealNeighbors = 0;
        pools[0].WsConfig.StarvationGuardLimit = 999999999;

        TActorSystem sys(setup);
        sys.Start();

        std::atomic<ui64> counter{0};
        std::atomic<bool> stop{false};

        for (ui32 i = 0; i < 200; ++i) {
            auto* pong = new TStressPongActor(counter, stop);
            TActorId pongId = sys.Register(pong, TMailboxType::HTSwap, 1);
            auto* ping = new TStressPingActor(pongId, counter, stop);
            TActorId pingId = sys.Register(ping, TMailboxType::HTSwap, 1);
            pong->SetPeer(pingId);
            sys.Send(pingId, new TEvStressPing());
        }

        Sleep(TDuration::Seconds(5));
        stop.store(true, std::memory_order_relaxed);

        Cerr << "  PingPong_NoSteal slots=32 pairs=200 ops="
             << counter.load() << Endl;

        Sleep(TDuration::MilliSeconds(200));
        sys.Stop();

        UNIT_ASSERT(counter.load() > 0);
    }

} // Y_UNIT_TEST_SUITE(WsStress)
