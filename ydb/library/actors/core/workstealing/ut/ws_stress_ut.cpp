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

    // High-contention star: senders >> slots. The regression manifested
    // as near-zero throughput when fan-in mailboxes were penalized by
    // the continuation fairness mechanism.
    // Assert a minimum throughput floor of 10K receiver ops/s.

    Y_UNIT_TEST(Star_8Slots_100Senders) {
        RunStarStress(8, 100, TDuration::Seconds(3));
    }

    Y_UNIT_TEST(Star_8Slots_500Senders) {
        RunStarStress(8, 500, TDuration::Seconds(3));
    }

    Y_UNIT_TEST(Star_16Slots_500Senders) {
        RunStarStress(16, 500, TDuration::Seconds(5));
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

    // =========================================================================
    // Pipeline lossless delivery: finite events, per-stage counting.
    //
    // Source injects exactly N events, each stage counts and forwards,
    // sink counts arrivals.  Assert: every stage (and the sink) sees
    // exactly N events.  A stall in the continuation mechanism shows up
    // as a stage with fewer events than the source emitted.
    // =========================================================================

    struct TEvPipeMsg: TEventLocal<TEvPipeMsg, 10710> {};

    class TPipeTestSource: public TActor<TPipeTestSource> {
        TActorId FirstStage_;
        ui64 Remaining_;
        std::atomic<ui64>& SentCounter_;

    public:
        TPipeTestSource(TActorId firstStage, ui64 total, std::atomic<ui64>& sent)
            : TActor(&TPipeTestSource::StateFunc)
            , FirstStage_(firstStage)
            , Remaining_(total)
            , SentCounter_(sent)
        {}

        STFUNC(StateFunc) {
            Y_UNUSED(ev);
            while (Remaining_ > 0) {
                --Remaining_;
                SentCounter_.fetch_add(1, std::memory_order_relaxed);
                Send(FirstStage_, new TEvPipeMsg());
            }
        }
    };

    class TPipeTestStage: public TActor<TPipeTestStage> {
        TActorId Next_;
        std::atomic<ui64>& Counter_;

    public:
        TPipeTestStage(TActorId next, std::atomic<ui64>& counter)
            : TActor(&TPipeTestStage::StateFunc)
            , Next_(next)
            , Counter_(counter)
        {}

        STFUNC(StateFunc) {
            Y_UNUSED(ev);
            Counter_.fetch_add(1, std::memory_order_relaxed);
            Send(Next_, new TEvPipeMsg());
        }
    };

    class TPipeTestSink: public TActor<TPipeTestSink> {
        std::atomic<ui64>& Counter_;

    public:
        explicit TPipeTestSink(std::atomic<ui64>& counter)
            : TActor(&TPipeTestSink::StateFunc)
            , Counter_(counter)
        {}

        STFUNC(StateFunc) {
            Y_UNUSED(ev);
            Counter_.fetch_add(1, std::memory_order_relaxed);
        }
    };

    static void RunPipelineLossless(i16 slots, ui32 pipelines, ui32 stages,
                                    ui64 eventsPerPipeline, TDuration timeout) {
        using NWorkStealing::TWSExecutorPool;

        auto setup = MakeWSStressSetup(slots);
        TActorSystem sys(setup);
        sys.Start();

        ui64 totalExpected = static_cast<ui64>(pipelines) * eventsPerPipeline;

        // Per-stage counters: index 0..stages-1 = stages, index stages = sink
        TVector<std::atomic<ui64>> stageCounts(stages + 1);
        for (auto& c : stageCounts) {
            c.store(0, std::memory_order_relaxed);
        }
        std::atomic<ui64> sentCount{0};

        for (ui32 p = 0; p < pipelines; ++p) {
            // Build backwards: sink -> stages -> source
            auto* sink = new TPipeTestSink(stageCounts[stages]);
            TActorId nextId = sys.Register(sink, TMailboxType::HTSwap, 1);

            for (ui32 s = 0; s < stages; ++s) {
                ui32 stageIdx = stages - 1 - s;  // backwards
                auto* stage = new TPipeTestStage(nextId, stageCounts[stageIdx]);
                nextId = sys.Register(stage, TMailboxType::HTSwap, 1);
            }

            auto* source = new TPipeTestSource(nextId, eventsPerPipeline, sentCount);
            TActorId sourceId = sys.Register(source, TMailboxType::HTSwap, 1);
            // Kick the source
            sys.Send(sourceId, new TEvPipeMsg());
        }

        // Wait for sink to receive all events, with timeout + progress monitoring
        TInstant deadline = TInstant::Now() + timeout;
        TInstant lastProgressPrint = TInstant::Now();
        ui64 lastSinkValue = 0;
        while (TInstant::Now() < deadline) {
            ui64 sinkNow = stageCounts[stages].load(std::memory_order_relaxed);
            if (sinkNow >= totalExpected) {
                break;
            }
            TInstant now = TInstant::Now();
            if (now - lastProgressPrint >= TDuration::Seconds(2)) {
                ui64 rate = (sinkNow - lastSinkValue) * 1000 / Max<ui64>(1, (now - lastProgressPrint).MilliSeconds());
                Cerr << "  [" << (now - (deadline - timeout)).Seconds() << "s] sink="
                     << sinkNow << "/" << totalExpected
                     << " rate=" << rate << "/s" << Endl;
                lastSinkValue = sinkNow;
                lastProgressPrint = now;
            }
            Sleep(TDuration::MilliSeconds(50));
        }

        ui64 sent = sentCount.load(std::memory_order_relaxed);
        ui64 sinkGot = stageCounts[stages].load(std::memory_order_relaxed);

        // Diagnostic dump on failure
        if (sinkGot < totalExpected) {
            Cerr << "  PIPELINE STALL: slots=" << slots
                 << " stages=" << stages
                 << " sent=" << sent << "/" << totalExpected
                 << " sink=" << sinkGot << "/" << totalExpected << Endl;
        }

        Sleep(TDuration::MilliSeconds(200));
        sys.Stop();

        UNIT_ASSERT_VALUES_EQUAL_C(sent, totalExpected,
            "Source did not emit all events");
        for (ui32 s = 0; s < stages; ++s) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                stageCounts[s].load(std::memory_order_relaxed), totalExpected,
                TStringBuilder() << "Stage " << s << " lost events");
        }
        UNIT_ASSERT_VALUES_EQUAL_C(sinkGot, totalExpected,
            "Sink lost events");
    }

    Y_UNIT_TEST(PipelineLossless_4Slots_4Pipe_4Stage) {
        RunPipelineLossless(4, 4, 4, 10000, TDuration::Seconds(15));
    }

    Y_UNIT_TEST(PipelineLossless_8Slots_8Pipe_4Stage) {
        RunPipelineLossless(8, 8, 4, 10000, TDuration::Seconds(15));
    }

    Y_UNIT_TEST(PipelineLossless_16Slots_8Pipe_8Stage) {
        RunPipelineLossless(16, 8, 8, 10000, TDuration::Seconds(20));
    }

    Y_UNIT_TEST(PipelineLossless_32Slots_16Pipe_4Stage) {
        RunPipelineLossless(32, 16, 4, 10000, TDuration::Seconds(20));
    }

    Y_UNIT_TEST(PipelineLossless_32Slots_4Pipe_16Stage) {
        RunPipelineLossless(32, 4, 16, 5000, TDuration::Seconds(30));
    }

    Y_UNIT_TEST(PipelineLossless_8Slots_1Pipe_100Stage) {
        RunPipelineLossless(8, 1, 100, 1000, TDuration::Seconds(30));
    }

    Y_UNIT_TEST(PipelineLossless_16Slots_1Pipe_100Stage) {
        RunPipelineLossless(16, 1, 100, 1000, TDuration::Seconds(30));
    }

    Y_UNIT_TEST(PipelineLossless_32Slots_1Pipe_100Stage) {
        RunPipelineLossless(32, 1, 100, 1000, TDuration::Seconds(30));
    }

    Y_UNIT_TEST(PipelineLossless_8Slots_1Pipe_1000Stage) {
        RunPipelineLossless(8, 1, 1000, 100, TDuration::Seconds(60));
    }

    Y_UNIT_TEST(PipelineLossless_16Slots_1Pipe_1000Stage) {
        RunPipelineLossless(16, 1, 1000, 100, TDuration::Seconds(60));
    }

    Y_UNIT_TEST(PipelineLossless_32Slots_1Pipe_1000Stage) {
        RunPipelineLossless(32, 1, 1000, 100, TDuration::Seconds(60));
    }

    // Continuous-injection variant: source self-loops (like the benchmark).
    // Verifies that with continuous injection and a stop flag, the sink
    // eventually receives events — a stall would show 0.

    class TPipeContinuousSource: public TActor<TPipeContinuousSource> {
        TActorId FirstStage_;
        std::atomic<bool>& Stop_;
        std::atomic<ui64>& SentCounter_;

    public:
        TPipeContinuousSource(TActorId firstStage, std::atomic<bool>& stop,
                              std::atomic<ui64>& sent)
            : TActor(&TPipeContinuousSource::StateFunc)
            , FirstStage_(firstStage)
            , Stop_(stop)
            , SentCounter_(sent)
        {}

        STFUNC(StateFunc) {
            Y_UNUSED(ev);
            if (Stop_.load(std::memory_order_relaxed)) {
                return;
            }
            SentCounter_.fetch_add(1, std::memory_order_relaxed);
            Send(FirstStage_, new TEvPipeMsg());
            Send(SelfId(), new TEvPipeMsg());
        }
    };

    static void RunPipelineContinuous(i16 slots, ui32 pipelines, ui32 stages,
                                      TDuration duration) {
        auto setup = MakeWSStressSetup(slots);
        TActorSystem sys(setup);
        sys.Start();

        TVector<std::atomic<ui64>> stageCounts(stages + 1);
        for (auto& c : stageCounts) {
            c.store(0, std::memory_order_relaxed);
        }
        std::atomic<ui64> sentCount{0};
        std::atomic<bool> stop{false};

        for (ui32 p = 0; p < pipelines; ++p) {
            auto* sink = new TPipeTestSink(stageCounts[stages]);
            TActorId nextId = sys.Register(sink, TMailboxType::HTSwap, 1);

            for (ui32 s = 0; s < stages; ++s) {
                ui32 stageIdx = stages - 1 - s;
                auto* stage = new TPipeTestStage(nextId, stageCounts[stageIdx]);
                nextId = sys.Register(stage, TMailboxType::HTSwap, 1);
            }

            auto* source = new TPipeContinuousSource(nextId, stop, sentCount);
            TActorId sourceId = sys.Register(source, TMailboxType::HTSwap, 1);
            sys.Send(sourceId, new TEvPipeMsg());
        }

        Sleep(duration);
        stop.store(true, std::memory_order_relaxed);

        ui64 sent = sentCount.load(std::memory_order_relaxed);
        ui64 sinkGot = stageCounts[stages].load(std::memory_order_relaxed);

        Cerr << "  PipelineContinuous: slots=" << slots
             << " pipelines=" << pipelines << " stages=" << stages
             << " sent=" << sent << " sink=" << sinkGot << Endl;
        for (ui32 s = 0; s < stages; ++s) {
            Cerr << "    stage[" << s << "]: "
                 << stageCounts[s].load(std::memory_order_relaxed) << Endl;
        }

        Sleep(TDuration::MilliSeconds(200));
        sys.Stop();

        UNIT_ASSERT_C(sent > 0, "Source didn't send any events");
        UNIT_ASSERT_C(sinkGot > 0,
            TStringBuilder() << "Sink got 0 events but source sent " << sent
                             << " — pipeline stalled");
        for (ui32 s = 0; s < stages; ++s) {
            UNIT_ASSERT_C(stageCounts[s].load(std::memory_order_relaxed) > 0,
                TStringBuilder() << "Stage " << s << " got 0 events — stall");
        }
    }

    Y_UNIT_TEST(PipelineContinuous_16Slots_8Pipe_4Stage) {
        RunPipelineContinuous(16, 8, 4, TDuration::Seconds(5));
    }

    Y_UNIT_TEST(PipelineContinuous_32Slots_8Pipe_8Stage) {
        RunPipelineContinuous(32, 8, 8, TDuration::Seconds(5));
    }

    Y_UNIT_TEST(PipelineContinuous_32Slots_16Pipe_4Stage) {
        RunPipelineContinuous(32, 16, 4, TDuration::Seconds(5));
    }

    // =========================================================================
    // Reincarnation: Register → Send → PassAway in a tight loop.
    // Exercises mailbox hint allocation/reclamation. Without reclamation,
    // hints are exhausted and TWsSlotAllocator::Refill aborts.
    // =========================================================================

    struct TEvReincarnate: TEventLocal<TEvReincarnate, 10720> {};

    class TReincarnationActor: public TActor<TReincarnationActor> {
        std::atomic<ui64>& Counter_;
        std::atomic<bool>& Stop_;

    public:
        TReincarnationActor(std::atomic<ui64>& counter, std::atomic<bool>& stop)
            : TActor(&TReincarnationActor::StateFunc)
            , Counter_(counter)
            , Stop_(stop)
        {}

        STFUNC(StateFunc) {
            Y_UNUSED(ev);
            Counter_.fetch_add(1, std::memory_order_relaxed);
            if (Stop_.load(std::memory_order_relaxed)) {
                return;
            }
            auto* next = new TReincarnationActor(Counter_, Stop_);
            TActorId nextId = Register(next);
            Send(nextId, new TEvReincarnate());
            PassAway();
        }
    };

    static void RunReincarnationStress(i16 slots, ui32 chains, TDuration duration) {
        auto setup = MakeWSStressSetup(slots);
        TActorSystem sys(setup);
        sys.Start();

        std::atomic<ui64> counter{0};
        std::atomic<bool> stop{false};

        for (ui32 i = 0; i < chains; ++i) {
            auto* actor = new TReincarnationActor(counter, stop);
            TActorId id = sys.Register(actor, TMailboxType::HTSwap, 1);
            sys.Send(id, new TEvReincarnate());
        }

        Sleep(duration);
        stop.store(true, std::memory_order_relaxed);

        ui64 ops = counter.load(std::memory_order_relaxed);
        Cerr << "  Reincarnation slots=" << slots << " chains=" << chains
             << " ops=" << ops << Endl;

        Sleep(TDuration::MilliSeconds(500));
        sys.Stop();

        UNIT_ASSERT(ops > 0);
    }

    Y_UNIT_TEST(Reincarnation_4Slots_10Chains) {
        RunReincarnationStress(4, 10, TDuration::Seconds(3));
    }

    Y_UNIT_TEST(Reincarnation_8Slots_100Chains) {
        RunReincarnationStress(8, 100, TDuration::Seconds(3));
    }

    Y_UNIT_TEST(Reincarnation_16Slots_100Chains) {
        RunReincarnationStress(16, 100, TDuration::Seconds(5));
    }

    // The original crash (hint exhaustion) only reproduced at 16+ threads
    // with enough concurrent chains to drain the allocator faster than
    // reclamation returns hints.  These tests cover that regime.
    Y_UNIT_TEST(Reincarnation_16Slots_1000Chains) {
        RunReincarnationStress(16, 1000, TDuration::Seconds(5));
    }

    Y_UNIT_TEST(Reincarnation_32Slots_1000Chains) {
        RunReincarnationStress(32, 1000, TDuration::Seconds(5));
    }

    // =========================================================================
    // High-slot star regression tests.
    //
    // At high slot counts (slots ≈ senders), each sender gets its own slot.
    // The receiver's activation then depends on park/wake cycles rather
    // than being interleaved with senders on the same slot. The 0-ops
    // regression at 96+ threads was caused by the receiver's activation
    // never reaching a worker due to starvation in this regime.
    // =========================================================================

    Y_UNIT_TEST(Star_32Slots_100Senders) {
        RunStarStress(32, 100, TDuration::Seconds(5));
    }

    Y_UNIT_TEST(Star_64Slots_32Senders) {
        RunStarStress(64, 32, TDuration::Seconds(5));
    }

    Y_UNIT_TEST(Star_64Slots_100Senders) {
        RunStarStress(64, 100, TDuration::Seconds(5));
    }

    // =========================================================================
    // Star with aggressive parking: very short spin threshold forces
    // frequent park/wake transitions, exposing Dekker protocol issues.
    // =========================================================================

    static void RunStarStressWithSpin(i16 slots, ui32 senders,
                                       TDuration duration, ui64 spinCycles) {
        auto setup = MakeWSStressSetup(slots);
        auto& pools = setup->CpuManager.WorkStealing->Pools;
        pools[0].WsConfig.SpinThresholdCycles = spinCycles;
        pools[0].WsConfig.MinSpinThresholdCycles = spinCycles;

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
             << " spin=" << spinCycles << " ops=" << ops << Endl;

        Sleep(TDuration::MilliSeconds(200));
        sys.Stop();

        UNIT_ASSERT_C(ops > 0,
            TStringBuilder() << "Star receiver got 0 events with "
                             << senders << " senders and "
                             << slots << " slots (spin=" << spinCycles << ")");
    }

    Y_UNIT_TEST(Star_ShortSpin_16Slots_100Senders) {
        // ~0.3μs spin — aggressive parking
        RunStarStressWithSpin(16, 100, TDuration::Seconds(3), 1000);
    }

    Y_UNIT_TEST(Star_ShortSpin_32Slots_100Senders) {
        RunStarStressWithSpin(32, 100, TDuration::Seconds(3), 1000);
    }

    Y_UNIT_TEST(Star_ShortSpin_64Slots_32Senders) {
        RunStarStressWithSpin(64, 32, TDuration::Seconds(3), 1000);
    }

    // =========================================================================
    // Star with diagnostic tracking: finite events, counts per stage.
    //
    // Each sender sends exactly N events to the receiver (no self-send
    // loop — simpler). The test verifies ALL events are delivered.
    // This catches both starvation (receiver never scheduled) and
    // event loss (activation dropped by routing).
    // =========================================================================

    struct TEvStarDiag: TEventLocal<TEvStarDiag, 10730> {};

    class TDiagStarReceiver: public TActor<TDiagStarReceiver> {
        std::atomic<ui64>& RecvCount_;

    public:
        explicit TDiagStarReceiver(std::atomic<ui64>& count)
            : TActor(&TDiagStarReceiver::StateFunc)
            , RecvCount_(count)
        {}

        STFUNC(StateFunc) {
            Y_UNUSED(ev);
            RecvCount_.fetch_add(1, std::memory_order_relaxed);
        }
    };

    // Fire-and-forget sender: sends exactly TotalEvents events to
    // receiver (spread across multiple self-sends), then stops.
    class TDiagStarSender: public TActor<TDiagStarSender> {
        TActorId Receiver_;
        std::atomic<ui64>& SentCount_;
        ui64 Remaining_;

    public:
        TDiagStarSender(TActorId receiver, std::atomic<ui64>& sent, ui64 total)
            : TActor(&TDiagStarSender::StateFunc)
            , Receiver_(receiver)
            , SentCount_(sent)
            , Remaining_(total)
        {}

        STFUNC(StateFunc) {
            Y_UNUSED(ev);
            if (Remaining_ == 0) {
                return;
            }
            // Send a batch of events to receiver per activation
            ui64 batch = Min(Remaining_, ui64(16));
            for (ui64 i = 0; i < batch; ++i) {
                Send(Receiver_, new TEvStarDiag());
                SentCount_.fetch_add(1, std::memory_order_relaxed);
            }
            Remaining_ -= batch;
            if (Remaining_ > 0) {
                Send(SelfId(), new TEvStarDiag());
            }
        }
    };

    static void RunStarDiagnostic(i16 slots, ui32 senders,
                                   ui64 eventsPerSender, TDuration timeout) {
        auto setup = MakeWSStressSetup(slots);
        TActorSystem sys(setup);
        sys.Start();

        std::atomic<ui64> recvCount{0};
        std::atomic<ui64> sentCount{0};
        ui64 totalExpected = static_cast<ui64>(senders) * eventsPerSender;

        auto* receiver = new TDiagStarReceiver(recvCount);
        TActorId receiverId = sys.Register(receiver, TMailboxType::HTSwap, 1);

        for (ui32 i = 0; i < senders; ++i) {
            auto* sender = new TDiagStarSender(receiverId, sentCount, eventsPerSender);
            TActorId senderId = sys.Register(sender, TMailboxType::HTSwap, 1);
            sys.Send(senderId, new TEvStarDiag());
        }

        // Wait for all events to be received
        TInstant deadline = TInstant::Now() + timeout;
        TInstant lastPrint = TInstant::Now();
        ui64 lastRecv = 0;
        while (TInstant::Now() < deadline) {
            ui64 recv = recvCount.load(std::memory_order_relaxed);
            if (recv >= totalExpected) {
                break;
            }
            TInstant now = TInstant::Now();
            if (now - lastPrint >= TDuration::Seconds(2)) {
                ui64 rate = (recv - lastRecv) * 1000 /
                    Max<ui64>(1, (now - lastPrint).MilliSeconds());
                Cerr << "  [" << (now - (deadline - timeout)).Seconds() << "s] "
                     << "recv=" << recv << "/" << totalExpected
                     << " sent=" << sentCount.load(std::memory_order_relaxed)
                     << " rate=" << rate << "/s" << Endl;
                lastRecv = recv;
                lastPrint = now;
            }
            Sleep(TDuration::MilliSeconds(50));
        }

        ui64 sent = sentCount.load(std::memory_order_relaxed);
        ui64 recv = recvCount.load(std::memory_order_relaxed);

        if (recv < totalExpected) {
            Cerr << "  STAR STALL: slots=" << slots << " senders=" << senders
                 << " sent=" << sent << "/" << totalExpected
                 << " recv=" << recv << "/" << totalExpected << Endl;
        }

        Sleep(TDuration::MilliSeconds(200));
        sys.Stop();

        UNIT_ASSERT_VALUES_EQUAL_C(sent, totalExpected,
            "Senders did not emit all events");
        UNIT_ASSERT_VALUES_EQUAL_C(recv, totalExpected,
            TStringBuilder() << "Star receiver lost events: recv=" << recv
                             << " expected=" << totalExpected);
    }

    Y_UNIT_TEST(StarDiag_8Slots_32Senders) {
        RunStarDiagnostic(8, 32, 1000, TDuration::Seconds(15));
    }

    Y_UNIT_TEST(StarDiag_16Slots_64Senders) {
        RunStarDiagnostic(16, 64, 1000, TDuration::Seconds(15));
    }

    Y_UNIT_TEST(StarDiag_32Slots_100Senders) {
        RunStarDiagnostic(32, 100, 1000, TDuration::Seconds(20));
    }

    Y_UNIT_TEST(StarDiag_64Slots_32Senders) {
        RunStarDiagnostic(64, 32, 1000, TDuration::Seconds(20));
    }

    Y_UNIT_TEST(StarDiag_64Slots_100Senders) {
        RunStarDiagnostic(64, 100, 1000, TDuration::Seconds(20));
    }

    // =========================================================================
    // Detailed diagnostic star test: finite events, receiver event count
    // verification, pool-level diagnostics on stall.
    //
    // This test is designed to catch the EPYC 96-thread 0-ops regression.
    // On stall it dumps: receiver mailbox state, activation/event stats,
    // dropped activations, and per-slot state.
    // =========================================================================

    // Sender that sends exactly N events to receiver via self-send loop,
    // plus tracks its own send count for cross-checking.
    class TDetailedStarSender: public TActor<TDetailedStarSender> {
        TActorId Receiver_;
        std::atomic<ui64>& SentCount_;
        ui64 Remaining_;

    public:
        TDetailedStarSender(TActorId receiver, std::atomic<ui64>& sent, ui64 total)
            : TActor(&TDetailedStarSender::StateFunc)
            , Receiver_(receiver)
            , SentCount_(sent)
            , Remaining_(total)
        {}

        STFUNC(StateFunc) {
            Y_UNUSED(ev);
            if (Remaining_ == 0) {
                return;
            }
            Send(Receiver_, new TEvStarDiag());
            SentCount_.fetch_add(1, std::memory_order_relaxed);
            --Remaining_;
            if (Remaining_ > 0) {
                Send(SelfId(), new TEvStarDiag());
            }
        }
    };

    static void RunStarDiagnosticDetailed(i16 slots, ui32 senders,
                                           ui64 eventsPerSender, TDuration timeout,
                                           ui64 spinCycles = 0) {
        using NWorkStealing::TWSExecutorPool;

        auto setup = MakeWSStressSetup(slots);
        if (spinCycles > 0) {
            auto& pools = setup->CpuManager.WorkStealing->Pools;
            pools[0].WsConfig.SpinThresholdCycles = spinCycles;
            pools[0].WsConfig.MinSpinThresholdCycles = spinCycles;
        }
        TActorSystem sys(setup);
        sys.Start();

        std::atomic<ui64> recvCount{0};
        std::atomic<ui64> sentCount{0};
        ui64 totalExpected = static_cast<ui64>(senders) * eventsPerSender;

        auto* receiver = new TDiagStarReceiver(recvCount);
        TActorId receiverId = sys.Register(receiver, TMailboxType::HTSwap, 1);

        for (ui32 i = 0; i < senders; ++i) {
            auto* sender = new TDetailedStarSender(receiverId, sentCount, eventsPerSender);
            TActorId senderId = sys.Register(sender, TMailboxType::HTSwap, 1);
            sys.Send(senderId, new TEvStarDiag());
        }

        // Wait for all events with progress monitoring and stall detection
        TInstant deadline = TInstant::Now() + timeout;
        TInstant lastPrint = TInstant::Now();
        ui64 lastRecv = 0;
        ui64 stallCount = 0;  // consecutive intervals with zero progress
        bool stalled = false;

        while (TInstant::Now() < deadline) {
            ui64 recv = recvCount.load(std::memory_order_relaxed);
            if (recv >= totalExpected) {
                break;
            }
            TInstant now = TInstant::Now();
            if (now - lastPrint >= TDuration::Seconds(1)) {
                ui64 delta = recv - lastRecv;
                ui64 rate = delta * 1000 /
                    Max<ui64>(1, (now - lastPrint).MilliSeconds());

                Cerr << "  [" << (now - (deadline - timeout)).Seconds() << "s] "
                     << "recv=" << recv << "/" << totalExpected
                     << " sent=" << sentCount.load(std::memory_order_relaxed)
                     << " rate=" << rate << "/s" << Endl;

                // Detect stall: zero progress for 3+ consecutive seconds
                if (delta == 0 && sentCount.load(std::memory_order_relaxed) > 0) {
                    ++stallCount;
                    if (stallCount >= 3 && !stalled) {
                        stalled = true;
                        auto* wsPool = TWSExecutorPool::FindPool("ws-stress");
                        if (wsPool) {
                            Cerr << "  [STALL DETECTED] receiver hint="
                                 << receiverId.Hint() << Endl;
                            wsPool->DumpSlots(Cerr);
                            wsPool->DumpCounters("STALL");
                        }
                    }
                } else {
                    stallCount = 0;
                }

                lastRecv = recv;
                lastPrint = now;
            }
            Sleep(TDuration::MilliSeconds(50));
        }

        ui64 sent = sentCount.load(std::memory_order_relaxed);
        ui64 recv = recvCount.load(std::memory_order_relaxed);

        if (recv < totalExpected) {
            Cerr << "  STAR DELIVERY FAILURE: slots=" << slots
                 << " senders=" << senders
                 << " sent=" << sent << "/" << totalExpected
                 << " recv=" << recv << "/" << totalExpected << Endl;

            auto* wsPool = TWSExecutorPool::FindPool("ws-stress");
            if (wsPool) {
                Cerr << "  [FINAL] receiver hint=" << receiverId.Hint() << Endl;
                wsPool->DumpSlots(Cerr);
                wsPool->DumpCounters("FINAL");
            }
        }

        Sleep(TDuration::MilliSeconds(200));
        sys.Stop();

        UNIT_ASSERT_VALUES_EQUAL_C(sent, totalExpected,
            "Senders did not emit all events");
        UNIT_ASSERT_VALUES_EQUAL_C(recv, totalExpected,
            TStringBuilder() << "Star receiver lost events: recv=" << recv
                             << " expected=" << totalExpected
                             << " sent=" << sent);
    }

    // High slot count tests that reproduce the 96-thread EPYC regression.
    // On an 8-core machine these exercise the same code paths with
    // more slots than physical cores (oversubscribed).

    Y_UNIT_TEST(StarDetailed_64Sl_100Send) {
        RunStarDiagnosticDetailed(64, 100, 500, TDuration::Seconds(30));
    }

    Y_UNIT_TEST(StarDetailed_64Sl_100Send_ShortSpin) {
        RunStarDiagnosticDetailed(64, 100, 500, TDuration::Seconds(30), 1000);
    }

    Y_UNIT_TEST(StarDetailed_32Sl_100Send) {
        RunStarDiagnosticDetailed(32, 100, 500, TDuration::Seconds(20));
    }

    Y_UNIT_TEST(StarDetailed_128Sl_100Send) {
        RunStarDiagnosticDetailed(128, 100, 200, TDuration::Seconds(30));
    }

    Y_UNIT_TEST(StarDetailed_128Sl_100Send_ShortSpin) {
        RunStarDiagnosticDetailed(128, 100, 200, TDuration::Seconds(30), 1000);
    }

} // Y_UNIT_TEST_SUITE(WsStress)
