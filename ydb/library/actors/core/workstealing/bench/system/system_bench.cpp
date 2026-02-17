#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/config.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/core/workstealing/executor_pool_ws.h>
#include <ydb/library/actors/core/workstealing/ws_counters.h>

#include <library/cpp/getopt/last_getopt.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>
#include <sys/resource.h>

namespace {

    using TClock = std::chrono::steady_clock;

    // -------------------------------------------------------------------
    // Event types (10600+ range to avoid collisions)
    // -------------------------------------------------------------------

    struct TEvBenchPing: NActors::TEventLocal<TEvBenchPing, 10601> {};
    struct TEvBenchPong: NActors::TEventLocal<TEvBenchPong, 10602> {};
    struct TEvBenchMsg: NActors::TEventLocal<TEvBenchMsg, 10603> {};

    // -------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------

    std::vector<ui32> ParseList(const TString& s) {
        std::vector<ui32> result;
        size_t pos = 0;
        while (pos < s.size()) {
            size_t next = s.find(',', pos);
            if (next == TString::npos) {
                next = s.size();
            }
            ui32 val = std::atoi(s.substr(pos, next - pos).c_str());
            if (val > 0) {
                result.push_back(val);
            }
            pos = next + 1;
        }
        return result;
    }

    // Global overrides (0 = use default from TWsConfig)
    ui64 GSpinThresholdCycles = 0;
    ui64 GMinSpinThresholdCycles = 0;

    // -------------------------------------------------------------------
    // Actor system setup factories
    // -------------------------------------------------------------------

    THolder<NActors::TActorSystemSetup> MakeBasicSetup(ui32 threads) {
        auto setup = MakeHolder<NActors::TActorSystemSetup>();
        setup->NodeId = 1;

        NActors::TBasicExecutorPoolConfig cfg;
        cfg.PoolId = 0;
        cfg.PoolName = "bench";
        cfg.Threads = threads;
        cfg.MinThreadCount = threads;
        cfg.MaxThreadCount = threads;
        cfg.DefaultThreadCount = threads;
        cfg.SpinThreshold = 1'000'000;
        cfg.TimePerMailbox = TDuration::Hours(1);
        setup->CpuManager.Basic.push_back(cfg);

        setup->Scheduler.Reset(new NActors::TBasicSchedulerThread(NActors::TSchedulerConfig()));
        return setup;
    }

    THolder<NActors::TActorSystemSetup> MakeWSSetup(i16 slots, ui64 spinThresholdCycles = 0) {
        auto setup = MakeHolder<NActors::TActorSystemSetup>();
        setup->NodeId = 1;

        // Minimal basic pool for the scheduler to bind to
        NActors::TBasicExecutorPoolConfig basicCfg;
        basicCfg.PoolId = 0;
        basicCfg.PoolName = "scheduler";
        basicCfg.Threads = 1;
        basicCfg.MinThreadCount = 1;
        basicCfg.MaxThreadCount = 1;
        basicCfg.DefaultThreadCount = 1;
        setup->CpuManager.Basic.push_back(basicCfg);

        // WS pool as pool 1
        NActors::TWorkStealingPoolConfig wsCfg;
        wsCfg.PoolId = 1;
        wsCfg.PoolName = "bench-ws";
        wsCfg.MinSlotCount = slots;
        wsCfg.MaxSlotCount = slots;
        wsCfg.DefaultSlotCount = slots;
        if (spinThresholdCycles > 0) {
            wsCfg.WsConfig.SpinThresholdCycles = spinThresholdCycles;
        }
        if (GMinSpinThresholdCycles > 0) {
            wsCfg.WsConfig.MinSpinThresholdCycles = GMinSpinThresholdCycles;
        }
        setup->CpuManager.WorkStealing = NActors::TWorkStealingConfig{
            .Enabled = true,
            .Pools = {wsCfg},
        };

        setup->Scheduler.Reset(new NActors::TBasicSchedulerThread(NActors::TSchedulerConfig()));
        return setup;
    }

    // Pool id where benchmark actors should be registered
    ui32 BenchPoolId(const TString& poolType) {
        return (poolType == "ws") ? 1 : 0;
    }

    // -------------------------------------------------------------------
    // Ping-Pong actors
    // -------------------------------------------------------------------

    class TPingActor: public NActors::TActor<TPingActor> {
    public:
        TPingActor(NActors::TActorId peer, std::atomic<ui64>& counter, std::atomic<bool>& stop)
            : TActor(&TPingActor::StateFunc)
            , Peer_(peer)
            , Counter_(counter)
            , Stop_(stop)
        {
        }

        STFUNC(StateFunc) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvBenchPing, HandlePing);
                hFunc(TEvBenchPong, HandlePong);
            }
        }

    private:
        void HandlePing(TEvBenchPing::TPtr& /*ev*/) {
            Counter_.fetch_add(1, std::memory_order_relaxed);
            if (Stop_.load(std::memory_order_relaxed)) {
                return;
            }
            Send(Peer_, new TEvBenchPong());
        }

        void HandlePong(TEvBenchPong::TPtr& /*ev*/) {
            Counter_.fetch_add(1, std::memory_order_relaxed);
            if (Stop_.load(std::memory_order_relaxed)) {
                return;
            }
            Send(Peer_, new TEvBenchPing());
        }

        NActors::TActorId Peer_;
        std::atomic<ui64>& Counter_;
        std::atomic<bool>& Stop_;
    };

    // Lightweight actor that just accepts a peer later via SetPeer
    class TPongActor: public NActors::TActor<TPongActor> {
    public:
        TPongActor(std::atomic<ui64>& counter, std::atomic<bool>& stop)
            : TActor(&TPongActor::StateFunc)
            , Counter_(counter)
            , Stop_(stop)
        {
        }

        void SetPeer(NActors::TActorId peer) {
            Peer_ = peer;
        }

        STFUNC(StateFunc) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvBenchPing, HandlePing);
                hFunc(TEvBenchPong, HandlePong);
            }
        }

    private:
        void HandlePing(TEvBenchPing::TPtr& ev) {
            Counter_.fetch_add(1, std::memory_order_relaxed);
            if (Stop_.load(std::memory_order_relaxed)) {
                return;
            }
            Send(ev->Sender, new TEvBenchPong());
        }

        void HandlePong(TEvBenchPong::TPtr& /*ev*/) {
            Counter_.fetch_add(1, std::memory_order_relaxed);
            if (Stop_.load(std::memory_order_relaxed)) {
                return;
            }
            Send(Peer_, new TEvBenchPing());
        }

        NActors::TActorId Peer_;
        std::atomic<ui64>& Counter_;
        std::atomic<bool>& Stop_;
    };

    // -------------------------------------------------------------------
    // Star fan-out actors
    // -------------------------------------------------------------------

    class TStarReceiver: public NActors::TActor<TStarReceiver> {
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

    private:
        std::atomic<ui64>& Counter_;
    };

    class TStarSender: public NActors::TActor<TStarSender> {
    public:
        TStarSender(NActors::TActorId receiver, std::atomic<bool>& stop)
            : TActor(&TStarSender::StateFunc)
            , Receiver_(receiver)
            , Stop_(stop)
        {
        }

        STFUNC(StateFunc) {
            Y_UNUSED(ev);
            if (Stop_.load(std::memory_order_relaxed)) {
                return;
            }
            // Send a message to the receiver, then send another to self to keep going
            Send(Receiver_, new TEvBenchMsg());
            Send(SelfId(), new TEvBenchMsg());
        }

    private:
        NActors::TActorId Receiver_;
        std::atomic<bool>& Stop_;
    };

    // -------------------------------------------------------------------
    // Chain actors
    // -------------------------------------------------------------------

    class TChainActor: public NActors::TActor<TChainActor> {
    public:
        TChainActor(NActors::TActorId next, std::atomic<ui64>& counter, std::atomic<bool>& stop)
            : TActor(&TChainActor::StateFunc)
            , Next_(next)
            , Counter_(counter)
            , Stop_(stop)
        {
        }

        void SetNext(NActors::TActorId next) {
            Next_ = next;
        }

        STFUNC(StateFunc) {
            Y_UNUSED(ev);
            Counter_.fetch_add(1, std::memory_order_relaxed);
            if (Stop_.load(std::memory_order_relaxed)) {
                return;
            }
            Send(Next_, new TEvBenchMsg());
        }

    private:
        NActors::TActorId Next_;
        std::atomic<ui64>& Counter_;
        std::atomic<bool>& Stop_;
    };

    // -------------------------------------------------------------------
    // Benchmark result
    // -------------------------------------------------------------------

    double GetProcessCpuSeconds() {
        struct rusage ru;
        getrusage(RUSAGE_SELF, &ru);
        return ru.ru_utime.tv_sec + ru.ru_utime.tv_usec * 1e-6
             + ru.ru_stime.tv_sec + ru.ru_stime.tv_usec * 1e-6;
    }

    struct TBenchResult {
        TString PoolType;
        TString Scenario;
        ui32 Threads;
        ui32 ActorPairs;
        double OpsPerSec;
        double AvgLatencyUs;
        double CpuSeconds;    // total CPU time consumed (user + sys)
        double WallSeconds;   // wall-clock time

        void PrintCSV() const {
            double utilization = (WallSeconds > 0 && Threads > 0)
                ? (CpuSeconds / (WallSeconds * Threads) * 100.0) : 0;
            std::printf("%s,%s,%u,%u,%.0f,%.2f,%.2f,%.1f\n",
                        PoolType.c_str(), Scenario.c_str(), Threads, ActorPairs,
                        OpsPerSec, AvgLatencyUs, CpuSeconds, utilization);
        }
    };

    // -------------------------------------------------------------------
    // Benchmark runners
    // -------------------------------------------------------------------

    void ResetWsCounters() {
        auto* pool = NActors::NWorkStealing::TWSExecutorPool::LastCreated;
        if (pool) {
            pool->ResetCounters();
        }
    }

    void DumpWsCounters(const char* label) {
        auto* pool = NActors::NWorkStealing::TWSExecutorPool::LastCreated;
        if (pool) {
            pool->DumpCounters(label);
        }
    }

    TBenchResult RunPingPong(const TString& poolType, ui32 threads, ui32 pairs,
                             ui32 warmupSec, ui32 durationSec) {
        auto setup = (poolType == "ws") ? MakeWSSetup(threads, GSpinThresholdCycles) : MakeBasicSetup(threads);
        NActors::TActorSystem sys(setup);
        sys.Start();

        ui32 pool = BenchPoolId(poolType);

        std::atomic<ui64> counter{0};
        std::atomic<bool> stop{false};

        // Create pairs: each pair is a pong actor + ping actor
        for (ui32 i = 0; i < pairs; ++i) {
            auto* pong = new TPongActor(counter, stop);
            NActors::TActorId pongId = sys.Register(pong, NActors::TMailboxType::HTSwap, pool);

            auto* ping = new TPingActor(pongId, counter, stop);
            NActors::TActorId pingId = sys.Register(ping, NActors::TMailboxType::HTSwap, pool);

            pong->SetPeer(pingId);

            // Kick off ping-pong
            sys.Send(pingId, new TEvBenchPing());
        }

        // Warmup
        std::this_thread::sleep_for(std::chrono::seconds(warmupSec));
        counter.store(0, std::memory_order_relaxed);
        ResetWsCounters();

        // Measure
        double cpuBefore = GetProcessCpuSeconds();
        auto start = TClock::now();
        std::this_thread::sleep_for(std::chrono::seconds(durationSec));
        auto end = TClock::now();
        double cpuAfter = GetProcessCpuSeconds();

        stop.store(true, std::memory_order_relaxed);

        ui64 ops = counter.load(std::memory_order_relaxed);
        double elapsed = std::chrono::duration<double>(end - start).count();

        double opsPerSec = (elapsed > 0) ? (double)ops / elapsed : 0;
        double avgLatencyUs = (ops > 0) ? (elapsed * 1e6 / ops) : 0;

        char label[128];
        std::snprintf(label, sizeof(label), "%s/ping-pong t=%u p=%u", poolType.c_str(), threads, pairs);
        DumpWsCounters(label);

        // Allow actors to drain
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        sys.Stop();

        return TBenchResult{
            .PoolType = poolType,
            .Scenario = "ping-pong",
            .Threads = threads,
            .ActorPairs = pairs,
            .OpsPerSec = opsPerSec,
            .AvgLatencyUs = avgLatencyUs,
            .CpuSeconds = cpuAfter - cpuBefore,
            .WallSeconds = elapsed,
        };
    }

    TBenchResult RunStar(const TString& poolType, ui32 threads, ui32 senders,
                         ui32 warmupSec, ui32 durationSec) {
        auto setup = (poolType == "ws") ? MakeWSSetup(threads, GSpinThresholdCycles) : MakeBasicSetup(threads);
        NActors::TActorSystem sys(setup);
        sys.Start();

        ui32 pool = BenchPoolId(poolType);

        std::atomic<ui64> counter{0};
        std::atomic<bool> stop{false};

        // One receiver
        auto* receiver = new TStarReceiver(counter);
        NActors::TActorId receiverId = sys.Register(receiver, NActors::TMailboxType::HTSwap, pool);

        // N senders
        for (ui32 i = 0; i < senders; ++i) {
            auto* sender = new TStarSender(receiverId, stop);
            NActors::TActorId senderId = sys.Register(sender, NActors::TMailboxType::HTSwap, pool);
            // Kick off
            sys.Send(senderId, new TEvBenchMsg());
        }

        // Warmup
        std::this_thread::sleep_for(std::chrono::seconds(warmupSec));
        counter.store(0, std::memory_order_relaxed);
        ResetWsCounters();

        // Measure
        double cpuBefore = GetProcessCpuSeconds();
        auto start = TClock::now();
        std::this_thread::sleep_for(std::chrono::seconds(durationSec));
        auto end = TClock::now();
        double cpuAfter = GetProcessCpuSeconds();

        stop.store(true, std::memory_order_relaxed);

        ui64 ops = counter.load(std::memory_order_relaxed);
        double elapsed = std::chrono::duration<double>(end - start).count();

        double opsPerSec = (elapsed > 0) ? (double)ops / elapsed : 0;
        double avgLatencyUs = (ops > 0) ? (elapsed * 1e6 / ops) : 0;

        char starLabel[128];
        std::snprintf(starLabel, sizeof(starLabel), "%s/star t=%u p=%u", poolType.c_str(), threads, senders);
        DumpWsCounters(starLabel);

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        sys.Stop();

        return TBenchResult{
            .PoolType = poolType,
            .Scenario = "star",
            .Threads = threads,
            .ActorPairs = senders,
            .OpsPerSec = opsPerSec,
            .AvgLatencyUs = avgLatencyUs,
            .CpuSeconds = cpuAfter - cpuBefore,
            .WallSeconds = elapsed,
        };
    }

    TBenchResult RunChain(const TString& poolType, ui32 threads, ui32 chainLen,
                          ui32 warmupSec, ui32 durationSec) {
        auto setup = (poolType == "ws") ? MakeWSSetup(threads, GSpinThresholdCycles) : MakeBasicSetup(threads);
        NActors::TActorSystem sys(setup);
        sys.Start();

        ui32 pool = BenchPoolId(poolType);

        std::atomic<ui64> counter{0};
        std::atomic<bool> stop{false};

        // Build chain: last actor wraps around to first
        std::vector<NActors::TActorId> ids;
        std::vector<TChainActor*> actors;

        for (ui32 i = 0; i < chainLen; ++i) {
            // Next is unknown at creation; we'll wire up all actors after registration
            auto* actor = new TChainActor(NActors::TActorId(), counter, stop);
            actors.push_back(actor);
            NActors::TActorId id = sys.Register(actor, NActors::TMailboxType::HTSwap, pool);
            ids.push_back(id);
        }

        // Wire up: each actor points to the next, last wraps to first
        for (ui32 i = 0; i < chainLen; ++i) {
            NActors::TActorId next = ids[(i + 1) % chainLen];
            actors[i]->SetNext(next);
        }

        // Kick off
        sys.Send(ids[0], new TEvBenchMsg());

        // Warmup
        std::this_thread::sleep_for(std::chrono::seconds(warmupSec));
        counter.store(0, std::memory_order_relaxed);
        ResetWsCounters();

        // Measure
        double cpuBefore = GetProcessCpuSeconds();
        auto start = TClock::now();
        std::this_thread::sleep_for(std::chrono::seconds(durationSec));
        auto end = TClock::now();
        double cpuAfter = GetProcessCpuSeconds();

        stop.store(true, std::memory_order_relaxed);

        ui64 ops = counter.load(std::memory_order_relaxed);
        double elapsed = std::chrono::duration<double>(end - start).count();

        double opsPerSec = (elapsed > 0) ? (double)ops / elapsed : 0;
        double avgLatencyUs = (ops > 0) ? (elapsed * 1e6 / ops) : 0;

        char chainLabel[128];
        std::snprintf(chainLabel, sizeof(chainLabel), "%s/chain t=%u p=%u", poolType.c_str(), threads, chainLen);
        DumpWsCounters(chainLabel);

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        sys.Stop();

        return TBenchResult{
            .PoolType = poolType,
            .Scenario = "chain",
            .Threads = threads,
            .ActorPairs = chainLen,
            .OpsPerSec = opsPerSec,
            .AvgLatencyUs = avgLatencyUs,
            .CpuSeconds = cpuAfter - cpuBefore,
            .WallSeconds = elapsed,
        };
    }

} // anonymous namespace

int main(int argc, const char* argv[]) {
    NLastGetopt::TOpts opts;
    opts.SetTitle("Work-stealing vs Basic executor pool A/B benchmark");

    TString poolType = "both";
    opts.AddLongOption("pool-type", "Pool type: basic, ws, or both")
        .DefaultValue("both")
        .StoreResult(&poolType);

    TString scenario = "all";
    opts.AddLongOption("scenario", "Scenario: ping-pong, star, chain, or all")
        .DefaultValue("all")
        .StoreResult(&scenario);

    TString threadsList = "1,2,4,8";
    opts.AddLongOption("threads", "Comma-separated thread counts")
        .DefaultValue("1,2,4,8")
        .StoreResult(&threadsList);

    TString pairsList = "10,100";
    opts.AddLongOption("pairs", "Comma-separated actor pair counts (ping-pong/star/chain)")
        .DefaultValue("10,100")
        .StoreResult(&pairsList);

    ui32 durationSec = 5;
    opts.AddLongOption("duration", "Duration per scenario in seconds")
        .DefaultValue("5")
        .StoreResult(&durationSec);

    ui32 warmupSec = 1;
    opts.AddLongOption("warmup", "Warmup seconds before measurement")
        .DefaultValue("1")
        .StoreResult(&warmupSec);

    ui64 spinThreshold = 0;
    opts.AddLongOption("spin-threshold", "WS spin threshold in CPU cycles (0 = use default)")
        .DefaultValue("0")
        .StoreResult(&spinThreshold);

    ui64 minSpinThreshold = 0;
    opts.AddLongOption("min-spin-threshold", "WS min spin threshold in CPU cycles (0 = use default)")
        .DefaultValue("0")
        .StoreResult(&minSpinThreshold);

    NLastGetopt::TOptsParseResult res(&opts, argc, argv);

    GSpinThresholdCycles = spinThreshold;
    GMinSpinThresholdCycles = minSpinThreshold;

    auto threads = ParseList(threadsList);
    auto pairs = ParseList(pairsList);

    std::vector<TString> poolTypes;
    if (poolType == "both") {
        poolTypes = {"basic", "ws"};
    } else {
        poolTypes = {poolType};
    }

    std::vector<TString> scenarios;
    if (scenario == "all") {
        scenarios = {"ping-pong", "star", "chain"};
    } else {
        scenarios = {scenario};
    }

    // CSV header
    std::printf("pool_type,scenario,threads,actor_pairs,ops_per_sec,avg_latency_us,cpu_seconds,cpu_util_pct\n");

    for (const auto& sc : scenarios) {
        for (ui32 t : threads) {
            for (ui32 p : pairs) {
                for (const auto& pt : poolTypes) {
                    TBenchResult result;
                    if (sc == "ping-pong") {
                        result = RunPingPong(pt, t, p, warmupSec, durationSec);
                    } else if (sc == "star") {
                        result = RunStar(pt, t, p, warmupSec, durationSec);
                    } else if (sc == "chain") {
                        result = RunChain(pt, t, p, warmupSec, durationSec);
                    }
                    result.PrintCSV();
                    std::fflush(stdout);
                }
            }
        }
    }

    return 0;
}
