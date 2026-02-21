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
#include <random>
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

    // Burn CPU for a calibrated number of iterations (~1 iter ≈ 1 ns on modern CPUs)
    Y_NO_INLINE void BusyWork(ui64 iterations) {
        volatile ui64 x = 0;
        for (ui64 i = 0; i < iterations; ++i) {
            x += i * 7 + 1;
        }
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
    // Reincarnation actors
    // -------------------------------------------------------------------

    class TReincarnationActor: public NActors::TActor<TReincarnationActor> {
    public:
        TReincarnationActor(std::atomic<ui64>& counter, std::atomic<bool>& stop)
            : TActor(&TReincarnationActor::StateFunc)
            , Counter_(counter)
            , Stop_(stop)
        {
        }

        STFUNC(StateFunc) {
            Y_UNUSED(ev);
            Counter_.fetch_add(1, std::memory_order_relaxed);
            if (Stop_.load(std::memory_order_relaxed)) {
                return;
            }
            // Register a fresh actor, send it a message, then die
            auto* next = new TReincarnationActor(Counter_, Stop_);
            NActors::TActorId nextId = Register(next);
            Send(nextId, new TEvBenchMsg());
            PassAway();
        }

    private:
        std::atomic<ui64>& Counter_;
        std::atomic<bool>& Stop_;
    };

    // -------------------------------------------------------------------
    // Pipeline scenario: N independent linear pipelines, each with S stages
    //
    // Tests scheduling of dependent sequential processing stages where
    // multiple stages may be hot simultaneously on the same slot.
    // Unlike chain (circular ring with one message), pipeline has
    // continuous injection — multiple items in-flight per pipeline.
    //
    //   Source → Stage[0] → Stage[1] → ... → Stage[S-1] → Sink
    //     ↻ (self-loop)                                   (counter++)
    //
    // Each stage does configurable BusyWork before forwarding.
    // Source self-loops to keep the pipeline fed continuously.
    // -------------------------------------------------------------------

    class TPipelineSource: public NActors::TActor<TPipelineSource> {
    public:
        TPipelineSource(NActors::TActorId firstStage, std::atomic<bool>& stop)
            : TActor(&TPipelineSource::StateFunc)
            , FirstStage_(firstStage)
            , Stop_(stop)
        {}

        STFUNC(StateFunc) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvBenchMsg, Handle);
            }
        }

    private:
        void Handle(TEvBenchMsg::TPtr& /*ev*/) {
            if (Stop_.load(std::memory_order_relaxed)) {
                return;
            }
            Send(FirstStage_, new TEvBenchMsg());
            Send(SelfId(), new TEvBenchMsg());
        }

        NActors::TActorId FirstStage_;
        std::atomic<bool>& Stop_;
    };

    class TPipelineStage: public NActors::TActor<TPipelineStage> {
    public:
        TPipelineStage(NActors::TActorId next, ui64 workIters)
            : TActor(&TPipelineStage::StateFunc)
            , Next_(next)
            , WorkIters_(workIters)
        {}

        STFUNC(StateFunc) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvBenchMsg, Handle);
            }
        }

    private:
        void Handle(TEvBenchMsg::TPtr& /*ev*/) {
            BusyWork(WorkIters_);
            Send(Next_, new TEvBenchMsg());
        }

        NActors::TActorId Next_;
        ui64 WorkIters_;
    };

    class TPipelineSink: public NActors::TActor<TPipelineSink> {
    public:
        TPipelineSink(std::atomic<ui64>& counter)
            : TActor(&TPipelineSink::StateFunc)
            , Counter_(counter)
        {}

        STFUNC(StateFunc) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvBenchMsg, Handle);
            }
        }

    private:
        void Handle(TEvBenchMsg::TPtr& /*ev*/) {
            Counter_.fetch_add(1, std::memory_order_relaxed);
        }

        std::atomic<ui64>& Counter_;
    };

    // -------------------------------------------------------------------
    // Storage-node scenario: models YDB VDisk actor patterns under load
    //
    // Actors:
    //   - TStorageClient: sends put/get requests to random VDisk skeletons
    //   - TVDiskSkeleton: central coordinator, routes puts to logger,
    //                     spawns query actors for gets, handles compaction
    //   - TLoggerActor: receives log writes, does brief work, responds
    //   - TQueryActor: spawned per get, does CPU work, responds to client
    //   - TCompactionActor: periodic CPU-intensive background work
    //
    // This approximates the patterns in a YDB storage node where:
    //   - The Skeleton is a fan-in serialization point
    //   - Gets spawn concurrent child actors (like real VGet query actors)
    //   - Puts go through a serial pipeline (Skeleton → Logger → Skeleton)
    //   - Compaction creates periodic bursts of background CPU work
    // -------------------------------------------------------------------

    struct TEvClientRequest: NActors::TEventLocal<TEvClientRequest, 10701> {
        bool IsPut;
        TEvClientRequest(bool isPut) : IsPut(isPut) {}
    };

    struct TEvClientResponse: NActors::TEventLocal<TEvClientResponse, 10702> {};

    struct TEvLogWrite: NActors::TEventLocal<TEvLogWrite, 10703> {
        NActors::TActorId Client; // original client to respond to
        TEvLogWrite(NActors::TActorId client) : Client(client) {}
    };

    struct TEvLogResult: NActors::TEventLocal<TEvLogResult, 10704> {
        NActors::TActorId Client;
        TEvLogResult(NActors::TActorId client) : Client(client) {}
    };

    struct TEvCompactionTick: NActors::TEventLocal<TEvCompactionTick, 10705> {};
    struct TEvCompactionTask: NActors::TEventLocal<TEvCompactionTask, 10706> {};
    struct TEvCompactionDone: NActors::TEventLocal<TEvCompactionDone, 10707> {};

    // Parameters for storage-node scenario
    struct TStorageNodeParams {
        ui32 VDisks = 8;
        ui32 Clients = 32;
        double PutRatio = 0.5;
        ui64 PutWorkIters = 500;        // ~0.5 us: log record creation
        ui64 LogWorkIters = 300;         // ~0.3 us: log serialization
        ui64 GetWorkIters = 5000;        // ~5 us: LSM tree lookup + read
        ui64 CompactionWorkIters = 50000; // ~50 us: merge/sort burst
        ui32 CompactionPeriodMs = 50;    // how often compaction fires
    };

    // --- TQueryActor: spawned per get request, does CPU work, responds ---

    class TQueryActor: public NActors::TActorBootstrapped<TQueryActor> {
    public:
        TQueryActor(NActors::TActorId client, ui64 workIters)
            : Client_(client)
            , WorkIters_(workIters)
        {}

        void Bootstrap() {
            BusyWork(WorkIters_);
            Send(Client_, new TEvClientResponse());
            PassAway();
        }

    private:
        NActors::TActorId Client_;
        ui64 WorkIters_;
    };

    // --- TLoggerActor: one per VDisk, serializes log writes ---

    class TLoggerActor: public NActors::TActor<TLoggerActor> {
    public:
        TLoggerActor(ui64 workIters)
            : TActor(&TLoggerActor::StateFunc)
            , WorkIters_(workIters)
        {}

        STFUNC(StateFunc) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvLogWrite, HandleLogWrite);
            }
        }

    private:
        void HandleLogWrite(TEvLogWrite::TPtr& ev) {
            BusyWork(WorkIters_);
            Send(ev->Sender, new TEvLogResult(ev->Get()->Client));
        }

        ui64 WorkIters_;
    };

    // --- TCompactionActor: periodic background CPU bursts ---

    class TCompactionActor: public NActors::TActor<TCompactionActor> {
    public:
        TCompactionActor(ui64 workIters)
            : TActor(&TCompactionActor::StateFunc)
            , WorkIters_(workIters)
        {}

        STFUNC(StateFunc) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvCompactionTask, HandleTask);
            }
        }

    private:
        void HandleTask(TEvCompactionTask::TPtr& ev) {
            BusyWork(WorkIters_);
            Send(ev->Sender, new TEvCompactionDone());
        }

        ui64 WorkIters_;
    };

    // --- TVDiskSkeleton: central coordinator ---

    class TVDiskSkeleton: public NActors::TActor<TVDiskSkeleton> {
    public:
        TVDiskSkeleton(NActors::TActorId loggerId,
                       NActors::TActorId compactionId,
                       std::atomic<ui64>& counter,
                       std::atomic<bool>& stop,
                       const TStorageNodeParams& params,
                       ui32 poolId)
            : TActor(&TVDiskSkeleton::StateFunc)
            , LoggerId_(loggerId)
            , CompactionId_(compactionId)
            , Counter_(counter)
            , Stop_(stop)
            , Params_(params)
            , PoolId_(poolId)
        {}

        STFUNC(StateFunc) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvClientRequest, HandleClientRequest);
                hFunc(TEvLogResult, HandleLogResult);
                hFunc(TEvCompactionTick, HandleCompactionTick);
                hFunc(TEvCompactionDone, HandleCompactionDone);
            }
        }

    private:
        void HandleClientRequest(TEvClientRequest::TPtr& ev) {
            if (ev->Get()->IsPut) {
                // Put: small CPU work (validation/LSN allocation), then forward to logger
                BusyWork(Params_.PutWorkIters);
                Send(LoggerId_, new TEvLogWrite(ev->Sender));
            } else {
                // Get: spawn a query actor that does CPU work and responds directly
                auto* query = new TQueryActor(ev->Sender, Params_.GetWorkIters);
                Register(query, NActors::TMailboxType::HTSwap, PoolId_);
            }
        }

        void HandleLogResult(TEvLogResult::TPtr& ev) {
            // Log write completed, respond to original client
            Counter_.fetch_add(1, std::memory_order_relaxed);
            if (!Stop_.load(std::memory_order_relaxed)) {
                Send(ev->Get()->Client, new TEvClientResponse());
            }
        }

        void HandleCompactionTick(TEvCompactionTick::TPtr& /*ev*/) {
            if (!Stop_.load(std::memory_order_relaxed)) {
                Send(CompactionId_, new TEvCompactionTask());
            }
        }

        void HandleCompactionDone(TEvCompactionDone::TPtr& /*ev*/) {
            // Compaction finished, schedule next tick
            if (!Stop_.load(std::memory_order_relaxed)) {
                Schedule(TDuration::MilliSeconds(Params_.CompactionPeriodMs),
                         new TEvCompactionTick());
            }
        }

        NActors::TActorId LoggerId_;
        NActors::TActorId CompactionId_;
        std::atomic<ui64>& Counter_;
        std::atomic<bool>& Stop_;
        TStorageNodeParams Params_;
        ui32 PoolId_;
    };

    // --- TStorageClient: sends put/get requests to random VDisks ---

    class TStorageClient: public NActors::TActor<TStorageClient> {
    public:
        TStorageClient(std::vector<NActors::TActorId> vdisks,
                       std::atomic<ui64>& counter,
                       std::atomic<bool>& stop,
                       double putRatio,
                       ui32 seed)
            : TActor(&TStorageClient::StateFunc)
            , VDisks_(std::move(vdisks))
            , Counter_(counter)
            , Stop_(stop)
            , PutThreshold_(static_cast<ui32>(putRatio * 0xFFFFFFFF))
            , Rng_(seed)
        {}

        STFUNC(StateFunc) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvClientResponse, HandleResponse);
                hFunc(TEvBenchMsg, HandleKick);
            }
        }

    private:
        void HandleResponse(TEvClientResponse::TPtr& /*ev*/) {
            Counter_.fetch_add(1, std::memory_order_relaxed);
            SendNextRequest();
        }

        void HandleKick(TEvBenchMsg::TPtr& /*ev*/) {
            SendNextRequest();
        }

        void SendNextRequest() {
            if (Stop_.load(std::memory_order_relaxed)) {
                return;
            }
            ui32 vdiskIdx = Rng_() % VDisks_.size();
            bool isPut = (Rng_() < PutThreshold_);
            Send(VDisks_[vdiskIdx], new TEvClientRequest(isPut));
        }

        std::vector<NActors::TActorId> VDisks_;
        std::atomic<ui64>& Counter_;
        std::atomic<bool>& Stop_;
        ui32 PutThreshold_;
        std::minstd_rand Rng_;
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

    TBenchResult RunReincarnation(const TString& poolType, ui32 threads, ui32 actors,
                                  ui32 warmupSec, ui32 durationSec) {
        auto setup = (poolType == "ws") ? MakeWSSetup(threads, GSpinThresholdCycles) : MakeBasicSetup(threads);
        NActors::TActorSystem sys(setup);
        sys.Start();

        ui32 pool = BenchPoolId(poolType);

        std::atomic<ui64> counter{0};
        std::atomic<bool> stop{false};

        // Seed N independent reincarnation chains
        for (ui32 i = 0; i < actors; ++i) {
            auto* actor = new TReincarnationActor(counter, stop);
            NActors::TActorId id = sys.Register(actor, NActors::TMailboxType::HTSwap, pool);
            sys.Send(id, new TEvBenchMsg());
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
        std::snprintf(label, sizeof(label), "%s/reincarnation t=%u p=%u", poolType.c_str(), threads, actors);
        DumpWsCounters(label);

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        sys.Stop();

        return TBenchResult{
            .PoolType = poolType,
            .Scenario = "reincarnation",
            .Threads = threads,
            .ActorPairs = actors,
            .OpsPerSec = opsPerSec,
            .AvgLatencyUs = avgLatencyUs,
            .CpuSeconds = cpuAfter - cpuBefore,
            .WallSeconds = elapsed,
        };
    }

    TBenchResult RunPipeline(const TString& poolType, ui32 threads, ui32 pipelines,
                             ui32 stages, ui64 stageWork,
                             ui32 warmupSec, ui32 durationSec) {
        auto setup = (poolType == "ws") ? MakeWSSetup(threads, GSpinThresholdCycles) : MakeBasicSetup(threads);
        NActors::TActorSystem sys(setup);
        sys.Start();

        ui32 pool = BenchPoolId(poolType);

        std::atomic<ui64> counter{0};
        std::atomic<bool> stop{false};

        for (ui32 i = 0; i < pipelines; ++i) {
            // Build pipeline backwards: sink → stages → source
            auto* sink = new TPipelineSink(counter);
            NActors::TActorId nextId = sys.Register(sink, NActors::TMailboxType::HTSwap, pool);

            for (ui32 s = 0; s < stages; ++s) {
                auto* stage = new TPipelineStage(nextId, stageWork);
                nextId = sys.Register(stage, NActors::TMailboxType::HTSwap, pool);
            }

            auto* source = new TPipelineSource(nextId, stop);
            NActors::TActorId sourceId = sys.Register(source, NActors::TMailboxType::HTSwap, pool);
            sys.Send(sourceId, new TEvBenchMsg());
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
        std::snprintf(label, sizeof(label), "%s/pipeline t=%u p=%u s=%u w=%lu",
                      poolType.c_str(), threads, pipelines, stages, stageWork);
        DumpWsCounters(label);

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        sys.Stop();

        return TBenchResult{
            .PoolType = poolType,
            .Scenario = "pipeline",
            .Threads = threads,
            .ActorPairs = pipelines,
            .OpsPerSec = opsPerSec,
            .AvgLatencyUs = avgLatencyUs,
            .CpuSeconds = cpuAfter - cpuBefore,
            .WallSeconds = elapsed,
        };
    }

    TBenchResult RunStorageNode(const TString& poolType, ui32 threads,
                                const TStorageNodeParams& params,
                                ui32 warmupSec, ui32 durationSec) {
        auto setup = (poolType == "ws")
            ? MakeWSSetup(threads, GSpinThresholdCycles)
            : MakeBasicSetup(threads);
        NActors::TActorSystem sys(setup);
        sys.Start();

        ui32 pool = BenchPoolId(poolType);

        std::atomic<ui64> counter{0};
        std::atomic<bool> stop{false};

        // Create per-VDisk actor groups
        std::vector<NActors::TActorId> skeletonIds;
        for (ui32 i = 0; i < params.VDisks; ++i) {
            // Logger for this VDisk
            auto* logger = new TLoggerActor(params.LogWorkIters);
            auto loggerId = sys.Register(logger, NActors::TMailboxType::HTSwap, pool);

            // Compaction worker for this VDisk
            auto* compaction = new TCompactionActor(params.CompactionWorkIters);
            auto compactionId = sys.Register(compaction, NActors::TMailboxType::HTSwap, pool);

            // VDisk skeleton
            auto* skeleton = new TVDiskSkeleton(
                loggerId, compactionId, counter, stop, params, pool);
            auto skeletonId = sys.Register(skeleton, NActors::TMailboxType::HTSwap, pool);
            skeletonIds.push_back(skeletonId);

            // Kick off compaction cycle
            sys.Send(skeletonId, new TEvCompactionTick());
        }

        // Create client actors
        for (ui32 i = 0; i < params.Clients; ++i) {
            auto* client = new TStorageClient(
                skeletonIds, counter, stop, params.PutRatio, 42 + i);
            auto clientId = sys.Register(client, NActors::TMailboxType::HTSwap, pool);
            // Kick off the request loop
            sys.Send(clientId, new TEvBenchMsg());
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
        std::snprintf(label, sizeof(label), "%s/storage-node t=%u v=%u c=%u",
                      poolType.c_str(), threads, params.VDisks, params.Clients);
        DumpWsCounters(label);

        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        sys.Stop();

        return TBenchResult{
            .PoolType = poolType,
            .Scenario = "storage-node",
            .Threads = threads,
            .ActorPairs = params.VDisks,
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
    opts.AddLongOption("scenario", "Scenario: ping-pong, star, chain, reincarnation, pipeline, storage-node, or all")
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

    // Storage-node scenario parameters
    ui32 vdisks = 8;
    opts.AddLongOption("vdisks", "Number of VDisk actor groups (storage-node scenario)")
        .DefaultValue("8")
        .StoreResult(&vdisks);

    ui32 clients = 32;
    opts.AddLongOption("clients", "Number of client actors (storage-node scenario)")
        .DefaultValue("32")
        .StoreResult(&clients);

    double putRatio = 0.5;
    opts.AddLongOption("put-ratio", "Fraction of requests that are puts vs gets (0.0-1.0)")
        .DefaultValue("0.5")
        .StoreResult(&putRatio);

    ui64 putWorkIters = 500;
    opts.AddLongOption("put-work", "CPU iterations per put in skeleton (~1 iter = 1 ns)")
        .DefaultValue("500")
        .StoreResult(&putWorkIters);

    ui64 logWorkIters = 300;
    opts.AddLongOption("log-work", "CPU iterations per log write in logger")
        .DefaultValue("300")
        .StoreResult(&logWorkIters);

    ui64 getWorkIters = 5000;
    opts.AddLongOption("get-work", "CPU iterations per get in query actor")
        .DefaultValue("5000")
        .StoreResult(&getWorkIters);

    ui64 compactionWorkIters = 50000;
    opts.AddLongOption("compaction-work", "CPU iterations per compaction burst")
        .DefaultValue("50000")
        .StoreResult(&compactionWorkIters);

    ui32 compactionPeriodMs = 50;
    opts.AddLongOption("compaction-period", "Compaction trigger interval in ms")
        .DefaultValue("50")
        .StoreResult(&compactionPeriodMs);

    // Pipeline scenario parameters
    ui32 pipelineStages = 4;
    opts.AddLongOption("stages", "Number of processing stages per pipeline")
        .DefaultValue("4")
        .StoreResult(&pipelineStages);

    ui64 stageWorkIters = 100;
    opts.AddLongOption("stage-work", "CPU iterations per pipeline stage (~1 iter = 1 ns)")
        .DefaultValue("100")
        .StoreResult(&stageWorkIters);

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

    TStorageNodeParams storageParams;
    storageParams.VDisks = vdisks;
    storageParams.Clients = clients;
    storageParams.PutRatio = putRatio;
    storageParams.PutWorkIters = putWorkIters;
    storageParams.LogWorkIters = logWorkIters;
    storageParams.GetWorkIters = getWorkIters;
    storageParams.CompactionWorkIters = compactionWorkIters;
    storageParams.CompactionPeriodMs = compactionPeriodMs;

    std::vector<TString> scenarios;
    if (scenario == "all") {
        scenarios = {"ping-pong", "star", "chain", "reincarnation", "pipeline", "storage-node"};
    } else {
        scenarios = {scenario};
    }

    // CSV header
    std::printf("pool_type,scenario,threads,actor_pairs,ops_per_sec,avg_latency_us,cpu_seconds,cpu_util_pct\n");

    for (const auto& sc : scenarios) {
        for (ui32 t : threads) {
            if (sc == "storage-node") {
                // Storage-node uses vdisks/clients instead of pairs
                for (const auto& pt : poolTypes) {
                    auto result = RunStorageNode(pt, t, storageParams, warmupSec, durationSec);
                    result.PrintCSV();
                    std::fflush(stdout);
                }
            } else if (sc == "pipeline") {
                // Pipeline uses pairs as pipeline count, plus stages/stage-work
                for (ui32 p : pairs) {
                    for (const auto& pt : poolTypes) {
                        auto result = RunPipeline(pt, t, p, pipelineStages, stageWorkIters,
                                                  warmupSec, durationSec);
                        result.PrintCSV();
                        std::fflush(stdout);
                    }
                }
            } else {
                for (ui32 p : pairs) {
                    for (const auto& pt : poolTypes) {
                        TBenchResult result;
                        if (sc == "ping-pong") {
                            result = RunPingPong(pt, t, p, warmupSec, durationSec);
                        } else if (sc == "star") {
                            result = RunStar(pt, t, p, warmupSec, durationSec);
                        } else if (sc == "chain") {
                            result = RunChain(pt, t, p, warmupSec, durationSec);
                        } else if (sc == "reincarnation") {
                            result = RunReincarnation(pt, t, p, warmupSec, durationSec);
                        }
                        result.PrintCSV();
                        std::fflush(stdout);
                    }
                }
            }
        }
    }

    return 0;
}
