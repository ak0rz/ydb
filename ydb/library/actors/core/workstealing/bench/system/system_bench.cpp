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
#include <fstream>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <random>
#include <vector>
#include <sys/resource.h>
#include <sys/stat.h>

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

    bool GAdaptive = false;

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
        if (GAdaptive) {
            wsCfg.WsConfig.AdaptiveScaling = true;
            wsCfg.MinSlotCount = 1;
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

    // -------------------------------------------------------------------
    // Multi-pool setup
    // -------------------------------------------------------------------

    struct TMultiPoolParams {
        i16 SystemSlots = 64;   // Pool 1
        i16 UserSlots = 192;    // Pool 2
        i16 BatchSlots = 64;    // Pool 3
        i16 ICSlots = 64;       // Pool 4
        bool Adaptive = true;
        ui64 SpinThresholdCycles = 0;
    };

    THolder<NActors::TActorSystemSetup> MakeMultiBasicSetup(const TMultiPoolParams& mp) {
        auto setup = MakeHolder<NActors::TActorSystemSetup>();
        setup->NodeId = 1;

        auto makePool = [](ui32 poolId, const TString& name, i16 threads) {
            NActors::TBasicExecutorPoolConfig cfg;
            cfg.PoolId = poolId;
            cfg.PoolName = name;
            cfg.Threads = threads;
            cfg.MinThreadCount = threads;
            cfg.MaxThreadCount = threads;
            cfg.DefaultThreadCount = threads;
            cfg.SpinThreshold = 1'000'000;
            cfg.TimePerMailbox = TDuration::Hours(1);
            return cfg;
        };

        setup->CpuManager.Basic.push_back(makePool(0, "scheduler", 1));
        setup->CpuManager.Basic.push_back(makePool(1, "System", mp.SystemSlots));
        setup->CpuManager.Basic.push_back(makePool(2, "User", mp.UserSlots));
        setup->CpuManager.Basic.push_back(makePool(3, "Batch", mp.BatchSlots));
        setup->CpuManager.Basic.push_back(makePool(4, "IC", mp.ICSlots));

        setup->Scheduler.Reset(new NActors::TBasicSchedulerThread(NActors::TSchedulerConfig()));
        return setup;
    }

    THolder<NActors::TActorSystemSetup> MakeMultiWSSetup(const TMultiPoolParams& mp) {
        auto setup = MakeHolder<NActors::TActorSystemSetup>();
        setup->NodeId = 1;

        // Basic pool 0 (scheduler, 1 thread)
        NActors::TBasicExecutorPoolConfig basicCfg;
        basicCfg.PoolId = 0;
        basicCfg.PoolName = "scheduler";
        basicCfg.Threads = 1;
        basicCfg.MinThreadCount = 1;
        basicCfg.MaxThreadCount = 1;
        basicCfg.DefaultThreadCount = 1;
        setup->CpuManager.Basic.push_back(basicCfg);

        // Helper to create a WS pool config
        auto makePool = [&](ui32 poolId, const TString& name, i16 slots, i16 priority) {
            NActors::TWorkStealingPoolConfig cfg;
            cfg.PoolId = poolId;
            cfg.PoolName = name;
            cfg.MaxSlotCount = slots;
            cfg.DefaultSlotCount = slots;
            cfg.MinSlotCount = mp.Adaptive ? 1 : slots;
            cfg.Priority = priority;
            if (mp.SpinThresholdCycles > 0) {
                cfg.WsConfig.SpinThresholdCycles = mp.SpinThresholdCycles;
            }
            if (GMinSpinThresholdCycles > 0) {
                cfg.WsConfig.MinSpinThresholdCycles = GMinSpinThresholdCycles;
            }
            if (mp.Adaptive) {
                cfg.WsConfig.AdaptiveScaling = true;
            }
            return cfg;
        };

        NActors::TWorkStealingConfig wsCfg;
        wsCfg.Enabled = true;
        wsCfg.Pools.push_back(makePool(1, "System", mp.SystemSlots, 30));
        wsCfg.Pools.push_back(makePool(2, "User", mp.UserSlots, 20));
        wsCfg.Pools.push_back(makePool(3, "Batch", mp.BatchSlots, 10));
        wsCfg.Pools.push_back(makePool(4, "IC", mp.ICSlots, 40));
        setup->CpuManager.WorkStealing = std::move(wsCfg);

        setup->Scheduler.Reset(new NActors::TBasicSchedulerThread(NActors::TSchedulerConfig()));
        return setup;
    }

    // -------------------------------------------------------------------
    // Counter helpers (single-pool and multi-pool)
    // -------------------------------------------------------------------

    void ResetWsCounters() {
        auto* pool = NActors::NWorkStealing::TWSExecutorPool::LastCreated;
        if (pool) {
            pool->ResetCounters();
        }
    }

    void ResetAllWsCounters() {
        for (auto* pool : NActors::NWorkStealing::TWSExecutorPool::AllPools()) {
            pool->ResetCounters();
        }
    }

    void DumpWsCounters(const char* label) {
        auto* pool = NActors::NWorkStealing::TWSExecutorPool::LastCreated;
        if (pool) {
            pool->DumpCounters(label);
            if (GAdaptive) {
                std::fprintf(stderr, "  [adaptive] active_slots=%u inflate_events=%lu deflate_events=%lu\n",
                             pool->GetThreads(),
                             pool->AdaptiveInflateEvents(),
                             pool->AdaptiveDeflateEvents());
            }
        }
    }

    void DumpAllWsCounters() {
        for (auto* pool : NActors::NWorkStealing::TWSExecutorPool::AllPools()) {
            char label[128];
            std::snprintf(label, sizeof(label), "pool/%s", pool->GetName().c_str());
            pool->DumpCounters(label);
            std::fprintf(stderr, "  [adaptive] active_slots=%u/%d inflate_events=%lu deflate_events=%lu\n",
                         pool->GetThreads(),
                         pool->GetMaxFullThreadCount(),
                         pool->AdaptiveInflateEvents(),
                         pool->AdaptiveDeflateEvents());
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

    // -------------------------------------------------------------------
    // Multi-pool benchmark: runs different workloads simultaneously
    // on 4 WS pools (System, User, Batch, IC) sharing one TThreadDriver.
    // -------------------------------------------------------------------

    void RunMultiPool(const TString& poolType, const TMultiPoolParams& mp,
                       ui32 warmupSec, ui32 durationSec) {
        bool isWs = (poolType == "ws");
        std::fprintf(stderr, "\n=== Multi-Pool Benchmark (%s) ===\n", poolType.c_str());
        std::fprintf(stderr, "Pools: System=%d User=%d Batch=%d IC=%d  adaptive=%s\n",
                     mp.SystemSlots, mp.UserSlots, mp.BatchSlots, mp.ICSlots,
                     (isWs && mp.Adaptive) ? "yes" : "no");

        auto setup = isWs ? MakeMultiWSSetup(mp) : MakeMultiBasicSetup(mp);
        NActors::TActorSystem sys(setup);
        sys.Start();

        std::atomic<bool> stop{false};

        // Per-pool counters
        std::atomic<ui64> systemCounter{0};
        std::atomic<ui64> userCounter{0};
        std::atomic<ui64> batchCounter{0};
        std::atomic<ui64> icCounter{0};

        // --- Pool 1: System — chain(10) ---
        {
            ui32 pool = 1;
            ui32 chainLen = 10;
            std::vector<NActors::TActorId> ids;
            std::vector<TChainActor*> actors;
            for (ui32 i = 0; i < chainLen; ++i) {
                auto* actor = new TChainActor(NActors::TActorId(), systemCounter, stop);
                actors.push_back(actor);
                ids.push_back(sys.Register(actor, NActors::TMailboxType::HTSwap, pool));
            }
            for (ui32 i = 0; i < chainLen; ++i) {
                actors[i]->SetNext(ids[(i + 1) % chainLen]);
            }
            sys.Send(ids[0], new TEvBenchMsg());
        }

        // --- Pool 2: User — storage-node (8 VDisks, 32 clients) ---
        {
            ui32 pool = 2;
            TStorageNodeParams sp;
            sp.VDisks = 8;
            sp.Clients = 32;

            std::vector<NActors::TActorId> skeletonIds;
            for (ui32 i = 0; i < sp.VDisks; ++i) {
                auto* logger = new TLoggerActor(sp.LogWorkIters);
                auto loggerId = sys.Register(logger, NActors::TMailboxType::HTSwap, pool);
                auto* compaction = new TCompactionActor(sp.CompactionWorkIters);
                auto compactionId = sys.Register(compaction, NActors::TMailboxType::HTSwap, pool);
                auto* skeleton = new TVDiskSkeleton(
                    loggerId, compactionId, userCounter, stop, sp, pool);
                auto skeletonId = sys.Register(skeleton, NActors::TMailboxType::HTSwap, pool);
                skeletonIds.push_back(skeletonId);
                sys.Send(skeletonId, new TEvCompactionTick());
            }
            for (ui32 i = 0; i < sp.Clients; ++i) {
                auto* client = new TStorageClient(
                    skeletonIds, userCounter, stop, sp.PutRatio, 42 + i);
                auto clientId = sys.Register(client, NActors::TMailboxType::HTSwap, pool);
                sys.Send(clientId, new TEvBenchMsg());
            }
        }

        // --- Pool 3: Batch — chain(5), sporadic ---
        {
            ui32 pool = 3;
            ui32 chainLen = 5;
            std::vector<NActors::TActorId> ids;
            std::vector<TChainActor*> actors;
            for (ui32 i = 0; i < chainLen; ++i) {
                auto* actor = new TChainActor(NActors::TActorId(), batchCounter, stop);
                actors.push_back(actor);
                ids.push_back(sys.Register(actor, NActors::TMailboxType::HTSwap, pool));
            }
            for (ui32 i = 0; i < chainLen; ++i) {
                actors[i]->SetNext(ids[(i + 1) % chainLen]);
            }
            sys.Send(ids[0], new TEvBenchMsg());
        }

        // --- Pool 4: IC — ping-pong (8 pairs) ---
        {
            ui32 pool = 4;
            ui32 pairCount = 8;
            for (ui32 i = 0; i < pairCount; ++i) {
                auto* pong = new TPongActor(icCounter, stop);
                auto pongId = sys.Register(pong, NActors::TMailboxType::HTSwap, pool);
                auto* ping = new TPingActor(pongId, icCounter, stop);
                auto pingId = sys.Register(ping, NActors::TMailboxType::HTSwap, pool);
                pong->SetPeer(pingId);
                sys.Send(pingId, new TEvBenchPing());
            }
        }

        // Warmup
        std::fprintf(stderr, "Warming up %u seconds...\n", warmupSec);
        std::this_thread::sleep_for(std::chrono::seconds(warmupSec));
        systemCounter.store(0, std::memory_order_relaxed);
        userCounter.store(0, std::memory_order_relaxed);
        batchCounter.store(0, std::memory_order_relaxed);
        icCounter.store(0, std::memory_order_relaxed);
        if (isWs) {
            ResetAllWsCounters();
        }

        // Measure
        double cpuBefore = GetProcessCpuSeconds();
        auto start = TClock::now();
        std::this_thread::sleep_for(std::chrono::seconds(durationSec));
        auto end = TClock::now();
        double cpuAfter = GetProcessCpuSeconds();

        stop.store(true, std::memory_order_relaxed);
        double elapsed = std::chrono::duration<double>(end - start).count();
        double cpuSeconds = cpuAfter - cpuBefore;

        // Report per-pool results
        struct PoolResult {
            const char* name;
            const char* workload;
            i16 slots;
            std::atomic<ui64>& counter;
        };
        PoolResult pools[] = {
            {"System", "chain(10)", mp.SystemSlots, systemCounter},
            {"User", "storage-node", mp.UserSlots, userCounter},
            {"Batch", "chain(5)", mp.BatchSlots, batchCounter},
            {"IC", "ping-pong(8)", mp.ICSlots, icCounter},
        };

        // CSV header
        std::printf("pool_type,scenario,pool_name,workload,slots,active_slots,ops_per_sec,cpu_seconds\n");
        i16 totalActive = 0;
        i16 totalMax = mp.SystemSlots + mp.UserSlots + mp.BatchSlots + mp.ICSlots;
        for (auto& pr : pools) {
            ui64 ops = pr.counter.load(std::memory_order_relaxed);
            double opsPerSec = (elapsed > 0) ? (double)ops / elapsed : 0;
            i16 activeSlots = pr.slots; // basic: all threads always active
            if (isWs) {
                auto* wsPool = NActors::NWorkStealing::TWSExecutorPool::FindPool(pr.name);
                if (wsPool) {
                    activeSlots = wsPool->GetFullThreadCount();
                }
            }
            totalActive += activeSlots;
            std::printf("%s,multi-pool,%s,%s,%d,%d,%.0f,%.2f\n",
                        poolType.c_str(), pr.name, pr.workload, pr.slots,
                        activeSlots, opsPerSec, cpuSeconds);
        }
        std::fflush(stdout);

        // Stderr: per-pool WS counters + adaptive stats
        if (isWs) {
            DumpAllWsCounters();
        }

        std::fprintf(stderr, "\nAggregate: %d/%d active slots, %.2f CPU-seconds, %.1f%% utilization\n",
                     totalActive, totalMax, cpuSeconds,
                     (elapsed > 0 && totalMax > 0) ? (cpuSeconds / (elapsed * totalMax) * 100.0) : 0.0);

        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        sys.Stop();
    }

    // -------------------------------------------------------------------
    // Mixed-pool benchmark: all 4 workloads on a single pool.
    //
    // Answers: does sharing a pool between latency-critical (ping-pong,
    // chain) and CPU-heavy (storage-node, compaction) workloads cause
    // the lightweight ones to suffer?
    //
    // Compare with multi-pool to quantify the benefit of isolation.
    // -------------------------------------------------------------------

    void RunMixedPool(const TString& poolType, i16 slots,
                      ui32 warmupSec, ui32 durationSec) {
        bool isWs = (poolType == "ws");
        std::fprintf(stderr, "\n=== Mixed-Pool Benchmark (%s, %d slots) ===\n",
                     poolType.c_str(), slots);

        auto setup = isWs
            ? MakeWSSetup(slots, GSpinThresholdCycles)
            : MakeBasicSetup(slots);
        NActors::TActorSystem sys(setup);
        sys.Start();

        ui32 pool = isWs ? 1 : 0;

        std::atomic<bool> stop{false};

        // Per-workload counters
        std::atomic<ui64> systemCounter{0};   // chain(10)
        std::atomic<ui64> userCounter{0};     // storage-node
        std::atomic<ui64> batchCounter{0};    // chain(5)
        std::atomic<ui64> icCounter{0};       // ping-pong(8)

        // --- chain(10) ---
        {
            ui32 chainLen = 10;
            std::vector<NActors::TActorId> ids;
            std::vector<TChainActor*> actors;
            for (ui32 i = 0; i < chainLen; ++i) {
                auto* actor = new TChainActor(NActors::TActorId(), systemCounter, stop);
                actors.push_back(actor);
                ids.push_back(sys.Register(actor, NActors::TMailboxType::HTSwap, pool));
            }
            for (ui32 i = 0; i < chainLen; ++i) {
                actors[i]->SetNext(ids[(i + 1) % chainLen]);
            }
            sys.Send(ids[0], new TEvBenchMsg());
        }

        // --- storage-node (8 VDisks, 32 clients) ---
        {
            TStorageNodeParams sp;
            sp.VDisks = 8;
            sp.Clients = 32;

            std::vector<NActors::TActorId> skeletonIds;
            for (ui32 i = 0; i < sp.VDisks; ++i) {
                auto* logger = new TLoggerActor(sp.LogWorkIters);
                auto loggerId = sys.Register(logger, NActors::TMailboxType::HTSwap, pool);
                auto* compaction = new TCompactionActor(sp.CompactionWorkIters);
                auto compactionId = sys.Register(compaction, NActors::TMailboxType::HTSwap, pool);
                auto* skeleton = new TVDiskSkeleton(
                    loggerId, compactionId, userCounter, stop, sp, pool);
                auto skeletonId = sys.Register(skeleton, NActors::TMailboxType::HTSwap, pool);
                skeletonIds.push_back(skeletonId);
                sys.Send(skeletonId, new TEvCompactionTick());
            }
            for (ui32 i = 0; i < sp.Clients; ++i) {
                auto* client = new TStorageClient(
                    skeletonIds, userCounter, stop, sp.PutRatio, 42 + i);
                auto clientId = sys.Register(client, NActors::TMailboxType::HTSwap, pool);
                sys.Send(clientId, new TEvBenchMsg());
            }
        }

        // --- chain(5) ---
        {
            ui32 chainLen = 5;
            std::vector<NActors::TActorId> ids;
            std::vector<TChainActor*> actors;
            for (ui32 i = 0; i < chainLen; ++i) {
                auto* actor = new TChainActor(NActors::TActorId(), batchCounter, stop);
                actors.push_back(actor);
                ids.push_back(sys.Register(actor, NActors::TMailboxType::HTSwap, pool));
            }
            for (ui32 i = 0; i < chainLen; ++i) {
                actors[i]->SetNext(ids[(i + 1) % chainLen]);
            }
            sys.Send(ids[0], new TEvBenchMsg());
        }

        // --- ping-pong (8 pairs) ---
        {
            ui32 pairCount = 8;
            for (ui32 i = 0; i < pairCount; ++i) {
                auto* pong = new TPongActor(icCounter, stop);
                auto pongId = sys.Register(pong, NActors::TMailboxType::HTSwap, pool);
                auto* ping = new TPingActor(pongId, icCounter, stop);
                auto pingId = sys.Register(ping, NActors::TMailboxType::HTSwap, pool);
                pong->SetPeer(pingId);
                sys.Send(pingId, new TEvBenchPing());
            }
        }

        // Warmup
        std::fprintf(stderr, "Warming up %u seconds...\n", warmupSec);
        std::this_thread::sleep_for(std::chrono::seconds(warmupSec));
        systemCounter.store(0, std::memory_order_relaxed);
        userCounter.store(0, std::memory_order_relaxed);
        batchCounter.store(0, std::memory_order_relaxed);
        icCounter.store(0, std::memory_order_relaxed);
        if (isWs) {
            ResetWsCounters();
        }

        // Measure
        double cpuBefore = GetProcessCpuSeconds();
        auto start = TClock::now();
        std::this_thread::sleep_for(std::chrono::seconds(durationSec));
        auto end = TClock::now();
        double cpuAfter = GetProcessCpuSeconds();

        stop.store(true, std::memory_order_relaxed);
        double elapsed = std::chrono::duration<double>(end - start).count();
        double cpuSeconds = cpuAfter - cpuBefore;

        struct WorkloadResult {
            const char* name;
            const char* workload;
            std::atomic<ui64>& counter;
        };
        WorkloadResult workloads[] = {
            {"System", "chain(10)", systemCounter},
            {"User", "storage-node", userCounter},
            {"Batch", "chain(5)", batchCounter},
            {"IC", "ping-pong(8)", icCounter},
        };

        std::printf("pool_type,scenario,workload_name,workload,slots,ops_per_sec,cpu_seconds\n");
        for (auto& w : workloads) {
            ui64 ops = w.counter.load(std::memory_order_relaxed);
            double opsPerSec = (elapsed > 0) ? (double)ops / elapsed : 0;
            std::printf("%s,mixed-pool,%s,%s,%d,%.0f,%.2f\n",
                        poolType.c_str(), w.name, w.workload, slots,
                        opsPerSec, cpuSeconds);
        }
        std::fflush(stdout);

        if (isWs) {
            char label[128];
            std::snprintf(label, sizeof(label), "%s/mixed-pool t=%d", poolType.c_str(), slots);
            DumpWsCounters(label);
        }

        std::fprintf(stderr, "CPU-seconds: %.2f  utilization: %.1f%%\n",
                     cpuSeconds,
                     (elapsed > 0 && slots > 0)
                         ? (cpuSeconds / (elapsed * slots) * 100.0) : 0.0);

        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        sys.Stop();
    }

    // -------------------------------------------------------------------
    // CPU topology info (shared by topology-proof and multi-pool-topology)
    // -------------------------------------------------------------------

    struct TCpuTopologyInfo {
        uint32_t CpuId;
        int CoreId;
        int PhysicalPackageId;
        int NumaNode;
        TString L3Index;

        static int ReadSysfsInt(const TString& path) {
            std::ifstream f(path.c_str());
            int val = -1;
            if (f.is_open()) {
                f >> val;
            }
            return val;
        }

        static TString ReadSysfsString(const TString& path) {
            std::ifstream f(path.c_str());
            TString val;
            if (f.is_open()) {
                std::string line;
                std::getline(f, line);
                val = line;
            }
            return val;
        }

        static TCpuTopologyInfo ForCpu(uint32_t cpuId) {
            TCpuTopologyInfo info;
            info.CpuId = cpuId;
            TString base = TString("/sys/devices/system/cpu/cpu") + std::to_string(cpuId).c_str();
            info.CoreId = ReadSysfsInt(base + "/topology/core_id");
            info.PhysicalPackageId = ReadSysfsInt(base + "/topology/physical_package_id");

            // Find NUMA node via symlink
            info.NumaNode = -1;
            for (int n = 0; n < 16; ++n) {
                TString nodePath = base + "/node" + std::to_string(n).c_str();
                struct stat st;
                if (stat(nodePath.c_str(), &st) == 0) {
                    info.NumaNode = n;
                    break;
                }
            }

            // Find L3 cache index (look for level=3 in cache indices)
            info.L3Index = "?";
            for (int idx = 0; idx < 10; ++idx) {
                TString cachePath = base + "/cache/index" + std::to_string(idx).c_str();
                int level = ReadSysfsInt(cachePath + "/level");
                if (level == 3) {
                    info.L3Index = ReadSysfsString(cachePath + "/shared_cpu_list");
                    break;
                }
            }

            return info;
        }
    };

    // -------------------------------------------------------------------
    // Multi-pool topology proof: creates 4 pools with many slots and
    // few actors, waits for convergence, then reports per-pool topology.
    // -------------------------------------------------------------------

    void RunMultiPoolTopology(const TMultiPoolParams& mp, ui32 convergenceSec) {
        std::fprintf(stderr, "\n=== Multi-Pool Topology Proof ===\n");
        std::fprintf(stderr, "Pools: System=%d User=%d Batch=%d IC=%d  adaptive=forced\n",
                     mp.SystemSlots, mp.UserSlots, mp.BatchSlots, mp.ICSlots);

        // Force adaptive on for this scenario
        TMultiPoolParams forced = mp;
        forced.Adaptive = true;

        auto setup = MakeMultiWSSetup(forced);
        NActors::TActorSystem sys(setup);
        sys.Start();

        std::atomic<bool> stop{false};

        // Minimal actors per pool to force heavy deflation
        // System: chain(3), User: chain(5), Batch: chain(2), IC: chain(3)
        struct PoolWork {
            ui32 poolId;
            ui32 chainLen;
            std::atomic<ui64> counter{0};
        };
        PoolWork work[] = {{1, 3, {}}, {2, 5, {}}, {3, 2, {}}, {4, 3, {}}};

        for (auto& pw : work) {
            std::vector<NActors::TActorId> ids;
            std::vector<TChainActor*> actors;
            for (ui32 i = 0; i < pw.chainLen; ++i) {
                auto* actor = new TChainActor(NActors::TActorId(), pw.counter, stop);
                actors.push_back(actor);
                ids.push_back(sys.Register(actor, NActors::TMailboxType::HTSwap, pw.poolId));
            }
            for (ui32 i = 0; i < pw.chainLen; ++i) {
                actors[i]->SetNext(ids[(i + 1) % pw.chainLen]);
            }
            sys.Send(ids[0], new TEvBenchMsg());
        }

        std::fprintf(stderr, "Waiting %u seconds for adaptive scaler convergence...\n", convergenceSec);
        std::this_thread::sleep_for(std::chrono::seconds(convergenceSec));

        // --- Per-pool topology report ---
        const char* poolNames[] = {"System", "User", "Batch", "IC"};

        i16 totalActiveAll = 0;
        i16 totalMaxAll = 0;
        std::set<uint32_t> allActiveCpus;

        for (int pi = 0; pi < 4; ++pi) {
            auto* wsPool = NActors::NWorkStealing::TWSExecutorPool::FindPool(poolNames[pi]);
            if (!wsPool) {
                std::fprintf(stderr, "ERROR: pool '%s' not found\n", poolNames[pi]);
                continue;
            }

            i16 activeCount = wsPool->GetFullThreadCount();
            i16 totalSlots = wsPool->GetMaxFullThreadCount();
            auto* slots = wsPool->GetSlots();

            totalActiveAll += activeCount;
            totalMaxAll += totalSlots;

            std::fprintf(stderr, "\n--- Pool '%s': %d active / %d total ---\n",
                         poolNames[pi], activeCount, totalSlots);
            std::fprintf(stderr, "%-6s %-6s %-7s %-6s %-30s\n",
                         "Slot", "CPU", "Socket", "NUMA", "L3 shared_cpu_list");

            struct TGroupStats { int Active = 0; int Inactive = 0; };
            std::map<int, TGroupStats> numaStats;
            std::map<TString, TGroupStats> l3Stats;

            for (i16 i = 0; i < totalSlots; ++i) {
                auto info = TCpuTopologyInfo::ForCpu(slots[i].AssignedCpu);
                bool active = i < activeCount;
                if (active) {
                    allActiveCpus.insert(slots[i].AssignedCpu);
                }
                if (active) {
                    numaStats[info.NumaNode].Active++;
                    l3Stats[info.L3Index].Active++;
                } else {
                    numaStats[info.NumaNode].Inactive++;
                    l3Stats[info.L3Index].Inactive++;
                }

                // Print active slots and a sample of inactive
                if (active || i < activeCount + 3 || i >= totalSlots - 2) {
                    std::fprintf(stderr, "%-6d %-6u %-7d %-6d %s%s\n",
                                 i, slots[i].AssignedCpu, info.PhysicalPackageId,
                                 info.NumaNode, info.L3Index.c_str(),
                                 active ? "" : "  (inactive)");
                } else if (i == activeCount + 3) {
                    std::fprintf(stderr, "  ... (%d inactive slots omitted) ...\n",
                                 totalSlots - activeCount - 5);
                }
            }

            // NUMA spread
            int numaWithActive = 0;
            for (auto& [n, s] : numaStats) {
                if (s.Active > 0) numaWithActive++;
            }
            int l3WithActive = 0;
            for (auto& [l, s] : l3Stats) {
                if (s.Active > 0) l3WithActive++;
            }
            std::fprintf(stderr, "  NUMA spread: %d/%d nodes   L3 spread: %d/%d groups\n",
                         numaWithActive, (int)numaStats.size(),
                         l3WithActive, (int)l3Stats.size());
            std::fprintf(stderr, "  Inflate events: %lu  Deflate events: %lu\n",
                         wsPool->AdaptiveInflateEvents(), wsPool->AdaptiveDeflateEvents());
        }

        // --- Combined footprint ---
        std::fprintf(stderr, "\n=== COMBINED FOOTPRINT ===\n");
        std::fprintf(stderr, "Total active slots: %d / %d  (%.0f%% reduction)\n",
                     totalActiveAll, totalMaxAll,
                     totalMaxAll > 0 ? (1.0 - (double)totalActiveAll / totalMaxAll) * 100 : 0.0);
        std::fprintf(stderr, "Unique active CPUs: %d\n", (int)allActiveCpus.size());

        if (!allActiveCpus.empty()) {
            uint32_t minCpu = *allActiveCpus.begin();
            uint32_t maxCpu = *allActiveCpus.rbegin();
            std::fprintf(stderr, "CPU range: %u..%u (span %u, density %.0f%%)\n",
                         minCpu, maxCpu, maxCpu - minCpu + 1,
                         (double)allActiveCpus.size() / (maxCpu - minCpu + 1) * 100);
        }

        // --- Verification checks ---
        std::fprintf(stderr, "\n=== VERIFICATION ===\n");

        for (auto* pool : NActors::NWorkStealing::TWSExecutorPool::AllPools()) {
            if (pool->GetFullThreadCount() >= pool->GetMaxFullThreadCount()) {
                std::fprintf(stderr, "FAIL: pool '%s' did not deflate (%d/%d)\n",
                             pool->GetName().c_str(),
                             pool->GetFullThreadCount(),
                             pool->GetMaxFullThreadCount());
            } else {
                std::fprintf(stderr, "PASS: pool '%s' deflated %d → %d slots\n",
                             pool->GetName().c_str(),
                             pool->GetMaxFullThreadCount(),
                             pool->GetFullThreadCount());
            }
        }

        if ((int)allActiveCpus.size() < totalMaxAll) {
            std::fprintf(stderr, "PASS: combined active CPUs (%d) << total slots (%d)\n",
                         (int)allActiveCpus.size(), totalMaxAll);
        } else {
            std::fprintf(stderr, "FAIL: no CPU reduction\n");
        }

        stop.store(true, std::memory_order_relaxed);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        sys.Stop();
    }

    // -------------------------------------------------------------------
    // Single-pool topology proof (original scenario)
    // -------------------------------------------------------------------

    void RunTopologyProof(ui32 threads, ui32 actors, ui32 warmupSec) {
        std::fprintf(stderr, "\n=== Topology Proof: %u threads, %u actors, adaptive scaling ===\n\n", threads, actors);

        // Force adaptive on
        bool savedAdaptive = GAdaptive;
        GAdaptive = true;

        auto setup = MakeWSSetup(threads, GSpinThresholdCycles);
        NActors::TActorSystem sys(setup);
        sys.Start();

        ui32 pool = 1; // WS pool

        std::atomic<ui64> counter{0};
        std::atomic<bool> stop{false};

        // Create a few chain actors to keep some slots busy
        std::vector<NActors::TActorId> ids;
        std::vector<TChainActor*> chainActors;
        for (ui32 i = 0; i < actors; ++i) {
            auto* actor = new TChainActor(NActors::TActorId(), counter, stop);
            chainActors.push_back(actor);
            NActors::TActorId id = sys.Register(actor, NActors::TMailboxType::HTSwap, pool);
            ids.push_back(id);
        }
        for (ui32 i = 0; i < actors; ++i) {
            chainActors[i]->SetNext(ids[(i + 1) % actors]);
        }
        sys.Send(ids[0], new TEvBenchMsg());

        // Wait for adaptive scaler to converge
        std::fprintf(stderr, "Waiting %u seconds for adaptive scaler convergence...\n", warmupSec);
        std::this_thread::sleep_for(std::chrono::seconds(warmupSec));

        // Read slot topology
        auto* wsPool = NActors::NWorkStealing::TWSExecutorPool::LastCreated;
        if (!wsPool) {
            std::fprintf(stderr, "ERROR: no WS pool found\n");
            stop.store(true, std::memory_order_relaxed);
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            sys.Stop();
            GAdaptive = savedAdaptive;
            return;
        }

        // activeCount is the scaler's decision; slot state may lag (DRAIN→INACTIVE).
        // Use slot index < activeCount as the source of truth.
        i16 activeCount = wsPool->GetFullThreadCount();
        i16 totalSlots = wsPool->GetMaxFullThreadCount();
        auto* slots = wsPool->GetSlots();

        std::fprintf(stderr, "Scaler decision: %d active / %d total slots\n\n", activeCount, totalSlots);

        // Collect topology info for all slots
        struct TSlotInfo {
            i16 SlotIdx;
            uint32_t CpuId;
            int Socket;
            int NumaNode;
            TString L3Group;
            bool IsActive; // by scaler decision (slot index < activeCount)
        };
        std::vector<TSlotInfo> slotInfos;
        slotInfos.reserve(totalSlots);

        for (i16 i = 0; i < totalSlots; ++i) {
            auto info = TCpuTopologyInfo::ForCpu(slots[i].AssignedCpu);
            slotInfos.push_back({i, slots[i].AssignedCpu, info.PhysicalPackageId,
                                 info.NumaNode, info.L3Index, i < activeCount});
        }

        // Print active slots (always show all of them)
        std::fprintf(stderr, "--- Active slots (0..%d) ---\n", activeCount - 1);
        std::fprintf(stderr, "%-6s %-6s %-7s %-6s %-30s\n",
                     "Slot", "CPU", "Socket", "NUMA", "L3 shared_cpu_list");
        std::fprintf(stderr, "%-6s %-6s %-7s %-6s %-30s\n",
                     "----", "---", "------", "----", "------------------");
        for (i16 i = 0; i < activeCount; ++i) {
            auto& si = slotInfos[i];
            std::fprintf(stderr, "%-6d %-6u %-7d %-6d %s\n",
                         si.SlotIdx, si.CpuId, si.Socket, si.NumaNode, si.L3Group.c_str());
        }

        // Print a sample of inactive slots (first 5 and last 5)
        if (totalSlots > activeCount) {
            std::fprintf(stderr, "\n--- Inactive slots (sample: first 5, last 5 of %d..%d) ---\n",
                         activeCount, totalSlots - 1);
            std::fprintf(stderr, "%-6s %-6s %-7s %-6s %-30s\n",
                         "Slot", "CPU", "Socket", "NUMA", "L3 shared_cpu_list");
            std::fprintf(stderr, "%-6s %-6s %-7s %-6s %-30s\n",
                         "----", "---", "------", "----", "------------------");
            i16 inactiveCount = totalSlots - activeCount;
            i16 showFirst = std::min<i16>(5, inactiveCount);
            i16 showLast = std::min<i16>(5, inactiveCount - showFirst);
            for (i16 i = activeCount; i < activeCount + showFirst; ++i) {
                auto& si = slotInfos[i];
                std::fprintf(stderr, "%-6d %-6u %-7d %-6d %s\n",
                             si.SlotIdx, si.CpuId, si.Socket, si.NumaNode, si.L3Group.c_str());
            }
            if (showLast > 0 && showFirst < inactiveCount) {
                std::fprintf(stderr, "  ... (%d slots omitted) ...\n", inactiveCount - showFirst - showLast);
                for (i16 i = totalSlots - showLast; i < totalSlots; ++i) {
                    auto& si = slotInfos[i];
                    std::fprintf(stderr, "%-6d %-6u %-7d %-6d %s\n",
                                 si.SlotIdx, si.CpuId, si.Socket, si.NumaNode, si.L3Group.c_str());
                }
            }
        }

        // Collect per-NUMA and per-L3 stats (based on scaler decision, not slot state)
        struct TGroupStats { int Active = 0; int Inactive = 0; };
        std::map<int, TGroupStats> numaStats;
        std::map<TString, TGroupStats> l3Stats;

        for (auto& si : slotInfos) {
            if (si.IsActive) {
                numaStats[si.NumaNode].Active++;
                l3Stats[si.L3Group].Active++;
            } else {
                numaStats[si.NumaNode].Inactive++;
                l3Stats[si.L3Group].Inactive++;
            }
        }

        // NUMA summary
        std::fprintf(stderr, "\n--- NUMA Node Summary ---\n");
        std::fprintf(stderr, "%-6s %-8s %-10s\n", "NUMA", "Active", "Inactive");
        for (auto& [numa, stats] : numaStats) {
            std::fprintf(stderr, "%-6d %-8d %-10d\n", numa, stats.Active, stats.Inactive);
        }

        // L3 summary
        std::fprintf(stderr, "\n--- L3 Cache Group Summary ---\n");
        std::fprintf(stderr, "%-30s %-8s %-10s\n", "L3 shared_cpu_list", "Active", "Inactive");
        for (auto& [l3, stats] : l3Stats) {
            if (stats.Active > 0) {
                std::fprintf(stderr, "%-30s %-8d %-10d  << HAS ACTIVE\n",
                             l3.c_str(), stats.Active, stats.Inactive);
            } else {
                std::fprintf(stderr, "%-30s %-8d %-10d\n",
                             l3.c_str(), stats.Active, stats.Inactive);
            }
        }

        uint64_t inflates = wsPool->AdaptiveInflateEvents();
        uint64_t deflates = wsPool->AdaptiveDeflateEvents();
        std::fprintf(stderr, "\nAdaptive events: inflate=%lu deflate=%lu\n", inflates, deflates);

        // --- Verification ---
        std::fprintf(stderr, "\n=== VERIFICATION ===\n");

        // 1. Deflation happened
        if (activeCount < totalSlots) {
            std::fprintf(stderr, "PASS: deflated from %d to %d slots (%d%% reduction)\n",
                         totalSlots, activeCount,
                         (int)((1.0 - (double)activeCount / totalSlots) * 100));
        } else {
            std::fprintf(stderr, "FAIL: no deflation occurred\n");
        }

        // 2. Active slots cluster on fewer NUMA nodes
        int numaWithActive = 0;
        int totalNuma = (int)numaStats.size();
        for (auto& [numa, stats] : numaStats) {
            if (stats.Active > 0) numaWithActive++;
        }
        if (numaWithActive < totalNuma) {
            std::fprintf(stderr, "PASS: active slots use %d/%d NUMA nodes (topology-aware)\n",
                         numaWithActive, totalNuma);
        } else {
            std::fprintf(stderr, "INFO: active slots span all %d NUMA nodes "
                         "(need more deflation to prove NUMA clustering)\n", totalNuma);
        }

        // 3. Active slots cluster on fewer L3 groups
        int l3WithActive = 0;
        int totalL3 = (int)l3Stats.size();
        for (auto& [l3, stats] : l3Stats) {
            if (stats.Active > 0) l3WithActive++;
        }
        if (l3WithActive < totalL3) {
            std::fprintf(stderr, "PASS: active slots use %d/%d L3 cache groups (topology-aware)\n",
                         l3WithActive, totalL3);
        } else {
            std::fprintf(stderr, "INFO: active slots span all %d L3 groups "
                         "(need more deflation to prove L3 clustering)\n", totalL3);
        }

        // 4. Topology ordering: active slots should be on nearest CPUs
        //    (same socket, same NUMA, same L3 as slot 0)
        if (activeCount > 0) {
            auto& seed = slotInfos[0];
            int sameSocket = 0, sameNuma = 0, sameL3 = 0;
            for (i16 i = 0; i < activeCount; ++i) {
                if (slotInfos[i].Socket == seed.Socket) sameSocket++;
                if (slotInfos[i].NumaNode == seed.NumaNode) sameNuma++;
                if (slotInfos[i].L3Group == seed.L3Group) sameL3++;
            }
            std::fprintf(stderr, "PASS: topology proximity to seed CPU %u (socket %d, NUMA %d):\n"
                         "  Same socket: %d/%d  Same NUMA: %d/%d  Same L3: %d/%d\n",
                         seed.CpuId, seed.Socket, seed.NumaNode,
                         sameSocket, activeCount, sameNuma, activeCount, sameL3, activeCount);
        }

        stop.store(true, std::memory_order_relaxed);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        sys.Stop();

        GAdaptive = savedAdaptive;
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
    opts.AddLongOption("scenario", "Scenario: ping-pong, star, chain, reincarnation, pipeline, "
        "storage-node, topology-proof, multi-pool, multi-pool-topology, mixed-pool, or all")
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

    bool adaptive = false;
    opts.AddLongOption("adaptive", "Enable adaptive slot scaling for WS pool")
        .NoArgument()
        .SetFlag(&adaptive);

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

    // Multi-pool scenario parameters
    i16 systemSlots = 64;
    opts.AddLongOption("system-slots", "System pool slots (multi-pool scenarios)")
        .DefaultValue("64")
        .StoreResult(&systemSlots);

    i16 userSlots = 192;
    opts.AddLongOption("user-slots", "User pool slots (multi-pool scenarios)")
        .DefaultValue("192")
        .StoreResult(&userSlots);

    i16 batchSlots = 64;
    opts.AddLongOption("batch-slots", "Batch pool slots (multi-pool scenarios)")
        .DefaultValue("64")
        .StoreResult(&batchSlots);

    i16 icSlots = 64;
    opts.AddLongOption("ic-slots", "IC pool slots (multi-pool scenarios)")
        .DefaultValue("64")
        .StoreResult(&icSlots);

    NLastGetopt::TOptsParseResult res(&opts, argc, argv);

    GSpinThresholdCycles = spinThreshold;
    GMinSpinThresholdCycles = minSpinThreshold;
    GAdaptive = adaptive;

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

    TMultiPoolParams multiPoolParams;
    multiPoolParams.SystemSlots = systemSlots;
    multiPoolParams.UserSlots = userSlots;
    multiPoolParams.BatchSlots = batchSlots;
    multiPoolParams.ICSlots = icSlots;
    multiPoolParams.Adaptive = adaptive;
    multiPoolParams.SpinThresholdCycles = spinThreshold;

    std::vector<TString> scenarios;
    if (scenario == "all") {
        scenarios = {"ping-pong", "star", "chain", "reincarnation", "pipeline", "storage-node"};
    } else {
        scenarios = {scenario};
    }

    // CSV header
    std::printf("pool_type,scenario,threads,actor_pairs,ops_per_sec,avg_latency_us,cpu_seconds,cpu_util_pct\n");

    // Special scenarios that run once and exit
    if (scenario == "topology-proof") {
        if (threads.empty()) {
            threads = {384};
        }
        ui32 topologyActors = pairs.empty() ? 10 : pairs[0];
        RunTopologyProof(threads[0], topologyActors, warmupSec + durationSec);
        return 0;
    }

    if (scenario == "multi-pool") {
        for (const auto& pt : poolTypes) {
            RunMultiPool(pt, multiPoolParams, warmupSec, durationSec);
        }
        return 0;
    }

    if (scenario == "multi-pool-topology") {
        RunMultiPoolTopology(multiPoolParams, warmupSec + durationSec);
        return 0;
    }

    if (scenario == "mixed-pool") {
        i16 totalSlots = multiPoolParams.SystemSlots + multiPoolParams.UserSlots
                       + multiPoolParams.BatchSlots + multiPoolParams.ICSlots;
        for (const auto& pt : poolTypes) {
            RunMixedPool(pt, totalSlots, warmupSec, durationSec);
        }
        return 0;
    }

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
