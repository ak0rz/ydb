#include <ydb/library/actors/core/workstealing/mpmc_unbounded_queue.h>
#include <ydb/library/actors/queues/mpmc_ring_queue_v4_correct.h>
#include <ydb/library/actors/core/thread_context.h>
#include <ydb/library/actors/queues/activation_queue.h>

#include <library/cpp/getopt/last_getopt.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

namespace {

    using TClock = std::chrono::steady_clock;

    // ---------------------------------------------------------------
    // Queue adapters — uniform push/pop interface for benchmarking
    // ---------------------------------------------------------------

    struct TV4CorrectAdapter {
        static constexpr const char* Name = "v4correct";
        NActors::TMPMCRingQueueV4Correct<20> Queue;

        TV4CorrectAdapter() : Queue(8) {}

        bool TryPush(ui32 val) { return Queue.TryPush(val); }
        std::optional<ui32> TryPop() { return Queue.TryPop(); }
    };

    template <size_t SegBytes>
    struct TUnboundedAdapter {
        static const char* Name;
        NActors::NWorkStealing::TMPMCUnboundedQueue<SegBytes> Queue;

        bool TryPush(ui32 val) { Queue.Push(val); return true; }
        std::optional<ui32> TryPop() { return Queue.TryPop(); }
    };

    template <size_t SegBytes>
    const char* TUnboundedAdapter<SegBytes>::Name = "unbounded";

    // Specializations with distinct names for CSV output.
    template <> const char* TUnboundedAdapter<4096>::Name = "unbounded-4k";
    template <> const char* TUnboundedAdapter<16384>::Name = "unbounded-16k";
    template <> const char* TUnboundedAdapter<65536>::Name = "unbounded-64k";
    template <> const char* TUnboundedAdapter<524288>::Name = "unbounded-512k";

    struct TActivationV4Adapter {
        static constexpr const char* Name = "activation-v4";
        NActors::TRingActivationQueueV4 Queue;
        std::atomic<ui64> Counter{0};

        TActivationV4Adapter() : Queue(8) {}
        ~TActivationV4Adapter() {
            while (Queue.Pop(Counter.fetch_add(1, std::memory_order_relaxed))) {}
        }

        bool TryPush(ui32 val) { Queue.Push(val, Counter.fetch_add(1, std::memory_order_relaxed)); return true; }
        std::optional<ui32> TryPop() {
            ui32 v = Queue.Pop(Counter.fetch_add(1, std::memory_order_relaxed));
            return v ? std::optional<ui32>(v) : std::nullopt;
        }
    };

    // ---------------------------------------------------------------
    // Scenarios
    // ---------------------------------------------------------------

    template <typename TAdapter>
    size_t RunSymmetric(size_t threads, double durationSec) {
        TAdapter adapter;
        const size_t nProd = threads / 2;
        const size_t nCons = threads - nProd;
        std::atomic<bool> stop{false};
        std::atomic<size_t> totalOps{0};

        std::vector<std::thread> workers;
        workers.reserve(threads);

        for (size_t i = 0; i < nProd; ++i) {
            workers.emplace_back([&] {
                size_t ops = 0;
                ui32 val = 1;
                while (!stop.load(std::memory_order_relaxed)) {
                    if (adapter.TryPush(val)) {
                        ++ops;
                        ++val;
                    }
                }
                totalOps.fetch_add(ops, std::memory_order_relaxed);
            });
        }
        for (size_t i = 0; i < nCons; ++i) {
            workers.emplace_back([&] {
                size_t ops = 0;
                while (!stop.load(std::memory_order_relaxed)) {
                    if (adapter.TryPop()) {
                        ++ops;
                    }
                }
                totalOps.fetch_add(ops, std::memory_order_relaxed);
            });
        }

        std::this_thread::sleep_for(std::chrono::duration<double>(durationSec));
        stop.store(true, std::memory_order_relaxed);
        for (auto& t : workers) t.join();
        return totalOps.load();
    }

    template <typename TAdapter>
    size_t Run1PNC(size_t threads, double durationSec) {
        TAdapter adapter;
        const size_t nCons = threads - 1;
        std::atomic<bool> stop{false};
        std::atomic<size_t> totalOps{0};

        std::vector<std::thread> workers;
        workers.reserve(threads);

        workers.emplace_back([&] {
            size_t ops = 0;
            ui32 val = 1;
            while (!stop.load(std::memory_order_relaxed)) {
                if (adapter.TryPush(val)) {
                    ++ops;
                    ++val;
                }
            }
            totalOps.fetch_add(ops, std::memory_order_relaxed);
        });

        for (size_t i = 0; i < nCons; ++i) {
            workers.emplace_back([&] {
                size_t ops = 0;
                while (!stop.load(std::memory_order_relaxed)) {
                    if (adapter.TryPop()) {
                        ++ops;
                    }
                }
                totalOps.fetch_add(ops, std::memory_order_relaxed);
            });
        }

        std::this_thread::sleep_for(std::chrono::duration<double>(durationSec));
        stop.store(true, std::memory_order_relaxed);
        for (auto& t : workers) t.join();
        return totalOps.load();
    }

    template <typename TAdapter>
    size_t RunNP1C(size_t threads, double durationSec) {
        TAdapter adapter;
        const size_t nProd = threads - 1;
        std::atomic<bool> stop{false};
        std::atomic<size_t> totalOps{0};

        std::vector<std::thread> workers;
        workers.reserve(threads);

        for (size_t i = 0; i < nProd; ++i) {
            workers.emplace_back([&] {
                size_t ops = 0;
                ui32 val = 1;
                while (!stop.load(std::memory_order_relaxed)) {
                    if (adapter.TryPush(val)) {
                        ++ops;
                        ++val;
                    }
                }
                totalOps.fetch_add(ops, std::memory_order_relaxed);
            });
        }

        workers.emplace_back([&] {
            size_t ops = 0;
            while (!stop.load(std::memory_order_relaxed)) {
                if (adapter.TryPop()) {
                    ++ops;
                }
            }
            totalOps.fetch_add(ops, std::memory_order_relaxed);
        });

        std::this_thread::sleep_for(std::chrono::duration<double>(durationSec));
        stop.store(true, std::memory_order_relaxed);
        for (auto& t : workers) t.join();
        return totalOps.load();
    }

    template <typename TAdapter>
    size_t RunBurst(size_t threads, double) {
        TAdapter adapter;
        constexpr size_t BurstSize = 2000000;

        // Single-thread push (stop if bounded queue is full)
        for (ui32 i = 1; i <= BurstSize; ++i) {
            if (!adapter.TryPush(i)) {
                break;
            }
        }

        // Multi-thread drain
        std::atomic<size_t> totalOps{0};
        std::vector<std::thread> workers;
        workers.reserve(threads);
        for (size_t i = 0; i < threads; ++i) {
            workers.emplace_back([&] {
                size_t ops = 0;
                while (adapter.TryPop()) {
                    ++ops;
                }
                totalOps.fetch_add(ops, std::memory_order_relaxed);
            });
        }
        for (auto& t : workers) t.join();
        return totalOps.load();
    }

    // ---------------------------------------------------------------
    // Dispatch
    // ---------------------------------------------------------------

    template <typename TAdapter>
    void RunScenario(const TString& scenario, size_t threads, double duration) {
        auto start = TClock::now();
        size_t ops = 0;

        if (scenario == "symmetric") {
            ops = RunSymmetric<TAdapter>(threads, duration);
        } else if (scenario == "1p-nc") {
            ops = Run1PNC<TAdapter>(threads, duration);
        } else if (scenario == "np-1c") {
            ops = RunNP1C<TAdapter>(threads, duration);
        } else if (scenario == "burst") {
            ops = RunBurst<TAdapter>(threads, duration);
        } else {
            Cerr << "Unknown scenario: " << scenario << Endl;
            return;
        }

        auto elapsed = std::chrono::duration<double>(TClock::now() - start).count();
        double opsPerSec = ops ? static_cast<double>(ops) / elapsed : 0;
        double nsPerOp = ops ? elapsed * 1e9 / static_cast<double>(ops) : 0;

        printf("%s,%s,%zu,%.3f,%zu,%.0f,%.1f\n",
            TAdapter::Name, scenario.c_str(), threads, elapsed, ops, opsPerSec, nsPerOp);
        fflush(stdout);
    }

    template <typename TAdapter>
    void MaybeRun(const TString& queueType, const TString& scenario, size_t threads, double duration) {
        if (queueType == "all" || queueType == TAdapter::Name) {
            RunScenario<TAdapter>(scenario, threads, duration);
        }
    }

    void RunAll(const TString& queueType, const TString& scenario, size_t threads, double duration) {
        MaybeRun<TV4CorrectAdapter>(queueType, scenario, threads, duration);
        MaybeRun<TUnboundedAdapter<4096>>(queueType, scenario, threads, duration);
        MaybeRun<TUnboundedAdapter<16384>>(queueType, scenario, threads, duration);
        MaybeRun<TUnboundedAdapter<65536>>(queueType, scenario, threads, duration);
        MaybeRun<TUnboundedAdapter<524288>>(queueType, scenario, threads, duration);
        MaybeRun<TActivationV4Adapter>(queueType, scenario, threads, duration);
        if (queueType != "all"
            && queueType != TV4CorrectAdapter::Name
            && queueType != TUnboundedAdapter<4096>::Name
            && queueType != TUnboundedAdapter<16384>::Name
            && queueType != TUnboundedAdapter<65536>::Name
            && queueType != TUnboundedAdapter<524288>::Name
            && queueType != TActivationV4Adapter::Name) {
            Cerr << "Unknown queue type: " << queueType << Endl;
        }
    }

} // anonymous namespace

int main(int argc, const char* argv[]) {
    NLastGetopt::TOpts opts;
    opts.SetTitle("MPMC queue benchmark");

    TString queueType = "all";
    size_t threads = 8;
    double duration = 2.0;
    TString scenario = "symmetric";

    opts.AddLongOption("queue-type", "v4correct|unbounded|activation-v4|all")
        .DefaultValue("all")
        .StoreResult(&queueType);
    opts.AddLongOption("threads", "Number of threads")
        .DefaultValue("8")
        .StoreResult(&threads);
    opts.AddLongOption("duration", "Duration in seconds")
        .DefaultValue("2.0")
        .StoreResult(&duration);
    opts.AddLongOption("scenario", "symmetric|1p-nc|np-1c|burst")
        .DefaultValue("symmetric")
        .StoreResult(&scenario);

    NLastGetopt::TOptsParseResult parsed(&opts, argc, argv);

    printf("queue_type,scenario,threads,duration_s,total_ops,ops_per_sec,ns_per_op\n");

    if (scenario == "all") {
        for (const auto& s : {"symmetric", "1p-nc", "np-1c", "burst"}) {
            RunAll(queueType, s, threads, duration);
        }
    } else {
        RunAll(queueType, scenario, threads, duration);
    }

    return 0;
}
