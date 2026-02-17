#include <ydb/library/actors/core/workstealing/chase_lev_deque.h>
#include <ydb/library/actors/core/workstealing/vyukov_mpsc_queue.h>

#include <library/cpp/getopt/last_getopt.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

namespace {

    using TClock = std::chrono::steady_clock;

    // Parse comma-separated list of thread counts
    std::vector<int> ParseThreadCounts(const std::string& s) {
        std::vector<int> result;
        size_t pos = 0;
        while (pos < s.size()) {
            size_t next = s.find(',', pos);
            if (next == std::string::npos) {
                next = s.size();
            }
            int val = std::atoi(s.substr(pos, next - pos).c_str());
            if (val > 0) {
                result.push_back(val);
            }
            pos = next + 1;
        }
        return result;
    }

    struct TBenchResult {
        std::string Scenario;
        int Threads;
        double OpsPerSec;
        int64_t P50Ns;
        int64_t P99Ns;

        void Print() const {
            std::printf("%s,%d,%.0f,%ld,%ld\n",
                        Scenario.c_str(), Threads, OpsPerSec, (long)P50Ns, (long)P99Ns);
        }
    };

    // Compute percentiles from a sorted vector of nanosecond timings
    std::pair<int64_t, int64_t> Percentiles(std::vector<int64_t>& samples) {
        if (samples.empty()) {
            return {0, 0};
        }
        std::sort(samples.begin(), samples.end());
        size_t n = samples.size();
        int64_t p50 = samples[n * 50 / 100];
        int64_t p99 = samples[std::min(n - 1, n * 99 / 100)];
        return {p50, p99};
    }

    // ---- Scenario 1: Chase-Lev push/pop single-thread ----

    void BenchChaseLevPushPop(int64_t iterations) {
        NActors::NWorkStealing::TChaseLevDeque<ui32, 4096> deque;

        // Sample every Nth op for latency
        constexpr int kSampleRate = 64;
        std::vector<int64_t> samples;
        samples.reserve(static_cast<size_t>(iterations / kSampleRate) + 1);

        auto start = TClock::now();

        for (int64_t i = 0; i < iterations; ++i) {
            if (i % kSampleRate == 0) {
                auto t0 = TClock::now();
                deque.Push(static_cast<ui32>(i));
                deque.PopOwner();
                auto t1 = TClock::now();
                samples.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
            } else {
                deque.Push(static_cast<ui32>(i));
                deque.PopOwner();
            }
        }

        auto end = TClock::now();
        double secs = std::chrono::duration<double>(end - start).count();
        double opsPerSec = static_cast<double>(iterations * 2) / secs; // push + pop = 2 ops

        auto [p50, p99] = Percentiles(samples);

        TBenchResult{
            .Scenario = "chase-lev-push-pop",
            .Threads = 1,
            .OpsPerSec = opsPerSec,
            .P50Ns = p50,
            .P99Ns = p99,
        }
            .Print();
    }

    // ---- Scenario 2: Chase-Lev steal (1 owner + N stealers) ----

    void BenchChaseLevSteal(int threadCount, int durationSecs) {
        NActors::NWorkStealing::TChaseLevDeque<ui32, 4096> deque;

        std::atomic<bool> running{false};
        std::atomic<int64_t> totalSteals{0};

        // Stealer threads
        std::vector<std::thread> stealers;
        stealers.reserve(threadCount);

        for (int i = 0; i < threadCount; ++i) {
            stealers.emplace_back([&]() {
                ui32 buf[2048];
                int64_t localSteals = 0;
                while (!running.load(std::memory_order_acquire)) {
                    // spin-wait for start
                }
                while (running.load(std::memory_order_relaxed)) {
                    size_t n = deque.StealHalf(buf, 2048);
                    localSteals += static_cast<int64_t>(n);
                }
                totalSteals.fetch_add(localSteals, std::memory_order_relaxed);
            });
        }

        // Owner thread: push continuously
        running.store(true, std::memory_order_release);
        auto start = TClock::now();
        auto deadline = start + std::chrono::seconds(durationSecs);

        int64_t pushCount = 0;
        ui32 val = 0;
        while (TClock::now() < deadline) {
            if (deque.Push(val++)) {
                ++pushCount;
            }
            // If full, pop some to make room
            if (deque.SizeEstimate() > 3800) {
                for (int j = 0; j < 100; ++j) {
                    deque.PopOwner();
                }
            }
        }

        running.store(false, std::memory_order_release);
        for (auto& t : stealers) {
            t.join();
        }

        auto end = TClock::now();
        double secs = std::chrono::duration<double>(end - start).count();
        int64_t steals = totalSteals.load(std::memory_order_relaxed);
        double opsPerSec = static_cast<double>(pushCount + steals) / secs;

        TBenchResult{
            .Scenario = "chase-lev-steal",
            .Threads = threadCount + 1, // owner + stealers
            .OpsPerSec = opsPerSec,
            .P50Ns = 0,
            .P99Ns = 0,
        }
            .Print();
    }

    // ---- Scenario 3: MPSC push/pop single-thread ----

    void BenchMPSCPushPop(int64_t iterations) {
        NActors::NWorkStealing::TVyukovMPSCQueue<ui32, 64> queue;

        constexpr int kSampleRate = 64;
        std::vector<int64_t> samples;
        samples.reserve(static_cast<size_t>(iterations / kSampleRate) + 1);

        auto start = TClock::now();

        for (int64_t i = 0; i < iterations; ++i) {
            if (i % kSampleRate == 0) {
                auto t0 = TClock::now();
                queue.Push(static_cast<ui32>(i));
                queue.TryPop();
                auto t1 = TClock::now();
                samples.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
            } else {
                queue.Push(static_cast<ui32>(i));
                queue.TryPop();
            }
        }

        auto end = TClock::now();
        double secs = std::chrono::duration<double>(end - start).count();
        double opsPerSec = static_cast<double>(iterations * 2) / secs;

        auto [p50, p99] = Percentiles(samples);

        TBenchResult{
            .Scenario = "mpsc-push-pop",
            .Threads = 1,
            .OpsPerSec = opsPerSec,
            .P50Ns = p50,
            .P99Ns = p99,
        }
            .Print();
    }

    // ---- Scenario 4: MPSC multi-producer (N producers + 1 consumer) ----

    void BenchMPSCMultiProducer(int threadCount, int durationSecs) {
        NActors::NWorkStealing::TVyukovMPSCQueue<ui32, 64> queue;

        std::atomic<bool> running{false};
        std::atomic<int64_t> totalPushes{0};

        // Producer threads
        std::vector<std::thread> producers;
        producers.reserve(threadCount);

        for (int i = 0; i < threadCount; ++i) {
            producers.emplace_back([&, i]() {
                int64_t localPushes = 0;
                ui32 val = static_cast<ui32>(i * 1000000);
                while (!running.load(std::memory_order_acquire)) {
                    // spin-wait for start
                }
                while (running.load(std::memory_order_relaxed)) {
                    queue.Push(val++);
                    ++localPushes;
                }
                totalPushes.fetch_add(localPushes, std::memory_order_relaxed);
            });
        }

        // Consumer: drain on main thread
        running.store(true, std::memory_order_release);
        auto start = TClock::now();
        auto deadline = start + std::chrono::seconds(durationSecs);

        int64_t popCount = 0;
        ui32 drainBuf[256];
        while (TClock::now() < deadline) {
            size_t n = queue.DrainTo(drainBuf, 256);
            popCount += static_cast<int64_t>(n);
            if (n == 0) {
                // Yield briefly to avoid burning CPU when queue is empty
                std::this_thread::yield();
            }
        }

        running.store(false, std::memory_order_release);
        for (auto& t : producers) {
            t.join();
        }

        // Drain remaining items
        while (auto item = queue.TryPop()) {
            ++popCount;
        }

        auto end = TClock::now();
        double secs = std::chrono::duration<double>(end - start).count();
        int64_t pushes = totalPushes.load(std::memory_order_relaxed);
        double opsPerSec = static_cast<double>(pushes + popCount) / secs;

        TBenchResult{
            .Scenario = "mpsc-multi-producer",
            .Threads = threadCount + 1, // producers + consumer
            .OpsPerSec = opsPerSec,
            .P50Ns = 0,
            .P99Ns = 0,
        }
            .Print();
    }

    // ---- Scenario 5: MPSC drain single-thread ----

    void BenchMPSCDrain(int64_t iterations) {
        NActors::NWorkStealing::TVyukovMPSCQueue<ui32, 64> queue;

        constexpr int kBatchSize = 256;
        constexpr int kSampleRate = 16; // sample every Nth batch
        std::vector<int64_t> samples;
        samples.reserve(static_cast<size_t>(iterations / kBatchSize / kSampleRate) + 1);

        ui32 drainBuf[kBatchSize];

        auto start = TClock::now();

        int64_t totalOps = 0;
        int batchIdx = 0;
        while (totalOps < iterations) {
            // Push a batch
            int64_t batchCount = std::min(static_cast<int64_t>(kBatchSize), iterations - totalOps);
            for (int64_t j = 0; j < batchCount; ++j) {
                queue.Push(static_cast<ui32>(totalOps + j));
            }

            // Drain it
            if (batchIdx % kSampleRate == 0) {
                auto t0 = TClock::now();
                queue.DrainTo(drainBuf, kBatchSize);
                auto t1 = TClock::now();
                samples.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
            } else {
                queue.DrainTo(drainBuf, kBatchSize);
            }

            totalOps += batchCount;
            ++batchIdx;
        }

        auto end = TClock::now();
        double secs = std::chrono::duration<double>(end - start).count();
        // Count push + drain as ops
        double opsPerSec = static_cast<double>(totalOps * 2) / secs;

        auto [p50, p99] = Percentiles(samples);

        TBenchResult{
            .Scenario = "mpsc-drain",
            .Threads = 1,
            .OpsPerSec = opsPerSec,
            .P50Ns = p50,
            .P99Ns = p99,
        }
            .Print();
    }

    // ---- Runner ----

    void RunScenario(const std::string& scenario, const std::vector<int>& threads,
                     int durationSecs, int64_t iterations)
    {
        bool all = (scenario == "all");

        if (all || scenario == "chase-lev-push-pop") {
            BenchChaseLevPushPop(iterations);
        }
        if (all || scenario == "chase-lev-steal") {
            for (int tc : threads) {
                BenchChaseLevSteal(tc, durationSecs);
            }
        }
        if (all || scenario == "mpsc-push-pop") {
            BenchMPSCPushPop(iterations);
        }
        if (all || scenario == "mpsc-multi-producer") {
            for (int tc : threads) {
                BenchMPSCMultiProducer(tc, durationSecs);
            }
        }
        if (all || scenario == "mpsc-drain") {
            BenchMPSCDrain(iterations);
        }
    }

} // anonymous namespace

int main(int argc, const char* argv[]) {
    NLastGetopt::TOpts opts;

    std::string scenario = "all";
    std::string threadsStr = "1,2,4,8,16,32";
    int durationSecs = 5;
    int64_t iterations = 1000000;

    opts.AddLongOption("scenario", "Benchmark scenario to run")
        .DefaultValue("all")
        .StoreResult(&scenario);
    opts.AddLongOption("threads", "Comma-separated thread counts for multi-threaded scenarios")
        .DefaultValue("1,2,4,8,16,32")
        .StoreResult(&threadsStr);
    opts.AddLongOption("duration", "Duration in seconds for timed scenarios")
        .DefaultValue("5")
        .StoreResult(&durationSecs);
    opts.AddLongOption("iterations", "Number of operations for single-thread scenarios")
        .DefaultValue("1000000")
        .StoreResult(&iterations);
    opts.AddHelpOption();

    NLastGetopt::TOptsParseResult parseResult(&opts, argc, argv);

    std::vector<int> threads = ParseThreadCounts(threadsStr);

    std::printf("scenario,threads,ops_per_sec,p50_ns,p99_ns\n");
    RunScenario(scenario, threads, durationSecs, iterations);

    return 0;
}
