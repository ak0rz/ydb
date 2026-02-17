#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/config.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/core/workstealing/executor_pool_ws.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

Y_UNIT_TEST_SUITE(CpuManagerWs) {

    Y_UNIT_TEST(ConfigHelpersWithWsPools) {
        TCpuManagerConfig config;
        config.Basic.push_back({.PoolId = 0, .PoolName = "sys", .Threads = 2});
        config.WorkStealing = TWorkStealingConfig{
            .Enabled = true,
            .Pools = {{.PoolId = 1, .PoolName = "ws", .DefaultSlotCount = 4}},
        };

        UNIT_ASSERT_VALUES_EQUAL(config.GetExecutorsCount(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(config.GetPoolName(0), "sys");
        UNIT_ASSERT_VALUES_EQUAL(config.GetPoolName(1), "ws");
    }

    Y_UNIT_TEST(GetThreadsOptionalWithWsPools) {
        TCpuManagerConfig config;
        config.Basic.push_back({.PoolId = 0, .PoolName = "sys", .Threads = 2, .DefaultThreadCount = 2});
        config.WorkStealing = TWorkStealingConfig{
            .Enabled = true,
            .Pools = {{.PoolId = 1, .PoolName = "ws", .DefaultSlotCount = 6}},
        };

        auto sysThreads = config.GetThreadsOptional(0);
        UNIT_ASSERT(sysThreads.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*sysThreads, 2u);

        auto wsThreads = config.GetThreadsOptional(1);
        UNIT_ASSERT(wsThreads.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*wsThreads, 6u);

        auto missing = config.GetThreadsOptional(99);
        UNIT_ASSERT(!missing.has_value());
    }

    Y_UNIT_TEST(GetExecutorsCountNoWs) {
        TCpuManagerConfig config;
        config.Basic.push_back({.PoolId = 0, .PoolName = "sys", .Threads = 2});
        config.IO.push_back({.PoolId = 1, .PoolName = "io", .Threads = 1});

        UNIT_ASSERT_VALUES_EQUAL(config.GetExecutorsCount(), 2u);
    }

    Y_UNIT_TEST(GetExecutorsCountWsDisabled) {
        TCpuManagerConfig config;
        config.Basic.push_back({.PoolId = 0, .PoolName = "sys", .Threads = 2});
        config.WorkStealing = TWorkStealingConfig{
            .Enabled = false,
            .Pools = {{.PoolId = 1, .PoolName = "ws", .DefaultSlotCount = 4}},
        };

        // Even when disabled, the pools are counted for executor array sizing
        UNIT_ASSERT_VALUES_EQUAL(config.GetExecutorsCount(), 2u);
    }

    Y_UNIT_TEST(GetThreadsForWsPool) {
        TCpuManagerConfig config;
        config.Basic.push_back({.PoolId = 0, .PoolName = "sys", .Threads = 2, .DefaultThreadCount = 2});
        config.WorkStealing = TWorkStealingConfig{
            .Enabled = true,
            .Pools = {{.PoolId = 1, .PoolName = "ws", .DefaultSlotCount = 5}},
        };

        UNIT_ASSERT_VALUES_EQUAL(config.GetThreads(0), 2u);
        UNIT_ASSERT_VALUES_EQUAL(config.GetThreads(1), 5u);
    }

    Y_UNIT_TEST(WsPoolCreatedWhenEnabled) {
        auto setup = MakeHolder<TActorSystemSetup>();
        setup->NodeId = 1;

        TBasicExecutorPoolConfig basicCfg;
        basicCfg.PoolId = 0;
        basicCfg.PoolName = "sys";
        basicCfg.Threads = 2;
        basicCfg.MinThreadCount = 2;
        basicCfg.MaxThreadCount = 2;
        basicCfg.DefaultThreadCount = 2;
        setup->CpuManager.Basic.push_back(basicCfg);

        TWorkStealingPoolConfig wsCfg;
        wsCfg.PoolId = 1;
        wsCfg.PoolName = "ws";
        wsCfg.MinSlotCount = 1;
        wsCfg.MaxSlotCount = 4;
        wsCfg.DefaultSlotCount = 2;
        setup->CpuManager.WorkStealing = TWorkStealingConfig{
            .Enabled = true,
            .Pools = {wsCfg},
        };

        setup->Scheduler.Reset(new TBasicSchedulerThread(TSchedulerConfig()));

        TActorSystem system(setup);
        system.Start();

        // Verify the WS pool was created: GetPoolMaxThreadsCount
        // returns MaxSlotCount for WS pools
        UNIT_ASSERT_DOUBLES_EQUAL(system.GetPoolMaxThreadsCount(1), 4.0f, 0.01f);

        // Verify pool thread count via setup (resolved from config)
        auto wsThreads = system.GetPoolThreadsCount(1);
        UNIT_ASSERT(wsThreads.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*wsThreads, 2u);

        system.Stop();
    }

    Y_UNIT_TEST(MixedBasicAndWsPools) {
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
        wsCfg.PoolName = "ws-pool";
        wsCfg.MinSlotCount = 1;
        wsCfg.MaxSlotCount = 4;
        wsCfg.DefaultSlotCount = 2;
        setup->CpuManager.WorkStealing = TWorkStealingConfig{
            .Enabled = true,
            .Pools = {wsCfg},
        };

        setup->Scheduler.Reset(new TBasicSchedulerThread(TSchedulerConfig()));

        TActorSystem system(setup);
        system.Start();

        // Verify both pools are created by querying each pool's thread count
        // Basic pool: MaxThreadCount is 2
        UNIT_ASSERT_DOUBLES_EQUAL(system.GetPoolMaxThreadsCount(0), 2.0f, 0.01f);

        // WS pool: MaxSlotCount is 4
        UNIT_ASSERT_DOUBLES_EQUAL(system.GetPoolMaxThreadsCount(1), 4.0f, 0.01f);

        system.Stop();
    }

    Y_UNIT_TEST(WsDisabledFallsToBasicOnly) {
        auto setup = MakeHolder<TActorSystemSetup>();
        setup->NodeId = 1;

        TBasicExecutorPoolConfig basicCfg;
        basicCfg.PoolId = 0;
        basicCfg.PoolName = "sys";
        basicCfg.Threads = 2;
        basicCfg.MinThreadCount = 2;
        basicCfg.MaxThreadCount = 2;
        basicCfg.DefaultThreadCount = 2;
        setup->CpuManager.Basic.push_back(basicCfg);

        setup->Scheduler.Reset(new TBasicSchedulerThread(TSchedulerConfig()));

        // No WorkStealing at all
        TActorSystem system(setup);
        system.Start();

        // Only 1 pool (basic) exists; verify by querying it
        UNIT_ASSERT_DOUBLES_EQUAL(system.GetPoolMaxThreadsCount(0), 2.0f, 0.01f);

        system.Stop();
    }

    Y_UNIT_TEST(WsPoolSlotCountReflectedInThreadCount) {
        auto setup = MakeHolder<TActorSystemSetup>();
        setup->NodeId = 1;

        TBasicExecutorPoolConfig basicCfg;
        basicCfg.PoolId = 0;
        basicCfg.PoolName = "sys";
        basicCfg.Threads = 1;
        basicCfg.MinThreadCount = 1;
        basicCfg.MaxThreadCount = 1;
        basicCfg.DefaultThreadCount = 1;
        setup->CpuManager.Basic.push_back(basicCfg);

        TWorkStealingPoolConfig wsCfg;
        wsCfg.PoolId = 1;
        wsCfg.PoolName = "ws";
        wsCfg.MinSlotCount = 1;
        wsCfg.MaxSlotCount = 8;
        wsCfg.DefaultSlotCount = 3;
        setup->CpuManager.WorkStealing = TWorkStealingConfig{
            .Enabled = true,
            .Pools = {wsCfg},
        };

        setup->Scheduler.Reset(new TBasicSchedulerThread(TSchedulerConfig()));

        TActorSystem system(setup);
        system.Start();

        // WS pool: MaxSlotCount is 8, DefaultSlotCount is 3
        UNIT_ASSERT_DOUBLES_EQUAL(system.GetPoolMaxThreadsCount(1), 8.0f, 0.01f);

        auto wsThreads = system.GetPoolThreadsCount(1);
        UNIT_ASSERT(wsThreads.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*wsThreads, 3u);

        system.Stop();
    }

    Y_UNIT_TEST(LifecycleStartStop) {
        // Verify clean start/stop cycle with WS pool
        auto setup = MakeHolder<TActorSystemSetup>();
        setup->NodeId = 1;

        TBasicExecutorPoolConfig basicCfg;
        basicCfg.PoolId = 0;
        basicCfg.PoolName = "sys";
        basicCfg.Threads = 1;
        basicCfg.MinThreadCount = 1;
        basicCfg.MaxThreadCount = 1;
        basicCfg.DefaultThreadCount = 1;
        setup->CpuManager.Basic.push_back(basicCfg);

        TWorkStealingPoolConfig wsCfg;
        wsCfg.PoolId = 1;
        wsCfg.PoolName = "ws";
        wsCfg.MinSlotCount = 1;
        wsCfg.MaxSlotCount = 4;
        wsCfg.DefaultSlotCount = 2;
        setup->CpuManager.WorkStealing = TWorkStealingConfig{
            .Enabled = true,
            .Pools = {wsCfg},
        };

        setup->Scheduler.Reset(new TBasicSchedulerThread(TSchedulerConfig()));

        {
            TActorSystem system(setup);
            system.Start();
            Sleep(TDuration::MilliSeconds(50));
            system.Stop();
        }

        // Should not crash on destruction
        UNIT_ASSERT(true);
    }

} // Y_UNIT_TEST_SUITE(CpuManagerWs)
