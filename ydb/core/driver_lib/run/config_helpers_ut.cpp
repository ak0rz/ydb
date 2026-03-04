#include "config_helpers.h"

#include <ydb/core/protos/config.pb.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NActorSystemConfigHelpers;

namespace {

NKikimrConfig::TActorSystemConfig::TExecutor MakeWsPool(
    const TString& name, ui32 threads, ui32 minThreads, ui32 maxThreads,
    i32 priority = 0, ui32 timePerMailboxUs = 0, ui32 eventsPerMailbox = 0)
{
    NKikimrConfig::TActorSystemConfig::TExecutor pool;
    pool.SetType(NKikimrConfig::TActorSystemConfig::TExecutor::WORK_STEALING);
    pool.SetName(name);
    pool.SetThreads(threads);
    pool.SetMinThreads(minThreads);
    pool.SetMaxThreads(maxThreads);
    pool.SetPriority(priority);
    if (timePerMailboxUs) {
        pool.SetTimePerMailboxMicroSecs(timePerMailboxUs);
    }
    if (eventsPerMailbox) {
        pool.SetEventsPerMailbox(eventsPerMailbox);
    }
    return pool;
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(ConfigHelpersWorkStealing) {

Y_UNIT_TEST(BasicFields) {
    NActors::TCpuManagerConfig cpuManager;
    NKikimrConfig::TActorSystemConfig systemConfig;
    auto pool = MakeWsPool("WsPool", 8, 2, 16, 5);

    AddExecutorPool(cpuManager, pool, systemConfig, 3, nullptr);

    UNIT_ASSERT(cpuManager.WorkStealing.has_value());
    UNIT_ASSERT(cpuManager.WorkStealing->Enabled);
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.WorkStealing->Pools.size(), 1);

    const auto& ws = cpuManager.WorkStealing->Pools[0];
    UNIT_ASSERT_VALUES_EQUAL(ws.PoolId, 3);
    UNIT_ASSERT_STRINGS_EQUAL(ws.PoolName, "WsPool");
    UNIT_ASSERT_VALUES_EQUAL(ws.DefaultSlotCount, 8);
    UNIT_ASSERT_VALUES_EQUAL(ws.MinSlotCount, 2);
    UNIT_ASSERT_VALUES_EQUAL(ws.MaxSlotCount, 16);
    UNIT_ASSERT_VALUES_EQUAL(ws.Priority, 5);
}

Y_UNIT_TEST(TimePerMailboxFromPool) {
    NActors::TCpuManagerConfig cpuManager;
    NKikimrConfig::TActorSystemConfig systemConfig;
    auto pool = MakeWsPool("WsPool", 4, 1, 8, 0, 5000);

    AddExecutorPool(cpuManager, pool, systemConfig, 0, nullptr);

    const auto& ws = cpuManager.WorkStealing->Pools[0];
    UNIT_ASSERT_VALUES_EQUAL(ws.TimePerMailbox, TDuration::MicroSeconds(5000));
}

Y_UNIT_TEST(TimePerMailboxFromSystem) {
    NActors::TCpuManagerConfig cpuManager;
    NKikimrConfig::TActorSystemConfig systemConfig;
    systemConfig.SetTimePerMailboxMicroSecs(3000);
    auto pool = MakeWsPool("WsPool", 4, 1, 8);

    AddExecutorPool(cpuManager, pool, systemConfig, 0, nullptr);

    const auto& ws = cpuManager.WorkStealing->Pools[0];
    UNIT_ASSERT_VALUES_EQUAL(ws.TimePerMailbox, TDuration::MicroSeconds(3000));
}

Y_UNIT_TEST(TimePerMailboxPoolOverridesSystem) {
    NActors::TCpuManagerConfig cpuManager;
    NKikimrConfig::TActorSystemConfig systemConfig;
    systemConfig.SetTimePerMailboxMicroSecs(3000);
    auto pool = MakeWsPool("WsPool", 4, 1, 8, 0, 7000);

    AddExecutorPool(cpuManager, pool, systemConfig, 0, nullptr);

    const auto& ws = cpuManager.WorkStealing->Pools[0];
    UNIT_ASSERT_VALUES_EQUAL(ws.TimePerMailbox, TDuration::MicroSeconds(7000));
}

Y_UNIT_TEST(EventsPerMailboxFromPool) {
    NActors::TCpuManagerConfig cpuManager;
    NKikimrConfig::TActorSystemConfig systemConfig;
    auto pool = MakeWsPool("WsPool", 4, 1, 8, 0, 0, 200);

    AddExecutorPool(cpuManager, pool, systemConfig, 0, nullptr);

    const auto& ws = cpuManager.WorkStealing->Pools[0];
    UNIT_ASSERT_VALUES_EQUAL(ws.EventsPerMailbox, 200);
}

Y_UNIT_TEST(EventsPerMailboxFromSystem) {
    NActors::TCpuManagerConfig cpuManager;
    NKikimrConfig::TActorSystemConfig systemConfig;
    systemConfig.SetEventsPerMailbox(150);
    auto pool = MakeWsPool("WsPool", 4, 1, 8);

    AddExecutorPool(cpuManager, pool, systemConfig, 0, nullptr);

    const auto& ws = cpuManager.WorkStealing->Pools[0];
    UNIT_ASSERT_VALUES_EQUAL(ws.EventsPerMailbox, 150);
}

Y_UNIT_TEST(MultiplePools) {
    NActors::TCpuManagerConfig cpuManager;
    NKikimrConfig::TActorSystemConfig systemConfig;

    AddExecutorPool(cpuManager, MakeWsPool("WsA", 4, 1, 8, 10), systemConfig, 0, nullptr);
    AddExecutorPool(cpuManager, MakeWsPool("WsB", 8, 2, 16, 20), systemConfig, 1, nullptr);

    UNIT_ASSERT(cpuManager.WorkStealing.has_value());
    UNIT_ASSERT(cpuManager.WorkStealing->Enabled);
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.WorkStealing->Pools.size(), 2);

    UNIT_ASSERT_STRINGS_EQUAL(cpuManager.WorkStealing->Pools[0].PoolName, "WsA");
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.WorkStealing->Pools[0].PoolId, 0);
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.WorkStealing->Pools[0].Priority, 10);

    UNIT_ASSERT_STRINGS_EQUAL(cpuManager.WorkStealing->Pools[1].PoolName, "WsB");
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.WorkStealing->Pools[1].PoolId, 1);
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.WorkStealing->Pools[1].Priority, 20);
}

Y_UNIT_TEST(DefaultTimeAndEvents) {
    NActors::TCpuManagerConfig cpuManager;
    NKikimrConfig::TActorSystemConfig systemConfig;
    auto pool = MakeWsPool("WsPool", 4, 1, 8);

    AddExecutorPool(cpuManager, pool, systemConfig, 0, nullptr);

    const auto& ws = cpuManager.WorkStealing->Pools[0];
    // When neither pool nor system config set these, defaults from TWorkStealingPoolConfig apply
    UNIT_ASSERT_VALUES_EQUAL(ws.TimePerMailbox, TDuration::MilliSeconds(10));
    UNIT_ASSERT_VALUES_EQUAL(ws.EventsPerMailbox, 100);
}

Y_UNIT_TEST(DoesNotAffectBasicOrIO) {
    NActors::TCpuManagerConfig cpuManager;
    NKikimrConfig::TActorSystemConfig systemConfig;
    auto pool = MakeWsPool("WsPool", 4, 1, 8);

    AddExecutorPool(cpuManager, pool, systemConfig, 0, nullptr);

    UNIT_ASSERT(cpuManager.Basic.empty());
    UNIT_ASSERT(cpuManager.IO.empty());
}

} // Y_UNIT_TEST_SUITE(ConfigHelpersWorkStealing)
