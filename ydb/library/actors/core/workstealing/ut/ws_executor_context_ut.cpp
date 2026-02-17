#include <ydb/library/actors/core/workstealing/ws_executor_context.h>

#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/actors/core/actor.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;
using namespace NActors::NWorkStealing;

Y_UNIT_TEST_SUITE(WsExecutorContext) {

    Y_UNIT_TEST(ConstructionDoesNotCrash) {
        // Create a minimal actor system via the test runtime.
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        TActorSystem* actorSystem = runtime.GetAnyNodeActorSystem();

        // Construct with nullptr pool -- the base TExecutorThread
        // constructor handles nullptr safely.
        TWSExecutorContext ctx(0, actorSystem, nullptr);

        UNIT_ASSERT_EQUAL(ctx.ActorSystem, actorSystem);
    }

    Y_UNIT_TEST(GetThreadCtxReturnsValidContext) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        TActorSystem* actorSystem = runtime.GetAnyNodeActorSystem();

        TWSExecutorContext ctx(42, actorSystem, nullptr);

        TThreadContext& threadCtx = ctx.GetThreadCtx();
        UNIT_ASSERT_EQUAL(threadCtx.WorkerId(), 42);
    }

    Y_UNIT_TEST(SetupAndClearTLS) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        TActorSystem* actorSystem = runtime.GetAnyNodeActorSystem();

        TWSExecutorContext ctx(7, actorSystem, nullptr);

        // Save current TLS state
        auto* prevThreadCtx = TlsThreadContext;

        ctx.SetupTLS();

        UNIT_ASSERT(TlsThreadContext != nullptr);
        UNIT_ASSERT_EQUAL(TlsThreadContext->WorkerId(), 7);
        UNIT_ASSERT_EQUAL(TlsThreadContext, &ctx.GetThreadCtx());

        ctx.ClearTLS();
        UNIT_ASSERT(TlsThreadContext == nullptr);

        // Restore previous TLS state.
        TlsThreadContext = prevThreadCtx;
    }

    Y_UNIT_TEST(MultipleContextsDifferentWorkerIds) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        TActorSystem* actorSystem = runtime.GetAnyNodeActorSystem();

        TWSExecutorContext ctx0(0, actorSystem, nullptr);
        TWSExecutorContext ctx1(1, actorSystem, nullptr);
        TWSExecutorContext ctx2(2, actorSystem, nullptr);

        UNIT_ASSERT_EQUAL(ctx0.GetThreadCtx().WorkerId(), 0);
        UNIT_ASSERT_EQUAL(ctx1.GetThreadCtx().WorkerId(), 1);
        UNIT_ASSERT_EQUAL(ctx2.GetThreadCtx().WorkerId(), 2);
    }

} // Y_UNIT_TEST_SUITE(WsExecutorContext)
