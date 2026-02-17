#include "ws_executor_context.h"

#include <ydb/library/actors/core/mailbox_lockfree.h>
#include <ydb/library/actors/util/datetime.h>

namespace NActors::NWorkStealing {

    TWSExecutorContext::TWSExecutorContext(
        TWorkerId workerId,
        TActorSystem* actorSystem,
        IExecutorPool* pool)
        : TExecutorThread(workerId, actorSystem, pool, TString("ws_ctx"))
    {
    }

    void TWSExecutorContext::SetupTLS() {
        ThreadCtx.ExecutionStats = &ExecutionStats;
        ThreadCtx.ActivityContext.ActorSystemIndex = ActorSystemIndex;
        ThreadCtx.ActivityContext.ElapsingActorActivity = ActorSystemIndex;
        NHPTimer::STime now = GetCycleCountFast();
        ThreadCtx.ActivityContext.StartOfProcessingEventTS = now;
        ThreadCtx.ActivityContext.ActivationStartTS = now;
        TlsThreadContext = &ThreadCtx;
    }

    void TWSExecutorContext::ClearTLS() {
        TlsThreadContext = nullptr;
    }

    bool TWSExecutorContext::ExecuteMailbox(TMailbox* mailbox) {
        Execute(mailbox, false);
        return false;
    }

} // namespace NActors::NWorkStealing
