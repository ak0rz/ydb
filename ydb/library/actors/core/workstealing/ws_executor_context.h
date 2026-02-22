#pragma once

#include <ydb/library/actors/core/executor_thread.h>

namespace NActors {
    class TMailbox;
} // namespace NActors

namespace NActors::NWorkStealing {

    class TBucketMap;
    class TWsMailboxTable;
    class TWsSlotAllocator;

    // TWSExecutorContext inherits TExecutorThread but is never started as a thread.
    //
    // Each Driver worker holds one TWSExecutorContext per assigned pool.
    // Worker threads call SetupTLS() at start and ClearTLS() at end.
    // PollSlot calls ExecuteMailbox() per activation.
    //
    // All existing code paths work unchanged:
    //   TActivationContext::Send() -> ExecutorThread.Send() -> ActorSystem->Send()
    //   No virtual dispatch, no branching on the hot path.
    class TWSExecutorContext: public TExecutorThread {
    public:
        // workerId: identifies this worker
        // actorSystem: the actor system (must outlive this context)
        // pool: the executor pool this context is associated with
        TWSExecutorContext(
            TWorkerId workerId,
            TActorSystem* actorSystem,
            IExecutorPool* pool);

        // Access the thread context for TLS setup
        TThreadContext& GetThreadCtx() {
            return ThreadCtx;
        }

        // Initialize TLS for the current worker thread.
        // Must be called once at thread start, before any ExecuteMailbox calls.
        void SetupTLS();

        // Clear TLS for the current worker thread.
        // Must be called once at thread end.
        void ClearTLS();

        // Execute a single event from the mailbox.
        // Returns true if an event was processed (more might remain).
        // Returns false if no event was available (Pop() returned nullptr).
        // Does NOT finalize the mailbox — caller must call FinishMailbox
        // when ExecuteSingleEvent returns false.
        // Writes the cycle counter (from GetCycleCountFast() already used
        // for stats) to hpnowOut, so callers can reuse it.
        bool ExecuteSingleEvent(TMailbox* mailbox, NHPTimer::STime& hpnowOut);

        // Finalize the mailbox after the last ExecuteSingleEvent returns false.
        // Unlocks or frees the mailbox as appropriate.
        void FinishMailbox(TMailbox* mailbox);

        void SetBucketMap(TBucketMap* bucketMap) { BucketMap_ = bucketMap; }
        void SetWsMailboxTable(TWsMailboxTable* table) { WsMailboxTable_ = table; }
        void SetSlotAllocator(TWsSlotAllocator* alloc) { SlotAllocator_ = alloc; }

        // Do NOT start the thread.
        // TThread::Start() is never called.
    private:
        TBucketMap* BucketMap_ = nullptr;
        TWsMailboxTable* WsMailboxTable_ = nullptr;
        TWsSlotAllocator* SlotAllocator_ = nullptr;
    };

} // namespace NActors::NWorkStealing
