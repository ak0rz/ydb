#pragma once

#include <ydb/library/actors/core/executor_thread.h>
#include <ydb/library/actors/util/datetime.h>

#include <memory>

namespace NActors {
    class TMailbox;
    struct TActorClassStats;
} // namespace NActors

namespace NActors::NWorkStealing {

    class TBucketMap;
    class IStealIterator;
    struct TMailboxExecStats;
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

        ~TWSExecutorContext();

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
        // Returns EPopResult::Processed if an event was dispatched (more may exist).
        // Returns EPopResult::Empty if no event was available.
        // Returns EPopResult::Idle if an event was dispatched and the mailbox is now idle.
        // Writes the cycle counter to hpnowOut so callers can reuse it.
        EPopResult ExecuteSingleEvent(TMailbox* mailbox, NHPTimer::STime& hpnowOut);

        // Finalize the mailbox after ExecuteSingleEvent returns Empty.
        // Unlocks or frees the mailbox as appropriate.
        // Not needed when ExecuteSingleEvent returns Idle (mailbox already idle).
        void FinishMailbox(TMailbox* mailbox);

        void SetBucketMap(TBucketMap* bucketMap) { BucketMap_ = bucketMap; }
        void SetWsMailboxTable(TWsMailboxTable* table) { WsMailboxTable_ = table; }
        void SetSlotAllocator(TWsSlotAllocator* alloc) { SlotAllocator_ = alloc; }

        void SetStealIterator(std::unique_ptr<IStealIterator> iter);
        IStealIterator* GetStealIterator() const;

        // Commit any remaining local cursor events back to the mailbox's EventHead.
        // Called by EndBatch callback after each activation batch.
        void CommitLocalCursor();

        // Result of an activation batch.
        struct TActivationResult {
            ui32 EventsProcessed = 0;
            bool IsIdle = false; // true if mailbox went idle (do NOT access mailbox after)
            bool MailboxWasEmpty = false; // cached inside Pop callback before CAS
            ui32 Hint = 0; // cached mailbox hint for safe post-Idle reclamation
        };

        // Execute a mailbox activation batch. Dispatches events inside
        // the Pop callback (before the deferred CAS) so there is never
        // a window where two consumers touch the mailbox concurrently.
        // Does NOT finalize the mailbox — call FinalizeActivation after
        // (only when IsIdle is false).
        TActivationResult ExecuteActivation(
            TMailbox* mailbox,
            ui32 eventBudget,
            NHPTimer::STime deadline,
            NHPTimer::STime& hpnow,
            i16 slotIdx);

        // Finalize a mailbox after an activation batch (non-Idle path only).
        // Flushes accumulated stats, tries TryUnlock for idle transition,
        // and reclaims empty+idle mailboxes.
        // Slot affinity (LastPoolSlotIdx) is already stamped by ExecuteActivation.
        // Returns true if the mailbox is done (reclaimed or went idle).
        // Returns false if the mailbox needs rescheduling (events remain).
        bool FinalizeActivation(TMailbox* mailbox, NHPTimer::STime hpnow);

        // Flush accumulated stats without touching the mailbox queue.
        // Used when mailbox went idle inside Pop (CAS succeeded) and
        // queue must not be accessed further.
        void FlushStatsOnly(NHPTimer::STime hpnow);

        // Free a hint back to the slot allocator.
        void FreeHint(ui32 hint);

        // Do NOT start the thread.
        // TThread::Start() is never called.
    private:
        void DispatchEvent(TMailbox* mailbox, IEventHandle* ev, NHPTimer::STime& hpnow);
        void DispatchEvent(TMailbox* mailbox, IActor* actor, IEventHandle* ev, NHPTimer::STime& hpnow);
        // Batch accumulator for per-event stats.
        // Eliminates atomic operations on the hot path by accumulating
        // stats in plain variables and flushing once per mailbox batch.
        struct TStatsAccumulator {
            // Mailbox stats (flushed once per mailbox in FlushAllStats)
            TMailboxExecStats* MboxStats = nullptr;
            ui64 Events = 0;
            ui64 TotalExecCycles = 0;
            ui64 MaxExecCycles = 0;
            NHPTimer::STime FirstEventHpprev = 0;

            // Class stats (flushed on actor class change or at batch end)
            TActorClassStats* LastClassStats = nullptr;
            ui64 ClassMessages = 0;
            ui64 ClassCycles = 0;

            void Reset() {
                MboxStats = nullptr;
                Events = 0;
                TotalExecCycles = 0;
                MaxExecCycles = 0;
                FirstEventHpprev = 0;
                LastClassStats = nullptr;
                ClassMessages = 0;
                ClassCycles = 0;
            }
        };

        void FlushClassStats();
        void FlushAllStats(NHPTimer::STime hpnow);

        TBucketMap* BucketMap_ = nullptr;
        TWsMailboxTable* WsMailboxTable_ = nullptr;
        TWsSlotAllocator* SlotAllocator_ = nullptr;
        std::unique_ptr<IStealIterator> StealIterator_;
        TStatsAccumulator Accum_;
    };

} // namespace NActors::NWorkStealing
