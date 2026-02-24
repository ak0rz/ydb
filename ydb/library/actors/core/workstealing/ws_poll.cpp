#include "ws_poll.h"
#include "ws_counters.h"

#include <ydb/library/actors/util/datetime.h>

namespace NActors::NWorkStealing {

    namespace {
        // Execute events from a single mailbox until one of three stop conditions:
        //   (a) Mailbox drained: callback returns false (mailbox finalized/unlocked)
        //   (b) Time budget expired: hpnow >= deadlineCycles
        //   (c) Event budget exhausted: execBudget reaches 0
        //
        // execBudget is decremented per event. hpnow is updated by the callback
        // after each event (via internal GetCycleCountFast()), so the deadline
        // check is always fresh without extra rdtsc calls.
        //
        // Returns true if the mailbox still has events (conditions b or c).
        // Returns false if drained (condition a). Caller decides whether to
        // push the activation back or save it as a continuation.
        bool ExecuteBatch(
            const TExecuteCallback& executeCallback,
            ui32 activation,
            NHPTimer::STime deadlineCycles,
            size_t& execBudget,
            bool& didWork,
            TWsSlotCounters& counters,
            NHPTimer::STime& hpnow,
            const TEndBatchCallback& endBatchCallback)
        {
            bool hasMore = false;
            size_t localExecs = 0;
            while (execBudget > 0) {
                hasMore = executeCallback(activation, hpnow);
                if (!hasMore) {
                    break;
                }
                didWork = true;
                --execBudget;
                ++localExecs;
                if (hpnow >= deadlineCycles) {
                    break;
                }
            }
            if (localExecs > 0)
                counters.Executions.fetch_add(localExecs, std::memory_order_relaxed);
            // Commit local batch cursor before the hint can leave this slot
            if (endBatchCallback)
                endBatchCallback(activation);
            return hasMore;
        }

        // Try to save an activation that still has work.
        // Prefer ring (hot L1 path), fall back to queue.
        void SaveContinuation(ui32 activation, TContinuationRing& ring,
                              TSlot& slot, TWsSlotCounters& counters) {
            if (!ring.Push(activation)) {
                slot.Push(activation);
                counters.RingOverflows.fetch_add(1, std::memory_order_relaxed);
            } else {
                counters.HotContinuations.fetch_add(1, std::memory_order_relaxed);
            }
        }
    } // anonymous namespace

    EPollResult PollSlot(
        TSlot& slot,
        IStealIterator* stealIterator,
        const TExecuteCallback& executeCallback,
        const TOverflowCallback& overflowCallback,
        const TWsConfig& config,
        TPollState& pollState,
        const TBeginBatchCallback& beginBatchCallback,
        const TEndBatchCallback& endBatchCallback)
    {
        Y_UNUSED(overflowCallback);  // reserved for future active load-shedding

        bool didWork = false;
        // Phase 1: Take up to two activations — one from ring, one from queue.
        // Execute each sequentially with its own time budget.
        // Survivors go back to ring (or queue if ring full).
        std::optional<ui32> ringItem = pollState.Ring.Pop();
        std::optional<ui32> queueItem = slot.Pop();

        // Execute ring item first (proven hot from prior PollSlot)
        if (ringItem) {
            ui64 preBatchCount = beginBatchCallback ? beginBatchCallback(*ringItem) : 0;
            size_t execBudget = config.MaxExecBatch;
            slot.Executing.store(true, std::memory_order_release);
            NHPTimer::STime hpnow = GetCycleCountFast();
            NHPTimer::STime deadline = hpnow + config.MailboxBatchCycles;
            bool hasMore = ExecuteBatch(executeCallback, *ringItem, deadline,
                                        execBudget, didWork, slot.Counters, hpnow,
                                        endBatchCallback);
            slot.Executing.store(false, std::memory_order_release);

            if (hasMore) {
                // Single-event activations (self-send / ping-pong) skip the
                // ring and go back to the queue. This prevents self-senders
                // from monopolizing the continuation ring indefinitely.
                // Only applies when beginBatchCallback is present (provides
                // the drain count). Without it, always use ring.
                if (beginBatchCallback && preBatchCount <= 1) {
                    slot.Push(*ringItem);
                } else {
                    SaveContinuation(*ringItem, pollState.Ring, slot, slot.Counters);
                }
            }
        }

        // Execute queue item
        if (queueItem) {
            ui64 preBatchCount = beginBatchCallback ? beginBatchCallback(*queueItem) : 0;
            size_t execBudget = config.MaxExecBatch;
            slot.Executing.store(true, std::memory_order_release);
            NHPTimer::STime hpnow = GetCycleCountFast();
            NHPTimer::STime deadline = hpnow + config.MailboxBatchCycles;
            bool hasMore = ExecuteBatch(executeCallback, *queueItem, deadline,
                                        execBudget, didWork, slot.Counters, hpnow,
                                        endBatchCallback);
            slot.Executing.store(false, std::memory_order_release);

            if (hasMore) {
                if (beginBatchCallback && preBatchCount <= 1) {
                    slot.Push(*queueItem);
                } else {
                    SaveContinuation(*queueItem, pollState.Ring, slot, slot.Counters);
                }
            }
        }

        // Update ring occupancy + content snapshot for router/diagnostic visibility
        slot.ContinuationCount.store(pollState.Ring.Size(), std::memory_order_relaxed);
        pollState.Ring.SnapshotTo(slot);

        if (didWork) {
            slot.Counters.BusyPolls.fetch_add(1, std::memory_order_relaxed);
            pollState.ConsecutiveIdle = 0;
            pollState.NextStealAtIdle = 0;
            pollState.StealInterval = 0;
            pollState.HadLocalWork = true;
            return EPollResult::Busy;
        }

        // No work found locally. Try stealing from neighbor
        // slots with exponential backoff on consecutive idle polls.
        ++pollState.ConsecutiveIdle;

        if (pollState.NextStealAtIdle == 0) {
            pollState.NextStealAtIdle = config.StarvationGuardLimit;
            pollState.StealInterval = config.StarvationGuardLimit;
        }

        bool shouldSteal = (pollState.ConsecutiveIdle >= pollState.NextStealAtIdle);

        if (shouldSteal && stealIterator) {
            constexpr size_t kStealBufSize = 1;
            ui32 stealBuf[kStealBufSize];

            stealIterator->Reset();
            while (TSlot* victim = stealIterator->Next()) {
                if (victim->SizeEstimate() == 0 ||
                    !victim->Executing.load(std::memory_order_acquire)) {
                    continue;
                }
                slot.Counters.StealAttempts.fetch_add(1, std::memory_order_relaxed);

                size_t stolen = victim->Steal(stealBuf, kStealBufSize,
                                              config.MailboxBatchCycles);
                if (stolen == 0) {
                    continue;
                }
                slot.Counters.StolenItems.fetch_add(stolen, std::memory_order_relaxed);

                // Execute directly from the steal buffer — items are not
                // pushed to our queue first, so they can't be re-stolen
                // while we process them. Only items that still have events
                // after ExecuteBatch are pushed to our local queue.
                size_t execBudget = config.MaxExecBatch;
                NHPTimer::STime hpnow = GetCycleCountFast();
                size_t processed = 0;
                for (size_t i = 0; i < stolen && execBudget > 0; ++i) {
                    if (beginBatchCallback) (void)beginBatchCallback(stealBuf[i]);
                    slot.Executing.store(true, std::memory_order_release);
                    NHPTimer::STime deadline = hpnow + config.MailboxBatchCycles;
                    bool hasMore = ExecuteBatch(
                        executeCallback, stealBuf[i], deadline,
                        execBudget, didWork, slot.Counters, hpnow,
                        endBatchCallback);
                    slot.Executing.store(false, std::memory_order_release);

                    if (hasMore) {
                        slot.Push(stealBuf[i]);  // push back only if mailbox still has events
                    }
                    processed = i + 1;
                }
                // Spill back unprocessed stolen items — they were already
                // popped from the victim's queue, so we must re-queue them
                // to prevent activation loss.
                for (size_t i = processed; i < stolen; ++i) {
                    slot.Push(stealBuf[i]);
                }

                if (didWork) {
                    slot.Counters.BusyPolls.fetch_add(1, std::memory_order_relaxed);
                    pollState.ConsecutiveIdle = 0;
                    pollState.NextStealAtIdle = 0;
                    pollState.StealInterval = 0;
                    pollState.HadLocalWork = false;
                    return EPollResult::Busy;
                }
            }

            // Steal attempted but found nothing — exponential backoff.
            // Double the interval between steal attempts, capped at
            // StarvationGuardLimit × 64. This prevents idle workers
            // from burning cycles probing empty neighbors.
            pollState.StealInterval = std::min(pollState.StealInterval * 2,
                                               config.StarvationGuardLimit * 64);
            pollState.NextStealAtIdle = pollState.ConsecutiveIdle + pollState.StealInterval;
        }

        // Nothing found anywhere — no local work, no stolen work.
        slot.Counters.IdlePolls.fetch_add(1, std::memory_order_relaxed);
        return EPollResult::Idle;
    }

} // namespace NActors::NWorkStealing
