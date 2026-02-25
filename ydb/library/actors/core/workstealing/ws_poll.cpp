#include "ws_poll.h"
#include "ws_counters.h"
#include "ws_executor_context.h"
#include "ws_mailbox_table.h"

#include <ydb/library/actors/core/mailbox_lockfree.h>
#include <ydb/library/actors/util/datetime.h>

namespace NActors::NWorkStealing {

    namespace {
        struct TBatchResult {
            bool HasMore = false;
            ui32 EventsProcessed = 0;
        };

        // Execute a mailbox activation using TMailboxSnapshot.
        // Resolves hint → mailbox, processes events up to budget/time,
        // finalizes (stats, dead actor cleanup, idle transition).
        TBatchResult ExecuteActivationBatch(
            ui32 hint,
            TWsMailboxTable* wsTable,
            TWSExecutorContext* ctx,
            i16 slotIdx,
            ui32 eventBudget,
            NHPTimer::STime deadlineCycles,
            bool& didWork,
            TWsSlotCounters& counters,
            NHPTimer::STime& hpnow)
        {
            TBatchResult batch;
            if (!wsTable || !ctx) {
                return batch;
            }
            TMailbox* mailbox = wsTable->Get(hint);
            if (!mailbox) {
                return batch;
            }

            auto result = ctx->ExecuteActivation(mailbox, eventBudget, deadlineCycles, hpnow);
            batch.EventsProcessed = result.EventsProcessed;

            if (result.EventsProcessed > 0) {
                didWork = true;
                counters.Executions.fetch_add(result.EventsProcessed, std::memory_order_relaxed);
            }

            bool done = ctx->FinalizeActivation(mailbox, slotIdx, hpnow);
            batch.HasMore = !done;

            return batch;
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

    namespace {
        EPollResult PollSlotImpl(
            TSlot& slot,
            IStealIterator* stealIterator,
            const TWsConfig& config,
            TPollState& pollState,
            TWsMailboxTable* wsTable,
            TWSExecutorContext* ctx,
            i16 slotIdx)
        {
            bool didWork = false;
            // Phase 1: Take up to two activations — one from ring, one from queue.
            // Execute each sequentially with its own time budget.
            // Survivors go back to ring (or queue if single-event).
            std::optional<ui32> ringItem = pollState.Ring.Pop();
            std::optional<ui32> queueItem = slot.Pop();

            // Execute ring item first (proven hot from prior PollSlot)
            if (ringItem) {
                slot.Executing.store(true, std::memory_order_release);
                NHPTimer::STime hpnow = GetCycleCountFast();
                NHPTimer::STime deadline = hpnow + config.MailboxBatchCycles;

                auto batch = ExecuteActivationBatch(
                    *ringItem, wsTable, ctx, slotIdx,
                    config.MaxExecBatch, deadline, didWork, slot.Counters, hpnow);
                slot.Executing.store(false, std::memory_order_release);

                if (batch.HasMore) {
                    // Single-event activations (self-send / ping-pong) skip the
                    // ring and go back to the queue. This prevents self-senders
                    // from monopolizing the continuation ring indefinitely.
                    if (batch.EventsProcessed <= 1) {
                        slot.Push(*ringItem);
                    } else {
                        SaveContinuation(*ringItem, pollState.Ring, slot, slot.Counters);
                    }
                }
            }

            // Execute queue item
            if (queueItem) {
                slot.Executing.store(true, std::memory_order_release);
                NHPTimer::STime hpnow = GetCycleCountFast();
                NHPTimer::STime deadline = hpnow + config.MailboxBatchCycles;

                auto batch = ExecuteActivationBatch(
                    *queueItem, wsTable, ctx, slotIdx,
                    config.MaxExecBatch, deadline, didWork, slot.Counters, hpnow);
                slot.Executing.store(false, std::memory_order_release);

                if (batch.HasMore) {
                    if (batch.EventsProcessed <= 1) {
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

                    // Execute stolen items directly — not pushed to our queue
                    // first, so they can't be re-stolen while we process them.
                    size_t processed = 0;
                    for (size_t i = 0; i < stolen; ++i) {
                        slot.Executing.store(true, std::memory_order_release);
                        NHPTimer::STime hpnow = GetCycleCountFast();
                        NHPTimer::STime deadline = hpnow + config.MailboxBatchCycles;

                        auto batch = ExecuteActivationBatch(
                            stealBuf[i], wsTable, ctx, slotIdx,
                            config.MaxExecBatch, deadline, didWork, slot.Counters, hpnow);
                        slot.Executing.store(false, std::memory_order_release);

                        if (batch.HasMore) {
                            slot.Push(stealBuf[i]);
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
                pollState.StealInterval = std::min(pollState.StealInterval * 2,
                                                   config.StarvationGuardLimit * 64);
                pollState.NextStealAtIdle = pollState.ConsecutiveIdle + pollState.StealInterval;
            }

            // Nothing found anywhere — no local work, no stolen work.
            slot.Counters.IdlePolls.fetch_add(1, std::memory_order_relaxed);
            return EPollResult::Idle;
        }
    } // anonymous namespace

    EPollResult PollSlot(TSlot& slot, TWSExecutorContext& ctx) {
        auto& pollState = slot.GetPollState();
        auto* config = slot.Config;
        auto* wsTable = slot.WsMailboxTable;
        i16 slotIdx = slot.SlotIdx;
        IStealIterator* stealIterator = (slot.GetState() != ESlotState::Draining)
            ? ctx.GetStealIterator() : nullptr;
        return PollSlotImpl(slot, stealIterator, *config, pollState, wsTable, &ctx, slotIdx);
    }

    EPollResult PollSlot(TSlot& slot, IStealIterator* stealIterator) {
        auto& pollState = slot.GetPollState();
        static const TWsConfig defaultConfig;
        const TWsConfig& config = slot.Config ? *slot.Config : defaultConfig;
        return PollSlotImpl(slot, stealIterator, config, pollState, slot.WsMailboxTable, nullptr, slot.SlotIdx);
    }

} // namespace NActors::NWorkStealing
