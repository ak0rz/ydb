#include "ws_poll.h"
#include "ws_counters.h"

#include <ydb/library/actors/util/datetime.h>

namespace NActors::NWorkStealing {

    namespace {
        // Execute events from one mailbox until the time budget expires
        // or the mailbox is drained. Returns true if the mailbox still
        // has events (caller should push back).
        bool ExecuteBatch(
            const TExecuteCallback& executeCallback,
            ui32 activation,
            NHPTimer::STime deadlineCycles,
            size_t& execBudget,
            bool& didWork,
            TWsSlotCounters& counters,
            NHPTimer::STime& hpnow)
        {
            bool hasMore = false;
            while (execBudget > 0) {
                hasMore = executeCallback(activation, hpnow);
                if (!hasMore) {
                    break;
                }
                didWork = true;
                --execBudget;
                counters.Executions.fetch_add(1, std::memory_order_relaxed);
                if (hpnow >= deadlineCycles) {
                    break;
                }
            }
            return hasMore;
        }
    } // anonymous namespace

    EPollResult PollSlot(
        TSlot& slot,
        IStealIterator* stealIterator,
        const TExecuteCallback& executeCallback,
        const TWsConfig& config,
        TPollState& pollState)
    {
        bool didWork = false;
        size_t execBudget = config.MaxExecBatch;

        // Step 1: Pop and execute from our queue.
        // Each mailbox runs for up to MailboxBatchCycles before checking
        // the queue. If the queue is empty, continue the same mailbox
        // without bouncing it (avoids false steal potential and redundant
        // mailbox table walks).
        std::optional<ui32> activation;
        while (execBudget > 0) {
            if (!activation) {
                activation = slot.Pop();
                if (!activation) {
                    break;
                }
            }

            slot.Executing.store(true, std::memory_order_release);
            NHPTimer::STime hpnow = GetCycleCountFast();
            NHPTimer::STime deadline = hpnow + config.MailboxBatchCycles;

            bool hasMore = false;
            for (;;) {
                hasMore = executeCallback(*activation, hpnow);
                if (!hasMore) {
                    break;
                }
                didWork = true;
                --execBudget;
                slot.Counters.Executions.fetch_add(1, std::memory_order_relaxed);
                if (execBudget == 0) {
                    break;
                }
                if (hpnow >= deadline) {
                    // Time slice expired — check if queue has other work
                    auto next = slot.Pop();
                    if (next) {
                        slot.Push(*activation);
                        activation = next;
                        break;
                    }
                    // Queue empty — continue same mailbox, reset deadline
                    deadline = hpnow + config.MailboxBatchCycles;
                }
            }

            slot.Executing.store(false, std::memory_order_release);

            if (!hasMore) {
                activation.reset();
            }
        }
        if (activation) {
            slot.Push(*activation);
        }

        if (didWork) {
            slot.Counters.BusyPolls.fetch_add(1, std::memory_order_relaxed);
            pollState.ConsecutiveIdle = 0;
            pollState.NextStealAtIdle = 0;
            pollState.StealInterval = 0;
            pollState.HadLocalWork = true;
            return EPollResult::Busy;
        }

        // Step 2: Nothing local -- try stealing with exponential backoff.
        ++pollState.ConsecutiveIdle;

        if (pollState.NextStealAtIdle == 0) {
            pollState.NextStealAtIdle = config.StarvationGuardLimit;
            pollState.StealInterval = config.StarvationGuardLimit;
        }

        bool shouldSteal = (pollState.ConsecutiveIdle >= pollState.NextStealAtIdle);

        if (shouldSteal && stealIterator) {
            constexpr size_t kStealBufSize = 128;
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

                // Execute directly from steal buffer — no reinjection.
                // Stolen items can't be re-stolen while we process them.
                NHPTimer::STime hpnow = GetCycleCountFast();
                for (size_t i = 0; i < stolen && execBudget > 0; ++i) {
                    slot.Executing.store(true, std::memory_order_release);
                    NHPTimer::STime deadline = hpnow + config.MailboxBatchCycles;
                    bool hasMore = ExecuteBatch(
                        executeCallback, stealBuf[i], deadline,
                        execBudget, didWork, slot.Counters, hpnow);
                    slot.Executing.store(false, std::memory_order_release);

                    if (hasMore) {
                        slot.Push(stealBuf[i]);  // push back only if mailbox still has events
                    }
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

            // Steal was attempted but found nothing — exponential backoff
            pollState.StealInterval = std::min(pollState.StealInterval * 2,
                                               config.StarvationGuardLimit * 64);
            pollState.NextStealAtIdle = pollState.ConsecutiveIdle + pollState.StealInterval;
        }

        // Step 3: Nothing found anywhere
        slot.Counters.IdlePolls.fetch_add(1, std::memory_order_relaxed);
        return EPollResult::Idle;
    }

} // namespace NActors::NWorkStealing
