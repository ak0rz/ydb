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
            uint64_t deadlineCycles,
            size_t& execBudget,
            bool& didWork,
            TWsSlotCounters& counters)
        {
            bool hasMore = false;
            while (execBudget > 0) {
                hasMore = executeCallback(activation);
                if (!hasMore) {
                    break;
                }
                didWork = true;
                --execBudget;
                counters.Executions.fetch_add(1, std::memory_order_relaxed);
                if (GetCycleCountFast() >= deadlineCycles) {
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
        // Process events from each mailbox for up to MailboxBatchCycles
        // before push-back. This amortizes MPMC queue overhead for hot
        // mailboxes while still interleaving activations.
        while (execBudget > 0) {
            auto activation = slot.Pop();
            if (!activation) {
                break;
            }

            slot.Executing.store(true, std::memory_order_release);
            uint64_t deadline = GetCycleCountFast() + config.MailboxBatchCycles;
            bool hasMore = ExecuteBatch(
                executeCallback, *activation, deadline,
                execBudget, didWork, slot.Counters);
            slot.Executing.store(false, std::memory_order_release);

            if (hasMore) {
                slot.Push(*activation);
            }
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

                size_t stolen = victim->StealHalf(stealBuf, kStealBufSize);
                if (stolen == 0) {
                    continue;
                }
                slot.Counters.StolenItems.fetch_add(stolen, std::memory_order_relaxed);

                for (size_t i = 0; i < stolen; ++i) {
                    slot.Push(stealBuf[i]);
                }

                // Execute stolen items with same batched model
                while (execBudget > 0) {
                    auto activation = slot.Pop();
                    if (!activation) {
                        break;
                    }

                    slot.Executing.store(true, std::memory_order_release);
                    uint64_t deadline = GetCycleCountFast() + config.MailboxBatchCycles;
                    bool hasMore = ExecuteBatch(
                        executeCallback, *activation, deadline,
                        execBudget, didWork, slot.Counters);
                    slot.Executing.store(false, std::memory_order_release);

                    if (hasMore) {
                        slot.Push(*activation);
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
