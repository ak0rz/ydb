#include "ws_poll.h"

namespace NActors::NWorkStealing {

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
        while (execBudget > 0) {
            auto activation = slot.Pop();
            if (!activation) {
                break;
            }
            didWork = true;
            --execBudget;
            slot.Counters.Executions.fetch_add(1, std::memory_order_relaxed);

            bool budgetDepleted = executeCallback(*activation);
            if (budgetDepleted) {
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
                if (victim->SizeEstimate() == 0) {
                    continue;
                }
                slot.Counters.StealAttempts.fetch_add(1, std::memory_order_relaxed);

                // Tight pop loop: pull from victim into stack buffer
                size_t stolen = victim->StealHalf(stealBuf, kStealBufSize);
                if (stolen == 0) {
                    continue;
                }
                slot.Counters.StolenItems.fetch_add(stolen, std::memory_order_relaxed);

                // Tight push loop: transfer buffer into our queue
                for (size_t i = 0; i < stolen; ++i) {
                    slot.Push(stealBuf[i]);
                }

                // Execute from our queue
                while (execBudget > 0) {
                    auto activation = slot.Pop();
                    if (!activation) {
                        break;
                    }
                    didWork = true;
                    --execBudget;
                    slot.Counters.Executions.fetch_add(1, std::memory_order_relaxed);

                    bool budgetDepleted = executeCallback(*activation);
                    if (budgetDepleted) {
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
