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

        // Step 1: Drain injection queue into local deque
        size_t drained = slot.DrainInjectionQueue(config.MaxDrainBatch);
        slot.Counters.DrainedItems.fetch_add(drained, std::memory_order_relaxed);

        // Step 2: Pop and execute in a loop until deque is empty or budget exhausted.
        while (execBudget > 0) {
            auto activation = slot.PopActivation();
            if (!activation) {
                break;
            }
            didWork = true;
            --execBudget;
            slot.Counters.Executions.fetch_add(1, std::memory_order_relaxed);

            bool budgetDepleted = executeCallback(*activation);
            if (budgetDepleted) {
                slot.Reinject(*activation);
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

        // Step 3: Nothing local -- try stealing with exponential backoff.
        // First steal at StarvationGuardLimit idle polls, then double the interval on each failure.
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
                // Skip empty victims — avoids CAS overhead in StealHalf
                if (victim->SizeEstimate() == 0) {
                    continue;
                }
                slot.Counters.StealAttempts.fetch_add(1, std::memory_order_relaxed);
                size_t stolen = victim->StealHalf(stealBuf, kStealBufSize);
                if (stolen > 0) {
                    slot.Counters.StolenItems.fetch_add(stolen, std::memory_order_relaxed);

                    // Push stolen items into our local deque
                    for (size_t i = 0; i < stolen; ++i) {
                        if (!slot.PushStolen(stealBuf[i])) {
                            for (size_t j = i; j < stolen; ++j) {
                                slot.Reinject(stealBuf[j]);
                            }
                            break;
                        }
                    }

                    // Execute stolen items
                    while (execBudget > 0) {
                        auto activation = slot.PopActivation();
                        if (!activation) {
                            break;
                        }
                        didWork = true;
                        --execBudget;
                        slot.Counters.Executions.fetch_add(1, std::memory_order_relaxed);

                        bool budgetDepleted = executeCallback(*activation);
                        if (budgetDepleted) {
                            slot.Reinject(*activation);
                        }
                    }

                    if (didWork) {
                        slot.Counters.BusyPolls.fetch_add(1, std::memory_order_relaxed);
                        pollState.ConsecutiveIdle = 0;
                        pollState.NextStealAtIdle = 0;
                        pollState.StealInterval = 0;
                        pollState.HadLocalWork = false; // stolen work, not local
                        return EPollResult::Busy;
                    }
                }
            }

            // Steal was attempted but found nothing — exponential backoff
            pollState.StealInterval = std::min(pollState.StealInterval * 2,
                                               config.StarvationGuardLimit * 64);
            pollState.NextStealAtIdle = pollState.ConsecutiveIdle + pollState.StealInterval;
        }

        // Step 4: Nothing found anywhere
        slot.Counters.IdlePolls.fetch_add(1, std::memory_order_relaxed);
        return EPollResult::Idle;
    }

} // namespace NActors::NWorkStealing
