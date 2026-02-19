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

        // Step 1: Pop and execute from our queue, one event at a time.
        // After each event, push the activation back if it has more events.
        // This interleaves activations from different mailboxes, preventing
        // starvation when an actor sends to itself.
        while (execBudget > 0) {
            auto activation = slot.Pop();
            if (!activation) {
                break;
            }

            slot.Executing.store(true, std::memory_order_release);
            bool hasMore = executeCallback(*activation);
            slot.Executing.store(false, std::memory_order_release);

            if (hasMore) {
                didWork = true;
                --execBudget;
                slot.Counters.Executions.fetch_add(1, std::memory_order_relaxed);
                slot.Push(*activation);
            }
            // If !hasMore: activation finalized (mailbox unlocked), no pushback.
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

                // Execute stolen items with same single-event model
                while (execBudget > 0) {
                    auto activation = slot.Pop();
                    if (!activation) {
                        break;
                    }

                    slot.Executing.store(true, std::memory_order_release);
                    bool hasMore = executeCallback(*activation);
                    slot.Executing.store(false, std::memory_order_release);

                    if (hasMore) {
                        didWork = true;
                        --execBudget;
                        slot.Counters.Executions.fetch_add(1, std::memory_order_relaxed);
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
