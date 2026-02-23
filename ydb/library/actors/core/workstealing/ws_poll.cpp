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
            NHPTimer::STime& hpnow)
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
            return hasMore;
        }
    } // anonymous namespace

    EPollResult PollSlot(
        TSlot& slot,
        IStealIterator* stealIterator,
        const TExecuteCallback& executeCallback,
        const TOverflowCallback& overflowCallback,
        const TWsConfig& config,
        TPollState& pollState)
    {
        Y_UNUSED(overflowCallback);  // reserved for future active load-shedding

        bool didWork = false;

        // Queue processing with continuation ring seeding.
        //
        // The ring holds activations that exhausted the entire event budget
        // on a previous PollSlot — proven hottest. One ring item is popped
        // to seed the loop, giving it first-execution priority. Remaining
        // ring items are consumed in subsequent PollSlots.
        //
        // Each mailbox runs for up to MailboxBatchCycles. When the time
        // deadline expires, we check the queue inline:
        //   - Other work waiting: push current to queue tail, pop next.
        //     Only budget-exhaustion leftovers go to ring (not time swaps).
        //   - Queue empty: reset deadline, continue same mailbox. Avoids
        //     unnecessary overhead and false steal potential.
        //
        // When ring is empty: seed falls through to slot.Pop() — identical
        // to the pre-ring code path. Zero overhead.
        std::optional<ui32> activation = pollState.Ring.Pop();
        size_t execBudget = config.MaxExecBatch;
        size_t localExecs = 0;
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
                ++localExecs;
                if (execBudget == 0) {
                    break;
                }
                if (hpnow >= deadline) {
                    // Time slice expired — check if queue has other work
                    auto next = slot.Pop();
                    if (next) {
                        // Push current to queue tail for fair interleaving.
                        // Only budget-exhaustion leftovers go to the ring
                        // (proven hottest — survived the entire budget).
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
        if (localExecs > 0)
            slot.Counters.Executions.fetch_add(localExecs, std::memory_order_relaxed);

        // Save leftover to ring for next PollSlot's seeding.
        if (activation) {
            if (!pollState.Ring.Push(*activation)) {
                slot.Push(*activation);
                slot.Counters.RingOverflows.fetch_add(1, std::memory_order_relaxed);
            } else {
                slot.Counters.HotContinuations.fetch_add(1, std::memory_order_relaxed);
            }
        }

        // Update ring occupancy for router visibility
        slot.ContinuationCount.store(pollState.Ring.Size(), std::memory_order_relaxed);

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

                // Execute directly from the steal buffer — items are not
                // pushed to our queue first, so they can't be re-stolen
                // while we process them. Only items that still have events
                // after ExecuteBatch are pushed to our local queue.
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
