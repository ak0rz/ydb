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

        // Phase 1: One-shot hot continuation.
        //
        // If the previous PollSlot saved a hot continuation (a mailbox that
        // exhausted Phase 2's event budget while still having work), process
        // it first with a fresh MaxExecBatch budget.
        //
        // ONE-SHOT: The continuation is consumed here and NOT saved back.
        // If it still has events after this budget, it's pushed to the
        // queue tail for fair interleaving in Phase 2. Only Phase 2's
        // leftover can become the next continuation.
        //
        // Why not persistent? Saving it back after Phase 1 synchronizes
        // all workers: every PollSlot call starts with the same hot sender,
        // causing N threads to CAS-push events to the same receiver queue
        // simultaneously. Under 8-way contention, per-event cost inflates
        // from ~700 to ~6000 cycles (cache-line bouncing), hitting the
        // MailboxBatchCycles deadline after ~8 events instead of 64.
        // One-shot breaks this synchronization: workers desynchronize in
        // Phase 2, processing different queue depths at different times.
        std::optional<ui32> activation = std::exchange(pollState.HotContinuation, std::nullopt);
        if (activation) {
            size_t contBudget = config.MaxExecBatch;

            slot.Executing.store(true, std::memory_order_release);
            NHPTimer::STime hpnow = GetCycleCountFast();
            NHPTimer::STime deadline = hpnow + config.MailboxBatchCycles;

            bool hasMore = ExecuteBatch(
                executeCallback, *activation, deadline,
                contBudget, didWork, slot.Counters, hpnow);

            slot.Executing.store(false, std::memory_order_release);

            if (hasMore) {
                slot.Push(*activation);
            }
            activation.reset();
        }

        // Phase 2: Queue processing with inline interleaving.
        //
        // Pop activations from the MPMC queue and execute with a fresh
        // MaxExecBatch budget (independent of Phase 1's budget, so total
        // work per PollSlot is bounded at 2 × MaxExecBatch).
        //
        // Each mailbox runs for up to MailboxBatchCycles. When the time
        // deadline expires, we check the queue inline:
        //   - Other work waiting: push current activation, pop next (swap).
        //     Interleaves activations fairly without leaving the inner loop.
        //   - Queue empty: reset deadline, continue same mailbox. Avoids
        //     unnecessary push/pop overhead and false steal potential
        //     (items briefly visible in queue during push-pop round-trip).
        //
        // If execBudget is exhausted while a mailbox still has events,
        // that mailbox exits the loop and is saved as HotContinuation
        // for the next PollSlot call.
        size_t execBudget = config.MaxExecBatch;
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
        // Save Phase 2 leftover as hot continuation for the next PollSlot.
        // Only Phase 2 can create continuations — Phase 1 is one-shot.
        if (activation) {
            pollState.HotContinuation = activation;
            slot.Counters.HotContinuations.fetch_add(1, std::memory_order_relaxed);
        }

        if (didWork) {
            slot.Counters.BusyPolls.fetch_add(1, std::memory_order_relaxed);
            pollState.ConsecutiveIdle = 0;
            pollState.NextStealAtIdle = 0;
            pollState.StealInterval = 0;
            pollState.HadLocalWork = true;
            return EPollResult::Busy;
        }

        // No work found in Phase 1 or Phase 2. Try stealing from neighbor
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
