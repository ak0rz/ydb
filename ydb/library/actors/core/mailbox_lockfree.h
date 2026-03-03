#pragma once

#include "defs.h"
#include "event.h"
#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>
#include <atomic>
#include <memory>
#include <mutex>

namespace NActors {

    class IActor;
    class IExecutorPool;

    enum class EMailboxPush {
        Locked,
        Pushed,
    };

    struct TMailboxStats {
        ui64 ElapsedCycles = 0;
    };

    enum class EPopResult : ui8 {
        Empty,       // No event available (empty or incomplete push)
        Processed,   // Event processed, more events may exist
        Idle,        // Event processed, queue is now idle
    };

    enum class ESnapshotResult {
        Idle,
        NeedsReschedule,
    };

    class TMailboxSnapshot;

    class alignas(64) TMailbox {
        friend class TMailboxSnapshot;

    private:
        enum class EActorPack : ui8 {
            Empty = 0,
            Simple = 1,
            Array = 2,
            Map = 3,
        };

        struct TActorEmpty {
            // Points to the next free mailbox in the same free block
            TMailbox* NextFree = nullptr;
            // Points to the next complete free block
            TMailbox* NextFreeBlock = nullptr;
        };

        struct TActorPair {
            IActor* Actor;
            ui64 ActorId;
        };

        static constexpr ui64 ArrayCapacity = 8;

        struct alignas(64) TActorArray {
            TActorPair Actors[ArrayCapacity];
        };

        struct TActorMap : public absl::flat_hash_map<ui64, IActor*> {
            absl::flat_hash_map<ui64, IActor*> Aliases;
            TMailboxStats Stats;
        };

        union TActorsInfo {
            TActorEmpty Empty;
            TActorPair Simple;
            struct {
                TActorArray* ActorsArray;
                ui64 ActorsCount;
            } Array;
            struct {
                TActorMap* ActorsMap;
            } Map;
        };

    public:
        bool IsEmpty() const {
            return ActorPack == EActorPack::Empty;
        }

        template<typename T>
        void ForEach(T&& callback) noexcept {
            switch (ActorPack) {
                case EActorPack::Empty:
                    break;

                case EActorPack::Simple:
                    callback(ActorsInfo.Simple.ActorId, ActorsInfo.Simple.Actor);
                    break;

                case EActorPack::Array:
                    for (ui64 i = 0; i < ActorsInfo.Array.ActorsCount; ++i) {
                        auto& entry = ActorsInfo.Array.ActorsArray->Actors[i];
                        callback(entry.ActorId, entry.Actor);
                    }
                    break;

                case EActorPack::Map:
                    for (const auto& [actorId, actor] : *ActorsInfo.Map.ActorsMap) {
                        callback(actorId, actor);
                    }
                    break;
            }
        }

        IActor* FindActor(ui64 localActorId) noexcept;
        void AttachActor(ui64 localActorId, IActor* actor) noexcept;
        IActor* DetachActor(ui64 localActorId) noexcept;

        IActor* FindAlias(ui64 localActorId) noexcept;
        void AttachAlias(ui64 localActorId, IActor* actor) noexcept;
        IActor* DetachAlias(ui64 localActorId) noexcept;

        /**
         * Looks up the actor for the event's recipient: tries FindActor
         * first, then FindAlias (with Rewrite on hit). Returns null
         * when no actor or alias matches (non-delivery case).
         */
        IActor* ResolveActor(IEventHandle* ev) noexcept;

        void EnableStats();
        void AddElapsedCycles(ui64);
        std::optional<ui64> GetElapsedCycles();
        std::optional<double> GetElapsedSeconds();

        bool CleanupActors() noexcept;
        bool CleanupEvents() noexcept;
        bool Cleanup() noexcept;
        ~TMailbox() noexcept;

        TMailbox() {
            // Initialize Vyukov MPSC queue sentinels
            Head.store(StubPtr(), std::memory_order_relaxed);
            Tail.store(reinterpret_cast<uintptr_t>(StubPtr()), std::memory_order_relaxed);
            Stub.store(0, std::memory_order_relaxed);
        }
        TMailbox(const TMailbox&) = delete;
        TMailbox& operator=(const TMailbox&) = delete;

    public:
        /**
         * Pushes ev to the mailbox. Returns EMailboxPush::Locked when the
         * queue was idle (prev == StubPtr) — caller must schedule execution.
         * Returns EMailboxPush::Pushed when events already existed.
         *
         * Vyukov MPSC queue: wait-free (single xchg + one release store).
         * Exactly one Push per idle period gets StubPtr as prev (xchg
         * atomicity), so exactly one caller schedules.
         */
        EMailboxPush Push(std::unique_ptr<IEventHandle>& ev) noexcept;

        /**
         * Pops and processes one event from the mailbox.
         *
         * Resolves the recipient actor: FindActor, then FindAlias
         * with Rewrite if no direct actor is found. Calls
         * process(actor, ev) where actor is null for non-delivery.
         *
         * The callback receives a non-owning IEventHandle*; the
         * callback is responsible for wrapping it in TAutoPtr to
         * manage lifetime.
         *
         * Callback signature:
         *   void(IActor* actorOrNull, IEventHandle* ev)
         *
         * Returns EPopResult::Empty if no event was available,
         * EPopResult::Processed if an event was processed and more may exist,
         * EPopResult::Idle if the event was processed and the queue is now empty.
         */
        template<typename F>
        EPopResult Pop(F&& process) noexcept;

        /**
         * Counts the number of events for the given localActorId.
         * Walks the chain from Head through linked next pointers.
         */
        std::pair<ui32, ui32> CountMailboxEvents(ui64 localActorId, ui32 maxTraverse) noexcept;

        /**
         * Checks if the mailbox queue is empty (idle).
         *
         * Returns true only when Head is at stub, stub has no pending link,
         * and Tail is at StubPtr. Double-checks for Push races.
         */
        bool TryUnlock() noexcept;

        /**
         * Pushes ev to the front of the mailbox, which must be locked.
         * Used by test framework to inject events at the head of the queue.
         */
        void PushFront(std::unique_ptr<IEventHandle>&& ev) noexcept;


        /**
         * Tries to unlock and schedules for execution on failure
         */
        void Unlock(IExecutorPool* pool, NHPTimer::STime now, ui64& revolvingCounter);

        /**
         * Returns true when an empty mailbox can be reclaimed
         */
        bool CanReclaim() const noexcept {
            return Head.load(std::memory_order_relaxed) == StubPtr();
        }

        /**
         * Returns a pointer to the stub node, used as sentinel.
         */
        IEventHandle* StubPtr() const noexcept {
            return reinterpret_cast<IEventHandle*>(const_cast<std::atomic<uintptr_t>*>(&Stub));
        }

    private:
        static IEventHandle* GetNextPtrAcquire(IEventHandle* ev) noexcept {
            return reinterpret_cast<IEventHandle*>(ev->NextLinkPtr.load(std::memory_order_acquire));
        }

        static void SetNextPtr(IEventHandle* ev, IEventHandle* next) noexcept {
            ev->NextLinkPtr.store(reinterpret_cast<uintptr_t>(next), std::memory_order_relaxed);
        }

        void EnsureActorMap();
        void DrainAndDelete(uintptr_t tail) noexcept;
        void CleanupActor(IActor* actor) noexcept;

    public:
        ui32 Hint = 0;

        EActorPack ActorPack = EActorPack::Empty;

        // Set by Pop when the last-event CAS(Tail, original, StubPtr) fails.
        // The next Pop call skips (deletes) the already-processed head event
        // instead of re-dispatching it. Avoids spinning on NextLinkPtr.
        bool ProcessedHead_ = false;

        // Work-stealing: 1-based index of the last slot that executed this mailbox.
        // 0 means fresh/unassigned. Written by slot after execution (relaxed store).
        ui16 LastPoolSlotIdx = 0;

        static constexpr TMailboxType::EType Type = TMailboxType::LockFreeIntrusive;

        TActorsInfo ActorsInfo{ .Empty = {} };

        // Used by executor run list
        std::atomic<uintptr_t> NextRunPtr{ 0 };

        // Vyukov MPSC queue tail (producers xchg here)
        std::atomic<uintptr_t> Tail{ 0 };

        // Consumer head pointer. Atomic for cross-thread visibility when
        // the mailbox is transferred between workers via scheduling.
        std::atomic<IEventHandle*> Head{ nullptr };

        // Preallocated stub node for the Vyukov MPSC queue sentinel.
        // reinterpret_cast<IEventHandle*>(&Stub)->NextLinkPtr aliases Stub.
        std::atomic<uintptr_t> Stub{ 0 };

        // Used to track how much time until activation
        NHPTimer::STime ScheduleMoment{ 0 };
    };

    static_assert(sizeof(TMailbox) <= 64, "TMailbox is too large");

    // ---- Template Pop implementation ----

    /**
     * Pop dequeues one event, resolves the actor, and passes a non-owning
     * raw pointer to the callback. The callback is responsible for
     * wrapping it in TAutoPtr (or similar) to manage lifetime.
     *
     * For the last-event path, Pop creates a detached move-copy of the
     * event and runs the callback on the copy while the original shell
     * (with NextLinkPtr) stays in the queue. After the callback, CAS
     * Tail(original, StubPtr) idles the queue. This eliminates the
     * locked-empty window and removes the need for State.
     *
     * Callback signature:
     *   void(IActor* actorOrNull, IEventHandle* ev)
     */
    template<typename F>
    EPopResult TMailbox::Pop(F&& process) noexcept {
        IEventHandle* head = Head.load(std::memory_order_relaxed);

        // Skip a previously-processed last event from a prior CAS failure.
        // The original shell is still at Head; advance past it once the
        // producer's NextLinkPtr store becomes visible.
        if (ProcessedHead_) {
            IEventHandle* next = GetNextPtrAcquire(head);
            if (!next) {
                return EPopResult::Empty; // Push in progress; retry later
            }
            Head.store(next, std::memory_order_relaxed);
            delete head;
            ProcessedHead_ = false;
            head = next;
        }

        if (head == StubPtr()) {
            uintptr_t sn = Stub.load(std::memory_order_acquire);
            if (!sn) {
                return EPopResult::Empty;
            }
            head = reinterpret_cast<IEventHandle*>(sn);
            Stub.store(0, std::memory_order_relaxed);
            Head.store(head, std::memory_order_relaxed);
        }

        IEventHandle* result = head;
        IEventHandle* next = GetNextPtrAcquire(result);

        if (next) {
            // Non-last event: detach and give ownership to callback.
            Head.store(next, std::memory_order_relaxed);
            SetNextPtr(result, nullptr);
            IActor* actor = ResolveActor(result);
            process(actor, result);
            return EPopResult::Processed;
        }

        // Last event (or incomplete push). Resolve actor on the original
        // (still in the queue), then create a detached copy for the
        // callback. The original retains NextLinkPtr so Push can link
        // to it safely during the callback.
        IActor* actor = ResolveActor(result);
        IEventHandle* copy = new IEventHandle(std::move(*result));
        process(actor, copy);

        // Speculatively set Head to StubPtr before the CAS. After a
        // successful CAS the mailbox is idle — a new consumer may start
        // immediately on another thread and must see Head == StubPtr.
        Head.store(StubPtr(), std::memory_order_release);

        // CAS Tail(original, StubPtr) to idle the queue.
        uintptr_t expected = reinterpret_cast<uintptr_t>(result);
        if (Tail.compare_exchange_strong(expected,
                reinterpret_cast<uintptr_t>(StubPtr()),
                std::memory_order_acq_rel)) {
            // Queue is idle. Do NOT touch mailbox — new consumer may be active.
            delete result;
            return EPopResult::Idle;
        }

        // CAS failed — a concurrent Push changed Tail. The original shell
        // stays alive (producer links to it via NextLinkPtr). Mark it as
        // processed so the next Pop skips it instead of re-dispatching.
        Head.store(result, std::memory_order_release);
        ProcessedHead_ = true;
        return EPopResult::Processed;
    }

    /**
     * Lightweight wrapper for batch event processing.
     *
     * Each Pop() call delegates to TMailbox::Pop() (the template version),
     * which handles the deferred CAS idle transition and ProcessedHead_.
     * Result is updated after each successful Pop.
     *
     * Defaults to NeedsReschedule (safe fallback — causes TryUnlock).
     * Only set to Idle when Pop's CAS proves the queue is idle.
     */
    class TMailboxSnapshot {
    public:
        TMailboxSnapshot(TMailbox* mb, ESnapshotResult& result) noexcept
            : Mailbox(mb), Result(result)
        {
            Result = ESnapshotResult::NeedsReschedule;
        }

        ~TMailboxSnapshot() noexcept = default;

        TMailboxSnapshot(const TMailboxSnapshot&) = delete;
        TMailboxSnapshot& operator=(const TMailboxSnapshot&) = delete;

        std::unique_ptr<IEventHandle> Pop() noexcept {
            IEventHandle* captured = nullptr;
            EPopResult popResult = Mailbox->Pop([&](IActor*, IEventHandle* ev) {
                captured = ev;
            });
            if (captured) {
                Result = (popResult == EPopResult::Idle)
                    ? ESnapshotResult::Idle
                    : ESnapshotResult::NeedsReschedule;
            }
            return captured ? std::unique_ptr<IEventHandle>(captured) : nullptr;
        }

    private:
        TMailbox* Mailbox;
        ESnapshotResult& Result;
    };

    class TMailboxTable {
    public:
        static constexpr size_t LinesCount = 0x1FFE0u;
        static constexpr size_t MailboxesPerLine = 0x1000u;
        static constexpr size_t BlockSize = MailboxesPerLine / 2;

        static constexpr int LineIndexShift = 12;
        static constexpr ui32 LineIndexMask = 0x1FFFFu;
        static constexpr ui32 MailboxIndexMask = 0xFFFu;

    public:
        TMailboxTable();
        ~TMailboxTable();

        bool Cleanup() noexcept;

        TMailbox* Get(ui32 hint) const;
        TMailbox* Allocate();
        std::pair<TMailbox*, size_t> AllocateBlock();
        void Free(TMailbox*);
        void FreeBlock(TMailbox*, size_t);

        size_t GetAllocatedLinesCountSlow() const;

        size_t GetAllocatedMailboxCountFast() const {
            return AllocatedLines.load(std::memory_order_relaxed) * MailboxesPerLine;
        }

    private:
        void FreeFullBlock(TMailbox*) noexcept;
        TMailbox* AllocateFullBlockLocked();

    private:
        struct TMailboxLine {
            TMailbox Mailboxes[MailboxesPerLine];
        };

    private:
        // Mutex for a slow allocation path
        alignas(64) mutable std::mutex Lock;
        // This is protected by a mutex when allocating
        alignas(64) std::atomic<TMailbox*> FreeBlocks{ nullptr };
        // This is protected by a mutex
        alignas(64) TMailbox* FreeMailboxes{ nullptr };
        size_t FreeMailboxesCount = 0;
        std::atomic<size_t> AllocatedLines{ 0 };

        // A large array of mailbox lines so we don't need extra pointer chasing
        std::atomic<TMailboxLine*> Lines[LinesCount] = { { nullptr } };
    };

    static_assert(sizeof(TMailboxTable) <= 1048576, "TMailboxTable is too large");

    class TMailboxCache {
    public:
        TMailboxCache() = default;
        TMailboxCache(TMailboxTable* table);
        ~TMailboxCache();

        void Switch(TMailboxTable* table);

        TMailbox* Allocate();
        void Free(TMailbox*);

        explicit operator bool() const {
            return bool(Table);
        }

    private:
        TMailboxTable* Table{ nullptr };

        TMailbox* CurrentBlock{ nullptr };
        size_t CurrentSize = 0;

        TMailbox* BackupBlock{ nullptr };
        size_t BackupSize = 0;
    };

} // namespace NActors
