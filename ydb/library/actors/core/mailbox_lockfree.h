#pragma once

#include "defs.h"
#include "event.h"
#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>
#include <atomic>
#include <mutex>

namespace NActors {

    class IActor;
    class IExecutorPool;

    enum class EMailboxPush {
        Locked,
        Pushed,
        Free,
    };

    struct TMailboxStats {
        ui64 ElapsedCycles = 0;
    };

    enum class ESnapshotResult {
        Idle,
        NeedsReschedule,
    };

    class TMailboxSnapshot;

    class alignas(64) TMailbox {
        friend class TMailboxSnapshot;

    public:
        // Mailbox lifecycle state (visible to Push callers).
        // Locked = consumer running, Push → Pushed.
        // Idle = no consumer, first Push → Locked (caller schedules).
        // Free = no actors, Push → Free (rejected).
        enum class EState : ui8 {
            Locked = 0,
            Idle = 1,
            Free = 2,
        };

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

        void EnableStats();
        void AddElapsedCycles(ui64);
        std::optional<ui64> GetElapsedCycles();
        std::optional<double> GetElapsedSeconds();

        bool CleanupActors() noexcept;
        bool CleanupEvents() noexcept;
        bool Cleanup() noexcept;
        ~TMailbox() noexcept;

        TMailbox() = default;
        TMailbox(const TMailbox&) = delete;
        TMailbox& operator=(const TMailbox&) = delete;

    public:
        /**
         * Tries to push ev to the mailbox and returns the status. When it is
         * EMailboxPush::Locked a previously idle mailbox becomes locked
         * and needs to be scheduled for execution by the caller. When it is
         * EMailboxPush::Pushed the event is added to the queue. When it is
         * EMailboxPush::Free the mailbox is currently free (no actors)
         * and the event cannot be delivered.
         *
         * Vyukov MPSC queue: wait-free (single xchg + one release store).
         */
        EMailboxPush Push(std::unique_ptr<IEventHandle>& ev) noexcept;

        /**
         * Removes the next event from the mailbox. Returns nullptr when
         * the queue is empty or the head event's next pointer is not yet
         * visible (incomplete push). The mailbox stays locked.
         *
         * Standalone Pop handles the idle transition via CAS on Tail.
         */
        std::unique_ptr<IEventHandle> Pop() noexcept;

        /**
         * Counts the number of events for the given localActorId.
         * Walks the chain from Head through linked next pointers.
         */
        std::pair<ui32, ui32> CountMailboxEvents(ui64 localActorId, ui32 maxTraverse) noexcept;

        /**
         * Tries to lock an idle mailbox and returns true on success.
         *
         * Returns true only when Tail == StubPtr() (idle) and CAS succeeds.
         */
        bool TryLock() noexcept;

        /**
         * Tries to transition a locked empty mailbox to idle.
         *
         * Returns true only when Head is at stub, stub has no pending link,
         * and Tail can be CAS'd back to StubPtr().
         */
        bool TryUnlock() noexcept;

        /**
         * Pushes ev to the front of the mailbox, which must be locked.
         * Used by test framework to inject events at the head of the queue.
         */
        void PushFront(std::unique_ptr<IEventHandle>&& ev) noexcept;

        /**
         * Returns true for free mailboxes
         */
        bool IsFree() const noexcept;

        /**
         * Transitions a locked mailbox to free state.
         * Drains all remaining events for cleanup.
         */
        void LockToFree() noexcept;

        /**
         * Race-safe variant of LockToFree. Uses CAS on Tail, so it fails
         * (returns false) if events arrived between the caller's check
         * and this call. On failure the mailbox stays locked.
         */
        bool TryLockToFree() noexcept;

        /**
         * Transitions a free mailbox to locked state.
         * Drains any orphan events pushed between LockToFree and now.
         */
        void LockFromFree() noexcept;

        /**
         * Tries to unlock and schedules for execution on failure
         */
        void Unlock(IExecutorPool* pool, NHPTimer::STime now, ui64& revolvingCounter);

        /**
         * Returns true when a free mailbox can be reclaimed
         */
        bool CanReclaim() const noexcept {
            Y_DEBUG_ABORT_UNLESS(IsFree());
            return Head.load(std::memory_order_relaxed) == StubPtr();
        }

        /**
         * Returns a pointer to the stub node, used as sentinel.
         */
        IEventHandle* StubPtr() const noexcept {
            return reinterpret_cast<IEventHandle*>(const_cast<std::atomic<uintptr_t>*>(&Stub));
        }

    private:
        void EnsureActorMap();
        void DrainAndDelete(uintptr_t tail) noexcept;
        void CleanupActor(IActor* actor) noexcept;

    public:
        ui32 Hint = 0;

        EActorPack ActorPack = EActorPack::Empty;

        std::atomic<EState> State{ EState::Free };

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

    /**
     * Lightweight snapshot of a mailbox for batch event processing.
     *
     * Stores a local cursor and a snapshotted tail boundary. Pop() returns
     * events up to (and including) the snapshot tail. The destructor updates
     * the mailbox Head and writes the caller-provided ESnapshotResult.
     *
     * Does NOT handle idle transition — that's done by standalone Pop()/TryUnlock().
     */
    class TMailboxSnapshot {
    public:
        TMailboxSnapshot(TMailbox* mb, ESnapshotResult& result) noexcept;
        ~TMailboxSnapshot() noexcept;

        TMailboxSnapshot(const TMailboxSnapshot&) = delete;
        TMailboxSnapshot& operator=(const TMailboxSnapshot&) = delete;

        std::unique_ptr<IEventHandle> Pop() noexcept;

    private:
        TMailbox* Mailbox;
        IEventHandle* Cursor;
        IEventHandle* SnapshotTail;
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
