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

    class alignas(64) TMailbox {
    private:
        static constexpr uintptr_t MarkerUnlocked = 1;
        static constexpr uintptr_t MarkerFree = 2;

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
         * EMailboxPush::Locked a previously unlocked mailbox becomes locked
         * and needs to be scheduled for execution by the caller. When it is
         * EMailboxPush::Pushed the event is added to the queue. When it is
         * EMailboxPush::Free the mailbox is currently locked by a free list
         * and the event cannot be delivered.
         */
        EMailboxPush Push(std::unique_ptr<IEventHandle>& ev) noexcept;

        /**
         * Removes the next event from the mailbox. Returns nullptr for an
         * empty mailbox, which stays locked.
         */
        std::unique_ptr<IEventHandle> Pop() noexcept;

        /**
         * Counts the number of events for the given localActorId
         */
        std::pair<ui32, ui32> CountMailboxEvents(ui64 localActorId, ui32 maxTraverse) noexcept;

        /**
         * Tries to lock an unlocked empty mailbox and returns true on success.
         *
         * Returns true only when mailbox was empty and not locked by another thread.
         */
        bool TryLock() noexcept;

        /**
         * Tries to unlock an empty locked mailbox and returns true on success.
         *
         * Returns true only when mailbox is empty.
         */
        bool TryUnlock() noexcept;

        /**
         * Pushes ev to the front of the mailbox, which must be locked. This
         * is useful when an event needs to be injected at the front of the
         * queue.
         */
        void PushFront(std::unique_ptr<IEventHandle>&& ev) noexcept;

        /**
         * Returns true for free mailboxes
         */
        bool IsFree() const noexcept;

        /**
         * Locks the mailbox that had the last actor detached.
         *
         * All events currently in the mailbox are moved to the local queue
         * and need to be processed individually until Pop() returns nullptr.
         */
        void LockToFree() noexcept;

        /**
         * Locks the mailbox after initial state or a LockToFree call.
         */
        void LockFromFree() noexcept;

        /**
         * Tries to unlock and schedules for execution on failure
         */
        void Unlock(IExecutorPool* pool, NHPTimer::STime now, ui64& revolvingCounter);

        /**
         * Returns true when a free mailbox can be reclaimed
         */
        bool CanReclaim() const {
            Y_DEBUG_ABORT_UNLESS(IsFree());
            return !EventHead;
        }

    private:
        void EnsureActorMap();
        void OnPreProcessed(IEventHandle* head, IEventHandle* tail) noexcept;
        void AppendPreProcessed(IEventHandle* head, IEventHandle* tail) noexcept;
        void PrependPreProcessed(IEventHandle* head, IEventHandle* tail) noexcept;
        IEventHandle* PreProcessEvents() noexcept;
        void CleanupActor(IActor* actor) noexcept;

    public:
        ui32 Hint = 0;

        EActorPack ActorPack = EActorPack::Empty;

        // Work-stealing: 1-based index of the last slot that executed this mailbox.
        // 0 means fresh/unassigned. Written by slot after execution (relaxed store).
        ui16 LastPoolSlotIdx = 0;

        static constexpr TMailboxType::EType Type = TMailboxType::LockFreeIntrusive;

        TActorsInfo ActorsInfo{ .Empty = {} };

        // Used by executor run list
        std::atomic<uintptr_t> NextRunPtr{ 0 };

        // An atomic stack of new events in reverse order
        std::atomic<uintptr_t> NextEventPtr{ MarkerFree };

        // Preprocessed events ready for consumption
        IEventHandle* EventHead{ nullptr };
        IEventHandle* EventTail{ nullptr };

        // Used to track how much time until activation
        NHPTimer::STime ScheduleMoment{ 0 };
    };

    static_assert(sizeof(TMailbox) <= 64, "TMailbox is too large");

    // Per-mailbox execution/idle statistics, stored in a separate parallel
    // array in TMailboxTable so that TMailbox stays at 64 bytes.
    // Single-writer (mailbox locked during execution), but monitoring may
    // read from another thread, so all counters use relaxed atomics.
    struct alignas(64) TMailboxExecStats {
        std::atomic<ui64> EventsProcessed{0};
        std::atomic<ui64> TotalExecutionCycles{0};
        std::atomic<ui64> MaxExecutionCycles{0};
        std::atomic<ui64> MinExecutionCycles{Max<ui64>()};
        std::atomic<ui64> LastExecutionEndCycles{0};
        std::atomic<ui64> TotalIdleCycles{0};
        std::atomic<ui64> MaxIdleCycles{0};
        std::atomic<ui64> MinIdleCycles{Max<ui64>()};

        void Reset() noexcept;
    };

    static_assert(sizeof(TMailboxExecStats) == 64, "TMailboxExecStats must be one cache line");

    class TMailboxTable {
    public:
        static constexpr size_t LinesCount = 0x1FFE0u;
        static constexpr size_t MailboxesPerLine = 0x1000u;
        static constexpr size_t BlockSize = MailboxesPerLine / 2;

        static constexpr int LineIndexShift = 12;
        static constexpr ui32 LineIndexMask = 0x1FFFFu;
        static constexpr ui32 MailboxIndexMask = 0xFFFu;

    public:
        explicit TMailboxTable(size_t slabSize = 0);
        ~TMailboxTable();

        static TMailboxTable* Create();
        static void Destroy(TMailboxTable* table) noexcept;

        bool Cleanup() noexcept;

        TMailbox* Get(ui32 hint) const;
        TMailboxExecStats* GetStats(ui32 hint) const;
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

        void* SlabAllocate(size_t size, size_t alignment);
        void* AllocateNewSlab();
        static size_t DetectHugePageSize();

    private:
        struct alignas(4096) TMailboxLine {
            TMailbox Mailboxes[MailboxesPerLine];
        };

        struct alignas(4096) TMailboxStatLine {
            TMailboxExecStats Stats[MailboxesPerLine];
        };

        struct TSlabHeader {
            void* Next;
            size_t Size;
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

        // Parallel array of per-mailbox execution/idle stats (allocated alongside Lines)
        std::atomic<TMailboxStatLine*> StatLines[LinesCount] = { { nullptr } };

        // Slab allocator for hugepage support
        size_t SlabSize_{0};
        void* NextSlab_{nullptr};
        char* SlabCursor_{nullptr};
        size_t SlabRemaining_{0};
        size_t FirstHeapLineIndex_{Max<size_t>()};
    };

    static_assert(sizeof(TMailboxTable) <= 2097152, "TMailboxTable is too large");

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
