#include "mailbox_lockfree.h"
#include "actor.h"
#include "executor_pool.h"

namespace NActors {

    IActor* TMailbox::FindActor(ui64 localActorId) noexcept {
        switch (ActorPack) {
            case EActorPack::Empty:
                return nullptr;

            case EActorPack::Simple:
                if (ActorsInfo.Simple.ActorId == localActorId) {
                    return ActorsInfo.Simple.Actor;
                }
                return nullptr;

            case EActorPack::Array:
                for (ui64 i = 0; i < ActorsInfo.Array.ActorsCount; ++i) {
                    auto& entry = ActorsInfo.Array.ActorsArray->Actors[i];
                    if (entry.ActorId == localActorId) {
                        return entry.Actor;
                    }
                }
                return nullptr;

            case EActorPack::Map: {
                auto it = ActorsInfo.Map.ActorsMap->find(localActorId);
                if (it != ActorsInfo.Map.ActorsMap->end()) {
                    return it->second;
                }
                return nullptr;
            }
        }

        Y_ABORT();
    }

    IActor* TMailbox::FindAlias(ui64 localActorId) noexcept {
        switch (ActorPack) {
            case EActorPack::Empty:
            case EActorPack::Simple:
            case EActorPack::Array:
                return nullptr;

            case EActorPack::Map: {
                TActorMap* m = ActorsInfo.Map.ActorsMap;
                auto it = m->Aliases.find(localActorId);
                if (it != m->Aliases.end()) {
                    return it->second;
                }
                return nullptr;
            }
        }

        Y_ABORT();
    }

    void TMailbox::AttachActor(ui64 localActorId, IActor* actor) noexcept {
        switch (ActorPack) {
            case EActorPack::Empty:
                ActorsInfo.Simple = { actor, localActorId };
                ActorPack = EActorPack::Simple;
                return;

            case EActorPack::Simple: {
                TActorArray* a = new TActorArray;
                a->Actors[0] = ActorsInfo.Simple;
                a->Actors[1] = TActorPair{ actor, localActorId };
                ActorsInfo.Array = { a, 2 };
                ActorPack = EActorPack::Array;
                return;
            }

            case EActorPack::Array: {
                if (ActorsInfo.Array.ActorsCount < ArrayCapacity) {
                    ActorsInfo.Array.ActorsArray->Actors[ActorsInfo.Array.ActorsCount++] = TActorPair{ actor, localActorId };
                    return;
                }

                TActorMap* m = new TActorMap();
                TActorArray* a = ActorsInfo.Array.ActorsArray;
                for (ui64 i = 0; i < ArrayCapacity; ++i) {
                    m->emplace(a->Actors[i].ActorId, a->Actors[i].Actor);
                }
                m->emplace(localActorId, actor);

                ActorsInfo.Map = { m };
                ActorPack = EActorPack::Map;
                delete a;
                return;
            }

            case EActorPack::Map: {
                TActorMap* m = ActorsInfo.Map.ActorsMap;
                m->emplace(localActorId, actor);
                return;
            }
        }

        Y_ABORT();
    }

    void TMailbox::AttachAlias(ui64 localActorId, IActor* actor) noexcept {
        // Note: we assume the specified actor is registered and the alias is correct
        EnsureActorMap();
        actor->Aliases.insert(localActorId);
        ActorsInfo.Map.ActorsMap->Aliases.emplace(localActorId, actor);
    }

    IActor* TMailbox::DetachActor(ui64 localActorId) noexcept {
        switch (ActorPack) {
            case EActorPack::Empty:
                Y_ABORT("DetachActor(%" PRIu64 ") called for an empty mailbox", localActorId);

            case EActorPack::Simple: {
                if (ActorsInfo.Simple.ActorId == localActorId) {
                    IActor* actor = ActorsInfo.Simple.Actor;
                    Y_ABORT_UNLESS(actor->Aliases.empty(), "Unexpected actor aliases for EActorPack::Simple");
                    ActorsInfo.Empty = {};
                    ActorPack = EActorPack::Empty;
                    return actor;
                }
                break;
            }

            case EActorPack::Array: {
                TActorArray* a = ActorsInfo.Array.ActorsArray;
                for (ui64 i = 0; i < ActorsInfo.Array.ActorsCount; ++i) {
                    if (a->Actors[i].ActorId == localActorId) {
                        IActor* actor = a->Actors[i].Actor;
                        Y_ABORT_UNLESS(actor->Aliases.empty(), "Unexpected actor aliases for EActorPack::Array");
                        a->Actors[i] = a->Actors[ActorsInfo.Array.ActorsCount - 1];
                        if (0 == --ActorsInfo.Array.ActorsCount) {
                            ActorsInfo.Empty = {};
                            ActorPack = EActorPack::Empty;
                            delete a;
                        }
                        return actor;
                    }
                }
                break;
            }

            case EActorPack::Map: {
                TActorMap* m = ActorsInfo.Map.ActorsMap;
                auto it = m->find(localActorId);
                if (it != m->end()) {
                    IActor* actor = it->second;
                    if (!actor->Aliases.empty()) {
                        for (ui64 aliasId : actor->Aliases) {
                            bool removed = m->Aliases.erase(aliasId);
                            Y_ABORT_UNLESS(removed, "Unexpected failure to remove a register actor alias");
                        }
                        actor->Aliases.clear();
                    }
                    m->erase(it);
                    if (m->empty()) {
                        Y_ABORT_UNLESS(m->Aliases.empty(), "Unexpected actor aliases left in an empty EActorPack::Map");
                        ActorsInfo.Empty = {};
                        ActorPack = EActorPack::Empty;
                        delete m;
                    }
                    return actor;
                }
                break;
            }
        }

        Y_ABORT("DetachActor(%" PRIu64 ") called for an unknown actor", localActorId);
    }

    IActor* TMailbox::DetachAlias(ui64 localActorId) noexcept {
        switch (ActorPack) {
            case EActorPack::Empty:
                Y_ABORT("DetachAlias(%" PRIu64 ") called for an empty mailbox", localActorId);

            case EActorPack::Simple:
            case EActorPack::Array:
                break;

            case EActorPack::Map: {
                TActorMap* m = ActorsInfo.Map.ActorsMap;
                auto it = m->Aliases.find(localActorId);
                if (it != m->Aliases.end()) {
                    IActor* actor = it->second;
                    actor->Aliases.erase(localActorId);
                    m->Aliases.erase(it);
                    return actor;
                }
                break;
            }
        }

        Y_ABORT("DetachAlias(%" PRIu64 ") called for an unknown actor", localActorId);
    }

    void TMailbox::EnsureActorMap() {
        switch (ActorPack) {
            case EActorPack::Empty:
                Y_ABORT("Expected a non-empty mailbox");

            case EActorPack::Simple: {
                TActorMap* m = new TActorMap();
                m->emplace(ActorsInfo.Simple.ActorId, ActorsInfo.Simple.Actor);
                ActorsInfo.Map.ActorsMap = m;
                ActorPack = EActorPack::Map;
                return;
            }

            case EActorPack::Array: {
                TActorMap* m = new TActorMap();
                TActorArray* a = ActorsInfo.Array.ActorsArray;
                for (ui64 i = 0; i < ActorsInfo.Array.ActorsCount; ++i) {
                    m->emplace(a->Actors[i].ActorId, a->Actors[i].Actor);
                }
                ActorsInfo.Map.ActorsMap = m;
                ActorPack = EActorPack::Map;
                delete a;
                return;
            }

            case EActorPack::Map: {
                return;
            }
        }

        Y_ABORT();
    }

    void TMailbox::EnableStats() {
        EnsureActorMap();
    }

    void TMailbox::AddElapsedCycles(ui64 cycles) {
        if (ActorPack == EActorPack::Map) {
            ActorsInfo.Map.ActorsMap->Stats.ElapsedCycles += cycles;
        }
    }

    std::optional<ui64> TMailbox::GetElapsedCycles() {
        if (ActorPack == EActorPack::Map) {
            return ActorsInfo.Map.ActorsMap->Stats.ElapsedCycles;
        }
        return std::nullopt;
    }

    std::optional<double> TMailbox::GetElapsedSeconds() {
        if (auto x = GetElapsedCycles()) {
            return {NHPTimer::GetSeconds(*x)};
        }
        return std::nullopt;
    }

    void TMailbox::CleanupActor(IActor* actor) noexcept {
        actor->DestroyActorTasks();
        delete actor;
    }

    bool TMailbox::CleanupActors() noexcept {
        bool done = true;

        // Note: actor destructor might register more actors (including the same mailbox)
        for (int round = 0; round < 10; ++round) {
            switch (ActorPack) {
                case EActorPack::Empty: {
                    return done;
                }

                case EActorPack::Simple: {
                    IActor* actor = ActorsInfo.Simple.Actor;
                    ActorsInfo.Empty = {};
                    ActorPack = EActorPack::Empty;
                    CleanupActor(actor);
                    done = false;
                    continue;
                }

                case EActorPack::Array: {
                    TActorArray* a = ActorsInfo.Array.ActorsArray;
                    size_t count = ActorsInfo.Array.ActorsCount;
                    ActorsInfo.Empty = {};
                    ActorPack = EActorPack::Empty;
                    for (size_t i = 0; i < count; ++i) {
                        CleanupActor(a->Actors[i].Actor);
                    }
                    delete a;
                    done = false;
                    continue;
                }

                case EActorPack::Map: {
                    TActorMap* m = ActorsInfo.Map.ActorsMap;
                    ActorsInfo.Empty = {};
                    ActorPack = EActorPack::Empty;
                    for (auto& pr : *m) {
                        CleanupActor(pr.second);
                    }
                    delete m;
                    done = false;
                    continue;
                }
            }

            Y_ABORT("CleanupActors called with an unexpected state");
        }

        Y_ABORT_UNLESS(ActorPack == EActorPack::Empty, "Actor destructors keep registering more actors");
        return done;
    }

    bool TMailbox::CleanupEvents() noexcept {
        bool hadEvents = false;

        // Drain any events from the Vyukov queue
        uintptr_t tail = Tail.exchange(reinterpret_cast<uintptr_t>(StubPtr()),
                                       std::memory_order_acq_rel);
        if (tail != reinterpret_cast<uintptr_t>(StubPtr()) && tail != 0) {
            // Walk from Head through linked events
            IEventHandle* head = Head.load(std::memory_order_relaxed);
            if (head == StubPtr()) {
                uintptr_t sn = Stub.load(std::memory_order_acquire);
                if (sn) {
                    head = reinterpret_cast<IEventHandle*>(sn);
                } else {
                    head = nullptr;
                }
            }
            while (head && head != StubPtr()) {
                IEventHandle* ev = head;
                IEventHandle* next = GetNextPtrAcquire(ev);
                hadEvents = true;
                delete ev;
                head = next;
            }
        }

        Head.store(StubPtr(), std::memory_order_relaxed);
        Stub.store(0, std::memory_order_relaxed);

        return !hadEvents;
    }

    bool TMailbox::Cleanup() noexcept {
        bool doneActors = CleanupActors();
        bool doneEvents = CleanupEvents();
        return doneActors && doneEvents;
    }

    TMailbox::~TMailbox() noexcept {
        Cleanup();
    }

    // ---- Actor resolution ----

    IActor* TMailbox::ResolveActor(IEventHandle* ev) noexcept {
        IActor* actor = FindActor(ev->GetRecipientRewrite().LocalId());
        if (!actor) {
            actor = FindAlias(ev->GetRecipientRewrite().LocalId());
            if (actor) {
                ev->Rewrite(ev->GetTypeRewrite(), actor->SelfId());
            }
        }
        return actor;
    }

    // ---- Vyukov MPSC queue: Push (wait-free) ----

    EMailboxPush TMailbox::Push(std::unique_ptr<IEventHandle>& evPtr) noexcept {
        IEventHandle* ev = evPtr.release();
        ev->NextLinkPtr.store(0, std::memory_order_relaxed);

        uintptr_t prev = Tail.exchange(reinterpret_cast<uintptr_t>(ev),
                                       std::memory_order_acq_rel);

        reinterpret_cast<IEventHandle*>(prev)->NextLinkPtr.store(
            reinterpret_cast<uintptr_t>(ev), std::memory_order_release);

        if (prev == reinterpret_cast<uintptr_t>(StubPtr())) {
            return EMailboxPush::Locked;
        }
        return EMailboxPush::Pushed;
    }

    std::pair<ui32, ui32> TMailbox::CountMailboxEvents(ui64 localActorId, ui32 maxTraverse) noexcept {
        ui32 local = 0;
        ui32 total = 0;

        IEventHandle* head = Head.load(std::memory_order_relaxed);
        if (head == StubPtr()) {
            uintptr_t sn = Stub.load(std::memory_order_acquire);
            if (sn) {
                head = reinterpret_cast<IEventHandle*>(sn);
            } else {
                return { 0, 0 };
            }
        }

        for (IEventHandle* ev = head; ev; ev = GetNextPtrAcquire(ev)) {
            ++total;
            if (ev->GetRecipientRewrite().LocalId() == localActorId) {
                ++local;
            }
            if (total >= maxTraverse) {
                break;
            }
        }

        return { local, total };
    }

    // Used only by Unlock()/FinalizeActivation() to check if the queue is idle.
    // No State CAS needed — Push()'s xchg atomicity guarantees exactly one
    // Push per idle period gets StubPtr as prev and returns Locked.
    bool TMailbox::TryUnlock() noexcept {
        if (Head.load(std::memory_order_relaxed) != StubPtr()) {
            return false;
        }
        if (Stub.load(std::memory_order_acquire)) {
            return false;
        }
        if (Tail.load(std::memory_order_acquire) != reinterpret_cast<uintptr_t>(StubPtr())) {
            return false;
        }
        // Double-check for Push race: a producer may have done
        // Tail.exchange(ev) (getting StubPtr) between our checks above.
        if (Stub.load(std::memory_order_acquire)
                || Tail.load(std::memory_order_acquire) != reinterpret_cast<uintptr_t>(StubPtr())) {
            return false;
        }
        return true;
    }

    void TMailbox::PushFront(std::unique_ptr<IEventHandle>&& evPtr) noexcept {
        IEventHandle* e = evPtr.release();

#ifdef ACTORSLIB_COLLECT_EXEC_STATS
        e->SendTime = (::NHPTimer::STime)GetCycleCountFast();
#endif

        IEventHandle* head = Head.load(std::memory_order_relaxed);
        if (head == StubPtr()) {
            uintptr_t sn = Stub.load(std::memory_order_relaxed);
            SetNextPtr(e, sn ? reinterpret_cast<IEventHandle*>(sn) : nullptr);
            Stub.store(reinterpret_cast<uintptr_t>(e), std::memory_order_relaxed);
        } else {
            SetNextPtr(e, head);
            Head.store(e, std::memory_order_relaxed);
        }
    }

    // Really should be called "Schedule"
    // Called by pool to activate freshly created actor
    // Called by thread to activate mailbox that still has events
    void TMailbox::Unlock(IExecutorPool* pool, NHPTimer::STime now, ui64& revolvingCounter) {
        if (!TryUnlock()) {
            ScheduleMoment = now;
            pool->ScheduleActivationEx(this, ++revolvingCounter);
        }
    }

    // TMailboxSnapshot is now fully inline (delegates to template Pop).
    // No out-of-line methods needed.

    TMailboxCache::TMailboxCache(TMailboxTable* table)
        : Table(table)
    {}

    TMailboxCache::~TMailboxCache() {
        if (BackupBlock) {
            Table->FreeBlock(BackupBlock, BackupSize);
            BackupBlock = nullptr;
            BackupSize = 0;
        }

        if (CurrentBlock) {
            Table->FreeBlock(CurrentBlock, CurrentSize);
            CurrentBlock = nullptr;
            CurrentSize = 0;
        }
    }

    void TMailboxCache::Switch(TMailboxTable* table) {
        if (Table != table) {
            if (BackupBlock) {
                Table->FreeBlock(BackupBlock, BackupSize);
                BackupBlock = nullptr;
                BackupSize = 0;
            }
            if (CurrentBlock) {
                Table->FreeBlock(CurrentBlock, CurrentSize);
                CurrentBlock = nullptr;
                CurrentSize = 0;
            }
            Table = table;
        }
    }

    TMailbox* TMailboxCache::Allocate() {
        Y_ABORT_UNLESS(Table);

        if (!CurrentBlock) {
            if (BackupBlock) [[likely]] {
                CurrentBlock = BackupBlock;
                CurrentSize = BackupSize;
                BackupBlock = nullptr;
                BackupSize = 0;
            } else {
                auto block = Table->AllocateBlock();
                CurrentBlock = block.first;
                CurrentSize = block.second;
            }
        }

        Y_ABORT_UNLESS(CurrentBlock);
        Y_ABORT_UNLESS(CurrentSize > 0);

        TMailbox* mailbox = CurrentBlock;
        CurrentBlock = mailbox->ActorsInfo.Empty.NextFree;
        CurrentSize--;

        Y_DEBUG_ABORT_UNLESS(CurrentBlock ? CurrentSize > 0 : CurrentSize == 0);

        mailbox->ActorsInfo.Empty.NextFree = nullptr;
        return mailbox;
    }

    void TMailboxCache::Free(TMailbox* mailbox) {
        Y_ABORT_UNLESS(Table);

        if (CurrentSize >= TMailboxTable::BlockSize) {
            if (BackupBlock) {
                Table->FreeBlock(BackupBlock, BackupSize);
            }
            BackupBlock = CurrentBlock;
            BackupSize = CurrentSize;
            CurrentBlock = nullptr;
            CurrentSize = 0;
        }

        mailbox->ActorsInfo.Empty.NextFree = CurrentBlock;
        CurrentBlock = mailbox;
        CurrentSize++;
    }

    TMailboxTable::TMailboxTable() {}

    TMailboxTable::~TMailboxTable() {
        ui32 lineCount = GetAllocatedLinesCountSlow();
        for (size_t i = 0; i < lineCount; ++i) {
            if (auto* line = Lines[i].load(std::memory_order_acquire)) {
                delete line;
            }
        }
    }

    bool TMailboxTable::Cleanup() noexcept {
        bool done = true;
        ui32 lineCount = GetAllocatedLinesCountSlow();
        for (ui32 lineIndex = 0; lineIndex < lineCount; ++lineIndex) {
            auto* line = Lines[lineIndex].load(std::memory_order_acquire);
            if (line) [[likely]] {
                for (ui32 i = 0; i < MailboxesPerLine; ++i) {
                    done &= line->Mailboxes[i].Cleanup();
                }
            }
            if (lineCount == lineIndex + 1) {
                // In case cleanup allocated more mailboxes
                lineCount = GetAllocatedLinesCountSlow();
            }
        }
        return done;
    }

    size_t TMailboxTable::GetAllocatedLinesCountSlow() const {
        std::unique_lock g(Lock);
        return AllocatedLines.load(std::memory_order_relaxed);
    }

    TMailbox* TMailboxTable::Get(ui32 hint) const {
        ui32 lineIndex = (hint >> LineIndexShift) & LineIndexMask;
        if (lineIndex < LinesCount) [[likely]] {
            auto* line = Lines[lineIndex].load(std::memory_order_acquire);
            if (line) [[likely]] {
                return &line->Mailboxes[hint & MailboxIndexMask];
            }
        }
        return nullptr;
    }

    TMailbox* TMailboxTable::Allocate() {
        std::unique_lock g(Lock);

        if (!FreeMailboxes) [[unlikely]] {
            TMailbox* head = AllocateFullBlockLocked();
            if (!head) {
                throw std::bad_alloc();
            }
            FreeMailboxes = head;
            FreeMailboxesCount = BlockSize;
        }

        TMailbox* mailbox = FreeMailboxes;
        FreeMailboxes = mailbox->ActorsInfo.Empty.NextFree;
        FreeMailboxesCount--;

        Y_DEBUG_ABORT_UNLESS(FreeMailboxes ? FreeMailboxesCount > 0 : FreeMailboxesCount == 0);

        mailbox->ActorsInfo.Empty.NextFree = nullptr;
        return mailbox;
    }

    std::pair<TMailbox*, size_t> TMailboxTable::AllocateBlock() {
        std::unique_lock g(Lock);

        TMailbox* head = AllocateFullBlockLocked();
        if (head) [[likely]] {
            return { head, BlockSize };
        }

        if (!FreeMailboxes) [[unlikely]] {
            throw std::bad_alloc();
        }

        // Take a single free mailbox and return it as a 1-item block
        TMailbox* mailbox = FreeMailboxes;
        FreeMailboxes = mailbox->ActorsInfo.Empty.NextFree;
        FreeMailboxesCount--;

        Y_DEBUG_ABORT_UNLESS(FreeMailboxes ? FreeMailboxesCount > 0 : FreeMailboxesCount == 0);

        mailbox->ActorsInfo.Empty.NextFree = nullptr;
        return { mailbox, 1u };
    }

    void TMailboxTable::Free(TMailbox* mailbox) {
        std::unique_lock g(Lock);

        Y_DEBUG_ABORT_UNLESS(FreeMailboxesCount < BlockSize);

        mailbox->ActorsInfo.Empty.NextFree = FreeMailboxes;
        FreeMailboxes = mailbox;
        FreeMailboxesCount++;

        if (FreeMailboxesCount == BlockSize) {
            FreeFullBlock(FreeMailboxes);
            FreeMailboxes = nullptr;
            FreeMailboxesCount = 0;
        }
    }

    void TMailboxTable::FreeBlock(TMailbox* head, size_t count) {
        if (count == BlockSize) [[likely]] {
            FreeFullBlock(head);
            return;
        }

        std::unique_lock g(Lock);

        Y_DEBUG_ABORT_UNLESS(count < BlockSize);
        Y_DEBUG_ABORT_UNLESS(FreeMailboxesCount < BlockSize);

        while (head) {
            Y_DEBUG_ABORT_UNLESS(count > 0);
            TMailbox* mailbox = head;
            head = head->ActorsInfo.Empty.NextFree;
            count--;

            mailbox->ActorsInfo.Empty.NextFree = FreeMailboxes;
            FreeMailboxes = mailbox;
            FreeMailboxesCount++;
            if (FreeMailboxesCount == BlockSize) {
                FreeFullBlock(FreeMailboxes);
                FreeMailboxes = nullptr;
                FreeMailboxesCount = 0;
            }
        }

        Y_DEBUG_ABORT_UNLESS(count == 0);
    }

    void TMailboxTable::FreeFullBlock(TMailbox* head) noexcept {
        TMailbox* current = FreeBlocks.load(std::memory_order_relaxed);
        do {
            head->ActorsInfo.Empty.NextFreeBlock = current;
        } while (!FreeBlocks.compare_exchange_weak(current, head, std::memory_order_release));
    }

    TMailbox* TMailboxTable::AllocateFullBlockLocked() {
        TMailbox* current = FreeBlocks.load(std::memory_order_acquire);
        while (current) {
            // We are removing blocks under a mutex, so accessing NextFreeBlock
            // is safe. However other threads may free more blocks concurrently.
            TMailbox* head = current;
            TMailbox* next = current->ActorsInfo.Empty.NextFreeBlock;
            if (FreeBlocks.compare_exchange_weak(current, next, std::memory_order_acquire)) {
                head->ActorsInfo.Empty.NextFreeBlock = nullptr;
                return head;
            }
        }

        // We need to allocate a new line
        size_t lineIndex = AllocatedLines.load(std::memory_order_relaxed);
        if (lineIndex < LinesCount) [[likely]] {
            static_assert((MailboxesPerLine & (BlockSize - 1)) == 0,
                "Per line mailboxes are not divisible into blocks");

            // Note: this line may throw bad_alloc
            TMailboxLine* line = new TMailboxLine;

            TMailbox* head = &line->Mailboxes[0];
            TMailbox* tail = head;
            ui32 base = lineIndex << LineIndexShift;
            for (ui32 i = 0; i < MailboxesPerLine; ++i) {
                TMailbox* mailbox = &line->Mailboxes[i];
                mailbox->Hint = base + i;
                // Initialize Vyukov MPSC queue sentinels so every mailbox is always pushable
                mailbox->Head.store(mailbox->StubPtr(), std::memory_order_relaxed);
                mailbox->Tail.store(reinterpret_cast<uintptr_t>(mailbox->StubPtr()), std::memory_order_relaxed);
                mailbox->Stub.store(0, std::memory_order_relaxed);
                if (i > 0) {
                    if ((i & (BlockSize - 1)) == 0) {
                        // This is the first mailbox is the next block
                        tail->ActorsInfo.Empty.NextFreeBlock = mailbox;
                        tail = mailbox;
                    } else {
                        // This is the next free mailbox is the current block
                        line->Mailboxes[i - 1].ActorsInfo.Empty.NextFree = mailbox;
                    }
                }
            }

            // Publish the new line (mailboxes become available via Get using their hint)
            Lines[lineIndex].store(line, std::memory_order_release);
            AllocatedLines.store(lineIndex + 1, std::memory_order_relaxed);

            // Take the first new block as the result
            TMailbox* result = head;
            if (result->Hint == 0) [[unlikely]] {
                // Skip the very first block because it has a hint==0 mailbox
                result = std::exchange(result->ActorsInfo.Empty.NextFreeBlock, nullptr);
            }
            head = std::exchange(result->ActorsInfo.Empty.NextFreeBlock, nullptr);

            // Other blocks are atomically prepended to the list of free blocks
            if (head) [[likely]] {
                current = FreeBlocks.load(std::memory_order_relaxed);
                do {
                    tail->ActorsInfo.Empty.NextFreeBlock = current;
                } while (!FreeBlocks.compare_exchange_weak(current, head, std::memory_order_release));
            }

            return result;
        }

        // We don't have any more lines available (more than 536M actors)
        return nullptr;
    }

} // namespace NActors
