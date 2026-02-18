#pragma once

#include <util/system/compiler.h>

#include <atomic>
#include <cstdint>
#include <cstddef>

namespace NActors::NWorkStealing {

// -----------------------------------------------------------------------
// Global thread ID allocator — process-level Meyers singleton.
// Dense IDs starting from 0. Spinlock-protected free list for recycling.
// Thread create/destroy is rare, so a spinlock is fine here.
// -----------------------------------------------------------------------

struct TFreeIdNode {
    uint32_t Id;
    TFreeIdNode* Next;
};

struct TDebraThreadId {
    static constexpr uint32_t kInvalid = UINT32_MAX;

    std::atomic<uint32_t> NextId{0};
    std::atomic_flag Lock_{};
    TFreeIdNode* FreeHead_{nullptr};

    static TDebraThreadId& Instance() {
        static TDebraThreadId instance;
        return instance;
    }

    uint32_t Acquire() {
        while (Lock_.test_and_set(std::memory_order_acquire)) {}
        TFreeIdNode* head = FreeHead_;
        if (head) {
            FreeHead_ = head->Next;
            Lock_.clear(std::memory_order_release);
            uint32_t id = head->Id;
            delete head;
            return id;
        }
        Lock_.clear(std::memory_order_release);
        return NextId.fetch_add(1, std::memory_order_relaxed);
    }

    void Release(uint32_t id) {
        auto* node = new TFreeIdNode{id, nullptr};
        while (Lock_.test_and_set(std::memory_order_acquire)) {}
        node->Next = FreeHead_;
        FreeHead_ = node;
        Lock_.clear(std::memory_order_release);
    }

    ~TDebraThreadId() {
        TFreeIdNode* node = FreeHead_;
        while (node) {
            TFreeIdNode* next = node->Next;
            delete node;
            node = next;
        }
    }
};

// Thread-local RAII: acquires ID on first call, releases on thread exit.
inline uint32_t GetDebraThreadId() {
    struct TReg {
        uint32_t Id = TDebraThreadId::kInvalid;
        bool Active = false;
        ~TReg() {
            if (Active) TDebraThreadId::Instance().Release(Id);
        }
    };
    static thread_local TReg reg;
    if (Y_LIKELY(reg.Active)) return reg.Id;
    reg.Id = TDebraThreadId::Instance().Acquire();
    reg.Active = true;
    return reg.Id;
}

// -----------------------------------------------------------------------
// Epoch slab — page-sized (4096 bytes), 64 cache-line-aligned entries.
// Next pointer overlaid in Entry[0]'s padding via union.
// -----------------------------------------------------------------------

static constexpr uint8_t kEpochInactive = 0xFF;

struct alignas(64) TEpochEntry {
    std::atomic<uint8_t> Epoch{kEpochInactive};
};
static_assert(sizeof(TEpochEntry) == 64);

struct TEpochSlab {
    static constexpr size_t kEntries = 64;

    union {
        TEpochEntry Entries[kEntries];
        struct {
            char Pad_[64 - sizeof(std::atomic<TEpochSlab*>)];
            std::atomic<TEpochSlab*> Next;
        };
    };

    TEpochSlab() {
        for (size_t i = 0; i < kEntries; ++i) {
            Entries[i].Epoch.store(kEpochInactive, std::memory_order_relaxed);
        }
        Next.store(nullptr, std::memory_order_relaxed);
    }

    ~TEpochSlab() = default;
};
static_assert(sizeof(TEpochSlab) == 4096);

// -----------------------------------------------------------------------
// Limbo node for deferred reclamation.
// -----------------------------------------------------------------------

struct TLimboNode {
    TLimboNode* Next = nullptr;
    void* Data = nullptr;
    void (*FreeFn)(void*) = nullptr;
};

// -----------------------------------------------------------------------
// Per-queue DEBRA: epoch counter, slab-based epoch table, 3 limbo bags.
// -----------------------------------------------------------------------

struct TDebra {
    std::atomic<uint64_t> GlobalEpoch{0};
    TEpochSlab FirstSlab;
    std::atomic<TLimboNode*> LimboHead[3]{};

    // O(1) for id < 64 — direct array access, no slab walk.
    std::atomic<uint8_t>* GetEntry(uint32_t threadId) {
        size_t slabIdx = threadId / TEpochSlab::kEntries;
        size_t entryIdx = threadId % TEpochSlab::kEntries;

        TEpochSlab* slab = &FirstSlab;
        for (size_t i = 0; i < slabIdx; ++i) {
            TEpochSlab* next = slab->Next.load(std::memory_order_acquire);
            if (!next) {
                auto* fresh = new TEpochSlab();
                TEpochSlab* expected = nullptr;
                if (slab->Next.compare_exchange_strong(expected, fresh,
                        std::memory_order_release, std::memory_order_relaxed)) {
                    next = fresh;
                } else {
                    delete fresh;
                    next = expected;
                }
            }
            slab = next;
        }
        return &slab->Entries[entryIdx].Epoch;
    }

    void Enter(std::atomic<uint8_t>* entry) {
        uint8_t epoch = static_cast<uint8_t>(
            GlobalEpoch.load(std::memory_order_acquire) % 3);
        entry->store(epoch, std::memory_order_release);
    }

    void Leave(std::atomic<uint8_t>* entry) {
        entry->store(kEpochInactive, std::memory_order_release);
    }

    void Retire(void* data, void (*freeFn)(void*)) {
        auto* node = new TLimboNode{nullptr, data, freeFn};
        uint32_t bag = static_cast<uint32_t>(
            GlobalEpoch.load(std::memory_order_relaxed) % 3);
        TLimboNode* head = LimboHead[bag].load(std::memory_order_relaxed);
        do {
            node->Next = head;
        } while (!LimboHead[bag].compare_exchange_weak(
            head, node, std::memory_order_release, std::memory_order_relaxed));
    }

    // Attempt to advance epoch and free old limbo bag.
    // Returns number of freed nodes.
    size_t TryReclaim() {
        uint64_t current = GlobalEpoch.load(std::memory_order_relaxed);
        uint8_t reclaimEpoch = static_cast<uint8_t>((current + 1) % 3);

        // Walk all slabs, check every entry.
        TEpochSlab* slab = &FirstSlab;
        while (slab) {
            for (size_t i = 0; i < TEpochSlab::kEntries; ++i) {
                uint8_t e = slab->Entries[i].Epoch.load(std::memory_order_acquire);
                if (e == reclaimEpoch) return 0;
            }
            slab = slab->Next.load(std::memory_order_acquire);
        }

        // Advance global epoch.
        if (!GlobalEpoch.compare_exchange_strong(current, current + 1,
                std::memory_order_release, std::memory_order_relaxed)) {
            return 0;
        }

        // Free reclaimEpoch's limbo.
        TLimboNode* node = LimboHead[reclaimEpoch].exchange(
            nullptr, std::memory_order_acquire);
        size_t freed = 0;
        while (node) {
            TLimboNode* next = node->Next;
            node->FreeFn(node->Data);
            delete node;
            ++freed;
            node = next;
        }
        return freed;
    }

    ~TDebra() {
        for (int i = 0; i < 3; ++i) {
            TLimboNode* node = LimboHead[i].load(std::memory_order_relaxed);
            while (node) {
                TLimboNode* next = node->Next;
                node->FreeFn(node->Data);
                delete node;
                node = next;
            }
        }
        TEpochSlab* slab = FirstSlab.Next.load(std::memory_order_relaxed);
        while (slab) {
            TEpochSlab* next = slab->Next.load(std::memory_order_relaxed);
            delete slab;
            slab = next;
        }
    }
};

// -----------------------------------------------------------------------
// RAII guard: Enter on construct, Leave on destruct.
// -----------------------------------------------------------------------

class TDebraGuard {
public:
    explicit TDebraGuard(TDebra& debra) : Debra_(debra) {
        Entry_ = Debra_.GetEntry(GetDebraThreadId());
        Debra_.Enter(Entry_);
    }

    ~TDebraGuard() { Debra_.Leave(Entry_); }

    TDebraGuard(const TDebraGuard&) = delete;
    TDebraGuard& operator=(const TDebraGuard&) = delete;

private:
    TDebra& Debra_;
    std::atomic<uint8_t>* Entry_;
};

} // namespace NActors::NWorkStealing
