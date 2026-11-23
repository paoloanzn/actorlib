#pragma once

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <type_traits>
#include <utility>
#include <vector>

namespace actorlib {
// Slabs and the chunks carved from them are aligned to a cache line.
inline constexpr std::size_t kChunkAlign = 64;

inline constexpr std::size_t kDefaultChunkSize = 4096;
inline constexpr std::size_t kDefaultInitialBytes = std::size_t{1} << 20;  // 1 MiB
inline constexpr std::size_t kDefaultMaxBytes = std::size_t{64} << 20;     // 64 MiB

// A message lives entirely inside the arena: no heap pointers, no destructor to run.
template <class Msg>
concept Message = std::is_trivially_copyable_v<Msg> && !std::is_array_v<Msg> &&
                  std::same_as<Msg, std::remove_cv_t<Msg>> && alignof(Msg) <= kChunkAlign;

struct ArenaConfig {
    std::size_t chunk_size = kDefaultChunkSize;        // multiple of kChunkAlign
    std::size_t initial_bytes = kDefaultInitialBytes;  // first slab, and the size of each later slab
    std::size_t max_bytes = kDefaultMaxBytes;          // cap on chunk memory across all slabs
};

enum class SendResult : std::uint8_t {
    Ok,
    Full,       // no free chunk and the arena hit max_bytes (or the system is out of memory)
    DeadActor,  // the mailbox was destroyed, or the ref is null
};

// Typed handle to a mailbox. The generation makes refs to destroyed mailboxes fail
// instead of reaching whichever mailbox reused the slot.
template <Message Msg>
class ActorRef {
public:
    ActorRef() = default;  // null ref: never alive
    friend bool operator==(ActorRef, ActorRef) = default;

private:
    friend class MailboxArena;
    // Private and called only by MailboxArena, so the two ids cannot be swapped by users.
    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    ActorRef(std::uint32_t index, std::uint32_t generation) noexcept : index_{index}, generation_{generation} {}

    std::uint32_t index_ = std::numeric_limits<std::uint32_t>::max();
    std::uint32_t generation_ = 0;
};

// All mailboxes rent fixed-size chunks from one pool. A mailbox is a FIFO queue of
// chunks: it takes a free chunk when its tail chunk is full and returns each chunk once
// drained. Messages never move, so references to them stay valid until consumed.
// Not thread-safe: use one arena per thread.
class MailboxArena {
public:
    explicit MailboxArena(ArenaConfig config = {});
    ~MailboxArena();
    MailboxArena(const MailboxArena&) = delete;
    MailboxArena& operator=(const MailboxArena&) = delete;
    MailboxArena(MailboxArena&&) = delete;
    MailboxArena& operator=(MailboxArena&&) = delete;

    // Throws std::length_error if Msg does not fit in one chunk.
    template <Message Msg>
    [[nodiscard]] ActorRef<Msg> create() {
        const std::uint32_t index = createSlot(dataOffset<Msg>(), sizeof(Msg));
        return ActorRef<Msg>{index, slots_[index].generation};
    }

    // Drops pending messages and returns the mailbox's chunks. False if already dead.
    template <Message Msg>
    bool destroy(ActorRef<Msg> ref) noexcept {
        return destroySlot(ref.index_, ref.generation_);
    }

    template <Message Msg>
    [[nodiscard]] bool isAlive(ActorRef<Msg> ref) const noexcept {
        return isValid(ref.index_, ref.generation_);
    }

    template <Message Msg>
    [[nodiscard]] std::size_t pending(ActorRef<Msg> ref) const noexcept {
        return isValid(ref.index_, ref.generation_) ? slots_[ref.index_].count : 0;
    }

    // Constructs Msg(args...) in place at the tail of dest's mailbox.
    template <Message Msg, class... Args>
        requires std::constructible_from<Msg, Args...>
    [[nodiscard]] SendResult send(ActorRef<Msg> dest,
                                  Args&&... args) noexcept(std::is_nothrow_constructible_v<Msg, Args...>) {
        if (!isValid(dest.index_, dest.generation_)) return SendResult::DeadActor;
        MailboxSlot& slot = slots_[dest.index_];
        if (slot.tail_chunk == nullptr || slot.tail == slot.slots_per_chunk) [[unlikely]] {
            if (!appendChunk(slot)) return SendResult::Full;
        }
        ::new (static_cast<void*>(slotAt<Msg>(slot.tail_chunk, slot.tail))) Msg(std::forward<Args>(args)...);
        ++slot.tail;  // commit only once construction succeeded
        ++slot.count;
        return SendResult::Ok;
    }

    // Calls fn(msg) on up to `budget` messages in FIFO order; returns how many it handled.
    // fn may send (to this mailbox too), create and destroy mailboxes, but must not drain
    // this mailbox. If fn throws, the message stays at the head of the mailbox.
    // fn is taken by value, like STL algorithms; pass std::ref(fn) to avoid the copy.
    template <Message Msg, class F>
        requires std::invocable<F&, Msg&>
    std::size_t drain(ActorRef<Msg> ref, std::size_t budget, F fn) {
        std::size_t handled = 0;
        while (handled < budget && isValid(ref.index_, ref.generation_)) {
            const MailboxSlot& slot = slots_[ref.index_];
            if (slot.count == 0) break;
            // The slot holds a Msg constructed by send(); launder makes the cast well-defined.
            // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
            fn(*std::launder(reinterpret_cast<Msg*>(slotAt<Msg>(slot.head_chunk, slot.head))));
            ++handled;
            // fn may have destroyed this mailbox or grown slots_: look the slot up again.
            if (!isValid(ref.index_, ref.generation_)) break;
            consumeHead(slots_[ref.index_]);
        }
        return handled;
    }

    [[nodiscard]] std::size_t chunkSize() const noexcept { return chunk_size_; }
    [[nodiscard]] std::size_t chunksInUse() const noexcept { return chunks_in_use_; }
    [[nodiscard]] std::size_t chunksReserved() const noexcept { return chunks_reserved_; }

private:
    struct MailboxSlot {
        std::byte* head_chunk = nullptr;  // oldest chunk, read side
        std::byte* tail_chunk = nullptr;  // newest chunk, write side
        std::size_t count = 0;            // pending messages
        std::uint32_t head = 0;           // next slot to read in head_chunk
        std::uint32_t tail = 0;           // next slot to write in tail_chunk
        std::uint32_t slots_per_chunk = 0;
        std::uint32_t generation = 0;  // bumped on destroy
        std::uint32_t next_free = 0;   // free-slot list link while dead
        bool alive = false;
    };

    // Each chunk starts with a link to the next chunk, in its mailbox or in the free list.
    static constexpr std::size_t kChunkHeaderSize = sizeof(std::byte*);
    static constexpr std::uint32_t kNoSlot = std::numeric_limits<std::uint32_t>::max();

    template <class Msg>
    static constexpr std::size_t dataOffset() noexcept {
        return (kChunkHeaderSize + alignof(Msg) - 1) / alignof(Msg) * alignof(Msg);
    }

    template <class Msg>
    static std::byte* slotAt(std::byte* chunk, std::uint32_t i) noexcept {
        // Byte offsets inside a chunk are the arena's job; the chunk bounds come from slots_per_chunk.
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        return chunk + dataOffset<Msg>() + std::size_t{i} * sizeof(Msg);
    }

    [[nodiscard]] bool isValid(std::uint32_t index, std::uint32_t generation) const noexcept {
        return index < slots_.size() && slots_[index].alive && slots_[index].generation == generation;
    }

    void consumeHead(MailboxSlot& slot) noexcept {
        ++slot.head;
        if (--slot.count == 0)
            releaseChunks(slot);  // empty mailboxes hold no chunks
        else if (slot.head == slot.slots_per_chunk)
            popHeadChunk(slot);
    }

    // Slow paths, out of line in src/actorlib.cpp.
    std::uint32_t createSlot(std::size_t data_offset, std::size_t msg_size);
    bool destroySlot(std::uint32_t index, std::uint32_t generation) noexcept;
    bool appendChunk(MailboxSlot& slot) noexcept;
    void popHeadChunk(MailboxSlot& slot) noexcept;
    void releaseChunks(MailboxSlot& slot) noexcept;
    std::byte* acquireChunk() noexcept;
    void releaseChunk(std::byte* chunk) noexcept;
    bool grow() noexcept;

    std::size_t chunk_size_ = 0;
    std::size_t slab_chunks_ = 0;  // chunks added by each slab
    std::size_t max_chunks_ = 0;
    std::size_t chunks_reserved_ = 0;
    std::size_t chunks_in_use_ = 0;
    std::byte* free_chunks_ = nullptr;  // LIFO, so the most recently freed (cache-hot) chunk is reused first
    std::byte* slabs_ = nullptr;        // intrusive list, freed in the destructor
    std::uint32_t free_slots_ = kNoSlot;
    std::vector<MailboxSlot> slots_;
};

class Actor {
public:
    explicit Actor(int id) : id_{id} {}

    [[nodiscard]] int id() const noexcept { return id_; }

private:
    int id_;
};
}  // namespace actorlib
