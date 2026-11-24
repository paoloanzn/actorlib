#pragma once

#include <array>
#include <bit>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <new>
#include <type_traits>
#include <utility>
#include <vector>

namespace actorlib {
// Slabs and the chunks carved from them are aligned to a cache line.
inline constexpr std::size_t kChunkAlign = 64;

// 15 cache lines. Chunks are laid out back to back, so a power-of-two size puts the same
// offset of every chunk on a handful of cache sets: with many active mailboxes, 1024- and
// 4096-byte chunks measured 2-5x slower than 960 and 4032 (see benchmarks/).
inline constexpr std::size_t kDefaultChunkSize = 960;
inline constexpr std::size_t kDefaultInitialBytes = std::size_t{1} << 20;  // 1 MiB
inline constexpr std::size_t kDefaultMaxBytes = std::size_t{64} << 20;     // 64 MiB

// A message lives entirely inside the arena: no heap pointers, no destructor to run.
template <class Msg>
concept Message = std::is_trivially_copyable_v<Msg> && !std::is_array_v<Msg> &&
                  std::same_as<Msg, std::remove_cv_t<Msg>> && alignof(Msg) <= kChunkAlign;

struct ArenaConfig {
    std::size_t chunk_size = kDefaultChunkSize;        // multiple of kChunkAlign, ideally an odd one
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
//
// A mailbox that empties may keep its last chunk "parked", so a mailbox that keeps going
// empty and receiving again (request/response, ping-pong) does not trade a chunk with
// the free list per message. At most kParkingSpots chunks are parked at a time, and they
// are taken back before the arena grows.
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

    // Walks the mailbox's chunks: O(chunks), meant for tests and monitoring.
    template <Message Msg>
    [[nodiscard]] std::size_t pending(ActorRef<Msg> ref) const noexcept {
        return isValid(ref.index_, ref.generation_) ? pendingBytes(slots_[ref.index_]) / sizeof(Msg) : 0;
    }

    // Constructs Msg(args...) in place at the tail of dest's mailbox.
    template <Message Msg, class... Args>
        requires std::constructible_from<Msg, Args...>
    [[nodiscard]] SendResult send(ActorRef<Msg> dest,
                                  Args&&... args) noexcept(std::is_nothrow_constructible_v<Msg, Args...>) {
        if (!isValid(dest.index_, dest.generation_)) return SendResult::DeadActor;
        MailboxSlot& slot = slots_[dest.index_];
        // One compare covers both "tail chunk full" and "no chunk yet" (tail parks at data_end).
        if (slot.tail == slot.data_end) [[unlikely]] {
            if (!appendChunk(slot)) return SendResult::Full;
        }
        ::new (static_cast<void*>(at(slot.tail_chunk, slot.tail))) Msg(std::forward<Args>(args)...);
        slot.tail += static_cast<std::uint32_t>(sizeof(Msg));  // commit only once construction succeeded
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
        if (!isValid(ref.index_, ref.generation_)) return handled;
        // The read cursor lives in locals: fn cannot move it (it may not drain this mailbox,
        // and sends only touch the tail), so it is never reloaded through memory.
        MailboxSlot* slot = &slots_[ref.index_];
        std::byte* chunk = slot->head_chunk;
        std::uint32_t head = slot->head;
        for (;;) {
            // The messages of one chunk. Chunk bookkeeping stays out of this loop: its stores
            // could alias whatever fn writes, and would force fn's state through memory.
            while (handled < budget && head != slot->data_end && (head != slot->tail || chunk != slot->tail_chunk)) {
                // The slot holds a Msg constructed by send(); launder makes the cast well-defined.
                // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
                fn(*std::launder(reinterpret_cast<Msg*>(at(chunk, head))));
                ++handled;
                // fn may have destroyed this mailbox or grown slots_: look the slot up again.
                if (!isValid(ref.index_, ref.generation_)) [[unlikely]]
                    return handled;
                slot = &slots_[ref.index_];
                head += static_cast<std::uint32_t>(sizeof(Msg));
                slot->head = head;
            }
            if (head == slot->tail && chunk == slot->tail_chunk) {
                if (chunk != nullptr) park(ref.index_, *slot);
                return handled;
            }
            if (head != slot->data_end) return handled;  // budget used up inside the chunk
            popHeadChunk(*slot);
            chunk = slot->head_chunk;
            head = slot->head;
        }
    }

    // Empty mailboxes that keep a chunk; each mailbox index maps to one spot.
    static constexpr std::uint32_t kParkingSpots = 64;

    [[nodiscard]] std::size_t chunkSize() const noexcept { return chunk_size_; }
    [[nodiscard]] std::size_t chunksInUse() const noexcept { return chunks_reserved_ - free_top_; }
    [[nodiscard]] std::size_t chunksReserved() const noexcept { return chunks_reserved_; }

private:
    // head and tail are byte offsets into head_chunk and tail_chunk. They are integers, not
    // pointers, on purpose: compilers assume any pointer store may alias any pointer
    // (slots_' own included), so pointer cursors were reloaded through memory on every send.
    // An empty mailbox holds no chunk: both chunks are null and head == tail == data_end.
    // One cache line per mailbox: a send or drain touches one line of the slot table, and
    // the power-of-two size keeps index checks free of divisions.
    struct alignas(kChunkAlign) MailboxSlot {
        std::byte* head_chunk = nullptr;  // oldest chunk, read side
        std::byte* tail_chunk = nullptr;  // newest chunk, write side
        std::uint32_t head = 0;           // next message to read in head_chunk
        std::uint32_t tail = 0;           // where the next message goes in tail_chunk
        std::uint32_t data_offset = 0;    // first message's offset inside a chunk
        std::uint32_t data_end = 0;       // end of the last message that fits in a chunk
        std::uint32_t generation = 0;     // odd while alive, bumped on create and destroy
        std::uint32_t next_free = 0;      // free-slot list link while dead
    };
    static_assert(sizeof(MailboxSlot) == kChunkAlign);

    // Each chunk starts with a link to the next chunk of its mailbox. The tail chunk's link
    // is stale: walks stop at tail_chunk.
    static constexpr std::size_t kChunkHeaderSize = sizeof(std::byte*);
    static constexpr std::uint32_t kNoSlot = std::numeric_limits<std::uint32_t>::max();

    template <class Msg>
    static constexpr std::size_t dataOffset() noexcept {
        return (kChunkHeaderSize + alignof(Msg) - 1) / alignof(Msg) * alignof(Msg);
    }

    // Byte offsets inside a chunk are the arena's job; the bounds come from data_end.
    static std::byte* at(std::byte* chunk, std::size_t offset) noexcept {
        return chunk + offset;  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    }

    // Links are copied as bytes: the storage is raw bytes, not pointer objects. Compiles to
    // a single load or store.
    using LinkBytes = std::array<std::byte, sizeof(std::byte*)>;
    static std::byte* nextOf(const std::byte* chunk) noexcept {
        LinkBytes raw{};
        std::memcpy(raw.data(), chunk, raw.size());
        return std::bit_cast<std::byte*>(raw);
    }
    // Both are chunk (or slab) pointers by nature; the names say which is which.
    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    static void setNext(std::byte* chunk, std::byte* next) noexcept {
        const auto raw = std::bit_cast<LinkBytes>(next);
        std::memcpy(chunk, raw.data(), raw.size());
    }

    // In different chunks head and tail never compare equal as a pair.
    static bool isEmpty(const MailboxSlot& slot) noexcept {
        return slot.head == slot.tail && slot.head_chunk == slot.tail_chunk;
    }

    // Generations of live slots are odd and refs are only made for live slots, so equal
    // generations mean "this mailbox, still alive". A null ref fails the index check.
    [[nodiscard]] bool isValid(std::uint32_t index, std::uint32_t generation) const noexcept {
        return index < slots_.size() && slots_[index].generation == generation;
    }

    std::byte* acquireChunk() noexcept {
        if (free_top_ == 0 && !reclaimParked() && !grow()) [[unlikely]]
            return nullptr;
        return free_[--free_top_];
    }

    void releaseChunk(std::byte* chunk) noexcept { free_[free_top_++] = chunk; }

    bool appendChunk(MailboxSlot& slot) noexcept {
        std::byte* chunk = acquireChunk();
        if (chunk == nullptr) [[unlikely]]
            return false;
        if (slot.tail_chunk != nullptr) {
            setNext(slot.tail_chunk, chunk);
        } else {
            slot.head_chunk = chunk;
            slot.head = slot.data_offset;
        }
        slot.tail_chunk = chunk;
        slot.tail = slot.data_offset;
        return true;
    }

    // The head chunk is fully read and another chunk follows it.
    void popHeadChunk(MailboxSlot& slot) noexcept {
        std::byte* next = nextOf(slot.head_chunk);
        releaseChunk(slot.head_chunk);
        slot.head_chunk = next;
        slot.head = slot.data_offset;
    }

    // The mailbox at `index` was just emptied. It keeps its only chunk, rewound, if it can
    // take its parking spot: the spot is free, already its own, or held by a mailbox that
    // received messages since. Otherwise the spot's mailbox is still parked and keeps it,
    // and this mailbox gives its chunk back. (Evicting the incumbent measured slower: it
    // hands out the incumbent's cold chunk next instead of this mailbox's hot one.)
    void park(std::uint32_t index, MailboxSlot& slot) noexcept {
        if (spotFor(index) == index) [[likely]] {
            slot.head = slot.tail = slot.data_offset;
        } else {
            parkElsewhere(index, slot);
        }
    }

    std::uint32_t& spotFor(std::uint32_t index) noexcept {
        // The mask keeps the index inside the array.
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index)
        return parking_[index & (kParkingSpots - 1)];
    }

    static bool isParked(const MailboxSlot& slot) noexcept { return slot.head_chunk != nullptr && isEmpty(slot); }

    // No chunk; tail parks at data_end so send's full-chunk check also catches this.
    static void parkEmpty(MailboxSlot& slot) noexcept {
        slot.head_chunk = slot.tail_chunk = nullptr;
        slot.head = slot.tail = slot.data_end;
    }

    // Releases a parked chunk unless its mailbox received messages since (or was destroyed).
    void unpark(MailboxSlot& slot) noexcept {
        if (isParked(slot)) {
            releaseChunk(slot.head_chunk);
            parkEmpty(slot);
        }
    }

    // Slow paths, out of line in src/actorlib.cpp.
    [[nodiscard]] static std::size_t pendingBytes(const MailboxSlot& slot) noexcept;
    std::uint32_t createSlot(std::size_t data_offset, std::size_t msg_size);
    bool destroySlot(std::uint32_t index, std::uint32_t generation) noexcept;
    void parkElsewhere(std::uint32_t index, MailboxSlot& slot) noexcept;
    bool reclaimParked() noexcept;
    bool grow() noexcept;

    std::size_t chunk_size_ = 0;
    std::size_t slab_chunks_ = 0;  // chunks added by each slab
    std::size_t max_chunks_ = 0;
    std::size_t chunks_reserved_ = 0;
    // Free chunks as a stack, not an intrusive list: taking a chunk reads this array instead
    // of a link inside a (possibly cold) chunk, so consecutive takes do not wait on each
    // other's cache misses. LIFO, so the most recently freed (cache-hot) chunk goes first.
    std::vector<std::byte*> free_;  // sized to chunks_reserved_
    std::size_t free_top_ = 0;
    std::byte* slabs_ = nullptr;  // intrusive list, freed in the destructor
    std::uint32_t free_slots_ = kNoSlot;
    std::vector<MailboxSlot> slots_;
    std::array<std::uint32_t, kParkingSpots> parking_ = filledParking();

    static constexpr std::array<std::uint32_t, kParkingSpots> filledParking() noexcept {
        std::array<std::uint32_t, kParkingSpots> spots{};
        spots.fill(kNoSlot);
        return spots;
    }
};

class Actor {
public:
    explicit Actor(int id) : id_{id} {}

    [[nodiscard]] int id() const noexcept { return id_; }

private:
    int id_;
};
}  // namespace actorlib
