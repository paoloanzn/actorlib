#pragma once

#include <algorithm>
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

// The largest chunk: 15 cache lines. Chunks are laid out back to back, so a power-of-two
// size puts the same offset of every chunk on a handful of cache sets: with many active
// mailboxes, 1024- and 4096-byte chunks measured 2-5x slower than 960 and 4032 (see
// benchmarks/).
inline constexpr std::size_t kDefaultChunkSize = 960;
inline constexpr std::size_t kDefaultInitialBytes = std::size_t{1} << 20;  // 1 MiB
inline constexpr std::size_t kDefaultMaxBytes = std::size_t{64} << 20;     // 64 MiB

// A message lives entirely inside the arena: no heap pointers, no destructor to run.
template <class Msg>
concept Message = std::is_trivially_copyable_v<Msg> && !std::is_array_v<Msg> &&
                  std::same_as<Msg, std::remove_cv_t<Msg>> && alignof(Msg) <= kChunkAlign;

struct ArenaConfig {
    std::size_t chunk_size = kDefaultChunkSize;        // largest chunk; multiple of kChunkAlign, ideally an odd one
    std::size_t initial_bytes = kDefaultInitialBytes;  // size of each slab; every size class grows by slabs
    std::size_t max_bytes = kDefaultMaxBytes;          // cap on chunk memory across all slabs
};

enum class SendResult : std::uint8_t {
    Ok,
    Full,       // no free chunk and the arena hit max_bytes (or the system is out of memory)
    DeadActor,  // the mailbox was destroyed, or the ref is null
};

namespace detail {
class Worker;
}  // namespace detail

// Typed handle to a mailbox. The generation makes refs to destroyed mailboxes fail
// instead of reaching whichever mailbox reused the slot.
template <Message Msg>
class ActorRef {
public:
    ActorRef() = default;  // null ref: never alive
    friend bool operator==(ActorRef, ActorRef) = default;

private:
    friend class MailboxArena;
    friend class detail::Worker;  // the runtime (virtual_worker.hpp) keeps the worker id in index_'s top bits
    // Private and called only by MailboxArena and the runtime, so the two ids cannot be swapped by users.
    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    ActorRef(std::uint32_t index, std::uint32_t generation) noexcept : index_{index}, generation_{generation} {}

    std::uint32_t index_ = std::numeric_limits<std::uint32_t>::max();
    std::uint32_t generation_ = 0;
};

// All mailboxes rent chunks from one arena. A mailbox is a FIFO queue of chunks: it takes
// a free chunk when its tail chunk is full and returns each chunk once drained. Messages
// never move, so references to them stay valid until consumed.
//
// Chunks come in size classes: 64, 320, 960 and 4032 bytes, the ones at most half of
// chunk_size, then chunk_size itself. Growth is geometric: each chunk a busy mailbox adds is
// one class bigger than the last, so a mailbox holding one message takes 64 bytes while a
// busy one reads long runs. When an emptied mailbox gives its last chunk back, its next
// first chunk is one class below that one, so hot mailboxes restart near their size and
// quiet ones shrink. Each class has its own pool, grown by slabs of initial_bytes (the
// smallest class's first slab up front, the others on first use); max_bytes caps them
// together. When a class's pool is empty and the cap is reached, a mailbox takes a chunk of
// any class its message fits.
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
        return isValid(ref.index_, ref.generation_) ? pendingBytes(slots_[ref.index_], sizeof(Msg)) / sizeof(Msg) : 0;
    }

    // Constructs Msg(args...) in place at the tail of dest's mailbox.
    template <Message Msg, class... Args>
        requires std::constructible_from<Msg, Args...>
    [[nodiscard]] SendResult send(ActorRef<Msg> dest,
                                  Args&&... args) noexcept(std::is_nothrow_constructible_v<Msg, Args...>) {
        if (!isValid(dest.index_, dest.generation_)) return SendResult::DeadActor;
        MailboxSlot& slot = slots_[dest.index_];
        // One compare covers both "tail chunk full" and "no chunk yet" (both are 0 then).
        if (slot.tail == slot.tail_end) [[unlikely]] {
            if (!appendChunk<sizeof(Msg)>(slot)) return SendResult::Full;
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
        std::uint32_t end = slot->head_end;  // only this drain moves to another head chunk
        for (;;) {
            // The messages of one chunk. Chunk bookkeeping stays out of this loop: its stores
            // could alias whatever fn writes, and would force fn's state through memory.
            while (handled < budget && head != end && (head != slot->tail || chunk != slot->tail_chunk)) {
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
            if (head != end) return handled;  // budget used up inside the chunk
            popHeadChunk<sizeof(Msg)>(*slot);
            chunk = slot->head_chunk;
            head = slot->head;
            end = slot->head_end;
        }
    }

    // Empty mailboxes that keep a chunk; each mailbox index maps to one spot.
    static constexpr std::uint32_t kParkingSpots = 64;

    // The largest chunk size class.
    [[nodiscard]] std::size_t chunkSize() const noexcept { return poolOf(top_class_).size; }
    // Chunks and bytes held by mailboxes (parked chunks included), and reserved from the
    // system, across all size classes.
    [[nodiscard]] std::size_t chunksInUse() const noexcept;
    [[nodiscard]] std::size_t chunksReserved() const noexcept;
    [[nodiscard]] std::size_t bytesInUse() const noexcept;
    [[nodiscard]] std::size_t bytesReserved() const noexcept { return bytes_reserved_; }

private:
    // head and tail are byte offsets into head_chunk and tail_chunk. They are integers, not
    // pointers, on purpose: compilers assume any pointer store may alias any pointer
    // (slots_' own included), so pointer cursors were reloaded through memory on every send.
    // An empty mailbox holds no chunk: both chunks are null and head, tail and both ends are 0.
    // One cache line per mailbox: a send or drain touches one line of the slot table, and
    // the power-of-two size keeps index checks free of divisions.
    struct alignas(kChunkAlign) MailboxSlot {
        std::byte* head_chunk = nullptr;  // oldest chunk, read side
        std::byte* tail_chunk = nullptr;  // newest chunk, write side
        // A cursor and its end are not neighbors on purpose: send would load tail and tail_end
        // as one 8-byte pair right after storing tail alone, a store-to-load forwarding stall
        // that measured 1.6x slower on bursts.
        std::uint32_t head = 0;         // next message to read in head_chunk
        std::uint32_t tail = 0;         // where the next message goes in tail_chunk
        std::uint32_t head_end = 0;     // end of the last message that fits in head_chunk
        std::uint32_t tail_end = 0;     // end of the last message that fits in tail_chunk
        std::uint32_t data_offset = 0;  // first message's offset inside a chunk
        std::uint32_t generation = 0;   // odd while alive, bumped on create and destroy
        std::uint32_t next_free = 0;    // free-slot list link while dead
        std::uint8_t start_class = 0;   // size class of the next first chunk
        std::uint8_t min_class = 0;     // smallest class the message fits
    };
    static_assert(sizeof(MailboxSlot) == kChunkAlign);

    // Each chunk starts with a header: the address of the next chunk of its mailbox, with the
    // chunk's own size class in the low bits, which chunk alignment leaves free. The class is
    // written when the chunk is taken; the tail chunk's next address is stale, so walks stop
    // at tail_chunk. Keeping the class in the chunk rather than the slot saves slot stores
    // each time a mailbox takes or returns a chunk.
    static constexpr std::size_t kChunkHeaderSize = sizeof(std::uintptr_t);
    static constexpr std::uint32_t kNoSlot = std::numeric_limits<std::uint32_t>::max();
    static constexpr std::array<std::size_t, 4> kClassLadder{64, 320, 960, 4032};
    static constexpr std::size_t kMaxClasses = kClassLadder.size() + 1;  // the ladder, then chunk_size
    static_assert(kMaxClasses <= kChunkAlign);                           // classes fit in a header's low bits

    template <class Msg>
    static constexpr std::size_t dataOffset() noexcept {
        return (kChunkHeaderSize + alignof(Msg) - 1) / alignof(Msg) * alignof(Msg);
    }

    // Byte offsets inside a chunk are the arena's job; the bounds come from head_end and tail_end.
    static std::byte* at(std::byte* chunk, std::size_t offset) noexcept {
        return chunk + offset;  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    }

    // Headers are copied as bytes: the storage is raw bytes, not integer objects. Compiles to
    // a single load or store. Slabs are linked the same way, with class 0.
    using HeaderBytes = std::array<std::byte, sizeof(std::uintptr_t)>;
    static std::uintptr_t headerOf(const std::byte* chunk) noexcept {
        HeaderBytes raw{};
        std::memcpy(raw.data(), chunk, raw.size());
        return std::bit_cast<std::uintptr_t>(raw);
    }
    static void setHeader(std::byte* chunk, std::uintptr_t header) noexcept {
        const auto raw = std::bit_cast<HeaderBytes>(header);
        std::memcpy(chunk, raw.data(), raw.size());
    }
    static std::byte* nextOf(const std::byte* chunk) noexcept {
        return std::bit_cast<std::byte*>(headerOf(chunk) & ~std::uintptr_t{kChunkAlign - 1});
    }
    static std::uint8_t classOf(const std::byte* chunk) noexcept {
        return static_cast<std::uint8_t>(headerOf(chunk) & (kChunkAlign - 1));
    }
    static void setNext(std::byte* chunk, std::byte* next) noexcept {
        setHeader(chunk, std::bit_cast<std::uintptr_t>(next) | classOf(chunk));
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

    // Free chunks of one size class, as a stack, not an intrusive list: taking a chunk reads
    // this array instead of a link inside a (possibly cold) chunk, so consecutive takes do
    // not wait on each other's cache misses. LIFO, so the most recently freed (cache-hot)
    // chunk goes first.
    struct ChunkPool {
        std::vector<std::byte*> free;  // sized to `reserved`, so releases never allocate
        std::size_t top = 0;
        std::size_t reserved = 0;
        std::size_t slab_chunks = 0;  // chunks added by each slab
        std::uint32_t size = 0;
    };

    // Classes come from the arena's own slots and chunk headers, all at most top_class_.
    ChunkPool& poolOf(std::uint8_t cls) noexcept {
        return pools_[cls];  // NOLINT(cppcoreguidelines-pro-bounds-constant-array-index)
    }
    [[nodiscard]] const ChunkPool& poolOf(std::uint8_t cls) const noexcept {
        return pools_[cls];  // NOLINT(cppcoreguidelines-pro-bounds-constant-array-index)
    }

    // End of the last whole message in a chunk of class cls. The hot callers pass msg_size as
    // a template argument, so the division compiles to a multiplication even when they are
    // not inlined. The three are a class, an offset and a size; the names say which.
    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    [[nodiscard]] std::uint32_t endOf(std::uint8_t cls, std::uint32_t data_offset,
                                      std::size_t msg_size) const noexcept {
        const std::size_t size = poolOf(cls).size;
        return static_cast<std::uint32_t>(data_offset + (size - data_offset) / msg_size * msg_size);
    }

    // A chunk and its size class.
    struct Taken {
        std::byte* chunk;
        std::uint8_t cls;
    };

    // Takes a chunk of class cls for slot; see acquireSlow for when that pool is empty.
    Taken acquireChunk(std::uint8_t cls, const MailboxSlot& slot) noexcept {
        ChunkPool& pool = poolOf(cls);
        if (pool.top == 0) [[unlikely]]
            return acquireSlow(cls, slot);
        return {pool.free[--pool.top], cls};
    }

    void releaseChunk(std::byte* chunk) noexcept {
        ChunkPool& pool = poolOf(classOf(chunk));
        pool.free[pool.top++] = chunk;
    }

    // A first chunk takes start_class; each later one is one class bigger than the tail.
    template <std::size_t MsgSize>
    bool appendChunk(MailboxSlot& slot) noexcept {
        const bool first = slot.tail_chunk == nullptr;
        const auto [chunk, cls] = acquireChunk(
            first ? slot.start_class : std::min(static_cast<std::uint8_t>(classOf(slot.tail_chunk) + 1), top_class_),
            slot);
        if (chunk == nullptr) [[unlikely]]
            return false;
        setHeader(chunk, cls);
        const std::uint32_t end = endOf(cls, slot.data_offset, MsgSize);
        if (first) {
            slot.head_chunk = chunk;
            slot.head = slot.data_offset;
            slot.head_end = end;
        } else {
            setNext(slot.tail_chunk, chunk);
        }
        slot.tail_chunk = chunk;
        slot.tail = slot.data_offset;
        slot.tail_end = end;
        return true;
    }

    // The head chunk is fully read and another chunk follows it.
    template <std::size_t MsgSize>
    void popHeadChunk(MailboxSlot& slot) noexcept {
        std::byte* next = nextOf(slot.head_chunk);
        releaseChunk(slot.head_chunk);
        slot.head_chunk = next;
        slot.head = slot.data_offset;
        slot.head_end = endOf(classOf(next), slot.data_offset, MsgSize);
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

    // No chunk; tail == tail_end, so send's full-chunk check also catches this.
    static void parkEmpty(MailboxSlot& slot) noexcept {
        slot.head_chunk = slot.tail_chunk = nullptr;
        slot.head = slot.tail = slot.head_end = slot.tail_end = 0;
    }

    // Gives back the only chunk of an emptied mailbox. Its next first chunk is one class
    // below this one, so a busy mailbox restarts near its size and a quiet one shrinks.
    void releaseLast(MailboxSlot& slot) noexcept {
        const std::uint8_t cls = classOf(slot.head_chunk);
        slot.start_class = cls > slot.min_class ? static_cast<std::uint8_t>(cls - 1) : slot.min_class;
        releaseChunk(slot.head_chunk);
        parkEmpty(slot);
    }

    // Releases a parked chunk unless its mailbox received messages since (or was destroyed).
    void unpark(MailboxSlot& slot) noexcept {
        if (isParked(slot)) releaseLast(slot);
    }

    // Slow paths, out of line in src/actorlib.cpp.
    [[nodiscard]] std::size_t pendingBytes(const MailboxSlot& slot, std::size_t msg_size) const noexcept;
    std::uint32_t createSlot(std::size_t data_offset, std::size_t msg_size);
    bool destroySlot(std::uint32_t index, std::uint32_t generation) noexcept;
    void parkElsewhere(std::uint32_t index, MailboxSlot& slot) noexcept;
    Taken acquireSlow(std::uint8_t cls, const MailboxSlot& slot) noexcept;
    static constexpr std::uint8_t kAnyClass = std::numeric_limits<std::uint8_t>::max();
    void reclaimParked(std::uint8_t cls) noexcept;  // parked chunks of class cls, or kAnyClass
    bool grow(std::uint8_t cls) noexcept;

    std::array<ChunkPool, kMaxClasses> pools_{};
    std::uint8_t top_class_ = 0;  // the largest class; classes 0..top_class_ are in use
    std::size_t max_bytes_ = 0;
    std::size_t bytes_reserved_ = 0;
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
}  // namespace actorlib
