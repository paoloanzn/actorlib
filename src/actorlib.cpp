#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <stdexcept>

#include <actorlib/actorlib.hpp>

namespace actorlib {
namespace {
// Each slab starts with a cache-line-sized header holding the next-slab link, so the
// chunks after it stay aligned.
constexpr std::size_t kSlabHeaderSize = kChunkAlign;
}  // namespace

MailboxArena::MailboxArena(ArenaConfig config) {
    if (config.chunk_size < kChunkAlign || config.chunk_size % kChunkAlign != 0 ||
        config.chunk_size > std::numeric_limits<std::uint32_t>::max()) {
        throw std::invalid_argument{"actorlib: chunk_size must be a 32-bit multiple of 64"};
    }
    if (config.initial_bytes < config.chunk_size || config.max_bytes < config.initial_bytes) {
        throw std::invalid_argument{"actorlib: need chunk_size <= initial_bytes <= max_bytes"};
    }
    std::uint8_t classes = 0;
    const auto add_class = [&](std::size_t size) {
        ChunkPool& pool = poolOf(classes++);
        pool.size = static_cast<std::uint32_t>(size);
        pool.slab_chunks = config.initial_bytes / size;
    };
    for (const std::size_t size : kClassLadder) {
        if (2 * size <= config.chunk_size) add_class(size);
    }
    add_class(config.chunk_size);
    top_class_ = static_cast<std::uint8_t>(classes - 1);
    max_bytes_ = config.max_bytes;
    if (!grow(0)) throw std::bad_alloc{};
}

MailboxArena::~MailboxArena() {
    while (slabs_ != nullptr) {
        std::byte* next = nextOf(slabs_);
        ::operator delete(slabs_, std::align_val_t{kChunkAlign});
        slabs_ = next;
    }
}

std::size_t MailboxArena::chunksInUse() const noexcept {
    std::size_t chunks = 0;
    for (std::uint8_t cls = 0; cls <= top_class_; ++cls) chunks += poolOf(cls).reserved - poolOf(cls).top;
    return chunks;
}

std::size_t MailboxArena::chunksReserved() const noexcept {
    std::size_t chunks = 0;
    for (std::uint8_t cls = 0; cls <= top_class_; ++cls) chunks += poolOf(cls).reserved;
    return chunks;
}

std::size_t MailboxArena::bytesInUse() const noexcept {
    std::size_t bytes = 0;
    for (std::uint8_t cls = 0; cls <= top_class_; ++cls) {
        const ChunkPool& pool = poolOf(cls);
        bytes += (pool.reserved - pool.top) * pool.size;
    }
    return bytes;
}

std::uint32_t MailboxArena::createSlot(std::size_t data_offset, std::size_t msg_size) {
    if (data_offset + msg_size > chunkSize()) {
        throw std::length_error{"actorlib: message type does not fit in one chunk"};
    }
    std::uint32_t index = free_slots_;
    if (index != kNoSlot) {
        free_slots_ = slots_[index].next_free;
    } else {
        if (slots_.size() >= kNoSlot) throw std::length_error{"actorlib: too many mailboxes"};
        index = static_cast<std::uint32_t>(slots_.size());
        slots_.emplace_back();
    }
    MailboxSlot& slot = slots_[index];
    std::uint8_t min_class = 0;
    while (poolOf(min_class).size < data_offset + msg_size) ++min_class;
    slot.data_offset = static_cast<std::uint32_t>(data_offset);
    slot.min_class = slot.start_class = min_class;
    parkEmpty(slot);
    ++slot.generation;  // even (dead) -> odd (alive)
    return index;
}

bool MailboxArena::destroySlot(std::uint32_t index, std::uint32_t generation) noexcept {
    if (!isValid(index, generation)) return false;
    MailboxSlot& slot = slots_[index];
    // Every chunk from the head chunk to the tail chunk belongs to this mailbox.
    for (std::byte* chunk = slot.head_chunk; chunk != nullptr;) {
        std::byte* next = chunk == slot.tail_chunk ? nullptr : nextOf(chunk);
        releaseChunk(chunk);
        chunk = next;
    }
    parkEmpty(slot);
    ++slot.generation;  // odd -> even; after 2^31 reuses of one slot a stale ref could match again
    slot.next_free = free_slots_;
    free_slots_ = index;
    return true;
}

std::size_t MailboxArena::pendingBytes(const MailboxSlot& slot, std::size_t msg_size) const noexcept {
    if (isEmpty(slot)) return 0;
    if (slot.head_chunk == slot.tail_chunk) return slot.tail - slot.head;
    // The head chunk's rest, every full chunk in between, then the tail chunk's start.
    std::size_t bytes = slot.head_end - slot.head;
    for (const std::byte* chunk = nextOf(slot.head_chunk); chunk != slot.tail_chunk; chunk = nextOf(chunk)) {
        bytes += endOf(classOf(chunk), slot.data_offset, msg_size) - slot.data_offset;
    }
    return bytes + (slot.tail - slot.data_offset);
}

// park() for a mailbox that does not hold its spot yet.
void MailboxArena::parkElsewhere(std::uint32_t index, MailboxSlot& slot) noexcept {
    std::uint32_t& spot = spotFor(index);
    if (spot != kNoSlot && isParked(slots_[spot])) {
        releaseLast(slot);
        return;
    }
    spot = index;
    slot.head = slot.tail = slot.data_offset;
}

// The pool of class cls is empty. Take back the parked chunks of that class, then grow the
// pool, so parking never makes the arena grow. At max_bytes, take back every parked chunk
// and use any class from min_class up: Full only when no class the message fits has one.
MailboxArena::Taken MailboxArena::acquireSlow(std::uint8_t cls, const MailboxSlot& slot) noexcept {
    reclaimParked(cls);
    if (poolOf(cls).top == 0 && !grow(cls)) {
        reclaimParked(kAnyClass);
        std::uint8_t other = slot.min_class;
        while (other <= top_class_ && poolOf(other).top == 0) ++other;
        if (other > top_class_) return {nullptr, cls};
        cls = other;
    }
    ChunkPool& pool = poolOf(cls);
    return {pool.free[--pool.top], cls};
}

void MailboxArena::reclaimParked(std::uint8_t cls) noexcept {
    for (std::uint32_t& spot : parking_) {
        if (spot == kNoSlot) continue;
        MailboxSlot& slot = slots_[spot];
        if (!isParked(slot)) {
            spot = kNoSlot;  // its mailbox received messages since, or was destroyed
        } else if (cls == kAnyClass || classOf(slot.head_chunk) == cls) {
            unpark(slot);
            spot = kNoSlot;
        }
    }
}

bool MailboxArena::grow(std::uint8_t cls) noexcept {
    ChunkPool& pool = poolOf(cls);
    const std::size_t chunks = std::min(pool.slab_chunks, (max_bytes_ - bytes_reserved_) / pool.size);
    if (chunks == 0) return false;
    void* memory = ::operator new(kSlabHeaderSize + chunks * pool.size, std::align_val_t{kChunkAlign}, std::nothrow);
    if (memory == nullptr) return false;
    try {
        pool.free.resize(pool.reserved + chunks);  // room for every chunk, so releases never allocate
    } catch (const std::bad_alloc&) {
        ::operator delete(memory, std::align_val_t{kChunkAlign});
        return false;
    }
    auto* slab = static_cast<std::byte*>(memory);
    setHeader(slab, std::bit_cast<std::uintptr_t>(slabs_));
    slabs_ = slab;
    // Push in reverse so chunks are handed out in address order. Each chunk's header gets
    // its class written when the chunk is taken; until then the pool knows it.
    for (std::size_t i = chunks; i-- > 0;) pool.free[pool.top++] = at(slab, kSlabHeaderSize + i * pool.size);
    pool.reserved += chunks;
    bytes_reserved_ += chunks * pool.size;
    return true;
}
}  // namespace actorlib
