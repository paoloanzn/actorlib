#include <algorithm>
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
    chunk_size_ = config.chunk_size;
    slab_chunks_ = config.initial_bytes / chunk_size_;
    max_chunks_ = config.max_bytes / chunk_size_;
    if (!grow()) throw std::bad_alloc{};
}

MailboxArena::~MailboxArena() {
    while (slabs_ != nullptr) {
        std::byte* next = nextOf(slabs_);
        ::operator delete(slabs_, std::align_val_t{kChunkAlign});
        slabs_ = next;
    }
}

std::uint32_t MailboxArena::createSlot(std::size_t data_offset, std::size_t msg_size) {
    if (data_offset + msg_size > chunk_size_) {
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
    const std::size_t per_chunk = (chunk_size_ - data_offset) / msg_size;
    slot.data_offset = static_cast<std::uint32_t>(data_offset);
    slot.data_end = static_cast<std::uint32_t>(data_offset + per_chunk * msg_size);
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

std::size_t MailboxArena::pendingBytes(const MailboxSlot& slot) noexcept {
    if (isEmpty(slot)) return 0;
    if (slot.head_chunk == slot.tail_chunk) return slot.tail - slot.head;
    // The head chunk's rest, every full chunk in between, then the tail chunk's start.
    std::size_t bytes = slot.data_end - slot.head;
    for (const std::byte* chunk = nextOf(slot.head_chunk); chunk != slot.tail_chunk; chunk = nextOf(chunk)) {
        bytes += slot.data_end - slot.data_offset;
    }
    return bytes + (slot.tail - slot.data_offset);
}

// park() for a mailbox that does not hold its spot yet.
void MailboxArena::parkElsewhere(std::uint32_t index, MailboxSlot& slot) noexcept {
    std::uint32_t& spot = spotFor(index);
    if (spot != kNoSlot && isParked(slots_[spot])) {
        releaseChunk(slot.head_chunk);
        parkEmpty(slot);
        return;
    }
    spot = index;
    slot.head = slot.tail = slot.data_offset;
}

// Out of free chunks: take back the parked ones before asking for more memory.
bool MailboxArena::reclaimParked() noexcept {
    for (std::uint32_t& spot : parking_) {
        if (spot == kNoSlot) continue;
        unpark(slots_[spot]);
        spot = kNoSlot;
    }
    return free_top_ != 0;
}

bool MailboxArena::grow() noexcept {
    const std::size_t chunks = std::min(slab_chunks_, max_chunks_ - chunks_reserved_);
    if (chunks == 0) return false;
    void* memory = ::operator new(kSlabHeaderSize + chunks * chunk_size_, std::align_val_t{kChunkAlign}, std::nothrow);
    if (memory == nullptr) return false;
    try {
        free_.resize(chunks_reserved_ + chunks);  // room for every chunk, so releases never allocate
    } catch (const std::bad_alloc&) {
        ::operator delete(memory, std::align_val_t{kChunkAlign});
        return false;
    }
    auto* slab = static_cast<std::byte*>(memory);
    setNext(slab, slabs_);
    slabs_ = slab;
    // Push in reverse so chunks are handed out in address order.
    for (std::size_t i = chunks; i-- > 0;) releaseChunk(at(slab, kSlabHeaderSize + i * chunk_size_));
    chunks_reserved_ += chunks;
    return true;
}
}  // namespace actorlib
