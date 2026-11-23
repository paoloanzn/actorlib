#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <new>
#include <stdexcept>

#include <actorlib/actorlib.hpp>

namespace actorlib {
namespace {
// Each slab starts with a cache-line-sized header holding the next-slab link, so the
// chunks after it stay aligned.
constexpr std::size_t kSlabHeaderSize = kChunkAlign;

// Links are copied as bytes: the storage is raw bytes, not pointer objects. Compiles to a
// single load or store.
using LinkBytes = std::array<std::byte, sizeof(std::byte*)>;

std::byte* nextOf(const std::byte* block) noexcept {
    LinkBytes raw{};
    std::memcpy(raw.data(), block, raw.size());
    return std::bit_cast<std::byte*>(raw);
}

// Both are chunk (or slab) pointers by nature; the names say which is which.
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
void setNext(std::byte* block, std::byte* next) noexcept {
    const auto raw = std::bit_cast<LinkBytes>(next);
    std::memcpy(block, raw.data(), raw.size());
}
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
    slot.slots_per_chunk = static_cast<std::uint32_t>((chunk_size_ - data_offset) / msg_size);
    slot.alive = true;
    return index;
}

bool MailboxArena::destroySlot(std::uint32_t index, std::uint32_t generation) noexcept {
    if (!isValid(index, generation)) return false;
    MailboxSlot& slot = slots_[index];
    releaseChunks(slot);
    slot.alive = false;
    ++slot.generation;  // after 2^32 reuses of one slot a stale ref could match again
    slot.next_free = free_slots_;
    free_slots_ = index;
    return true;
}

bool MailboxArena::appendChunk(MailboxSlot& slot) noexcept {
    std::byte* chunk = acquireChunk();
    if (chunk == nullptr) return false;
    setNext(chunk, nullptr);
    if (slot.tail_chunk != nullptr) {
        setNext(slot.tail_chunk, chunk);
    } else {
        slot.head_chunk = chunk;
        slot.head = 0;
    }
    slot.tail_chunk = chunk;
    slot.tail = 0;
    return true;
}

void MailboxArena::popHeadChunk(MailboxSlot& slot) noexcept {
    std::byte* next = nextOf(slot.head_chunk);
    releaseChunk(slot.head_chunk);
    slot.head_chunk = next;
    slot.head = 0;
}

void MailboxArena::releaseChunks(MailboxSlot& slot) noexcept {
    for (std::byte* chunk = slot.head_chunk; chunk != nullptr;) {
        std::byte* next = nextOf(chunk);
        releaseChunk(chunk);
        chunk = next;
    }
    slot.head_chunk = nullptr;
    slot.tail_chunk = nullptr;
    slot.head = 0;
    slot.tail = 0;
    slot.count = 0;
}

std::byte* MailboxArena::acquireChunk() noexcept {
    if (free_chunks_ == nullptr && !grow()) return nullptr;
    std::byte* chunk = free_chunks_;
    free_chunks_ = nextOf(chunk);
    ++chunks_in_use_;
    return chunk;
}

void MailboxArena::releaseChunk(std::byte* chunk) noexcept {
    setNext(chunk, free_chunks_);
    free_chunks_ = chunk;
    --chunks_in_use_;
}

bool MailboxArena::grow() noexcept {
    const std::size_t chunks = std::min(slab_chunks_, max_chunks_ - chunks_reserved_);
    if (chunks == 0) return false;
    void* memory = ::operator new(kSlabHeaderSize + chunks * chunk_size_, std::align_val_t{kChunkAlign}, std::nothrow);
    if (memory == nullptr) return false;
    auto* slab = static_cast<std::byte*>(memory);
    setNext(slab, slabs_);
    slabs_ = slab;
    // Push in reverse so chunks are handed out in address order.
    for (std::size_t i = chunks; i-- > 0;) {
        // Carving a slab into chunks is byte-level arithmetic inside the block just allocated.
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        std::byte* chunk = slab + kSlabHeaderSize + i * chunk_size_;
        setNext(chunk, free_chunks_);
        free_chunks_ = chunk;
    }
    chunks_reserved_ += chunks;
    return true;
}
}  // namespace actorlib
