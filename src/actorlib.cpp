#include <algorithm>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <new>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include <actorlib/actorlib.hpp>
#include <actorlib/virtual_worker.hpp>

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

// --- Runtime: workers and the VirtualWorker -------------------------------------------------

namespace detail {
namespace {
constexpr std::uint32_t kFlushTurns = 16;  // a busy worker hands its outboxes over this often
constexpr std::size_t kSpareBatches = 64;  // empty batches a worker keeps for its outboxes
constexpr int kIdleSpins = 2000;           // inbox checks before an idle worker sleeps
constexpr std::size_t kInitialReady = 64;

void cpuRelax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    asm volatile("yield");
#else
    std::this_thread::yield();
#endif
}
}  // namespace

void Shared::release() noexcept {
    if (live.fetch_sub(1, std::memory_order_acq_rel) == 1 && started.load(std::memory_order_acquire)) finish();
}

void Shared::finish() noexcept {
    done.store(true, std::memory_order_seq_cst);
    done.notify_all();
    for (const auto& worker : workers) worker->wake();
}

Worker::Worker(std::uint32_t id, Shared& shared, std::size_t workers, ArenaConfig config)
    : id_{id}, shared_{&shared}, arena_{config}, ready_(kInitialReady), outboxes_(workers, nullptr) {
    dirty_.reserve(workers);
    spare_.reserve(kSpareBatches);
}

Worker::~Worker() {
    stopAll();
    for (Batch* batch : outboxes_) {
        if (batch != nullptr) freeBatch(batch);
    }
    for (Batch* batch : spare_) freeBatch(batch);
    for (Batch* batch = inbox_.exchange(nullptr, std::memory_order_acquire); batch != nullptr;) {
        freeBatch(std::exchange(batch, batch->next));
    }
}

// Each loop runs one turn: up to kBudget messages of the actor at the front of the queue.
// Remote messages are taken in before every turn; outboxes go out every kFlushTurns turns
// and whenever the queue runs dry, so messages never wait on a busy worker for long and an
// idle one sends right away.
void Worker::loop() {
    while (!shared_->done.load(std::memory_order_acquire)) {
        if (live_here_ != 0 && shared_->stopping.load(std::memory_order_relaxed)) [[unlikely]]
            stopAll();
        takeInbox();
        if (ready_head_ == ready_tail_) {
            flush();
            idle();
            continue;
        }
        runTurn(ready_[ready_head_++ & (ready_.size() - 1)]);
        if (!dirty_.empty() && ++turns_ >= kFlushTurns) flush();
    }
}

void Worker::runTurn(std::uint32_t slot) {
    ActorBox* box = entries_[slot].box.get();
    if (box == nullptr) {  // stopped while it waited in the queue
        entries_[slot].queued = false;
        return;
    }
    const Turn turn = box->run(*this);
    Entry& entry = entries_[slot];  // spawns during the turn may have moved entries_
    if (turn.stopped) {
        entry.queued = false;
        retire(slot);
    } else if (turn.handled == kBudget) {
        ready_[ready_tail_++ & (ready_.size() - 1)] = slot;  // may have more: back of the line
    } else {
        entry.queued = false;  // emptied; its next message queues it again
    }
}

void Worker::retire(std::uint32_t slot) noexcept {
    std::unique_ptr<ActorBox> box = std::move(entries_[slot].box);
    box->destroyMailbox(arena_);
    --live_here_;
    box.reset();  // the actor's destructor runs here, on its own worker's thread
    shared_->release();
}

void Worker::stopAll() noexcept {
    // By index: a destructor may spawn, and entries_ grow.
    for (std::uint32_t slot = 0; slot < entries_.size(); ++slot) {
        if (entries_[slot].box != nullptr) retire(slot);
    }
}

void Worker::reserve(std::uint32_t slot) {
    if (slot > kSlotMask) throw std::length_error{"actorlib: too many actors on one worker"};
    if (slot >= entries_.size()) entries_.resize(std::size_t{slot} + 1);
    if (ready_.size() >= entries_.size()) return;
    // Each mailbox is queued at most once, so a ring as large as entries_ never overflows.
    std::vector<std::uint32_t> ring(std::bit_ceil(entries_.size()));
    const std::uint32_t count = ready_tail_ - ready_head_;
    for (std::uint32_t i = 0; i < count; ++i) ring[i] = ready_[(ready_head_ + i) & (ready_.size() - 1)];
    ready_ = std::move(ring);
    ready_head_ = 0;
    ready_tail_ = count;
}

// A new outbox batch for worker, with `bytes` free; nullptr when out of memory. The full
// one goes to its worker at once. A worker id and a byte count; the names say which.
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
Worker::Batch* Worker::refill(std::uint32_t worker, std::size_t bytes) noexcept {
    Batch*& batch = outboxes_[worker];
    Batch* fresh = takeBatch(bytes);
    if (fresh == nullptr) return nullptr;
    if (batch != nullptr) {
        shared_->workers[worker]->push(batch);
    } else {
        dirty_.push_back(worker);  // reserved for every worker: never allocates
    }
    batch = fresh;
    return batch;
}

void Worker::flush() noexcept {
    for (const std::uint32_t worker : dirty_) shared_->workers[worker]->push(std::exchange(outboxes_[worker], nullptr));
    dirty_.clear();
    turns_ = 0;
}

// Lock-free: other threads only ever add to the list, and the owner takes it whole.
void Worker::push(Batch* batch) noexcept {
    batch->next = inbox_.load(std::memory_order_relaxed);
    while (!inbox_.compare_exchange_weak(batch->next, batch, std::memory_order_seq_cst, std::memory_order_relaxed)) {}
    wake();
}

// Seen by a sleeper: either it sees the new batch (or flag) after announcing its sleep, or
// the waker sees the announcement here. Both sides use seq_cst, so one of them always does.
void Worker::wake() noexcept {
    if (sleep_.load(std::memory_order_seq_cst) == kSleeping) {
        sleep_.store(kAwake, std::memory_order_seq_cst);
        sleep_.notify_one();
    }
}

void Worker::takeInbox() noexcept {
    if (inbox_.load(std::memory_order_relaxed) == nullptr) return;
    // The list is newest first: reverse it so each sender's batches arrive in send order.
    Batch* oldest = nullptr;
    for (Batch* batch = inbox_.exchange(nullptr, std::memory_order_acquire); batch != nullptr;) {
        Batch* next = batch->next;
        batch->next = oldest;
        oldest = std::exchange(batch, next);
    }
    while (oldest != nullptr) {
        Batch* batch = std::exchange(oldest, oldest->next);
        for (std::size_t offset = 0; offset < batch->used;) {
            Envelope& envelope = envelopeAt(*batch, offset);
            offset += envelope.size;
            envelope.run(*this, envelope);
        }
        recycle(batch);
    }
}

// Spins a little first: a reply often comes back within microseconds, and waking a
// sleeping thread costs more than that.
void Worker::idle() noexcept {
    for (int spin = 0; spin < kIdleSpins; ++spin) {
        if (hasWork()) return;
        cpuRelax();
    }
    sleep_.store(kSleeping, std::memory_order_seq_cst);
    if (hasWork()) {
        sleep_.store(kAwake, std::memory_order_relaxed);
        return;
    }
    sleep_.wait(kSleeping, std::memory_order_seq_cst);
}

bool Worker::hasWork() const noexcept {
    return inbox_.load(std::memory_order_seq_cst) != nullptr || shared_->done.load(std::memory_order_seq_cst) ||
           (live_here_ != 0 && shared_->stopping.load(std::memory_order_seq_cst));
}

Worker::Batch* Worker::takeBatch(std::size_t bytes) noexcept {
    if (bytes <= kBatchBytes && !spare_.empty()) {
        Batch* batch = spare_.back();
        spare_.pop_back();
        return batch;
    }
    return newBatch(static_cast<std::uint32_t>(std::max<std::size_t>(bytes, kBatchBytes)));
}

// Batches come back to whichever worker received them, which keeps them for its own outboxes.
void Worker::recycle(Batch* batch) noexcept {
    if (batch->capacity != kBatchBytes || spare_.size() == kSpareBatches) {
        freeBatch(batch);
        return;
    }
    batch->used = 0;
    batch->next = nullptr;
    spare_.push_back(batch);  // reserved: never allocates
}

Worker::Batch* Worker::newBatch(std::uint32_t capacity) noexcept {
    void* memory = ::operator new(kBatchHeader + capacity, std::align_val_t{kChunkAlign}, std::nothrow);
    if (memory == nullptr) return nullptr;
    // Raw storage owned by whichever outbox, inbox or spare list holds it; freeBatch releases it.
    // NOLINTNEXTLINE(cppcoreguidelines-owning-memory)
    return ::new (memory) Batch{.next = nullptr, .used = 0, .capacity = capacity};
}

void Worker::freeBatch(Batch* batch) noexcept { ::operator delete(batch, std::align_val_t{kChunkAlign}); }
}  // namespace detail

VirtualWorker::VirtualWorker(VirtualWorkerConfig config) {
    const std::size_t workers = std::max<std::size_t>(
        config.workers != 0 ? config.workers : std::size_t{std::thread::hardware_concurrency()}, 1);
    if (workers > detail::Worker::kMaxWorkers) throw std::invalid_argument{"actorlib: at most 256 workers"};
    shared_.workers.reserve(workers);
    for (std::uint32_t id = 0; id < workers; ++id) {
        shared_.workers.push_back(std::make_unique<detail::Worker>(id, shared_, workers, config.arena));
    }
}

VirtualWorker::~VirtualWorker() {
    if (shared_.started.load(std::memory_order_acquire)) {
        stop();
        try {
            join();
        } catch (...) {
            std::terminate();  // only if a worker thread cannot be joined: nothing left to recover
        }
    }
    // Before any worker goes: an actor's destructor may still send to the others.
    for (const auto& worker : shared_.workers) worker->stopAll();
}

void VirtualWorker::start() {
    if (shared_.started.exchange(true, std::memory_order_acq_rel)) {
        throw std::logic_error{"actorlib: start() called twice"};
    }
    if (shared_.live.load(std::memory_order_acquire) == 0) shared_.finish();
    try {
        threads_.reserve(shared_.workers.size());
        for (const auto& worker : shared_.workers) threads_.emplace_back([w = worker.get()] { w->loop(); });
    } catch (...) {
        shared_.finish();  // the workers that did start exit
        for (std::thread& thread : threads_) thread.join();
        threads_.clear();
        throw;
    }
}

void VirtualWorker::join() {
    if (!shared_.started.load(std::memory_order_acquire)) throw std::logic_error{"actorlib: join() before start()"};
    shared_.done.wait(false, std::memory_order_acquire);
    for (std::thread& thread : threads_) {
        if (thread.joinable()) thread.join();
    }
}

void VirtualWorker::stop() noexcept {
    shared_.stopping.store(true, std::memory_order_seq_cst);
    for (const auto& worker : shared_.workers) worker->wake();
}

std::uint64_t VirtualWorker::droppedMessages() const noexcept {
    std::uint64_t dropped = 0;
    for (const auto& worker : shared_.workers) dropped += worker->dropped();
    return dropped;
}
}  // namespace actorlib
