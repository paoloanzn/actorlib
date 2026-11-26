#include <algorithm>
#include <atomic>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <new>
#include <span>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include <actorlib/actorlib.hpp>
#include <actorlib/virtual_worker.hpp>

#if defined(__linux__)
#include <linux/membarrier.h>

#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace actorlib {
namespace {
// Each slab starts with a cache-line-sized header holding the next-slab link, so the
// chunks after it stay aligned.
constexpr std::size_t kSlabHeaderSize = kChunkAlign;

// Even, since a slot's generation is even while it is dead, and 2^32 divided by the golden
// ratio, so the start points of any few arenas lie far apart.
constexpr std::uint32_t kGenerationStride = 0x9E3779B8;
std::atomic<std::uint32_t> next_first_generation{0};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
}  // namespace

std::uint32_t MailboxArena::nextFirstGeneration() noexcept {
    return next_first_generation.fetch_add(kGenerationStride, std::memory_order_relaxed);
}

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
        slots_.emplace_back().generation = first_generation_;
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
    ++slot.generation;  // odd -> even; after 2^31 reuses of one slot a stale ref could match again,
                        // and after millions a ref made by another arena could
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
constexpr std::size_t kInitialReady = 64;
// Messages taken from each feed at a time: many when the worker has nothing else to do, one
// turn's worth while it is busy. The rest wait in the feed, not in the arena, so a fast
// sender cannot fill the arena and leave the actors no room to send to one another.
constexpr std::size_t kFeedReadIdle = 1024;
constexpr std::size_t kFeedReadBusy = Worker::kBudget;
// Bytes of segments a thread may have waiting for the workers of one VirtualWorker before its
// sends return Full. One budget for all of them: a sender that feeds the workers in turn can
// queue more for one that stalls a moment, while the others drain theirs.
constexpr std::uint64_t kThreadMaxBytes = std::uint64_t{32} << 20;
// A worker is pressured once its arena has more than max_bytes / kHighMark in use, and stops
// being so below max_bytes / kLowMark. The room above the high mark is for the messages its
// actors and the other workers send before they notice.
constexpr std::size_t kHighMark = 2;
constexpr std::size_t kLowMark = 4;

// An idle worker spins before it sleeps: waking a sleeping thread costs microseconds. How
// long adapts to the gaps it sees, between these bounds.
constexpr int kQuickSpins = 16;     // checks before the clock is read at all
constexpr int kChecksPerClock = 8;  // then the clock is read this often
// Pauses between two checks. Checking reads the cache lines senders write; checking less
// often leaves those lines with the sender, which then sends faster.
constexpr int kPausesPerCheck = 4;
constexpr std::int64_t kMinSpinNs = 2'000;  // when work comes rarely
constexpr std::int64_t kInitialSpinNs = 50'000;
constexpr std::int64_t kMaxSpinNs = 1'000'000;  // gaps longer than this are not worth spinning for
using Clock = std::chrono::steady_clock;

std::int64_t nanosSince(Clock::time_point start) noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count();
}

std::atomic<std::uint64_t> next_system_id{1};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

Segment* newSegment(std::uint32_t capacity) noexcept {
    void* memory = ::operator new(kChunkAlign + capacity, std::align_val_t{kChunkAlign}, std::nothrow);
    if (memory == nullptr) return nullptr;
    // Raw storage owned by its feed; freeSegment releases it.
    auto* segment = ::new (memory) Segment{};  // NOLINT(cppcoreguidelines-owning-memory)
    segment->capacity = capacity;
    return segment;
}

void freeSegment(Segment* segment) noexcept { ::operator delete(segment, std::align_val_t{kChunkAlign}); }

// Every feed this thread opened, by VirtualWorker. It lets go of them when the thread ends.
struct ThreadFeeds {
    struct System {
        std::uint64_t id;
        std::vector<Feed*> feeds;  // by worker; never resized, so FeedCache may point into it
    };
    std::vector<System> systems;

    ThreadFeeds() = default;
    ThreadFeeds(const ThreadFeeds&) = delete;
    ThreadFeeds& operator=(const ThreadFeeds&) = delete;
    ThreadFeeds(ThreadFeeds&&) = delete;
    ThreadFeeds& operator=(ThreadFeeds&&) = delete;
    ~ThreadFeeds();

    static void release(System& system) noexcept {
        for (Feed*& feed : system.feeds) {
            if (feed != nullptr) releaseFeed(std::exchange(feed, nullptr));
        }
    }
    // A VirtualWorker is gone once its workers let go of every feed this thread opened to it.
    static bool gone(const System& system) noexcept {
        return std::ranges::all_of(system.feeds, [](const Feed* feed) {
            return feed == nullptr || feed->owners.load(std::memory_order_acquire) == 1;
        });
    }
};

// NOLINTBEGIN(cppcoreguidelines-avoid-non-const-global-variables)
thread_local ThreadFeeds thread_feeds;
thread_local bool thread_feeds_ended = false;  // sends from later thread_local destructors fail
// NOLINTEND(cppcoreguidelines-avoid-non-const-global-variables)

ThreadFeeds::~ThreadFeeds() {
    for (System& system : systems) release(system);
    thread_feed_cache = {};
    thread_feeds_ended = true;
}

void cpuRelax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    asm volatile("isb");  // yield is a no-op on many cores; isb actually waits
#else
    std::this_thread::yield();
#endif
}

void pause() noexcept {
    for (int i = 0; i < kPausesPerCheck; ++i) cpuRelax();
}

// Lets this process use heavyFence; false where the system cannot.
bool registerHeavyFence() noexcept {
#if defined(__linux__)
    // NOLINTBEGIN(cppcoreguidelines-pro-type-vararg)
    const auto commands = syscall(SYS_membarrier, MEMBARRIER_CMD_QUERY, 0, 0);
    return commands > 0 && (commands & MEMBARRIER_CMD_PRIVATE_EXPEDITED) != 0 &&
           syscall(SYS_membarrier, MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED, 0, 0) == 0;
    // NOLINTEND(cppcoreguidelines-pro-type-vararg)
#else
    return false;
#endif
}

// A full fence on every running thread of the process, this one included. Costs a few
// microseconds, so only a worker about to sleep pays it.
void heavyFence() noexcept {
#if defined(__linux__)
    syscall(SYS_membarrier, MEMBARRIER_CMD_PRIVATE_EXPEDITED, 0, 0);  // NOLINT(cppcoreguidelines-pro-type-vararg)
#endif
}
}  // namespace

void Shared::release() noexcept {
    if (live.fetch_sub(1, std::memory_order_acq_rel) == 1 && started.load(std::memory_order_acquire)) finish();
}

FeedCache feedsOfThisThread(const Shared& shared) noexcept {
    if (thread_feeds_ended) return {};
    std::vector<ThreadFeeds::System>& systems = thread_feeds.systems;
    for (ThreadFeeds::System& system : systems) {
        if (system.id == shared.id) return {.system = system.id, .feeds = system.feeds.data()};
    }
    // First send through this VirtualWorker: forget the ones that are gone.
    std::erase_if(systems, [](ThreadFeeds::System& system) {
        if (!ThreadFeeds::gone(system)) return false;
        ThreadFeeds::release(system);
        return true;
    });
    try {
        systems.push_back({.id = shared.id, .feeds = std::vector<Feed*>(shared.workers.size(), nullptr)});
    } catch (...) {
        return {};
    }
    return {.system = shared.id, .feeds = systems.back().feeds.data()};
}

void releaseFeed(Feed* feed) noexcept {
    if (feed->owners.fetch_sub(1, std::memory_order_acq_rel) != 1) return;
    for (Segment* segment = feed->head; segment != nullptr;)
        freeSegment(std::exchange(segment, segment->next.load(std::memory_order_relaxed)));
    if (Segment* spare = feed->spare.load(std::memory_order_relaxed); spare != nullptr) freeSegment(spare);
    delete feed;  // NOLINT(cppcoreguidelines-owning-memory)
}

void Shared::finish() noexcept {
    done.store(true, std::memory_order_seq_cst);
    done.notify_all();
    for (const auto& worker : workers) worker->wake();
}

Worker::Worker(std::uint32_t id, Shared& shared, std::size_t workers, ArenaConfig config)
    : system_{shared.id},
      shared_{&shared},
      id_{id},
      arena_{config},
      ready_(kInitialReady),
      outboxes_(workers, nullptr),
      high_bytes_{config.max_bytes / kHighMark},
      low_bytes_{config.max_bytes / kLowMark},
      spin_ns_{kInitialSpinNs} {
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
    while (pending_ != nullptr) freeBatch(std::exchange(pending_, pending_->next));
    // Messages still in feeds are dropped: they hold no resources (Message is trivially copyable).
    for (Feed* feed = joining_.exchange(nullptr, std::memory_order_acquire); feed != nullptr;) {
        releaseFeed(std::exchange(feed, feed->next));
    }
    for (Feed* feed = feeds_; feed != nullptr;) releaseFeed(std::exchange(feed, feed->next));
}

// Each loop runs one turn: up to kBudget messages of the actor at the front of the queue.
// Batches from other workers are taken in before every turn. Feeds, outboxes too, are
// handled every kFlushTurns turns and whenever the queue runs dry, so messages never wait
// on a busy worker for long, an idle one sends and receives right away, and a turn costs
// no more for them.
void Worker::loop() {
    while (!shared_->done.load(std::memory_order_acquire)) {
        if (live_here_ != 0 && shared_->stopping.load(std::memory_order_relaxed)) [[unlikely]]
            stopAll();
        // Checked here, so the common case costs two loads and no call.
        if (pending_ != nullptr || inbox_.load(std::memory_order_relaxed) != nullptr) [[unlikely]]
            takeInbox();
        if (ready_head_ == ready_tail_) {
            readFeeds(kFeedReadIdle);
            if (ready_head_ != ready_tail_) continue;
            flush();
            idle();
            continue;
        }
        runTurn(ready_[ready_head_++ & (ready_.size() - 1)]);
        if (!dirty_.empty() && ++turns_ >= kFlushTurns) flush();
        if (++unread_turns_ >= kFlushTurns) readFeeds(kFeedReadBusy);
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

// Batches that met a full arena wait in pending_, and newer ones wait behind them in the
// inbox, so each sender's messages still arrive in order.
void Worker::takeInbox() noexcept {
    if (pending_ != nullptr && !deliverPending()) return;
    if (inbox_.load(std::memory_order_relaxed) == nullptr) return;
    // The list is newest first: reverse it so each sender's batches arrive in send order.
    for (Batch* batch = inbox_.exchange(nullptr, std::memory_order_acquire); batch != nullptr;) {
        Batch* next = batch->next;
        batch->next = pending_;
        pending_ = std::exchange(batch, next);
    }
    pending_offset_ = 0;
    deliverPending();
}

// Stops at the high mark, before each batch: the room above it is for the messages this
// worker's actors send one another, which return Full to them rather than wait.
bool Worker::deliverPending() noexcept {
    while (pending_ != nullptr) {
        Batch& batch = *pending_;
        if (pending_offset_ == 0 && pastHighMark()) [[unlikely]]
            return false;
        while (pending_offset_ < batch.used) {
            Envelope& envelope = envelopeAt(batch, pending_offset_);
            const SendResult result = envelope.run(*this, envelope);
            if (result == SendResult::Full) [[unlikely]]
                return false;
            if (result != SendResult::Ok) countDropped();
            pending_offset_ += envelope.size;
        }
        pending_ = batch.next;
        pending_offset_ = 0;
        recycle(&batch);
    }
    return true;
}

// With no actor ready nothing is left to drain, whatever parked chunks still hold.
bool Worker::pastHighMark() const noexcept { return ready_head_ != ready_tail_ && arena_.bytesInUse() > high_bytes_; }

// Counts this worker in shared_->pressured while its arena is past the high mark or it holds
// batches it could not deliver. The worker that brings the count back to 0 wakes the others:
// a sleeping worker does not count messages waiting in feeds as work while it is not 0.
void Worker::updatePressure() noexcept {
    if (!pressured_) {
        if (!pastHighMark() && pending_ == nullptr) return;
        pressured_ = true;
        shared_->pressured.fetch_add(1, std::memory_order_seq_cst);
    } else if (pending_ == nullptr && (ready_head_ == ready_tail_ || arena_.bytesInUse() < low_bytes_)) {
        pressured_ = false;
        if (shared_->pressured.fetch_sub(1, std::memory_order_seq_cst) == 1) {
            for (const auto& worker : shared_->workers) {
                if (worker.get() != this) worker->wake();
            }
        }
    }
}

// The sending thread. The worker learns of the feed through joining_.
Feed* Worker::openFeed() noexcept {
    auto* feed = new (std::nothrow) Feed{};  // NOLINT(cppcoreguidelines-owning-memory): freed by releaseFeed
    if (feed == nullptr) return nullptr;
    feed->tail = feed->head = newSegment(kSegmentBytes);
    feed->produced = kSegmentBytes;
    if (feed->tail == nullptr) {
        delete feed;  // NOLINT(cppcoreguidelines-owning-memory)
        return nullptr;
    }
    feed->next = joining_.load(std::memory_order_relaxed);
    while (!joining_.compare_exchange_weak(feed->next, feed, std::memory_order_seq_cst, std::memory_order_relaxed)) {}
    return feed;
}

// The sending thread: bytes of segments its feeds hold that the workers have not read to the
// end yet. Called right after feedOfThisThread, so the cache holds this VirtualWorker's feeds.
std::uint64_t Worker::backlogOfThisThread() const noexcept {
    std::uint64_t bytes = 0;
    for (const Feed* feed : std::span{thread_feed_cache.feeds, shared_->workers.size()}) {
        if (feed != nullptr) bytes += feed->produced - feed->consumed.load(std::memory_order_relaxed);
    }
    return bytes;
}

// The sending thread: a segment with room for `bytes` after the full tail, reused when the
// worker has handed one back. False once kThreadMaxBytes wait for the workers.
bool Worker::extend(Feed& feed, std::size_t bytes) noexcept {
    if (backlogOfThisThread() >= kThreadMaxBytes) return false;
    Segment* fresh = bytes <= kSegmentBytes ? feed.spare.exchange(nullptr, std::memory_order_acquire) : nullptr;
    if (fresh != nullptr) {
        fresh->next.store(nullptr, std::memory_order_relaxed);
        fresh->committed.store(0, std::memory_order_relaxed);
    } else {
        fresh = newSegment(static_cast<std::uint32_t>(std::max<std::size_t>(bytes, kSegmentBytes)));
        if (fresh == nullptr) return false;
    }
    feed.tail->next.store(fresh, std::memory_order_seq_cst);  // seq_cst: hasWork looks here too
    feed.produced += fresh->capacity;
    feed.tail = fresh;
    feed.used = 0;
    return true;
}

// Takes in the feeds opened since the last look, reads up to `budget` messages from each,
// and lets go of the feeds whose thread has ended once they are read to the end. Reads
// nothing while any worker is pressured.
void Worker::readFeeds(std::size_t budget) noexcept {
    unread_turns_ = 0;
    updatePressure();
    if (joining_.load(std::memory_order_relaxed) != nullptr) [[unlikely]] {
        for (Feed* feed = joining_.exchange(nullptr, std::memory_order_acquire); feed != nullptr;) {
            Feed* next = feed->next;
            feed->next = feeds_;
            feeds_ = feed;
            feed = next;
        }
    }
    if (shared_->pressured.load(std::memory_order_relaxed) != 0) return;
    for (Feed** link = &feeds_; *link != nullptr;) {
        Feed* feed = *link;
        // Once its thread has ended, what it sent is all there: read to the end, then let go.
        if (read(*feed, budget) && feed->owners.load(std::memory_order_acquire) == 1 && read(*feed, budget))
            [[unlikely]] {
            *link = feed->next;
            releaseFeed(feed);
            continue;
        }
        link = &feed->next;
    }
}

// Delivers up to `budget` of the messages the thread has committed; true once all of them
// are in. A message that finds the arena full stays where it is, ahead of the ones after
// it, until a later call.
bool Worker::read(Feed& feed, std::size_t budget) noexcept {
    for (;;) {
        Segment* segment = feed.head;
        const std::uint32_t committed = segment->committed.load(std::memory_order_acquire);
        while (feed.read < committed) {
            if (budget-- == 0) return false;
            Envelope& envelope = envelopeAt(*segment, feed.read);
            const SendResult result = envelope.run(*this, envelope);
            if (result == SendResult::Full) [[unlikely]]
                return false;
            if (result != SendResult::Ok) countDropped();
            feed.read += envelope.size;
        }
        Segment* next = segment->next.load(std::memory_order_acquire);
        if (next == nullptr) return true;
        // The thread moved on, so committed is final: read the rest before leaving it.
        if (segment->committed.load(std::memory_order_acquire) != feed.read) continue;
        feed.head = next;
        feed.read = 0;
        feed.consumed.store(feed.consumed.load(std::memory_order_relaxed) + segment->capacity,
                            std::memory_order_relaxed);
        if (segment->capacity != kSegmentBytes) {
            freeSegment(segment);
        } else if (Segment* old = feed.spare.exchange(segment, std::memory_order_acq_rel); old != nullptr) {
            freeSegment(old);
        }
    }
}

// Spins first, since waking a sleeping thread costs microseconds. How long adapts: when
// work came soon after a sleep, the next spin covers a gap that long; when it came only
// after more than kMaxSpinNs, spinning was wasted, and the next spin is half as long.
void Worker::idle() noexcept {
    for (int spin = 0; spin < kQuickSpins; ++spin) {
        if (hasWork()) return;
        pause();
    }
    const Clock::time_point start = Clock::now();
    for (int spin = 1;; ++spin) {
        if (hasWork()) return;
        pause();
        if (spin % kChecksPerClock == 0 && nanosSince(start) >= spin_ns_) break;
    }
    sleep_.store(kSleeping, std::memory_order_seq_cst);
    if (asymmetric_fences.load(std::memory_order_relaxed)) heavyFence();  // pairs with postMessage
    if (hasWork()) {
        sleep_.store(kAwake, std::memory_order_relaxed);
        return;
    }
    sleep_.wait(kSleeping, std::memory_order_seq_cst);
    const std::int64_t gap = nanosSince(start);
    spin_ns_ = gap > kMaxSpinNs ? std::max(kMinSpinNs, spin_ns_ / 2) : std::min(kMaxSpinNs, 2 * gap);
}

bool Worker::hasWork() const noexcept {
    if (pending_ != nullptr || inbox_.load(std::memory_order_seq_cst) != nullptr ||
        joining_.load(std::memory_order_seq_cst) != nullptr || shared_->done.load(std::memory_order_seq_cst) ||
        (live_here_ != 0 && shared_->stopping.load(std::memory_order_seq_cst))) {
        return true;
    }
    if (shared_->pressured.load(std::memory_order_seq_cst) != 0) return false;  // feeds wait; see updatePressure
    for (const Feed* feed = feeds_; feed != nullptr; feed = feed->next) {
        const Segment* segment = feed->head;
        if (segment->committed.load(std::memory_order_seq_cst) != feed->read ||
            segment->next.load(std::memory_order_seq_cst) != nullptr) {
            return true;
        }
    }
    return false;
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
    shared_.id = detail::next_system_id.fetch_add(1, std::memory_order_relaxed);
    // Before any worker starts, so the flag never changes while one runs.
    static const bool heavy_fence = detail::registerHeavyFence();
    detail::asymmetric_fences.store(heavy_fence, std::memory_order_relaxed);
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
