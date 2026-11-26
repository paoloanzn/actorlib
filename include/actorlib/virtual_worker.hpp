#pragma once

#include <atomic>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <future>
#include <limits>
#include <memory>
#include <new>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include <actorlib/actorlib.hpp>

namespace actorlib {
template <Message Msg>
class Actor;

// What addActor and spawn accept: a class deriving publicly from Actor<M>, with a public
// onMessage taking an M&. Its other members are the actor's state.
template <class A>
concept ActorType = requires { typename A::Message; } && Message<typename A::Message> &&
                    std::derived_from<A, Actor<typename A::Message>> &&
                    requires(A& actor, typename A::Message& msg) { actor.onMessage(msg); };

namespace detail {
class Worker;

// What the workers of one VirtualWorker share.
struct Shared {
    std::uint64_t id = 0;  // unique to this VirtualWorker, never reused: keys each thread's feeds
    std::vector<std::unique_ptr<Worker>> workers;
    // Actors not stopped yet, plus addActor calls waiting for their actor. Once it reaches 0
    // after start() it stays there: nothing is left that could create an actor.
    std::atomic<std::size_t> live{0};
    std::atomic<bool> started{false};
    std::atomic<bool> stopping{false};
    std::atomic<bool> done{false};  // every actor stopped: workers exit and join() returns
    // Workers whose arena is past its high mark. While any is, no worker takes messages from
    // outside: they wait in their feeds, and a thread's sends return Full once its backlog
    // reaches its cap.
    std::atomic<std::uint32_t> pressured{0};

    void release() noexcept;  // one fewer live; the last one after start() calls finish()
    void finish() noexcept;
};

// Part of a feed: a header line, then envelopes, like a batch.
struct Segment {
    std::atomic<Segment*> next{nullptr};      // set once the producer has moved on to it
    std::atomic<std::uint32_t> committed{0};  // bytes of complete envelopes
    std::uint32_t capacity = 0;
};

// Messages from one outside thread to one worker: the thread appends to its tail segment
// and the worker reads from its head one, so sending takes no lock, no compare-exchange
// and, once a segment comes back for reuse, no allocation. Each sender's messages stay in
// order. The thread and the worker both own it; the last to let go frees it.
// The two halves sit on separate cache lines on purpose: each side writes only its own.
struct Feed {  // NOLINT(clang-analyzer-optin.performance.Padding)
    // The sending thread only.
    Segment* tail = nullptr;
    std::uint32_t used = 0;      // in tail
    std::uint64_t produced = 0;  // capacity of every segment added, the first one included
    // The worker's thread only, after it took the feed from its joining list.
    alignas(kChunkAlign) Segment* head = nullptr;
    std::uint32_t read = 0;  // in head
    Feed* next = nullptr;    // the joining list, then the worker's list of feeds
    // Written by the worker, read by the sender when it needs a segment.
    std::atomic<std::uint64_t> consumed{0};  // capacity of every segment read to the end
    // Both.
    std::atomic<Segment*> spare{nullptr};  // a read segment, back for the thread to reuse
    std::atomic<int> owners{2};
};

// The calling thread's feeds to the workers of the VirtualWorker it last sent through.
struct FeedCache {
    std::uint64_t system = 0;  // a Shared::id; 0 for none
    Feed** feeds = nullptr;    // by worker; null until the thread first sends to that worker
};
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
inline constinit thread_local FeedCache thread_feed_cache{};
FeedCache feedsOfThisThread(const Shared& shared) noexcept;  // fills thread_feed_cache

// Whether senders from outside skip the store-load fence before checking that a worker
// sleeps. A sleeping worker cannot miss their messages anyway: before it sleeps, it makes
// every running thread of the process execute a full fence (Linux membarrier). Set once,
// before the first worker starts, and never changed after.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
inline constinit std::atomic<bool> asymmetric_fences{false};
void releaseFeed(Feed* feed) noexcept;  // by one of its two owners

// Allocates whole cache lines. A worker's vectors are small and written on every turn: on a
// line shared with another worker's data, each write would take the line from that worker.
template <class T>
struct LineAllocator {
    using value_type = T;
    LineAllocator() = default;
    template <class U>
    LineAllocator(const LineAllocator<U>& /*other*/) noexcept {}  // NOLINT(google-explicit-constructor)
    // T may be a pointer: sizeof(T) is meant.
    static std::size_t bytes(std::size_t n) noexcept {
        return (n * sizeof(T) + kChunkAlign - 1) & ~(kChunkAlign - 1);  // NOLINT(bugprone-sizeof-expression)
    }
    T* allocate(std::size_t n) { return static_cast<T*>(::operator new(bytes(n), std::align_val_t{kChunkAlign})); }
    void deallocate(T* p, std::size_t /*n*/) noexcept {
        // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion): T may be a pointer
        ::operator delete(p, std::align_val_t{kChunkAlign});
    }
    friend bool operator==(const LineAllocator& /*a*/, const LineAllocator& /*b*/) noexcept { return true; }
};
template <class T>
using LineVector = std::vector<T, LineAllocator<T>>;

// Bits of an actor's state that end its turn after the current message.
inline constexpr std::uint8_t kActorStopped = 1;  // stop() was called
inline constexpr std::uint8_t kActorPaused = 2;   // it sent past a transit pause mark

// One thread of a VirtualWorker. It owns an arena with its actors' mailboxes, the actors
// themselves and a queue of the ones with messages, and only its thread touches them.
// Other workers reach it through its inbox: a lock-free list of batches of messages.
// Threads outside the workers reach it through a feed each. The fields other threads write
// get a cache line of their own, which is padding the analyzer would rather not see.
class Worker {  // NOLINT(clang-analyzer-optin.performance.Padding)
public:
    // An ActorRef's index holds its worker in the top 8 bits and its mailbox in the rest.
    static constexpr std::uint32_t kSlotBits = 24;
    static constexpr std::uint32_t kSlotMask = (std::uint32_t{1} << kSlotBits) - 1;
    static constexpr std::size_t kMaxWorkers = std::size_t{1} << (32 - kSlotBits);
    static constexpr std::size_t kBudget = 64;  // messages per turn, then the next ready actor runs

    Worker(std::uint32_t id, Shared& shared, std::size_t workers, ArenaConfig config);
    ~Worker();
    Worker(const Worker&) = delete;
    Worker& operator=(const Worker&) = delete;
    Worker(Worker&&) = delete;
    Worker& operator=(Worker&&) = delete;

    template <Message Msg>
    static std::uint32_t workerOf(ActorRef<Msg> ref) noexcept {
        return ref.index_ >> kSlotBits;
    }

    // On this worker's thread, or on any single thread before start().
    template <ActorType A, class... Args>
    ActorRef<typename A::Message> spawn(Args&&... args) {
        using Msg = typename A::Message;
        const ActorRef<Msg> local = arena_.create<Msg>();
        std::unique_ptr<Box<A>> box;
        try {
            reserve(local.index_);
            box = std::make_unique<Box<A>>(std::forward<Args>(args)...);
        } catch (...) {
            arena_.destroy(local);
            throw;
        }
        const ActorRef<Msg> ref{id_ << kSlotBits | local.index_, local.generation_};
        Actor<Msg>& actor = box->actor;
        actor.worker_ = this;
        actor.self_ = ref;
        entries_[local.index_].box = std::move(box);  // a reused slot keeps its queued flag
        ++live_here_;
        shared_->live.fetch_add(1, std::memory_order_relaxed);
        if (actor.state_ != 0) [[unlikely]]
            retire(local.index_);  // it called stop() in its constructor
        return ref;
    }

    // Any thread, after start(): this worker's thread spawns the actor; waits for its ref.
    template <ActorType A, class... Args>
    ActorRef<typename A::Message> spawnFromOutside(Args&&... args) {
        // The extra live count keeps the workers running until the request is handled.
        std::size_t live = shared_->live.load(std::memory_order_relaxed);
        for (;;) {
            if (live == 0) throw std::logic_error{"actorlib: every actor has stopped"};
            if (shared_->live.compare_exchange_weak(live, live + 1, std::memory_order_relaxed)) break;
        }
        try {
            auto request =
                std::make_unique<SpawnRequest<A, Args...>>(std::forward_as_tuple(std::forward<Args>(args)...));
            auto future = request->result.get_future();
            if (post<SpawnRequest<A, Args...>*>(Envelope{.run = &SpawnRequest<A, Args...>::run}, request.get()) !=
                SendResult::Ok) {
                throw std::bad_alloc{};
            }
            static_cast<void>(request.release());  // the worker's thread deletes it
            const ActorRef<typename A::Message> result = future.get();
            shared_->release();
            return result;
        } catch (...) {
            shared_->release();
            throw;
        }
    }

    // On this worker's thread. Messages to another worker wait in an outbox batch.
    template <Message Msg, class... Args>
    SendResult send(ActorRef<Msg> to, Args&&... args) {
        const std::uint32_t worker = workerOf(to);
        if (worker == id_) [[likely]] {
            const ActorRef<Msg> local = localOf(to);
            const SendResult result = arena_.send(local, std::forward<Args>(args)...);
            if (result == SendResult::Ok) schedule(local.index_);
            return result;
        }
        if (worker >= outboxes_.size()) return SendResult::DeadActor;
        Batch* batch = outboxes_[worker];
        if (batch == nullptr || batch->used + bytesFor<Msg>() > batch->limit) [[unlikely]] {
            batch = refill(worker, bytesFor<Msg>());
            if (batch == nullptr) return SendResult::Full;
        }
        append<Msg>(*batch,
                    Envelope{.run = &deliver<Msg>, .index = to.index_ & kSlotMask, .generation = to.generation_},
                    std::forward<Args>(args)...);
        return SendResult::Ok;
    }

    // Any thread: appended to the calling thread's feed to this worker.
    template <Message Msg, class... Args>
    SendResult postMessage(ActorRef<Msg> to, Args&&... args) {
        Feed* feed = feedOfThisThread();
        if (feed == nullptr) [[unlikely]]
            return SendResult::Full;
        if (feed->tail->capacity - feed->used < bytesFor<Msg>()) [[unlikely]] {
            if (!extend(*feed, bytesFor<Msg>())) return SendResult::Full;
        }
        Segment& segment = *feed->tail;
        feed->used = static_cast<std::uint32_t>(
            write<Msg>(at(segment, 0), feed->used,
                       Envelope{.run = &deliver<Msg>, .index = to.index_ & kSlotMask, .generation = to.generation_},
                       std::forward<Args>(args)...));
        // A worker going to sleep sees the message, or this sees it sleeping. A store-load
        // fence costs a round trip to the core polling the line, so skip it when the worker
        // fences for both before it sleeps.
        if (asymmetric_fences.load(std::memory_order_relaxed)) [[likely]] {
            segment.committed.store(feed->used, std::memory_order_release);
            std::atomic_signal_fence(std::memory_order_seq_cst);  // keeps the compiler from swapping the two
            if (sleep_.load(std::memory_order_relaxed) == kSleeping) [[unlikely]]
                wake();
        } else {
            segment.committed.store(feed->used, std::memory_order_seq_cst);
            wake();
        }
        return SendResult::Ok;
    }

    void loop();              // the worker thread's whole life
    void wake() noexcept;     // any thread
    void stopAll() noexcept;  // this worker's thread, or after its thread has ended
    [[nodiscard]] std::uint64_t dropped() const noexcept { return dropped_.load(std::memory_order_relaxed); }

private:
    struct Turn {
        std::size_t handled;
        bool stopped;
        bool paused;
    };

    // One actor with its type erased: the worker makes one virtual call per turn, and the
    // drain loop inside it calls onMessage directly.
    class ActorBox {
    public:
        ActorBox() = default;
        ActorBox(const ActorBox&) = delete;
        ActorBox& operator=(const ActorBox&) = delete;
        ActorBox(ActorBox&&) = delete;
        ActorBox& operator=(ActorBox&&) = delete;
        virtual ~ActorBox() = default;

        virtual Turn run(Worker& worker) = 0;
        virtual void destroyMailbox(MailboxArena& arena) noexcept = 0;
    };

    // Whole cache lines: actors are created one after another on one thread and spread over the
    // workers, so neighbours in memory run on different workers and would share lines.
    template <ActorType A>
    struct alignas(kChunkAlign) Box final : ActorBox {
        using Msg = typename A::Message;

        template <class... Args>
        explicit Box(Args&&... args) : actor(std::forward<Args>(args)...) {}

        Turn run(Worker& worker) override {
            Actor<Msg>& base = actor;
            // The turn ends after a message that stopped the actor (the rest of its mailbox dies
            // with it) or paused it (see refill): the next message is left in the mailbox.
            worker.running_ = &base.state_;
            const std::size_t handled = worker.arena_.drain(localOf(base.self_), kBudget, [this, &base](Msg& msg) {
                if (base.state_ != 0) return false;  // checked first, as for stop() alone
                actor.onMessage(msg);
                return true;
            });
            worker.running_ = nullptr;
            const std::uint8_t state = base.state_;
            if ((state & kActorPaused) != 0) [[unlikely]]
                base.state_ = state & kActorStopped;  // written only then: the actor's line stays clean
            return {handled, (state & kActorStopped) != 0, (state & kActorPaused) != 0};
        }

        void destroyMailbox(MailboxArena& arena) noexcept override {
            const Actor<Msg>& base = actor;
            arena.destroy(localOf(base.self_));
        }

        A actor;  // the state lives and dies with the box
    };

    static constexpr std::uint32_t kNone = std::numeric_limits<std::uint32_t>::max();

    struct Entry {
        std::unique_ptr<ActorBox> box;  // null once its actor stopped
        bool queued = false;            // in ready_, running or paused; a stopped slot keeps it until popped
        std::uint32_t next_paused = kNone;
    };

    // A batch of messages for another worker: a header line, then envelopes.
    struct Batch {
        Batch* next = nullptr;  // inbox link
        std::uint32_t used = 0;
        std::uint32_t capacity = 0;
        // An outbox's appends past limit take the slow path: limit is the capacity, or 0 while
        // its worker is past the transit pause mark (see refill).
        std::uint32_t limit = 0;
        std::uint32_t from = kNone;  // the sending worker, credited once the batch is delivered
    };

    // Transit to one other worker, counted like a feed: the sender adds the capacity of every
    // batch it takes for that worker, which adds it back once it has delivered the batch.
    struct Transit {
        std::uint64_t sent = 0;
        std::uint64_t seen = 0;  // the worker's credit when last read: reading costs a cache miss
    };
    struct alignas(kChunkAlign) Credit {
        std::atomic<std::uint64_t> delivered{0};  // written by the receiving worker only
        std::atomic<bool> wake{false};            // set by the sender while it sleeps with paused actors
    };
    static constexpr std::size_t kBatchHeader = kChunkAlign;  // keeps the envelopes 64-byte aligned
    static constexpr std::uint32_t kBatchBytes = 4096 - kBatchHeader;
    static constexpr std::uint32_t kSegmentBytes = 16384 - kBatchHeader;  // a feed's usual segment

    // One message, or a command, in a batch or a feed. The payload follows at its own alignment.
    struct Envelope {
        using Run = SendResult (*)(Worker&, Envelope&);
        Run run = nullptr;  // delivers the payload, or runs the command
        std::uint32_t index = 0;
        std::uint32_t generation = 0;
        std::uint32_t size = 0;     // bytes to the next envelope
        std::uint32_t payload = 0;  // offset of the payload from the envelope
    };

    // An addActor from another thread. The worker's thread runs it and deletes it.
    template <ActorType A, class... Args>
    struct SpawnRequest {
        std::tuple<Args&&...> args;
        std::promise<ActorRef<typename A::Message>> result;

        static SendResult run(Worker& worker, Envelope& envelope) {
            const std::unique_ptr<SpawnRequest> request{payloadOf<SpawnRequest*>(envelope)};
            try {
                request->result.set_value(std::apply(
                    [&worker](Args&&... forwarded) { return worker.spawn<A>(std::forward<Args>(forwarded)...); },
                    std::move(request->args)));
            } catch (...) {
                request->result.set_exception(std::current_exception());
            }
            return SendResult::Ok;
        }
    };

    template <Message Msg>
    static ActorRef<Msg> localOf(ActorRef<Msg> ref) noexcept {
        return {ref.index_ & kSlotMask, ref.generation_};
    }

    static constexpr std::size_t alignUp(std::size_t n, std::size_t align) noexcept {
        return (n + align - 1) / align * align;
    }
    // Room an envelope with a T needs at the end of any batch. T may be a pointer (a
    // SpawnRequest travels as one), so sizeof(T) of a pointer is meant.
    template <class T>
    static constexpr std::size_t bytesFor() noexcept {
        return sizeof(Envelope) + (alignof(T) > alignof(Envelope) ? alignof(T) - alignof(Envelope) : 0) +
               alignUp(sizeof(T), alignof(Envelope));  // NOLINT(bugprone-sizeof-expression)
    }

    // Batches and segments are raw storage: these are the only places that compute addresses
    // in them. Both keep their envelopes one header line past their start.
    template <class Storage>
    static std::byte* at(Storage& storage, std::size_t offset) noexcept {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast,cppcoreguidelines-pro-bounds-pointer-arithmetic)
        return reinterpret_cast<std::byte*>(&storage) + kBatchHeader + offset;
    }
    template <class Storage>
    static Envelope& envelopeAt(Storage& storage, std::size_t offset) noexcept {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        return *std::launder(reinterpret_cast<Envelope*>(at(storage, offset)));
    }
    template <class T>
    static T& payloadOf(Envelope& envelope) noexcept {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast,cppcoreguidelines-pro-bounds-pointer-arithmetic)
        return *std::launder(reinterpret_cast<T*>(reinterpret_cast<std::byte*>(&envelope) + envelope.payload));
    }

    // Builds T(args...) and its envelope at offset start of data, which has bytesFor<T>() free
    // there, and returns where the next envelope goes.
    template <class T, class... Args>
    static std::size_t write(std::byte* data, std::size_t start, Envelope envelope, Args&&... args) {
        const std::size_t payload = alignUp(start + sizeof(Envelope), alignof(T));
        const std::size_t end = alignUp(payload + sizeof(T), alignof(Envelope));  // NOLINT(bugprone-sizeof-expression)
        // NOLINTBEGIN(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        ::new (static_cast<void*>(data + payload)) T(std::forward<Args>(args)...);  // throws before any commit
        envelope.size = static_cast<std::uint32_t>(end - start);
        envelope.payload = static_cast<std::uint32_t>(payload - start);
        ::new (static_cast<void*>(data + start)) Envelope{envelope};
        // NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        return end;
    }

    template <class T, class... Args>
    static void append(Batch& batch, Envelope envelope, Args&&... args) {
        batch.used =
            static_cast<std::uint32_t>(write<T>(at(batch, 0), batch.used, envelope, std::forward<Args>(args)...));
    }

    // The calling thread's feed to this worker, opened on its first send; null when out of memory.
    Feed* feedOfThisThread() noexcept {
        FeedCache& cache = thread_feed_cache;
        if (cache.system != system_) [[unlikely]] {
            cache = feedsOfThisThread(*shared_);
            if (cache.feeds == nullptr) return nullptr;
        }
        Feed*& feed = cache.feeds[id_];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        if (feed == nullptr) [[unlikely]]
            feed = openFeed();
        return feed;
    }

    // Any thread: a batch of its own with one envelope, pushed to this worker's inbox.
    template <class T, class... Args>
    SendResult post(Envelope envelope, Args&&... args) {
        Batch* batch = newBatch(static_cast<std::uint32_t>(bytesFor<T>()));
        if (batch == nullptr) return SendResult::Full;
        try {
            append<T>(*batch, envelope, std::forward<Args>(args)...);
        } catch (...) {
            freeBatch(batch);
            throw;
        }
        push(batch);
        return SendResult::Ok;
    }

    // Copies the payload into its mailbox. On Full the caller keeps the message, ahead of the
    // ones after it, until the arena has room.
    template <Message Msg>
    static SendResult deliver(Worker& worker, Envelope& envelope) noexcept {
        const ActorRef<Msg> local{envelope.index, envelope.generation};
        const SendResult result = worker.arena_.send(local, std::as_const(payloadOf<Msg>(envelope)));
        if (result == SendResult::Ok) worker.schedule(local.index_);
        return result;
    }

    void countDropped() noexcept {
        dropped_.store(dropped_.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
    }

    // Puts an actor with a new message in the ready queue, unless it is there already.
    // Never grows the queue: it has room for every mailbox (see reserve).
    void schedule(std::uint32_t slot) noexcept {
        Entry& entry = entries_[slot];
        if (entry.queued) return;
        entry.queued = true;
        ready_[ready_tail_++ & (ready_.size() - 1)] = slot;
    }

    void reserve(std::uint32_t slot);  // room for slot in entries_ and the ready queue
    void runTurn(std::uint32_t slot);
    void retire(std::uint32_t slot) noexcept;
    Batch* refill(std::uint32_t worker, std::size_t bytes) noexcept;
    [[nodiscard]] std::uint64_t creditFrom(std::uint32_t worker, std::memory_order order) const noexcept;
    void credit(const Batch& batch) noexcept;
    [[nodiscard]] bool pastResumeMark(std::uint32_t worker) noexcept;
    void pauseActor(std::uint32_t slot) noexcept;
    void resumePaused(bool all) noexcept;
    [[nodiscard]] bool canResume() const noexcept;
    void askForCredit(bool ask) noexcept;
    void flush() noexcept;
    void push(Batch* batch) noexcept;  // any thread
    void takeInbox() noexcept;
    bool deliverPending() noexcept;  // false while the arena is full
    void updatePressure() noexcept;
    [[nodiscard]] bool pastHighMark() const noexcept;
    Feed* openFeed() noexcept;                            // the sending thread
    bool extend(Feed& feed, std::size_t bytes) noexcept;  // the sending thread
    [[nodiscard]] std::uint64_t backlogOfThisThread() const noexcept;
    void readFeeds(std::size_t budget) noexcept;
    bool read(Feed& feed, std::size_t budget) noexcept;  // false while messages are left
    void idle() noexcept;
    [[nodiscard]] bool hasWork() const noexcept;
    Batch* takeBatch(std::size_t bytes) noexcept;
    void recycle(Batch* batch) noexcept;
    static Batch* newBatch(std::uint32_t capacity) noexcept;
    static void freeBatch(Batch* batch) noexcept;

    std::uint64_t system_;  // shared_->id, read on every send from outside
    Shared* shared_;
    std::uint32_t id_;
    MailboxArena arena_;
    LineVector<Entry> entries_;        // by mailbox index
    LineVector<std::uint32_t> ready_;  // a ring of mailbox indices, sized to a power of two
    std::uint32_t ready_head_ = 0;
    std::uint32_t ready_tail_ = 0;
    std::size_t live_here_ = 0;
    LineVector<Batch*> outboxes_;       // by destination worker
    LineVector<std::uint32_t> dirty_;   // destinations with an outbox batch
    LineVector<Batch*> spare_;          // empty batches for the next outboxes
    Batch* pending_ = nullptr;          // taken from the inbox, not delivered yet; oldest first
    std::uint32_t pending_offset_ = 0;  // of the next envelope in pending_
    bool pressured_ = false;            // counted in shared_->pressured
    std::size_t high_bytes_;            // arena bytes in use past which this worker is pressured
    std::size_t low_bytes_;             // and below which it is not any more
    Feed* feeds_ = nullptr;             // from the threads that send from outside, linked by next
    std::int64_t spin_ns_;              // how long idle() spins before it sleeps; adapts
    std::atomic<std::uint64_t> dropped_{0};
    std::uint32_t turns_ = 0;            // since the last flush
    std::uint32_t unread_turns_ = 0;     // since feeds were last read
    LineVector<Transit> transit_;        // by destination worker
    std::vector<Credit> credits_;        // by source worker; never resized
    std::uint64_t transit_budget_;       // bytes in transit to one worker; past it, sends return Full
    std::uint64_t pause_mark_;           // past it, actors that send there are paused
    std::uint64_t resume_mark_;          // below it everywhere, they run again
    std::uint8_t* running_ = nullptr;    // the state of the actor whose turn it is
    std::uint32_t paused_head_ = kNone;  // paused actors, linked by Entry::next_paused, oldest first
    std::uint32_t paused_tail_ = kNone;
    std::uint32_t paused_ = 0;

    // The fields other threads write, on a cache line of their own.
    static constexpr std::uint32_t kAwake = 0;
    static constexpr std::uint32_t kSleeping = 1;
    alignas(kChunkAlign) std::atomic<Batch*> inbox_{nullptr};  // newest first
    std::atomic<Feed*> joining_{nullptr};                      // feeds opened since the last look
    std::atomic<std::uint32_t> sleep_{kAwake};
};
}  // namespace detail

// Base class of every actor. Derive from Actor<M> publicly, M being the message type, and
// write a public onMessage(M&) (or const M&). The library calls it once per message, one
// message at a time, on the thread of the actor's worker. The derived class's members are
// the actor's state: the library builds the actor in addActor or spawn and destroys it
// once it stops, so the state cannot outlive it. Its constructor cannot send, spawn or use
// self() yet. If onMessage throws, the program terminates.
//
//     class Counter : public actorlib::Actor<Add> {
//     public:
//         void onMessage(Add& add) {
//             total_ += add.amount;
//             if (total_ >= kLimit) stop();
//         }
//     private:
//         std::uint64_t total_ = 0;
//     };
template <Message Msg>
class Actor {
public:
    using Message = Msg;

    Actor() = default;
    Actor(const Actor&) = delete;
    Actor& operator=(const Actor&) = delete;
    Actor(Actor&&) = delete;
    Actor& operator=(Actor&&) = delete;

protected:
    ~Actor() = default;

    // To any actor, on any worker. DeadActor is only known for actors on this actor's worker,
    // and so is Full, but for one case: messages in transit to each other worker have a budget,
    // and an actor that keeps sending to a worker that falls behind is paused between turns,
    // until that worker catches up. Only an actor that sends the rest of the budget within one
    // onMessage gets Full. A message for another worker waits there while that worker's arena
    // is full, and is dropped, and counted in VirtualWorker::droppedMessages, if the actor is gone.
    template <actorlib::Message To, class... Args>
        requires std::constructible_from<To, Args...>
    SendResult send(ActorRef<To> to, Args&&... args) {
        return worker_->send(to, std::forward<Args>(args)...);
    }

    // Creates an actor on this actor's worker: actors that talk a lot stay together.
    template <ActorType A, class... Args>
        requires std::constructible_from<A, Args...>
    ActorRef<typename A::Message> spawn(Args&&... args) {
        return worker_->spawn<A>(std::forward<Args>(args)...);
    }

    [[nodiscard]] ActorRef<Msg> self() const noexcept { return self_; }

    // Ends the actor after the current message: the messages still in its mailbox are
    // dropped, and it is destroyed.
    void stop() noexcept { state_ |= detail::kActorStopped; }

private:
    friend class detail::Worker;
    detail::Worker* worker_ = nullptr;
    ActorRef<Msg> self_;
    std::uint8_t state_ = 0;  // detail::kActorStopped and kActorPaused
};

struct VirtualWorkerConfig {
    std::size_t workers = 0;  // threads; 0 means one per hardware thread. At most 256.
    ArenaConfig arena{};      // for each worker's own arena
};

// Runs actors on worker threads; users only write actors. Each worker owns an arena with
// the mailboxes of its actors and runs those actors one message at a time, so no actor
// state is ever shared and the hot path takes no locks. A message to an actor on another
// worker is copied into an outbox batch, handed over with one compare-exchange, and copied
// into that worker's arena by its thread.
//
//     VirtualWorker system;
//     const auto trader = system.addActor<CopyTrader>(wallet, executor);
//     system.start();
//     system.send(trader, tx);  // from any thread
//     system.join();            // until every actor has stopped
class VirtualWorker {
public:
    explicit VirtualWorker(VirtualWorkerConfig config = {});
    ~VirtualWorker();  // stop() and join() if still running
    VirtualWorker(const VirtualWorker&) = delete;
    VirtualWorker& operator=(const VirtualWorker&) = delete;
    VirtualWorker(VirtualWorker&&) = delete;
    VirtualWorker& operator=(VirtualWorker&&) = delete;

    // Builds A(args...) on the next worker in turn and returns its ref. Before start(), call
    // it from one thread. After start(), any thread may: it waits until the worker's thread
    // has built the actor, and throws std::logic_error once every actor has stopped.
    // Actors create actors with spawn() instead.
    template <ActorType A, class... Args>
        requires std::constructible_from<A, Args...>
    ActorRef<typename A::Message> addActor(Args&&... args) {
        detail::Worker& worker =
            *shared_.workers[next_.fetch_add(1, std::memory_order_relaxed) % shared_.workers.size()];
        if (!shared_.started.load(std::memory_order_acquire)) return worker.spawn<A>(std::forward<Args>(args)...);
        return worker.spawnFromOutside<A>(std::forward<Args>(args)...);
    }

    // From outside the actors, any thread. Ok means the actor's worker has the message: it
    // drops it, and counts it in droppedMessages, if the actor is gone. Each thread's
    // messages arrive in the order it sent them.
    // Full means the workers are behind: while an arena is more than half full, workers
    // stop taking messages from outside, and once 32 MiB of this thread's messages wait for
    // the workers, its sends return Full until they catch up. Slow down and send again.
    template <Message Msg, class... Args>
        requires std::constructible_from<Msg, Args...>
    SendResult send(ActorRef<Msg> to, Args&&... args) {
        const std::uint32_t worker = detail::Worker::workerOf(to);
        if (worker >= shared_.workers.size()) return SendResult::DeadActor;
        return shared_.workers[worker]->postMessage(to, std::forward<Args>(args)...);
    }

    void start();          // runs each worker on a thread of its own and returns at once
    void join();           // waits until every actor has stopped (at once if there were none)
    void stop() noexcept;  // any thread: every actor stops after the message it is handling

    [[nodiscard]] std::size_t workers() const noexcept { return shared_.workers.size(); }
    [[nodiscard]] std::uint64_t droppedMessages() const noexcept;

private:
    detail::Shared shared_;
    std::vector<std::thread> threads_;
    std::atomic<std::size_t> next_{0};
};
}  // namespace actorlib
