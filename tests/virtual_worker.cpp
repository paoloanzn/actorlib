// Contracts of VirtualWorker that users rely on and that are easy to break while tuning the
// runtime. Each test names the contract and why it matters. None depends on internal sizes
// (feed caps, watermarks, spin times): they hold for any values of those.
#include <doctest/doctest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <vector>

#include <actorlib/actorlib.hpp>
#include <actorlib/virtual_worker.hpp>

using actorlib::Actor;
using actorlib::ActorRef;
using actorlib::ArenaConfig;
using actorlib::SendResult;
using actorlib::VirtualWorker;
using actorlib::VirtualWorkerConfig;

namespace {
using Clock = std::chrono::steady_clock;
constexpr auto kPatience = std::chrono::seconds(10);  // far beyond any correct run: only a bug waits this long

// Full from VirtualWorker::send means "the workers are behind": a sender waits and sends
// again. Gives up, returning Full, only after kPatience.
template <class Msg>
SendResult sendPatiently(VirtualWorker& system, ActorRef<Msg> to, const Msg& msg) {
    const auto deadline = Clock::now() + kPatience;
    SendResult result = system.send(to, msg);
    while (result == SendResult::Full && Clock::now() < deadline) {
        std::this_thread::yield();
        result = system.send(to, msg);
    }
    return result;
}

// Waits until value reaches at least target, for at most kPatience.
bool waitFor(const std::atomic<std::uint64_t>& value, std::uint64_t target) {
    const auto deadline = Clock::now() + kPatience;
    while (value.load(std::memory_order_acquire) < target) {
        if (Clock::now() >= deadline) return false;
    }
    return true;
}

struct Item {
    std::uint64_t seq;
    bool end;
};
}  // namespace

// A sender that outruns its actors must find out, through Full, instead of making memory
// grow without bound, and Full must not lose anything: every message sent with Ok arrives,
// in order, and sending works again once the actor catches up.
TEST_CASE("send returns Full while the workers are behind, and every message sent with Ok arrives") {
    struct Gate {
        std::atomic<bool> entered{false};  // the actor is in onMessage
        std::atomic<bool> open{false};     // set by the test to let it return
    };
    class Blocked : public Actor<Item> {
    public:
        Blocked(Gate* gate, std::vector<std::uint64_t>* seen) : gate_{gate}, seen_{seen} {}
        void onMessage(const Item& item) {
            gate_->entered.store(true, std::memory_order_release);
            gate_->open.wait(false, std::memory_order_acquire);  // holds the worker's thread until the test opens
            if (item.end) {
                stop();
                return;
            }
            seen_->push_back(item.seq);
        }

    private:
        Gate* gate_;
        std::vector<std::uint64_t>* seen_;
    };

    Gate gate;
    std::vector<std::uint64_t> seen;  // written by the worker's thread, read after join()
    VirtualWorker system{VirtualWorkerConfig{.workers = 1}};
    const auto actor = system.addActor<Blocked>(&gate, &seen);
    system.start();

    std::uint64_t sent = 0;
    REQUIRE(system.send(actor, Item{.seq = sent++, .end = false}) == SendResult::Ok);
    while (!gate.entered.load(std::memory_order_acquire)) std::this_thread::yield();
    // The worker is stuck in onMessage. Any finite limit on what may wait for it is reached
    // long before this bound, which only keeps a missing limit from eating all memory.
    constexpr std::uint64_t kBound = std::uint64_t{1} << 23;
    SendResult result = SendResult::Ok;
    while (sent < kBound && (result = system.send(actor, Item{.seq = sent, .end = false})) == SendResult::Ok) ++sent;
    CHECK(result == SendResult::Full);

    gate.open.store(true, std::memory_order_release);
    gate.open.notify_all();
    CHECK(sendPatiently(system, actor, Item{.seq = sent++, .end = false}) == SendResult::Ok);
    CHECK(sendPatiently(system, actor, Item{.seq = 0, .end = true}) == SendResult::Ok);
    system.join();

    CHECK(system.droppedMessages() == 0);
    REQUIRE(seen.size() == sent);
    bool in_order = true;
    for (std::uint64_t i = 0; i < sent; ++i) in_order = in_order && seen[i] == i;
    CHECK(in_order);
}

// Messages between workers must wait while the receiving worker's arena is full, never be
// dropped: a dropped end marker leaves an actor waiting forever, which is how the Raydium
// indexer benchmark once hung. Each item here becomes many parts for the other workers, so
// the parts already in flight outgrow a worker's small arena long before slowing the sender
// down could help. The test stops the actors itself, so a lost part fails it, not hangs it.
TEST_CASE("messages between workers wait for room in a full arena instead of being dropped") {
    constexpr std::size_t kWorkers = 3;
    constexpr std::uint64_t kItems = 20'000;
    constexpr std::uint64_t kFanOut = 16;  // parts per item for each other worker
    struct Part {
        std::uint32_t splitter;
        std::uint64_t seq;
    };
    struct Totals {
        std::atomic<std::uint64_t> parts{0};
        std::atomic<std::uint64_t> out_of_order{0};
        std::atomic<std::uint64_t> failed_sends{0};
    };

    // The slow stage: a little work per part. Checks that each splitter's parts arrive in order.
    class Sink : public Actor<Part> {
    public:
        explicit Sink(Totals* totals) : totals_{totals} {}
        void onMessage(const Part& part) {
            if (part.seq < next_.at(part.splitter)) totals_->out_of_order.fetch_add(1, std::memory_order_relaxed);
            next_.at(part.splitter) = part.seq + 1;
            for (int i = 0; i < 200; ++i) work_ = work_ * 31 + part.seq;  // NOLINT(readability-magic-numbers)
            totals_->parts.fetch_add(1, std::memory_order_relaxed);
        }

    private:
        Totals* totals_;
        std::array<std::uint64_t, kWorkers> next_{};
        volatile std::uint64_t work_ = 0;
    };

    // The fast stage. It sends only to the sinks on other workers: Full from a send to an
    // actor on the same worker is the sender's to handle, and is not what this test is about.
    class Splitter : public Actor<Item> {
    public:
        Splitter(std::uint32_t id, const std::vector<ActorRef<Part>>* sinks, Totals* totals)
            : id_{id}, sinks_{sinks}, totals_{totals} {}
        void onMessage(const Item& item) {
            for (std::uint32_t worker = 0; worker < sinks_->size(); ++worker) {
                if (worker == id_) continue;
                for (std::uint64_t k = 0; k < kFanOut; ++k) {
                    const Part part{.splitter = id_, .seq = item.seq * kFanOut + k};
                    if (send((*sinks_)[worker], part) != SendResult::Ok) {
                        totals_->failed_sends.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }
        }

    private:
        std::uint32_t id_;
        const std::vector<ActorRef<Part>>* sinks_;
        Totals* totals_;
    };

    Totals totals;
    std::vector<ActorRef<Part>> sinks;
    std::vector<ActorRef<Item>> splitters;
    VirtualWorker system{VirtualWorkerConfig{
        .workers = kWorkers,
        .arena = ArenaConfig{.chunk_size = 960, .initial_bytes = 64 << 10, .max_bytes = 256 << 10}}};
    // addActor goes round-robin: splitter i and sink i share worker i.
    for (std::uint32_t i = 0; i < kWorkers; ++i) splitters.push_back(system.addActor<Splitter>(i, &sinks, &totals));
    for (std::uint32_t i = 0; i < kWorkers; ++i) sinks.push_back(system.addActor<Sink>(&totals));
    system.start();

    std::uint64_t not_sent = 0;
    for (std::uint64_t seq = 0; seq < kItems; ++seq) {
        if (sendPatiently(system, splitters[seq % kWorkers], Item{.seq = seq / kWorkers, .end = false}) !=
            SendResult::Ok) {
            ++not_sent;
        }
    }
    CHECK(not_sent == 0);
    const bool all_arrived = waitFor(totals.parts, kItems * (kWorkers - 1) * kFanOut);
    system.stop();
    system.join();

    CHECK(all_arrived);
    CHECK(totals.failed_sends.load() == 0);
    CHECK(system.droppedMessages() == 0);
    CHECK(totals.out_of_order.load() == 0);
}

// A ref works only with the VirtualWorker that made it. Used with another one, it must fail
// like a ref to a stopped actor, not reach whichever actor sits in the same mailbox slot
// there: that actor may take another message type, and a message of the wrong size written
// into its mailbox corrupts memory.
TEST_CASE("a ref from another VirtualWorker reaches no actor of this one") {
    struct Big {
        std::array<std::uint64_t, 100> words;  // NOLINT(readability-magic-numbers)
    };
    class BigSink : public Actor<Big> {
    public:
        void onMessage(const Big& /*big*/) {}
    };
    class Counter : public Actor<Item> {
    public:
        explicit Counter(std::atomic<std::uint64_t>* received) : received_{received} {}
        void onMessage(const Item& item) {
            if (item.end) {
                stop();
                return;
            }
            received_->fetch_add(1, std::memory_order_relaxed);
        }

    private:
        std::atomic<std::uint64_t>* received_;
    };

    constexpr std::uint64_t kSends = 100;
    std::atomic<std::uint64_t> received{0};
    std::atomic<std::uint64_t> unused{0};
    VirtualWorker other{VirtualWorkerConfig{.workers = 1}};  // never started
    const auto foreign_item = other.addActor<Counter>(&unused);
    const auto foreign_big = other.addActor<BigSink>();
    // Actors in the same worker and mailbox slots as the foreign refs: the first takes the same
    // message type as its foreign twin, the second a far smaller one than its twin.
    VirtualWorker system{VirtualWorkerConfig{.workers = 1}};
    const auto first = system.addActor<Counter>(&received);
    const auto second = system.addActor<Counter>(&received);
    system.start();

    for (std::uint64_t i = 0; i < kSends; ++i) {
        CHECK(sendPatiently(system, foreign_item, Item{.seq = i, .end = false}) ==
              SendResult::Ok);  // Ok: the worker has it
        CHECK(sendPatiently(system, foreign_big, Big{}) == SendResult::Ok);
    }
    // This thread's messages arrive in order, so the foreign ones are all handled before these.
    CHECK(sendPatiently(system, first, Item{.seq = 0, .end = true}) == SendResult::Ok);
    CHECK(sendPatiently(system, second, Item{.seq = 0, .end = true}) == SendResult::Ok);
    system.join();

    CHECK(received.load() == 0);
    CHECK(system.droppedMessages() == 2 * kSends);
}

// Each sending thread's messages arrive in the order it sent them, whatever the other
// threads do, and they still arrive when the thread has already exited (its feed outlives
// it until the worker has read everything in it).
TEST_CASE("each thread's messages arrive in its order, even after the thread has exited") {
    constexpr std::size_t kWorkers = 2;
    constexpr std::size_t kThreads = 4;
    constexpr std::uint64_t kPerThread = 50'000;
    struct Tagged {
        std::uint32_t thread;
        std::uint64_t seq;
    };
    struct Totals {
        std::atomic<std::uint64_t> received{0};
        std::atomic<std::uint64_t> out_of_order{0};
    };

    class Checker : public Actor<Tagged> {
    public:
        explicit Checker(Totals* totals) : totals_{totals} {}
        void onMessage(const Tagged& msg) {
            if (msg.seq != next_.at(msg.thread)) totals_->out_of_order.fetch_add(1, std::memory_order_relaxed);
            next_.at(msg.thread) = msg.seq + 1;
            totals_->received.fetch_add(1, std::memory_order_relaxed);
            if (++count_ == kThreads * kPerThread) stop();
        }

    private:
        Totals* totals_;
        std::array<std::uint64_t, kThreads> next_{};
        std::uint64_t count_ = 0;
    };

    Totals totals;
    VirtualWorker system{VirtualWorkerConfig{.workers = kWorkers}};
    std::vector<ActorRef<Tagged>> checkers;
    for (std::size_t i = 0; i < kWorkers; ++i) checkers.push_back(system.addActor<Checker>(&totals));
    system.start();

    std::atomic<std::uint64_t> not_sent{0};
    std::vector<std::thread> senders;
    for (std::uint32_t t = 0; t < kThreads; ++t) {
        senders.emplace_back([&, t] {
            for (std::uint64_t seq = 0; seq < kPerThread; ++seq) {
                for (const auto& checker : checkers) {
                    if (sendPatiently(system, checker, Tagged{.thread = t, .seq = seq}) != SendResult::Ok) {
                        not_sent.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }
        });  // exits right after its last send
    }
    for (auto& sender : senders) sender.join();  // every sender is gone before most messages are read
    REQUIRE(not_sent.load() == 0);
    system.join();

    CHECK(totals.out_of_order.load() == 0);
    CHECK(totals.received.load() == kWorkers * kThreads * kPerThread);
    CHECK(system.droppedMessages() == 0);
}

// An idle worker spins, then sleeps; a message from outside must wake it whenever it comes.
// The hard moment is the one where the worker decides to sleep: if the sender and the worker
// each miss the other's write there (a missing fence on either side), the message sits
// unhandled with the worker asleep. That window is nanoseconds wide, so the test sends
// thousands of messages at random points of the first microseconds of idleness, where an
// idle worker that rarely gets work stops spinning, and now and then after a long pause.
TEST_CASE("a message from outside reaches an idle worker however long it has been idle") {
    class Echo : public Actor<Item> {
    public:
        explicit Echo(std::atomic<std::uint64_t>* handled) : handled_{handled} {}
        void onMessage(const Item& item) {
            if (item.end) {
                stop();
                return;
            }
            handled_->store(item.seq + 1, std::memory_order_release);
        }

    private:
        std::atomic<std::uint64_t>* handled_;
    };

    std::atomic<std::uint64_t> handled{0};
    VirtualWorker system{VirtualWorkerConfig{.workers = 1}};
    const auto echo = system.addActor<Echo>(&handled);
    system.start();

    constexpr std::uint64_t kMessages = 3000;
    constexpr std::uint64_t kLongPauseEvery = 16;  // long pauses also teach the worker to spin briefly
    constexpr auto kLongPause = std::chrono::milliseconds{2};
    constexpr std::uint64_t kMaxShortPauseNs = 8000;
    std::uint64_t random = 0x9E3779B97F4A7C15;  // fixed seed: every run tries the same pauses
    std::uint64_t lost = 0;
    for (std::uint64_t seq = 0; seq < kMessages; ++seq) {
        if (seq % kLongPauseEvery == 0) {
            std::this_thread::sleep_for(kLongPause);
        } else {
            random = random * 6364136223846793005 + 1442695040888963407;  // NOLINT(readability-magic-numbers)
            const auto until = Clock::now() + std::chrono::nanoseconds{(random >> 33) % kMaxShortPauseNs};
            while (Clock::now() < until) {}  // sleep_for is far too coarse for this
        }
        if (sendPatiently(system, echo, Item{.seq = seq, .end = false}) != SendResult::Ok ||
            !waitFor(handled, seq + 1)) {
            ++lost;
            break;  // the rest would only wait kPatience each
        }
    }
    CHECK(lost == 0);
    CHECK(sendPatiently(system, echo, Item{.seq = 0, .end = true}) == SendResult::Ok);
    system.join();
}
