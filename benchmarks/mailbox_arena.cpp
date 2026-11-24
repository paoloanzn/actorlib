// MailboxArena benchmarks, each workload run against MailboxArena and against a
// std::deque-per-mailbox baseline (the "relative" column is arena speed vs the baseline).
//
// Build and run in Release, pinned to one core:
//   cmake -B build/release -DCMAKE_BUILD_TYPE=Release -DACTORLIB_BUILD_BENCHMARKS=ON
//   cmake --build build/release --target actorlib_benchmarks
//   taskset -c 2 build/release/benchmarks/actorlib_benchmarks
// nanobench reports instructions, cycles and branch misses only if perf counters are
// readable (kernel.perf_event_paranoid <= 2).

#include <nanobench.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <actorlib/actorlib.hpp>

namespace {
using actorlib::ActorRef;
using actorlib::ArenaConfig;
using actorlib::MailboxArena;
using actorlib::SendResult;
using ankerl::nanobench::Bench;
using ankerl::nanobench::doNotOptimizeAway;

// Messages of 4 B, one cache line and four cache lines.
struct Ping {
    std::uint32_t seq;
};

template <std::size_t Bytes>
struct Padded {
    std::uint32_t seq;
    std::array<std::byte, Bytes - sizeof(std::uint32_t)> pad;
};

constexpr std::size_t kCacheLine = 64;
using Line = Padded<kCacheLine>;
using Block = Padded<4 * kCacheLine>;
static_assert(sizeof(Line) == kCacheLine && sizeof(Block) == 4 * kCacheLine);

template <class Msg>
Msg make(std::uint32_t seq) noexcept {
    Msg msg{};
    msg.seq = seq;
    return msg;
}

// Both backends expose the same index-based interface, so each workload is written once.

template <class Msg>
class ArenaMailboxes {
public:
    explicit ArenaMailboxes(ArenaConfig config = {}) : arena_{config} {}

    static std::string name() { return "arena"; }

    std::uint32_t create() {
        refs_.push_back(arena_.create<Msg>());
        return static_cast<std::uint32_t>(refs_.size() - 1);
    }

    void send(std::uint32_t box, const Msg& msg) {
        if (arena_.send(refs_[box], msg) != SendResult::Ok) [[unlikely]] {
            throw std::runtime_error{"arena full: raise ArenaConfig::max_bytes"};
        }
    }

    template <class F>
    std::size_t drain(std::uint32_t box, std::size_t budget, F& fn) {
        return arena_.drain(refs_[box], budget, std::ref(fn));
    }

    // Chunk memory only. Both backends leave out their per-mailbox struct (the arena's slot
    // table, the vector of deques).
    [[nodiscard]] std::size_t liveBytes() const noexcept { return arena_.bytesInUse(); }
    [[nodiscard]] std::size_t reservedBytes() const noexcept { return arena_.bytesReserved(); }

private:
    MailboxArena arena_;
    std::vector<ActorRef<Msg>> refs_;
};

// Counts the bytes the deques hold, so the baseline appears in the memory table too.
template <class T>
class CountingAllocator {
public:
    using value_type = T;

    explicit CountingAllocator(std::size_t* live_bytes) noexcept : live_bytes_{live_bytes} {}

    // The allocator requirements need an implicit conversion between rebound allocators.
    template <class U>
    // NOLINTNEXTLINE(google-explicit-constructor, hicpp-explicit-conversions)
    CountingAllocator(const CountingAllocator<U>& other) noexcept : live_bytes_{other.live_bytes_} {}

    // std::deque rebinds its allocator to Msg* for its map of nodes, so T may be a pointer:
    // counting the map's bytes is intended.
    T* allocate(std::size_t n) {
        *live_bytes_ += n * sizeof(T);  // NOLINT(bugprone-sizeof-expression)
        return std::allocator<T>{}.allocate(n);
    }

    void deallocate(T* ptr, std::size_t n) noexcept {
        *live_bytes_ -= n * sizeof(T);  // NOLINT(bugprone-sizeof-expression)
        std::allocator<T>{}.deallocate(ptr, n);
    }

    friend bool operator==(const CountingAllocator&, const CountingAllocator&) = default;

private:
    template <class U>
    friend class CountingAllocator;

    std::size_t* live_bytes_;
};

template <class Msg>
class DequeMailboxes {
public:
    DequeMailboxes() = default;
    ~DequeMailboxes() = default;
    // The deques' allocators point at live_bytes_.
    DequeMailboxes(const DequeMailboxes&) = delete;
    DequeMailboxes& operator=(const DequeMailboxes&) = delete;
    DequeMailboxes(DequeMailboxes&&) = delete;
    DequeMailboxes& operator=(DequeMailboxes&&) = delete;

    static std::string name() { return "std::deque"; }

    std::uint32_t create() {
        boxes_.emplace_back(CountingAllocator<Msg>{&live_bytes_});
        return static_cast<std::uint32_t>(boxes_.size() - 1);
    }

    void send(std::uint32_t box, const Msg& msg) { boxes_[box].push_back(msg); }

    // Same contract as MailboxArena::drain: push_back keeps references to elements valid,
    // so fn may send to the mailbox being drained.
    template <class F>
    std::size_t drain(std::uint32_t box, std::size_t budget, F& fn) {
        std::size_t handled = 0;
        while (handled < budget && !boxes_[box].empty()) {
            fn(boxes_[box].front());
            boxes_[box].pop_front();
            ++handled;
        }
        return handled;
    }

    [[nodiscard]] std::size_t liveBytes() const noexcept { return live_bytes_; }
    [[nodiscard]] std::size_t reservedBytes() const noexcept { return live_bytes_; }

private:
    std::size_t live_bytes_ = 0;
    std::vector<std::deque<Msg, CountingAllocator<Msg>>> boxes_;
};

// One table per workload. The baseline runs first, so "relative" is arena speed vs
// std::deque. 10 ms epochs keep err% low for the workloads that take ~1 ms per run.
constexpr std::chrono::milliseconds kMinEpochTime{10};

Bench table(const std::string& title, const char* unit) {
    Bench bench;
    bench.title(title).unit(unit).relative(true).minEpochTime(kMinEpochTime);
    return bench;
}

// --- Hot path: one mailbox, the inline send/drain code ---------------------------------

constexpr std::uint32_t kBurst = 1024;

// Send one, drain one: the mailbox keeps a single hot chunk (or deque node).
template <class Boxes, class Msg>
void steady(Bench& bench) {
    Boxes boxes;
    const std::uint32_t box = boxes.create();
    std::uint32_t seq = 0;
    std::uint64_t sum = 0;
    auto handle = [&](Msg& msg) { sum += msg.seq; };
    bench.batch(1).run(Boxes::name(), [&] {
        boxes.send(box, make<Msg>(seq++));
        boxes.drain(box, 1, handle);
        doNotOptimizeAway(sum);
    });
}

// Send kBurst, then drain them with drain(box, budget) calls: crosses chunk boundaries,
// and budget 1 adds drain's per-call cost to every message.
template <class Boxes, class Msg>
void burst(Bench& bench, std::size_t budget) {
    Boxes boxes;
    const std::uint32_t box = boxes.create();
    std::uint64_t sum = 0;
    auto handle = [&](Msg& msg) { sum += msg.seq; };
    bench.batch(kBurst).run(Boxes::name(), [&] {
        for (std::uint32_t i = 0; i < kBurst; ++i) boxes.send(box, make<Msg>(i));
        while (boxes.drain(box, budget, handle) != 0) {}
        doNotOptimizeAway(sum);
    });
}

template <class Msg>
void hotPath() {
    const std::string size = std::to_string(sizeof(Msg)) + " B";
    {
        Bench bench = table("steady send + drain, " + size, "msg");
        steady<DequeMailboxes<Msg>, Msg>(bench);
        steady<ArenaMailboxes<Msg>, Msg>(bench);
    }
    for (const std::size_t budget : {std::size_t{kBurst}, std::size_t{1}}) {
        Bench bench = table(
            "burst of " + std::to_string(kBurst) + ", drain budget " + std::to_string(budget) + ", " + size, "msg");
        burst<DequeMailboxes<Msg>, Msg>(bench, budget);
        burst<ArenaMailboxes<Msg>, Msg>(bench, budget);
    }
}

// --- Many mailboxes: the same messages spread over more and more mailboxes -------------

constexpr std::uint32_t kSpreadMessages = 1U << 16;
constexpr std::array<std::uint32_t, 5> kSpreadMailboxes{1, 64, 1024, 16384, kSpreadMessages};
// ArenaConfig::chunk_size, the largest size class; mailboxes start at 64 B and grow toward
// it. Odd numbers of cache lines (64, 320, 960, 4032) next to powers of two (1024, 4096):
// power-of-two chunks map many busy mailboxes' write positions onto the same cache sets.
constexpr std::array<std::size_t, 6> kChunkSizes{64, 320, 960, 1024, 4032, 4096};

// Enough room for one chunk per mailbox at the largest chunk size and mailbox count.
constexpr std::size_t kSpreadSlabBytes = std::size_t{1} << 20;  // 1 MiB
constexpr std::size_t kSpreadMaxBytes = std::size_t{1} << 30;   // 1 GiB

ArenaConfig spreadConfig(std::size_t chunk_size) {
    return ArenaConfig{.chunk_size = chunk_size, .initial_bytes = kSpreadSlabBytes, .max_bytes = kSpreadMaxBytes};
}

struct MemoryRow {
    std::string backend;
    std::uint32_t mailboxes;
    std::size_t peak_bytes;  // after the send phase
    std::size_t idle_bytes;  // after the drain phase
};

// Round-robin sends (message i goes to mailbox i % mailboxes), then drains each mailbox.
template <class Boxes>
MemoryRow spread(Bench& bench, const std::string& name, Boxes& boxes, std::uint32_t mailboxes) {
    const std::uint32_t per_mailbox = kSpreadMessages / mailboxes;
    for (std::uint32_t i = 0; i < mailboxes; ++i) static_cast<void>(boxes.create());
    std::uint64_t sum = 0;
    auto handle = [&](Ping& msg) { sum += msg.seq; };
    auto send_all = [&] {
        for (std::uint32_t round = 0; round < per_mailbox; ++round) {
            for (std::uint32_t box = 0; box < mailboxes; ++box) boxes.send(box, Ping{round});
        }
    };
    auto drain_all = [&] {
        for (std::uint32_t box = 0; box < mailboxes; ++box) boxes.drain(box, per_mailbox, handle);
    };

    // One cycle outside the timing grows the arena to its peak and records the memory use.
    send_all();
    const std::size_t peak = boxes.liveBytes();
    drain_all();
    const std::size_t idle = boxes.liveBytes();

    bench.run(name, [&] {
        send_all();
        drain_all();
        doNotOptimizeAway(sum);
    });
    return MemoryRow{.backend = name, .mailboxes = mailboxes, .peak_bytes = peak, .idle_bytes = idle};
}

void printMemory(const std::vector<MemoryRow>& rows) {
    constexpr double kKiB = 1024;
    constexpr double kPayload = double{kSpreadMessages} * sizeof(Ping);
    std::cout << "\n## Memory for " << kSpreadMessages << " x " << sizeof(Ping) << " B messages (payload "
              << kPayload / kKiB << " KiB)\n\n"
              << "| mailboxes | backend | peak KiB | peak / payload | idle KiB |\n"
              << "|----------:|:--------|---------:|---------------:|---------:|\n"
              << std::fixed << std::setprecision(1);
    for (const MemoryRow& row : rows) {
        const auto peak = static_cast<double>(row.peak_bytes);
        std::cout << "| " << row.mailboxes << " | " << row.backend << " | " << peak / kKiB << " | " << peak / kPayload
                  << "x | " << static_cast<double>(row.idle_bytes) / kKiB << " |\n";
    }
}

void manyMailboxes() {
    std::vector<MemoryRow> memory;
    for (const std::uint32_t mailboxes : kSpreadMailboxes) {
        Bench bench = table(std::to_string(kSpreadMessages) + " msgs over " + std::to_string(mailboxes) +
                                " mailboxes, " + std::to_string(kSpreadMessages / mailboxes) + " each",
                            "msg");
        bench.batch(kSpreadMessages);
        {
            DequeMailboxes<Ping> boxes;
            memory.push_back(spread(bench, DequeMailboxes<Ping>::name(), boxes, mailboxes));
        }
        for (const std::size_t chunk_size : kChunkSizes) {
            ArenaMailboxes<Ping> boxes{spreadConfig(chunk_size)};
            const std::string name = "arena, chunks up to " + std::to_string(chunk_size) + " B";
            memory.push_back(spread(bench, name, boxes, mailboxes));
        }
    }
    printMemory(memory);
}

// --- Actor patterns: a token passed around a ring of actors (Savina's ThreadRing) ------

constexpr std::uint32_t kRingHops = 1U << 16;
constexpr std::array<std::uint32_t, 3> kRingActors{2, 1024, 65536};  // 2 actors is PingPong

// Each actor forwards the token to the next one until the hop count runs out. One token
// means one ready actor at a time, so the "scheduler" is just the next mailbox to drain.
template <class Boxes>
void ring(Bench& bench, std::uint32_t actors) {
    Boxes boxes;
    for (std::uint32_t i = 0; i < actors; ++i) static_cast<void>(boxes.create());
    std::uint32_t current = 0;
    auto handle = [&](Ping& token) {
        if (token.seq == 0) return;
        current = (current + 1) % actors;
        boxes.send(current, Ping{token.seq - 1});
    };
    bench.run(Boxes::name(), [&] {
        current = 0;
        boxes.send(current, Ping{kRingHops});
        while (boxes.drain(current, 1, handle) != 0) {}
        doNotOptimizeAway(current);
    });
}

void actorPatterns() {
    for (const std::uint32_t actors : kRingActors) {
        const std::string name = actors == 2 ? "PingPong" : "ThreadRing, " + std::to_string(actors) + " actors";
        Bench bench = table(name + ", " + std::to_string(kRingHops) + " hops", "hop");
        bench.batch(kRingHops + 1);  // kRingHops forwards plus the final message
        ring<DequeMailboxes<Ping>>(bench, actors);
        ring<ArenaMailboxes<Ping>>(bench, actors);
    }
}
}  // namespace

int main() {
    try {
        hotPath<Ping>();
        hotPath<Line>();
        hotPath<Block>();
        manyMailboxes();
        actorPatterns();
    } catch (const std::exception& error) {
        std::cerr << "actorlib_benchmarks: " << error.what() << '\n';
        return 1;
    }
}
