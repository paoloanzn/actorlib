// The headline numbers of actorlib: ten metrics in four groups that say whether a change
// made the runtime faster or slower. Each one measures something a user of the library
// feels, not an internal counter. Each group pairs metrics that pull against each other,
// so none of them can improve by quietly making another one worse:
//
//   Speed       local hop, cross round trip     what one message costs, on one worker and between two
//   Scale       mesh, outside sends             throughput with every core busy, and from outside threads
//   Efficiency  idle actor, spawn tree,         what an actor costs, the actor lifecycle, and
//               flood peak + flood rate         backpressure that bounds memory without stalling
//   Idle        wake p50 + wake CPU             latency to a sleeping worker, against the CPU spent spinning
//
// Compare your working tree with main through the script. It builds both with these same
// scenarios and runs them interleaved on this machine:
//   benchmarks/bench.sh                   main vs working tree, 5 rounds
//   benchmarks/bench.sh v0.2.0 9          another base, more rounds
// Or run the binary yourself (Release build, -DACTORLIB_BUILD_BENCHMARKS=ON):
//   actorlib_benchmarks                   one run, as a table
//   actorlib_benchmarks --raw             one run, as "key value" lines
//   actorlib_benchmarks --compare B H     the A/B table from two files of --raw lines
//   actorlib_benchmarks --smoke           each scenario once at a tiny size, to check they still run
//
// Every scenario checks that each message it sent arrived, and aborts if one did not, so a
// broken runtime cannot post a good time.

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <exception>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <limits>
#include <map>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#if defined(__GLIBC__)
#include <malloc.h>
#endif
#if defined(__linux__)
#include <unistd.h>
#endif

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

// --- The metrics --------------------------------------------------------------------------

enum class Better : std::uint8_t { Lower, Higher };

struct Metric {
    std::string_view key;  // its name in --raw output: keep it stable
    std::string_view group;
    std::string_view unit;
    Better better;
    std::string_view what;
};

enum MetricId : std::uint8_t {
    LocalHop,
    CrossRoundTrip,
    Mesh,
    OutsideSends,
    IdleActor,
    SpawnTree,
    FloodPeak,
    FloodRate,
    WakeP50,
    WakeCpu,
    MetricCount,
};

constexpr std::array<Metric, MetricCount> kMetrics{{
    {"local_hop", "Speed", "ns", Better::Lower, "ping-pong on one worker, per message"},
    {"cross_round_trip", "Speed", "ns", Better::Lower, "ping-pong between two workers, per round trip"},
    {"mesh", "Scale", "M msg/s", Better::Higher, "actors on every worker send to random actors on any worker"},
    {"outside_sends", "Scale", "M msg/s", Better::Higher, "one outside thread sends to actors on the other cores"},
    {"idle_actor", "Efficiency", "B", Better::Lower, "heap one more actor takes, with an empty mailbox"},
    {"spawn_tree", "Efficiency", "M actors/s", Better::Higher,
     "each worker spawns a binary tree; each actor reports up, stops"},
    {"flood_peak", "Efficiency", "MiB", Better::Lower, "memory growth while an actor floods a slower one (8 MiB cap)"},
    {"flood_rate", "Efficiency", "M msg/s", Better::Higher, "the slower actor's rate during that flood"},
    {"wake_p50", "Idle", "us", Better::Lower, "median, outside send to onMessage on a sleeping worker, 1 per 2 ms"},
    {"wake_cpu", "Idle", "%", Better::Lower, "CPU used meanwhile, as a share of one core"},
}};

// One value per metric. NaN where a scenario does not produce that metric.
using Values = std::vector<double>;

// --- Sizes ----------------------------------------------------------------------------------

struct Sizes {
    int warmups;  // runs of each scenario thrown away before the measured ones
    int reps;     // measured runs; each metric reports their median
    std::uint64_t local_hops;
    std::uint64_t cross_round_trips;
    std::uint32_t mesh_actors_per_worker;
    std::uint32_t mesh_tokens_per_actor;
    std::uint32_t mesh_hops_per_token;
    std::uint64_t outside_messages;
    std::uint32_t idle_actors;
    std::uint32_t spawn_depth;  // each worker's tree has 2^(depth + 1) - 1 actors
    std::uint64_t flood_messages;
    std::uint32_t wake_samples;
};

// About 10 s on 6 cores. Each scenario runs for 0.1 to 0.5 s, long enough that starting
// and joining the threads is noise.
constexpr Sizes kFull{
    .warmups = 1,
    .reps = 3,
    .local_hops = 20'000'000,
    .cross_round_trips = 1'000'000,
    .mesh_actors_per_worker = 64,
    .mesh_tokens_per_actor = 16,
    .mesh_hops_per_token = 2'000,
    .outside_messages = 10'000'000,
    .idle_actors = 100'000,
    .spawn_depth = 16,
    .flood_messages = 4'000'000,
    .wake_samples = 300,
};

constexpr Sizes kSmoke{
    .warmups = 0,
    .reps = 1,
    .local_hops = 1'000,
    .cross_round_trips = 1'000,
    .mesh_actors_per_worker = 4,
    .mesh_tokens_per_actor = 2,
    .mesh_hops_per_token = 10,
    .outside_messages = 1'000,
    .idle_actors = 100,
    .spawn_depth = 3,
    .flood_messages = 10'000,
    .wake_samples = 5,
};

// --- Helpers --------------------------------------------------------------------------------

constexpr double kNanosPerSecond = 1e9;
constexpr double kNanosPerMicro = 1e3;
constexpr double kMillion = 1e6;
constexpr double kMiB = 1024.0 * 1024.0;
constexpr double kPercent = 100.0;

[[noreturn]] void fail(std::string_view what) {
    std::cerr << "actorlib_benchmarks: " << what << '\n';
    std::abort();
}

void check(bool ok, std::string_view what) {
    if (!ok) fail(what);
}

double secondsSince(Clock::time_point start) { return std::chrono::duration<double>(Clock::now() - start).count(); }

std::int64_t nowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count();
}

std::size_t hardwareThreads() { return std::max<std::size_t>(1, std::thread::hardware_concurrency()); }

// From outside the actors: Full means the workers are behind, so wait and send again.
template <class Msg>
void sendFromOutside(VirtualWorker& system, ActorRef<Msg> to, const Msg& msg) {
    SendResult result = system.send(to, msg);
    while (result == SendResult::Full) {
        std::this_thread::yield();
        result = system.send(to, msg);
    }
    check(result == SendResult::Ok, "send from outside failed");
}

// Counts down to zero from any thread; wait() blocks until it gets there.
class Countdown {
public:
    void reset(std::int64_t count) noexcept { left_.store(count, std::memory_order_relaxed); }
    void countDown() noexcept {
        if (left_.fetch_sub(1, std::memory_order_acq_rel) == 1) left_.notify_all();
    }
    [[nodiscard]] bool done() const noexcept { return left_.load(std::memory_order_acquire) == 0; }
    void wait() const noexcept {
        for (std::int64_t left = left_.load(std::memory_order_acquire); left != 0;
             left = left_.load(std::memory_order_acquire)) {
            left_.wait(left, std::memory_order_acquire);
        }
    }

private:
    std::atomic<std::int64_t> left_{0};
};

// xorshift64: cheap enough that picking a peer costs next to nothing next to a send.
class Random {
public:
    explicit Random(std::uint64_t seed) noexcept : state_{seed | 1U} {}
    std::uint64_t next() noexcept {
        state_ ^= state_ << kShiftA;
        state_ ^= state_ >> kShiftB;
        state_ ^= state_ << kShiftC;
        return state_;
    }

private:
    static constexpr int kShiftA = 13;
    static constexpr int kShiftB = 7;
    static constexpr int kShiftC = 17;
    std::uint64_t state_;
};

// Heap bytes in use, from glibc's allocator. Before start() every actor is built on the
// calling thread, so its allocations all show up here.
double heapBytes() {
#if defined(__GLIBC__)
    const struct mallinfo2 info = mallinfo2();
    return static_cast<double>(info.uordblks + info.hblkhd);
#else
    return std::numeric_limits<double>::quiet_NaN();
#endif
}

double residentBytes() {
#if defined(__linux__)
    std::ifstream statm{"/proc/self/statm"};
    std::size_t total_pages = 0;
    std::size_t resident_pages = 0;
    statm >> total_pages >> resident_pages;
    return static_cast<double>(resident_pages) * static_cast<double>(sysconf(_SC_PAGESIZE));
#else
    return std::numeric_limits<double>::quiet_NaN();
#endif
}

// CPU time of the whole process, all threads.
double cpuSeconds() { return static_cast<double>(std::clock()) / CLOCKS_PER_SEC; }

// --- Speed: ping-pong -----------------------------------------------------------------------

struct Ball {
    std::uint64_t left;  // hops still to go
};

class Player : public Actor<Ball> {
public:
    explicit Player(const ActorRef<Ball>* peer) : peer_{peer} {}

    void onMessage(const Ball& ball) {
        if (ball.left > 0) check(send(*peer_, Ball{ball.left - 1}) == SendResult::Ok, "ping-pong: send failed");
        if (ball.left <= 1) stop();  // the peer stops on the 0 it gets next
    }

private:
    const ActorRef<Ball>* peer_;
};

// Seconds for `hops` messages bounced between two actors. addActor places them on the
// workers in turn: with one worker they share it, with two each has its own.
double pingPong(const VirtualWorkerConfig& config, std::uint64_t hops) {
    VirtualWorker system{config};
    ActorRef<Ball> ping;
    ActorRef<Ball> pong;
    ping = system.addActor<Player>(&pong);
    pong = system.addActor<Player>(&ping);
    system.start();
    const auto start = Clock::now();
    sendFromOutside(system, ping, Ball{hops});
    system.join();
    return secondsSince(start);
}

void localHop(const Sizes& sizes, Values& values) {
    values[LocalHop] =
        pingPong({.workers = 1}, sizes.local_hops) * kNanosPerSecond / static_cast<double>(sizes.local_hops);
}

void crossRoundTrip(const Sizes& sizes, Values& values) {
    const auto rounds = static_cast<double>(sizes.cross_round_trips);
    values[CrossRoundTrip] = pingPong({.workers = 2}, 2 * sizes.cross_round_trips) * kNanosPerSecond / rounds;
}

// --- Scale: mesh (Savina's Big) -------------------------------------------------------------

struct Hop {
    std::uint32_t left;  // hops still to go, or kMeshStart
};
constexpr std::uint32_t kMeshStart = std::numeric_limits<std::uint32_t>::max();

struct MeshShared {
    std::vector<ActorRef<Hop>> nodes;  // filled before start(), read-only after
    std::uint32_t tokens_per_actor = 0;
    std::uint32_t hops_per_token = 0;
    Countdown tokens_left;
};

// On kMeshStart, sends its tokens to random nodes. Each node passes a token on to another
// random node until the token's hops run out. With W workers, (W - 1) / W of the hops cross
// to another worker, so this stresses batching between workers with every core busy.
class MeshNode : public Actor<Hop> {
public:
    MeshNode(MeshShared* shared, std::uint64_t seed) : shared_{shared}, random_{seed} {}

    void onMessage(const Hop& hop) {
        if (hop.left == kMeshStart) {
            for (std::uint32_t i = 0; i < shared_->tokens_per_actor; ++i) forward(shared_->hops_per_token);
        } else if (hop.left == 0) {
            shared_->tokens_left.countDown();
        } else {
            forward(hop.left - 1);
        }
    }

private:
    void forward(std::uint32_t left) {
        const ActorRef<Hop> to = shared_->nodes[random_.next() % shared_->nodes.size()];
        check(send(to, Hop{left}) == SendResult::Ok, "mesh: send failed");
    }

    MeshShared* shared_;
    Random random_;
};

void mesh(const Sizes& sizes, Values& values) {
    const std::size_t workers = hardwareThreads();
    const std::size_t actors = workers * sizes.mesh_actors_per_worker;
    MeshShared shared;
    shared.tokens_per_actor = sizes.mesh_tokens_per_actor;
    shared.hops_per_token = sizes.mesh_hops_per_token;
    shared.tokens_left.reset(static_cast<std::int64_t>(actors * sizes.mesh_tokens_per_actor));

    VirtualWorker system{VirtualWorkerConfig{.workers = workers}};
    for (std::size_t i = 0; i < actors; ++i) shared.nodes.push_back(system.addActor<MeshNode>(&shared, i + 1));
    system.start();
    const auto start = Clock::now();
    for (const ActorRef<Hop> node : shared.nodes) sendFromOutside(system, node, Hop{kMeshStart});
    shared.tokens_left.wait();
    const double seconds = secondsSince(start);
    system.stop();
    system.join();

    // Each token is delivered once per hop, plus once with 0 hops left.
    const double messages = static_cast<double>(actors) * sizes.mesh_tokens_per_actor * (sizes.mesh_hops_per_token + 1);
    values[Mesh] = messages / seconds / kMillion;
}

// --- Scale: sends from an outside thread ----------------------------------------------------

struct Tick {
    std::uint64_t seq;
};

class Counter : public Actor<Tick> {
public:
    Counter(std::uint64_t expected, Countdown* done) : expected_{expected}, done_{done} {}

    void onMessage(const Tick& /*tick*/) {
        if (++seen_ != expected_) return;
        done_->countDown();
        stop();
    }

private:
    std::uint64_t expected_;
    std::uint64_t seen_ = 0;
    Countdown* done_;
};

// This thread is the outside sender, so the workers take the other cores.
void outsideSends(const Sizes& sizes, Values& values) {
    const std::size_t workers = std::max<std::size_t>(1, hardwareThreads() - 1);
    const std::uint64_t per_worker = sizes.outside_messages / workers;
    Countdown done;
    done.reset(static_cast<std::int64_t>(workers));

    VirtualWorker system{VirtualWorkerConfig{.workers = workers}};
    std::vector<ActorRef<Tick>> counters;
    for (std::size_t i = 0; i < workers; ++i) counters.push_back(system.addActor<Counter>(per_worker, &done));
    system.start();
    const auto start = Clock::now();
    for (std::uint64_t seq = 0; seq < per_worker * workers; ++seq) {
        sendFromOutside(system, counters[seq % workers], Tick{seq});
    }
    done.wait();
    const double seconds = secondsSince(start);
    system.join();
    values[OutsideSends] = static_cast<double>(per_worker * workers) / seconds / kMillion;
}

// --- Efficiency: what an idle actor costs ---------------------------------------------------

class Idle : public Actor<std::uint8_t> {
public:
    void onMessage(std::uint8_t /*msg*/) { stop(); }
};

// On one worker, so the number does not depend on the machine's core count.
void idleActor(const Sizes& sizes, Values& values) {
    VirtualWorker system{VirtualWorkerConfig{.workers = 1}};
    const double before = heapBytes();
    for (std::uint32_t i = 0; i < sizes.idle_actors; ++i) static_cast<void>(system.addActor<Idle>());
    values[IdleActor] = (heapBytes() - before) / sizes.idle_actors;
    system.start();
    system.stop();
    system.join();
}

// --- Efficiency: spawn tree (CAF's actor_creation) ------------------------------------------

struct TreeMsg {
    std::uint64_t count;  // actors in the sender's subtree, in a result
    bool result;          // false: start
};

struct TreeShared {
    Countdown roots_left;
    std::atomic<std::uint64_t> actors{0};
};

// On start, a node of depth > 0 spawns two children and starts them. Once both report their
// subtree's size, it reports its own to its parent and stops. Every actor is spawned, gets a
// message, sends one and is destroyed: the whole lifecycle.
class TreeNode : public Actor<TreeMsg> {
public:
    TreeNode(ActorRef<TreeMsg> parent, std::uint32_t depth, TreeShared* shared)
        : parent_{parent}, depth_{depth}, shared_{shared} {}

    void onMessage(const TreeMsg& msg) {
        if (!msg.result) {
            if (depth_ == 0) {
                finish(1);
                return;
            }
            for (int i = 0; i < 2; ++i) {
                const ActorRef<TreeMsg> child = spawn<TreeNode>(self(), depth_ - 1, shared_);
                check(send(child, TreeMsg{.count = 0, .result = false}) == SendResult::Ok, "spawn tree: send failed");
            }
            return;
        }
        subtree_ += msg.count;
        if (++replies_ == 2) finish(subtree_ + 1);
    }

private:
    void finish(std::uint64_t count) {
        stop();
        if (parent_ == ActorRef<TreeMsg>{}) {  // the root
            shared_->actors.fetch_add(count, std::memory_order_relaxed);
            shared_->roots_left.countDown();
            return;
        }
        check(send(parent_, TreeMsg{.count = count, .result = true}) == SendResult::Ok, "spawn tree: reply failed");
    }

    ActorRef<TreeMsg> parent_;
    std::uint32_t depth_;
    TreeShared* shared_;
    std::uint64_t subtree_ = 0;
    int replies_ = 0;
};

// One tree per worker: spawn() places children on their parent's worker.
void spawnTree(const Sizes& sizes, Values& values) {
    const std::size_t workers = hardwareThreads();
    TreeShared shared;
    shared.roots_left.reset(static_cast<std::int64_t>(workers));

    VirtualWorker system{VirtualWorkerConfig{.workers = workers}};
    std::vector<ActorRef<TreeMsg>> roots;
    for (std::size_t i = 0; i < workers; ++i) {
        roots.push_back(system.addActor<TreeNode>(ActorRef<TreeMsg>{}, sizes.spawn_depth, &shared));
    }
    system.start();
    const auto start = Clock::now();
    for (const ActorRef<TreeMsg> root : roots) sendFromOutside(system, root, TreeMsg{.count = 0, .result = false});
    shared.roots_left.wait();
    const double seconds = secondsSince(start);
    system.join();

    const std::uint64_t expected = workers * ((std::uint64_t{2} << sizes.spawn_depth) - 1);
    check(shared.actors.load(std::memory_order_relaxed) == expected, "spawn tree: wrong actor count");
    values[SpawnTree] = static_cast<double>(expected) / seconds / kMillion;
}

// --- Efficiency: flood ----------------------------------------------------------------------

constexpr std::size_t kFloodCap = std::size_t{8} << 20;  // ArenaConfig::max_bytes, per worker
constexpr std::uint32_t kFloodBurst = 4096;              // messages the producer sends per turn
constexpr int kFloodWork = 16;  // steps of busy work per message: the consumer is the slower one
constexpr auto kFloodSampleEvery = std::chrono::microseconds(100);

struct Item {
    std::uint64_t seq;
    std::array<std::byte, actorlib::kChunkAlign - sizeof(std::uint64_t)> pad;  // one cache line in all
};

struct Go {};

struct FloodShared {
    ActorRef<Item> consumer;
    std::uint64_t total = 0;
    std::uint64_t checksum = 0;  // the consumer's busy work, written once: keeps it from being optimized away
    Countdown done;
};

class Producer : public Actor<Go> {
public:
    explicit Producer(FloodShared* shared) : shared_{shared} {}

    void onMessage(const Go& /*go*/) {
        for (std::uint32_t i = 0; i < kFloodBurst && sent_ < shared_->total; ++i) {
            const SendResult result = send(shared_->consumer, Item{.seq = sent_, .pad = {}});
            if (result == SendResult::Full) break;  // try again next turn
            check(result == SendResult::Ok, "flood: send failed");
            ++sent_;
        }
        if (sent_ == shared_->total) {
            stop();
            return;
        }
        check(send(self(), Go{}) == SendResult::Ok, "flood: send to self failed");
    }

private:
    FloodShared* shared_;
    std::uint64_t sent_ = 0;
};

class Consumer : public Actor<Item> {
public:
    explicit Consumer(FloodShared* shared) : shared_{shared} {}

    void onMessage(const Item& item) {
        for (int i = 0; i < kFloodWork; ++i) work_ = work_ * kLcgMultiplier + item.seq;
        if (++seen_ != shared_->total) return;
        shared_->checksum = work_;
        shared_->done.countDown();
        stop();
    }

private:
    static constexpr std::uint64_t kLcgMultiplier = 6364136223846793005U;
    FloodShared* shared_;
    std::uint64_t seen_ = 0;
    std::uint64_t work_ = 1;
};

// A producer on one worker sends far faster than a consumer on another can handle. The
// arena cap and the transit budget must hold memory near the cap (peak), and the consumer
// must still run at its own speed (rate): pausing the producer must not stall the consumer.
void flood(const Sizes& sizes, Values& values) {
    FloodShared shared;
    shared.total = sizes.flood_messages;
    shared.done.reset(1);

    VirtualWorker system{VirtualWorkerConfig{.workers = 2, .arena = ArenaConfig{.max_bytes = kFloodCap}}};
    const ActorRef<Go> producer = system.addActor<Producer>(&shared);
    shared.consumer = system.addActor<Consumer>(&shared);
    system.start();
    const double baseline = residentBytes();
    double peak = baseline;
    const auto start = Clock::now();
    sendFromOutside(system, producer, Go{});
    while (!shared.done.done()) {
        peak = std::max(peak, residentBytes());
        std::this_thread::sleep_for(kFloodSampleEvery);
    }
    const double seconds = secondsSince(start);
    system.join();

    check(shared.checksum != 0, "flood: consumer did no work");
    values[FloodPeak] = (peak - baseline) / kMiB;
    values[FloodRate] = static_cast<double>(shared.total) / seconds / kMillion;
}

// --- Idle: wake latency against CPU ---------------------------------------------------------

// Longer than the longest spin (1 ms), so the worker is asleep when each message comes: a
// longer spin would cut the latency here, and show up as CPU.
constexpr auto kWakeGap = std::chrono::milliseconds(2);

struct Stamp {
    std::int64_t sent_ns;
};

class LatencySink : public Actor<Stamp> {
public:
    explicit LatencySink(std::vector<std::int64_t>* latencies) : latencies_{latencies} {}

    void onMessage(const Stamp& stamp) { latencies_->push_back(nowNs() - stamp.sent_ns); }

private:
    std::vector<std::int64_t>* latencies_;  // reserved up front; read after join()
};

void wake(const Sizes& sizes, Values& values) {
    std::vector<std::int64_t> latencies;
    latencies.reserve(sizes.wake_samples);
    VirtualWorker system{VirtualWorkerConfig{.workers = 1}};
    const ActorRef<Stamp> sink = system.addActor<LatencySink>(&latencies);
    system.start();
    std::this_thread::sleep_for(kWakeGap);  // let the worker fall asleep

    const double cpu_start = cpuSeconds();
    const auto start = Clock::now();
    auto next = start;
    for (std::uint32_t i = 0; i < sizes.wake_samples; ++i) {
        next += kWakeGap;
        std::this_thread::sleep_until(next);
        sendFromOutside(system, sink, Stamp{nowNs()});
    }
    std::this_thread::sleep_for(kWakeGap);  // the last message's turn
    const double cpu = cpuSeconds() - cpu_start;
    const double seconds = secondsSince(start);
    system.stop();
    system.join();

    check(latencies.size() == sizes.wake_samples, "wake: lost a message");
    // The median: the tail mostly measures how the OS schedules threads, not the runtime.
    std::ranges::nth_element(latencies, latencies.begin() + static_cast<std::ptrdiff_t>(latencies.size() / 2));
    values[WakeP50] = static_cast<double>(latencies[latencies.size() / 2]) / kNanosPerMicro;
    values[WakeCpu] = cpu / seconds * kPercent;
}

// --- Running and reporting ------------------------------------------------------------------

using Scenario = void (*)(const Sizes&, Values&);

// Flood first: it reads the process's resident memory, which the others would leave inflated.
constexpr std::array<Scenario, 8> kScenarios{flood, idleActor,    localHop,  crossRoundTrip,
                                             mesh,  outsideSends, spawnTree, wake};

double median(std::vector<double> samples) {
    if (samples.empty()) return std::numeric_limits<double>::quiet_NaN();
    std::ranges::sort(samples);
    const std::size_t mid = samples.size() / 2;
    return samples.size() % 2 == 1 ? samples[mid] : (samples[mid - 1] + samples[mid]) / 2;
}

Values runAll(const Sizes& sizes) {
#if defined(__GLIBC__)
    // glibc raises its mmap threshold as memory is freed, after which big blocks come from the
    // heap and stay resident once freed. A fixed threshold maps every slab, and unmaps it on
    // free, so resident memory tracks what the runtime holds.
    constexpr int kMmapThreshold = 128 * 1024;
    mallopt(M_MMAP_THRESHOLD, kMmapThreshold);  // NOLINT(concurrency-mt-unsafe): no other thread exists yet
#endif
    std::vector<std::vector<double>> samples(MetricCount);
    for (const Scenario scenario : kScenarios) {
        for (int run = 0; run < sizes.warmups + sizes.reps; ++run) {
            Values values(MetricCount, std::numeric_limits<double>::quiet_NaN());
            scenario(sizes, values);
            if (run < sizes.warmups) continue;
            for (std::size_t id = 0; id < MetricCount; ++id) {
                if (!std::isnan(values[id])) samples[id].push_back(values[id]);
            }
        }
    }
    Values result(MetricCount);
    for (std::size_t id = 0; id < MetricCount; ++id) result[id] = median(samples[id]);
    return result;
}

// Three significant digits or so: enough to see a few percent, not so many that noise reads as signal.
std::string formatValue(double value) {
    if (std::isnan(value)) return "n/a";
    constexpr double kTwoDecimals = 10;
    constexpr double kOneDecimal = 100;
    const double magnitude = std::abs(value);
    int decimals = 0;
    if (magnitude < kTwoDecimals) {
        decimals = 2;
    } else if (magnitude < kOneDecimal) {
        decimals = 1;
    }
    std::ostringstream out;
    out << std::fixed << std::setprecision(decimals) << value;
    return out.str();
}

constexpr int kKeyWidth = 18;
constexpr int kValueWidth = 10;
constexpr int kUnitWidth = 12;

void printTable(const Values& values, const Sizes& sizes) {
    std::cout << "actorlib benchmarks: " << hardwareThreads() << " hardware threads, median of " << sizes.reps
              << " run(s)\n";
    std::string_view group;
    for (std::size_t id = 0; id < MetricCount; ++id) {
        const Metric& metric = kMetrics.at(id);
        if (metric.group != group) {
            group = metric.group;
            std::cout << '\n' << group << '\n';
        }
        std::cout << "  " << std::left << std::setw(kKeyWidth) << metric.key << std::right << std::setw(kValueWidth)
                  << formatValue(values[id]) << ' ' << std::left << std::setw(kUnitWidth) << metric.unit << metric.what
                  << '\n';
    }
}

void printRaw(const Values& values) {
    std::cout << std::setprecision(std::numeric_limits<double>::max_digits10);
    for (std::size_t id = 0; id < MetricCount; ++id) std::cout << kMetrics.at(id).key << ' ' << values[id] << '\n';
}

// --- A/B comparison -------------------------------------------------------------------------

// A change smaller than this is "same" even when the runs agree closely: below it, the
// interleaved medians still move between sessions on a quiet machine.
constexpr double kMinChange = 0.03;

std::map<std::string, std::vector<double>, std::less<>> readRaw(const std::string& path) {
    std::ifstream in{path};
    check(in.good(), "cannot read " + path);
    std::map<std::string, std::vector<double>, std::less<>> runs;
    std::string key;
    std::string value;
    while (in >> key >> value) {
        if (value != "nan") runs[key].push_back(std::stod(value));
    }
    return runs;
}

// The standard error of a median, from the median absolute deviation of the runs: unlike
// their range, one slow round barely moves it. 1.4826 turns the MAD into a standard
// deviation, and 1.2533 that into the standard error of the median.
double medianError(const std::vector<double>& runs) {
    if (runs.size() < 2) return 0;
    const double mid = median(runs);
    std::vector<double> deviations;
    deviations.reserve(runs.size());
    for (const double run : runs) deviations.push_back(std::abs(run - mid));
    constexpr double kMadToSigma = 1.4826;
    constexpr double kSigmaToMedianError = 1.2533;
    return kMadToSigma * kSigmaToMedianError * median(deviations) / std::sqrt(static_cast<double>(runs.size()));
}

// How far apart the two medians can be by noise alone, relative to the base: two standard
// errors of their difference.
double noise(const std::vector<double>& base_runs, const std::vector<double>& head_runs) {
    const double base = std::abs(median(base_runs));
    if (base == 0 || std::isnan(base)) return 0;
    return 2 * std::hypot(medianError(base_runs), medianError(head_runs)) / base;
}

constexpr int kChangeWidth = 9;
constexpr int kNoiseWidth = 8;

// Prints each metric's median on both sides, its change and the noise, and returns 1 if
// any metric got worse by more than the noise (and kMinChange).
int compare(const std::string& base_path, const std::string& head_path) {
    const auto base = readRaw(base_path);
    const auto head = readRaw(head_path);
    const std::size_t rounds = base.empty() ? 0 : base.begin()->second.size();
    std::cout << "median of " << rounds << " interleaved round(s); worse = beyond the noise and "
              << kMinChange * kPercent << "%\n\n"
              << std::left << std::setw(kKeyWidth) << "metric" << std::right << std::setw(kValueWidth) << "base"
              << std::setw(kValueWidth) << "head" << "  " << std::left << std::setw(kUnitWidth) << "" << std::right
              << std::setw(kChangeWidth) << "change" << std::setw(kNoiseWidth) << "noise" << "\n";
    bool worse_any = false;
    for (const Metric& metric : kMetrics) {
        const auto base_runs = base.find(metric.key);
        const auto head_runs = head.find(metric.key);
        if (base_runs == base.end() || head_runs == head.end()) {
            std::cout << std::left << std::setw(kKeyWidth) << metric.key << " n/a\n";
            continue;
        }
        const double before = median(base_runs->second);
        const double after = median(head_runs->second);
        const double change = before == 0 ? 0 : (after - before) / std::abs(before);
        const double spread = noise(base_runs->second, head_runs->second);
        const double gain = metric.better == Better::Higher ? change : -change;
        const char* verdict = "";
        if (std::abs(change) > std::max(kMinChange, spread)) {
            verdict = gain > 0 ? "better" : "WORSE";
            worse_any = worse_any || gain < 0;
        }
        std::ostringstream change_text;
        change_text << std::showpos << std::fixed << std::setprecision(1) << change * kPercent << '%';
        std::ostringstream noise_text;
        noise_text << "±" << std::fixed << std::setprecision(1) << spread * kPercent << '%';
        std::cout << std::left << std::setw(kKeyWidth) << metric.key << std::right << std::setw(kValueWidth)
                  << formatValue(before) << std::setw(kValueWidth) << formatValue(after) << "  " << std::left
                  << std::setw(kUnitWidth) << metric.unit << std::right << std::setw(kChangeWidth) << change_text.str()
                  << std::setw(kNoiseWidth + 1) << noise_text.str() << "  " << verdict << '\n';
    }
    return worse_any ? 1 : 0;
}

int usage() {
    std::cerr << "usage: actorlib_benchmarks [--raw | --smoke | --compare BASE_FILE HEAD_FILE]\n";
    return 2;
}
}  // namespace

int main(int argc, char** argv) {
    try {
        const std::vector<std::string_view> args(std::next(argv), std::next(argv, argc));
        if (args.empty()) {
            printTable(runAll(kFull), kFull);
        } else if (args.size() == 1 && args[0] == "--raw") {
            printRaw(runAll(kFull));
        } else if (args.size() == 1 && args[0] == "--smoke") {
            printTable(runAll(kSmoke), kSmoke);
        } else if (args.size() == 3 && args[0] == "--compare") {
            return compare(std::string{args[1]}, std::string{args[2]});
        } else {
            return usage();
        }
    } catch (const std::exception& error) {
        std::cerr << "actorlib_benchmarks: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
