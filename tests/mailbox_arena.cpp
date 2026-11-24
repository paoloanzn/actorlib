#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <actorlib/actorlib.hpp>

using actorlib::ActorRef;
using actorlib::ArenaConfig;
using actorlib::MailboxArena;
using actorlib::Message;
using actorlib::SendResult;

namespace {
struct Ping {
    std::uint32_t seq;
};

struct Login {
    std::uint32_t user_id;
    std::array<char, 32> name;  // fixed-size field: the bytes live in the arena
};

Login makeLogin(std::uint32_t user_id, std::string_view name) {
    Login login{.user_id = user_id, .name = {}};
    std::copy_n(name.begin(), std::min(name.size(), login.name.size() - 1), login.name.begin());
    return login;
}

struct alignas(32) Wide {
    std::uint64_t value;
};

struct Huge {
    std::array<std::byte, 512> payload;
};

// 64-byte chunks hold (64 - 8) / 4 = 14 Pings. 2 chunks per slab, 4 chunks at most.
constexpr ArenaConfig kSmall{.chunk_size = 64, .initial_bytes = 128, .max_bytes = 256};
constexpr std::uint32_t kPingsPerChunk = 14;

// 0, 1, ..., n - 1
std::vector<std::uint32_t> sequence(std::uint32_t n) {
    std::vector<std::uint32_t> out;
    for (std::uint32_t i = 0; i < n; ++i) out.push_back(i);
    return out;
}
}  // namespace

static_assert(Message<Ping>);
static_assert(Message<Login>);
static_assert(!Message<std::string>);
static_assert(!Message<std::vector<int>>);
static_assert(!Message<const Ping>);
static_assert(!Message<int[4]>);  // NOLINT(*-avoid-c-arrays): rejecting C arrays is the point

TEST_CASE("messages are delivered in FIFO order across chunk boundaries") {
    MailboxArena arena{kSmall};
    auto ref = arena.create<Ping>();
    CHECK(arena.chunksReserved() == 2);

    for (std::uint32_t i = 0; i < 40; ++i) REQUIRE(arena.send(ref, Ping{i}) == SendResult::Ok);
    CHECK(arena.pending(ref) == 40);
    CHECK(arena.chunksInUse() == 3);     // ceil(40 / 14)
    CHECK(arena.chunksReserved() == 4);  // grew by one slab

    std::vector<std::uint32_t> seen;
    CHECK(arena.drain(ref, 100, [&](Ping& p) { seen.push_back(p.seq); }) == 40);
    CHECK(seen == sequence(40));
    CHECK(arena.pending(ref) == 0);
    CHECK(arena.chunksInUse() == 1);  // the emptied mailbox keeps its last chunk parked
}

TEST_CASE("drain handles at most budget messages") {
    MailboxArena arena{kSmall};
    auto ref = arena.create<Ping>();
    for (std::uint32_t i = 0; i < 10; ++i) REQUIRE(arena.send(ref, Ping{i}) == SendResult::Ok);

    std::vector<std::uint32_t> seen;
    auto record = [&](Ping& p) { seen.push_back(p.seq); };
    CHECK(arena.drain(ref, 3, record) == 3);
    CHECK(seen == sequence(3));
    CHECK(arena.pending(ref) == 7);

    CHECK(arena.drain(ref, 100, record) == 7);
    CHECK(seen == sequence(10));
}

TEST_CASE("send returns Full once max_bytes is reached, and succeeds again after a drain") {
    MailboxArena arena{kSmall};
    auto ref = arena.create<Ping>();
    const std::uint32_t capacity = 4 * kPingsPerChunk;

    for (std::uint32_t i = 0; i < capacity; ++i) REQUIRE(arena.send(ref, Ping{i}) == SendResult::Ok);
    CHECK(arena.send(ref, Ping{capacity}) == SendResult::Full);
    CHECK(arena.pending(ref) == capacity);
    CHECK(arena.chunksReserved() == 4);

    CHECK(arena.drain(ref, kPingsPerChunk, [](Ping&) {}) == kPingsPerChunk);
    CHECK(arena.chunksInUse() == 3);
    CHECK(arena.send(ref, Ping{capacity}) == SendResult::Ok);
}

TEST_CASE("mailboxes of different message types share the arena independently") {
    MailboxArena arena;
    auto pings = arena.create<Ping>();
    auto logins = arena.create<Login>();

    for (std::uint32_t i = 0; i < 20; ++i) {
        REQUIRE(arena.send(pings, i) == SendResult::Ok);  // constructed in place: Ping(i)
        REQUIRE(arena.send(logins, makeLogin(i, "alice")) == SendResult::Ok);
    }

    std::vector<std::uint32_t> seq;
    CHECK(arena.drain(pings, 100, [&](Ping& p) { seq.push_back(p.seq); }) == 20);
    CHECK(seq == sequence(20));

    std::vector<std::uint32_t> ids;
    CHECK(arena.drain(logins, 100, [&](Login& l) {
        ids.push_back(l.user_id);
        CHECK(std::string_view{l.name.data()} == "alice");
    }) == 20);
    CHECK(ids == sequence(20));
    CHECK(arena.chunksInUse() == 2);  // one parked chunk per emptied mailbox
}

TEST_CASE("refs to a destroyed mailbox stay dead, even after its slot is reused") {
    MailboxArena arena{kSmall};
    auto old_ref = arena.create<Ping>();
    for (std::uint32_t i = 0; i < 3; ++i) REQUIRE(arena.send(old_ref, Ping{i}) == SendResult::Ok);
    CHECK(arena.chunksInUse() == 1);

    CHECK(arena.destroy(old_ref));
    CHECK_FALSE(arena.isAlive(old_ref));
    CHECK(arena.chunksInUse() == 0);
    CHECK(arena.pending(old_ref) == 0);
    CHECK(arena.send(old_ref, Ping{9}) == SendResult::DeadActor);
    CHECK(arena.drain(old_ref, 10, [](Ping&) { FAIL("dead mailbox delivered a message"); }) == 0);
    CHECK_FALSE(arena.destroy(old_ref));

    auto new_ref = arena.create<Ping>();  // reuses the freed slot with a new generation
    CHECK(new_ref != old_ref);
    CHECK(arena.isAlive(new_ref));
    CHECK(arena.send(old_ref, Ping{9}) == SendResult::DeadActor);
    CHECK(arena.send(new_ref, Ping{9}) == SendResult::Ok);

    const ActorRef<Ping> null_ref;
    CHECK_FALSE(arena.isAlive(null_ref));
    CHECK(arena.send(null_ref, Ping{0}) == SendResult::DeadActor);
}

TEST_CASE("a handler can send to its own mailbox while it is drained") {
    MailboxArena arena{kSmall};
    auto ref = arena.create<Ping>();
    REQUIRE(arena.send(ref, Ping{40}) == SendResult::Ok);

    // Each handled message enqueues the next one, crossing several chunk boundaries while a
    // reference to the current message is live.
    std::vector<std::uint32_t> seen;
    const auto handled = arena.drain(ref, 1000, [&](Ping& p) {
        seen.push_back(p.seq);
        if (p.seq > 0) REQUIRE(arena.send(ref, Ping{p.seq - 1}) == SendResult::Ok);
    });
    CHECK(handled == 41);
    CHECK(seen.front() == 40);
    CHECK(seen.back() == 0);
    CHECK(arena.chunksInUse() == 1);
}

TEST_CASE("a handler can create mailboxes while a mailbox is drained") {
    MailboxArena arena;
    auto ref = arena.create<Ping>();
    for (std::uint32_t i = 0; i < 4; ++i) REQUIRE(arena.send(ref, Ping{i}) == SendResult::Ok);

    // Creating mailboxes grows the slot table under drain's feet.
    std::vector<ActorRef<Ping>> created;
    std::vector<std::uint32_t> seen;
    const auto handled = arena.drain(ref, 10, [&](Ping& p) {
        for (int i = 0; i < 256; ++i) created.push_back(arena.create<Ping>());
        seen.push_back(p.seq);
    });
    CHECK(handled == 4);
    CHECK(seen == sequence(4));
    CHECK(arena.isAlive(created.back()));
}

TEST_CASE("a handler that destroys its own mailbox stops the drain") {
    MailboxArena arena{kSmall};
    auto ref = arena.create<Ping>();
    for (std::uint32_t i = 0; i < 5; ++i) REQUIRE(arena.send(ref, Ping{i}) == SendResult::Ok);

    CHECK(arena.drain(ref, 10, [&](Ping&) { arena.destroy(ref); }) == 1);
    CHECK_FALSE(arena.isAlive(ref));
    CHECK(arena.chunksInUse() == 0);
}

TEST_CASE("message slots honor the message's alignment") {
    MailboxArena arena;
    auto ref = arena.create<Wide>();
    for (std::uint64_t i = 0; i < 300; ++i) REQUIRE(arena.send(ref, Wide{i}) == SendResult::Ok);
    CHECK(arena.chunksInUse() == 11);  // (960 - 32) / 32 = 29 per chunk

    std::uint64_t expected = 0;
    const auto handled = arena.drain(ref, 1000, [&](Wide& w) {
        // Reading an address as an integer is the point here. (std::bit_cast would avoid the
        // cast but crashes clang-tidy 18's static analyzer.)
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        CHECK(reinterpret_cast<std::uintptr_t>(&w) % alignof(Wide) == 0);
        CHECK(w.value == expected);
        ++expected;
    });
    CHECK(handled == 300);
}

TEST_CASE("steady send/drain cycles reuse chunks without growing the arena") {
    MailboxArena arena{kSmall};
    auto a = arena.create<Ping>();
    auto b = arena.create<Ping>();
    for (std::uint32_t i = 0; i < 10'000; ++i) {
        REQUIRE(arena.send(a, Ping{i}) == SendResult::Ok);
        REQUIRE(arena.send(b, Ping{i}) == SendResult::Ok);
        REQUIRE(arena.drain(a, 1, [](Ping&) {}) == 1);
        REQUIRE(arena.drain(b, 1, [](Ping&) {}) == 1);
    }
    CHECK(arena.chunksReserved() == 2);
    CHECK(arena.chunksInUse() == 2);  // each mailbox refills its parked chunk
}

TEST_CASE("an emptied mailbox parks its chunk and refills it without taking another") {
    MailboxArena arena{kSmall};
    auto ref = arena.create<Ping>();
    for (std::uint32_t round = 0; round < 3; ++round) {
        for (std::uint32_t i = 0; i < kPingsPerChunk; ++i) REQUIRE(arena.send(ref, Ping{i}) == SendResult::Ok);
        CHECK(arena.chunksInUse() == 1);  // the parked chunk is rewound, so all 14 fit again
        CHECK(arena.drain(ref, 100, [](Ping&) {}) == kPingsPerChunk);
        CHECK(arena.pending(ref) == 0);
        CHECK(arena.chunksInUse() == 1);
    }
}

TEST_CASE("parked chunks are taken back before the arena grows or reports Full") {
    MailboxArena arena{kSmall};  // 2 chunks to start, 4 at most
    std::vector<ActorRef<Ping>> refs;
    for (int i = 0; i < 4; ++i) {
        refs.push_back(arena.create<Ping>());
        REQUIRE(arena.send(refs.back(), Ping{0}) == SendResult::Ok);  // 4 busy mailboxes: grows to the cap
    }
    for (const auto& ref : refs) REQUIRE(arena.drain(ref, 1, [](Ping&) {}) == 1);
    CHECK(arena.chunksInUse() == 4);  // all four parked
    CHECK(arena.chunksReserved() == 4);

    auto late = arena.create<Ping>();
    CHECK(arena.send(late, Ping{1}) == SendResult::Ok);  // no free chunk: the parked ones are reclaimed
    CHECK(arena.chunksInUse() == 1);
    CHECK(arena.chunksReserved() == 4);

    // The reclaimed mailboxes are empty and take chunks again, up to the cap.
    for (std::size_t i = 0; i < 3; ++i) {
        CHECK(arena.pending(refs[i]) == 0);
        CHECK(arena.send(refs[i], Ping{2}) == SendResult::Ok);
    }
    CHECK(arena.send(refs[3], Ping{2}) == SendResult::Full);
}

TEST_CASE("a mailbox that empties does not make the arena grow by parking") {
    MailboxArena arena{kSmall};
    auto a = arena.create<Ping>();
    auto b = arena.create<Ping>();
    auto c = arena.create<Ping>();
    for (const auto& ref : {a, b}) {
        REQUIRE(arena.send(ref, Ping{0}) == SendResult::Ok);
        REQUIRE(arena.drain(ref, 1, [](Ping&) {}) == 1);
    }
    CHECK(arena.chunksInUse() == 2);                  // both initial chunks parked
    CHECK(arena.send(c, Ping{0}) == SendResult::Ok);  // reclaims instead of growing
    CHECK(arena.chunksReserved() == 2);
}

TEST_CASE("a mailbox whose parking spot holds another parked mailbox gives its chunk back") {
    MailboxArena arena{kSmall};
    std::vector<ActorRef<Ping>> refs;
    for (std::uint32_t i = 0; i <= MailboxArena::kParkingSpots; ++i) refs.push_back(arena.create<Ping>());
    const auto first = refs.front();  // index 0 and index kParkingSpots share spot 0
    const auto clash = refs.back();
    auto cycle = [&](ActorRef<Ping> ref) {
        REQUIRE(arena.send(ref, Ping{0}) == SendResult::Ok);
        REQUIRE(arena.drain(ref, 1, [](Ping&) {}) == 1);
    };

    cycle(first);
    CHECK(arena.chunksInUse() == 1);  // first parked
    cycle(clash);
    CHECK(arena.chunksInUse() == 1);  // first keeps the spot; clash released its chunk

    REQUIRE(arena.send(first, Ping{1}) == SendResult::Ok);  // first is busy again...
    cycle(clash);                                           // ...so clash takes the spot
    CHECK(arena.chunksInUse() == 2);
}

TEST_CASE("destroying a parked mailbox returns its chunk") {
    MailboxArena arena{kSmall};
    auto ref = arena.create<Ping>();
    REQUIRE(arena.send(ref, Ping{0}) == SendResult::Ok);
    REQUIRE(arena.drain(ref, 1, [](Ping&) {}) == 1);
    CHECK(arena.chunksInUse() == 1);
    CHECK(arena.destroy(ref));
    CHECK(arena.chunksInUse() == 0);

    auto reused = arena.create<Ping>();  // same slot and parking spot, new generation
    REQUIRE(arena.send(reused, Ping{1}) == SendResult::Ok);
    REQUIRE(arena.drain(reused, 1, [](Ping&) {}) == 1);
    CHECK(arena.chunksInUse() == 1);
}

TEST_CASE("invalid configurations and oversized messages are rejected") {
    CHECK_THROWS_AS(MailboxArena(ArenaConfig{.chunk_size = 0}), std::invalid_argument);
    CHECK_THROWS_AS(MailboxArena(ArenaConfig{.chunk_size = 100}), std::invalid_argument);
    CHECK_THROWS_AS(MailboxArena(ArenaConfig{.chunk_size = 4096, .initial_bytes = 1024}), std::invalid_argument);
    CHECK_THROWS_AS(MailboxArena(ArenaConfig{.chunk_size = 64, .initial_bytes = 256, .max_bytes = 128}),
                    std::invalid_argument);

    MailboxArena arena{kSmall};
    CHECK_THROWS_AS(static_cast<void>(arena.create<Huge>()), std::length_error);
}
