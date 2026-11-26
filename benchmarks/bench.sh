#!/bin/sh
# A/B benchmarks: is the working tree faster or slower than BASE_REF on this machine?
#
#   benchmarks/bench.sh                 main vs working tree, 5 rounds
#   benchmarks/bench.sh v0.2.0 9        v0.2.0 vs working tree, 9 rounds
#
# Builds the library twice, at BASE_REF and from the working tree (uncommitted changes
# included). Both sides get the working tree's benchmarks/, so they run the same scenarios
# and only the library differs. Runs the two in turn, ROUNDS times each (about 8 s per run),
# and prints each metric's median, its change, and the noise between rounds. Taking turns
# spreads the machine's drift over both sides.
#
# Exits 1 if a metric got worse by more than the noise (and at least 3%). Close other busy
# programs first: the scenarios use every core.
set -eu

BASE_REF=${1:-main}
ROUNDS=${2:-5}

root=$(git rev-parse --show-toplevel)
out="$root/build/bench"
base_sha=$(git -C "$root" rev-parse --short "$BASE_REF^{commit}")

build() {  # source dir, build dir
    cmake -S "$1" -B "$2" -DCMAKE_BUILD_TYPE=Release -DACTORLIB_BUILD_BENCHMARKS=ON -DBUILD_TESTING=OFF >/dev/null
    cmake --build "$2" --target actorlib_benchmarks --parallel >/dev/null
}

echo "building the working tree"
build "$root" "$out/head"

echo "building $BASE_REF ($base_sha)"
rm -rf "$out/base-src"
mkdir -p "$out/base-src"
# -m: extracted files get the current time, so the build never reuses another ref's objects.
git -C "$root" archive "$BASE_REF" | tar -x -m -C "$out/base-src"
rm -rf "$out/base-src/benchmarks"
cp -R "$root/benchmarks" "$out/base-src/benchmarks"
build "$out/base-src" "$out/base"

base_bin="$out/base/benchmarks/actorlib_benchmarks"
head_bin="$out/head/benchmarks/actorlib_benchmarks"
: >"$out/base.txt"
: >"$out/head.txt"
round=1
while [ "$round" -le "$ROUNDS" ]; do
    echo "round $round of $ROUNDS"
    # Alternate which side goes first, so neither always runs on a warmer machine.
    if [ $((round % 2)) -eq 1 ]; then
        "$base_bin" --raw >>"$out/base.txt"
        "$head_bin" --raw >>"$out/head.txt"
    else
        "$head_bin" --raw >>"$out/head.txt"
        "$base_bin" --raw >>"$out/base.txt"
    fi
    round=$((round + 1))
done

echo
echo "base: $BASE_REF ($base_sha)    head: working tree    ($(nproc 2>/dev/null || echo '?') hardware threads)"
"$head_bin" --compare "$out/base.txt" "$out/head.txt"
