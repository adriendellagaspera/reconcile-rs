# Persistent-tree gate (#29)

This is the one-off architecture gate for the persistent `FingerprintTreeMap` / `ArcSwap` work in #36. The long-lived regression suite remains `benches/bench.rs` + `benches/system.rs`; this file pins the historical comparator and the COW-only measurements needed to decide #29.

## Baseline

The last commit before the persistent-tree implementation is:

```text
0dbc656e8f7e2ed4c978a5f46bb8364c01688304
```

Run the existing permanent benchmarks on both that commit and the candidate branch on the same machine, compiler, allocator and power settings. Do not compare absolute numbers copied from different machines.

```sh
# from a checkout of this repository
BASE=0dbc656e8f7e2ed4c978a5f46bb8364c01688304
ROOT=$(git rev-parse --show-toplevel)
git worktree add /tmp/reconcile-rs-pre-cow "$BASE"

# permanent lanes that exist on both sides
(
  cd /tmp/reconcile-rs-pre-cow
  RUSTFLAGS='--cfg reconcile_internal_testing' cargo bench --bench bench -- \
    'FingerprintTreeMap::(fill|insert|remove|aggregate)'
  RECONCILE_BENCH_SIZES=1000,10000,100000 cargo bench --bench system -- \
    'point_read|bulk_load|cold_sync'
)
(
  cd "$ROOT"
  RUSTFLAGS='--cfg reconcile_internal_testing' cargo bench --bench bench -- \
    'FingerprintTreeMap::(fill|insert|remove|aggregate)'
  RECONCILE_BENCH_SIZES=1000,10000,100000 cargo bench --bench system -- \
    'point_read|bulk_load|cold_sync'
)
```

Record candidate / pre-COW ratios for point reads, aggregate queries, fill/bulk load, single insert/remove and cold sync. #29 is a regression gate, not a claim that every row must improve: explain any material regression and decide whether the snapshot capability justifies it.

## COW-only lanes

The historical commit cannot measure retained snapshots because `FingerprintTreeMap::clone` was not the O(1) persistent-snapshot primitive. The COW-only harness therefore lives in the unpublished `devkit` crate rather than adding another permanent Criterion target or a packaged example.

Build it once, then run the same binary repeatedly for the decision run:

```sh
cargo build --release -p devkit --bin persistent_tree_gate
BIN=target/release/persistent_tree_gate

for n in 1000 10000 100000; do
  for trial in 1 2 3 4 5; do
    "$BIN" all --n "$n" --iters 10000
  done
done
```

It reports:

| lane | question |
|---|---|
| `snapshot_acquire` | Is an O(1) snapshot cheap enough to be a normal read primitive? |
| `point_read` | Does a direct read through the persistent tree preserve the expected point-read shape? |
| `iteration` | What does a full zero-copy scan cost? |
| `range_iteration` | What does a half-tree zero-copy range scan cost? |
| `mutation_retained` | What is one overwrite's write-path cost with `0 / 1 / 8 / 64` retained versions? |

`mutation_retained` keeps the exact current version alive when `retained > 0`, so the timed write must take the `Arc::make_mut` copy path. Historical-version construction and destruction are outside the timed interval; older retained versions add history pressure without contaminating the write timer.

For memory, measure the already-built process rather than `cargo run`, so Cargo/rustc RSS cannot contaminate the result:

```sh
for n in 1000 10000 100000; do
  for retained in 0 1 8 64; do
    /usr/bin/time -v "$BIN" rss-hold --n "$n" --retained "$retained"
  done
done
```

Use the `retained=0` row at each `n` as the process/tree baseline and report the incremental peak-RSS shape for `1 / 8 / 64`. Peak RSS is intentionally measured externally here: the architecture question is actual retained memory, including allocator effects, not just requested-byte accounting.

## Decision record

Record the decision in #29 with this compact table:

| axis | pre-COW | candidate | ratio / COW-only result | verdict |
|---|---:|---:|---:|---|
| point read | | | | |
| range aggregate | | | | |
| insert/remove | | | | |
| bulk load | | | | |
| cold sync | | | | |
| snapshot acquire | n/a | | | |
| mutation, 1 retained | n/a | | | |
| mutation, 8 retained | n/a | | | |
| mutation, 64 retained | n/a | | | |
| retained-version RSS | n/a | | | |

Close #29 only when the table is filled from one controlled same-machine run and the result explicitly says either **accept persistent tree** or **rework/revert before #36 closes**. #32 stays blocked until that decision exists.
