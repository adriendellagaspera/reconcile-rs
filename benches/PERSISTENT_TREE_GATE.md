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

Record ratios candidate / pre-COW for point reads, aggregate queries, fill/bulk load, single insert/remove and cold sync. #29 is a regression gate, not a claim that every row must improve: explain any material regression and decide whether the snapshot capability justifies it.

## COW-only lanes

The historical commit cannot measure retained snapshots because `FingerprintTreeMap::clone` was not the O(1) persistent snapshot primitive. Run the dedicated example on the candidate branch:

```sh
cargo run --release --example persistent_tree_bench -- --quick
# omit --quick for the decision run
cargo run --release --example persistent_tree_bench
```

It reports:

| lane | question |
|---|---|
| `persistent_tree/snapshot_acquire` | Is an O(1) snapshot cheap enough to be a normal read primitive? |
| `persistent_tree/point_read` | Does a direct read through the persistent tree preserve the expected point-read shape? |
| `persistent_tree/iteration` | What does a full zero-copy scan cost? |
| `persistent_tree/range_iteration` | What does a half-tree zero-copy range scan cost? |
| `persistent_tree/mutation_retained` | What is the write cost with `0 / 1 / 8 / 64` retained versions? |
| `[persistent_tree_memory]` | How much requested live heap do successive historical versions retain? |

`mutation_retained` keeps the exact current version alive when `retained > 0`, so the next mutation must take the `Arc::make_mut` copy path. Older retained versions then add realistic history pressure. The memory counter is deliberately the same kind of floor as `system::heap_footprint`: requested live heap only, not allocator rounding, fragmentation or RSS.

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
| retained-version heap | n/a | | | |

Close #29 only when the table is filled from one controlled run and the result explicitly says either **accept persistent tree** or **revert/rework before #36 closes**. #32 stays blocked until that decision exists.
