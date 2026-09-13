# Persistent-tree gate (#29)

This is the one-off architecture gate for the persistent `FingerprintTreeMap` / `ArcSwap` work in #36. The long-lived regression suite remains `benches/bench.rs` + `benches/system.rs`; this file pins the historical comparator and the COW-only measurements needed to decide #29. The issue body owns the measured decision and current verdict.

## Baseline

The last commit before the persistent-tree implementation is:

```text
0dbc656e8f7e2ed4c978a5f46bb8364c01688304
```

Run baseline and candidate on the same machine, compiler, allocator and power settings. Do not compare absolute numbers copied from different machines.

For the `FingerprintTreeMap` regression lanes, use the same small harness source against both revisions. Do **not** use a regex-filtered `cargo bench --bench bench` for this gate: that target has a custom `main`, and a 2026-09-14 gate run reproduced unrelated groups executing after the requested FTM filter (`service_reconcile_rtt` eventually failed while the FTM measurements themselves had already completed).

```sh
BASE=0dbc656e8f7e2ed4c978a5f46bb8364c01688304
ROOT=$(git rev-parse --show-toplevel)
PRE=/tmp/reconcile-rs-pre-cow

git worktree add "$PRE" "$BASE"
mkdir -p "$PRE/devkit/src/bin"
cp devkit/src/bin/persistent_tree_regression.rs \
  "$PRE/devkit/src/bin/persistent_tree_regression.rs"

cargo build --release -p devkit --bin persistent_tree_regression
(
  cd "$PRE"
  cargo build --release -p devkit --bin persistent_tree_regression
)

CANDIDATE="$ROOT/target/release/persistent_tree_regression"
BASELINE="$PRE/target/release/persistent_tree_regression"

for n in 1000 10000 100000; do
  for trial in 1 2 3; do
    echo "revision=baseline,n=$n,trial=$trial"
    "$BASELINE" --n "$n" --iters 100000
    echo "revision=candidate,n=$n,trial=$trial"
    "$CANDIDATE" --n "$n" --iters 100000
  done
done
```

The harness reports fill, direct point-read, range aggregate, overwrite, and insert+remove costs. Use the median of the repeated same-runner samples.

The public `ReplicatedMap` lanes remain in the normal `system` Criterion target, whose filtering works as expected:

```sh
(
  cd "$PRE"
  RECONCILE_BENCH_SIZES=1000,10000,100000 cargo bench --bench system -- \
    'point_read|bulk_load|cold_sync' --quick --noplot
)
(
  cd "$ROOT"
  RECONCILE_BENCH_SIZES=1000,10000,100000 cargo bench --bench system -- \
    'point_read|bulk_load|cold_sync' --quick --noplot
)
```

Omit `--quick` for a publication-quality run. For this engineering gate it is useful for the system lanes only after the repeated common harness has isolated the tree-level change.

## COW-only lanes

The historical commit cannot measure retained snapshots because `FingerprintTreeMap::clone` was not the O(1) persistent-snapshot primitive. The COW-only harness therefore lives in the unpublished `devkit` crate rather than adding another permanent Criterion target or a packaged example.

Build it once, then run the same binary repeatedly:

```sh
cargo build --release -p devkit --bin persistent_tree_gate
BIN=target/release/persistent_tree_gate

for n in 1000 10000 100000; do
  for trial in 1 2 3; do
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

For a more stable retained-write comparison, run each retained count separately with a larger iteration count:

```sh
for retained in 0 1 8 64; do
  for trial in 1 2 3; do
    "$BIN" mutation --n 100000 --retained "$retained" --iters 5000
  done
done
```

For memory, measure the already-built process rather than `cargo run`, so Cargo/rustc RSS cannot contaminate the result. Use a large enough tree that retained-version deltas have a chance to rise above process/runner noise:

```sh
for retained in 0 1 8 64; do
  for trial in 1 2 3; do
    /usr/bin/time -v "$BIN" rss-hold --n 1000000 --retained "$retained"
  done
done
```

Use `retained=0` as the process/tree baseline and report the incremental peak-RSS shape for `1 / 8 / 64`. Peak RSS is intentionally measured externally here: the architecture question is actual retained memory, including allocator effects, not just requested-byte accounting. If the deltas stay below run-to-run RSS noise, report them as unmeasurable rather than as negative memory.

## Decision record

Record the decision in #29 rather than duplicating a dated result here. At minimum cover:

| axis | pre-COW | candidate | ratio / COW-only result | verdict |
|---|---:|---:|---:|---|
| direct tree point read | | | | |
| range aggregate | | | | |
| insert/remove | | | | |
| public `ReplicatedMap::get` | | | | |
| bulk load | | | | |
| cold sync | | | | |
| snapshot acquire | n/a | | | |
| mutation, 1 retained | n/a | | | |
| mutation, 8 retained | n/a | | | |
| mutation, 64 retained | n/a | | | |
| retained-version RSS | n/a | | | |

Close #29 only when the measured result has an explicit architecture disposition and any material point-read regression has either been removed or consciously accepted as part of the API contract. #32 stays blocked until then.
