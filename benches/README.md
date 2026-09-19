# Benchmarks

The benchmark suite answers engineering questions about the code that ships in this repository.
It is not a research notebook or a cross-project leaderboard. Historical campaigns and the
decisions they produced live in their issues/PRs; this file documents how to reproduce and
interpret the current harness.

None of the benchmark targets execute in CI. CI only compile-checks them with
`cargo bench --no-run`.

## Targets

| target | scope |
|---|---|
| `bench` | `FingerprintTreeMap` micro-benchmarks and focused internal reconciliation timing |
| `system` | end-to-end behavior through the public `ReplicatedMap` API |
| `protocol` | one complete RBSR reconciliation under the shipped default `FixedFanOut` policy |
| `contention` | concurrent-writer cost of maintaining RSOS aggregates vs a `BTreeMap` control |
| `snapshot_write_amplification` | #46: full-snapshot logical bytes, elapsed time and verified restart after changing d of n entries |

The source-level module docs in each benchmark file define the exact corpus and measurement unit.
When a detail here and the harness disagree, the harness is authoritative.

## Running

Run one target:

```sh
cargo bench --bench bench
cargo bench --bench system
cargo bench --bench protocol
cargo bench --bench contention
```

Criterion arguments filter benchmark ids:

```sh
cargo bench --bench system -- point_read
cargo bench --bench system -- 'cold_sync/1000'
cargo bench --bench system -- 'gossip_fanout/64'
cargo bench --bench system -- --quick
```

Criterion writes reports under `target/criterion/`.

Some internal probes intentionally use the repository-only test cfg:

```sh
RUSTFLAGS='--cfg reconcile_internal_testing' cargo bench --bench bench -- bulk_load_just_insert
RUSTFLAGS='--cfg reconcile_internal_testing' cargo bench --bench contention -- 'no_such_benchmark'
```

The latter form runs the contention target's printed/counted report while filtering out timed
Criterion groups.

## Reproducibility

Benchmark corpora use deterministic seeds where randomness matters. That makes one harness run
repeatable; it does **not** make wall-clock measurements portable across machines.

Use these rules when comparing results:

- compare ratios/shapes on the same machine and build before comparing absolute nanoseconds;
- do not treat a `--quick` point as having the same statistical confidence as a full Criterion run;
- for concurrent benchmarks, keep writer count relative to the physical core count explicit;
- do not infer network behavior from loopback-only timing when an injected-RTT/loss lane exists;
- distinguish counted quantities (messages, ranges, aggregate writes) from wall-clock timing.

For larger structure sweeps, `system` accepts a comma-separated size override:

```sh
RECONCILE_BENCH_SIZES=10,100,1000,10000,100000,1000000 \
  cargo bench --bench system -- point_read
```

## `system`: public-API behavior

The system target covers the operational questions a caller sees.

| group | question |
|---|---|
| `point_read` / `point_read_heap` | point-read latency as the map grows |
| `bulk_load` / `bulk_load_heap` | bulk insertion throughput |
| `memory_footprint` / `heap_footprint` | dated-value and real heap cost per entry |
| `cold_sync` | empty-node convergence from a populated peer |
| `gossip_fanout` | origin-side datagrams/bytes as peer count grows |
| `gossip_propagation` | time until every peer observes a write |
| `broadcast_coalescing` | eager-write batching trade-off |
| injected RTT/loss groups | how propagation/reconciliation changes away from loopback |
| `durable_rejoin` | snapshot-assisted restart vs cold rejoin |

### Current baseline shape

The important current findings are shapes, not portable absolute timings:

- `FingerprintTreeMap` point reads remain logarithmic; the gap to `HashMap` widens with `n`.
- a cold sync is dominated by transferring the dataset/difference, while a snapshot-assisted
  rejoin transfers only the post-snapshot delta;
- injected RTT adds approximately one RTT to a multi-round anti-entropy convergence and about half
  an RTT to a one-hop eager propagation path;
- origin-side eager broadcast fan-out grows with the number of contacted peers.

Re-run the target on the machine/configuration being evaluated before using an absolute number for
capacity planning.

## `protocol`: RBSR cost

`protocol` drives `rsos` + `rbsr` directly, without sockets or the facade. It measures the
shipped default refinement policy (`FixedFanOut`, `b = 16`) across:

- store size `n`;
- symmetric-difference size `d`;
- scattered vs clustered differences;
- stored-value sizes.

The primary byte quantity is total payload bytes: refinement traffic plus enumerated values.
Messages/ranges/datagrams and RSOS-query counts remain separate because byte totals do not price a
round trip or local CPU work.

One protocol drive is reused across value sizes only because the harness verifies that payload size
does not change refinement decisions; only the encoded cost of an enumerated value changes.

This target does **not** compare alternative refinement policies or external reconciliation
implementations. Comparative algorithm research belongs outside this engineering benchmark.

## `contention`: write-side RSOS cost

The RSOS contract buys cheap range summaries by maintaining a cached aggregate on every node of the
root path. `contention` measures the write cost of that requirement against a plain `BTreeMap`
behind the same `parking_lot::RwLock`.

It has two halves:

1. a deterministic counter reports cached-aggregate writes per insert/overwrite;
2. a paired timed sweep measures throughput as writer count rises.

The control uses the same lock/acquisition pattern so the comparison isolates as much of the data
structure's additional critical-section work as practical.

### Interpreting the timed result

Do not use the raw ratio alone. Both arms pay lock/scheduler costs that change with writer count.
The useful paired statistic is the difference in reciprocal throughput:

```text
delta = 1 / X_fingerprint - 1 / X_btree
```

At one writer there is no contention, so `delta(1)` is the cleanest estimate of the extra
critical-section cost. Above one writer it is an upper bound if the longer RSOS critical section
also changes parking behavior.

On machines where writer count exceeds physical cores, preemption while holding the lock becomes a
separate confound. Conclusions about many-core scaling therefore require points with `N <= cores`.

For publishable/reviewable contention numbers, pool independent invocations rather than treating
trials inside one process as independent machine phases. `CONTENTION_RAW=1` emits per-trial data
for that purpose.

## #46 / #47 pre-change baselines

These probes describe the current post-#92 structure and the current whole-file
persistence implementation; they do not predict the improvement of either change.
Compare identical sizes, machine, compiler and commit configuration before/after.

### #47: Rust node layout, occupancy, requested heap bytes

The deliberately ignored unit probe inspects private `Node` fields without widening
the public API:

```sh
RECONCILE_BASELINE_SIZES=10000,100000 cargo test -p rsos --lib node_occupancy -- --ignored --nocapture
RECONCILE_BENCH_SIZES=10000,100000 cargo bench --bench system -- heap_footprint
```

`node_occupancy` reports exact Rust type layouts, node counts, occupancy and
inline fingerprint reservation for serial insertion and `from_sorted_iter`.
Reservation is not an RSS saving: alignment and allocator rounding matter.
`heap_footprint` measures *requested live heap* through a counting allocator
for both `u32/u32` and `String/Vec<u8>`; it excludes allocator bookkeeping,
fragmentation, and process RSS. Add `1000000` only as a manual opt-in.

### #46: full-snapshot rewrite vs n and d

```sh
RECONCILE_BASELINE_SIZES=10000,100000 \
RECONCILE_BASELINE_DELTAS=0,1,100,1000 \
RECONCILE_BASELINE_TRIALS=3 \
  cargo bench --bench snapshot_write_amplification
```

The standalone target prints CSV: initial/rewrite elapsed time, snapshot-file
bytes, rewrite/reference ratio and verified restart duration. Each trial
uses a new directory, seeds n keys with 64-byte values, writes the reference,
changes d distinct keys, explicitly calls `snapshot_now()`, then loads the
result in a fresh map and asserts identical fingerprint, count and values.
The d=0 case measures *explicit* `snapshot_now()`; the periodic idle threshold
is a separate behavior. File length counts the logical encoded bytes written
(not physical device I/O or allocator overhead). Elapsed time includes
collection, serialization, file sync and rename. Report actual measurements
instead of treating the expected approximately-1 rewrite ratio as a result.

Neither probe executes in CI: the standalone target is compile-checked by
the repository's existing `cargo bench --no-run` gate.
## What the suite does not claim

- Absolute timings are not portable across hardware.
- Loopback numbers are not WAN numbers.
- A benchmark against `BTreeMap` or `HashMap` is a component/control comparison, not a claim
  that `reconcile-rs` replaces those structures.
- The harness does not establish that RBSR beats Negentropy, RIBLT, AELMDB or another external
  implementation unless that implementation is actually run under a comparable workload.
- The repository does not run benchmarks in CI; benchmark regressions require an intentional local
  measurement campaign.

## Adding or changing a benchmark

A benchmark belongs here when it answers a question about code this repository ships. Keep research
parameter searches and alternative unshipped policies in the research repository instead.

When adding a measurement:

1. state the unit and experimental question in the benchmark file's module docs;
2. make corpora deterministic where possible;
3. keep counted and timed quantities distinct;
4. document hardware-sensitive caveats next to the interpretation;
5. do not add the timed benchmark to CI — compile-checking remains the CI contract.
