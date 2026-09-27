# Benchmarks

Benchmarks measure the code that ships in this repository. Results are not versioned documentation.

| target | scope |
|---|---|
| bench | FingerprintTreeMap microbenchmarks |
| system | end-to-end ReplicatedMap behavior |
| protocol | RBSR reconciliation cost |
| history_independent_catchup | RBSR catch-up cost vs superseded mutation history at fixed current state |
| runtime_history_independent_catchup | ReplicatedMap partition catch-up and causal-stability GC |
| state_repair_comparison | Prepared-state RBSR vs Rateless IBLT vs Merkle divergence discovery |
| riblt_preparation | RIBLT on-demand vs cached multi-peer preparation and rebuild cost |
| state_repair_mutations | RBSR vs RIBLT vs key-stable Merkle under arbitrary insert/delete/mixed divergence |
| cold_start_repair | Rebuild-from-current-state cost after reboot before repair |
| state_repair_sensitivity | Scale, fanout and symbol-size crossover mapping |
| contention | concurrent write cost |
| snapshot_write_amplification | snapshot write/restart cost |

Run a target with cargo bench --bench <target>. Criterion reports are written under target/criterion/.

Benchmark inputs are deterministic where practical. Compare timings on the same machine and build;
do not treat loopback measurements as WAN measurements or component controls as product comparisons.

The benchmark source files define their exact corpus, units, and environment variables. CI only
compile-checks benchmark targets.

The `history_independent_catchup` target defaults to `n=100000`, `d=100`, and
`h={0,1000,100000}`. Override those with `RECONCILE_HISTORY_N`, `RECONCILE_HISTORY_D`, and a
comma-separated `RECONCILE_HISTORY_OPS` respectively. History construction and the fixed
normalization pass happen before timing. The target first asserts that every history produces the
same two current states and the same exact protocol-cost trace as `h=0`; Criterion then times only
the subsequent reconciliation.

The `runtime_history_independent_catchup` report drives two authoritative `ReplicatedMap` peers
through a blocked in-memory transport. It holds their live states and fixed final deletions constant
while varying transient keys that are inserted and deleted during the partition, so only the raw
tombstone footprint grows. It counts the raw dated RBSR trace before healing, reports actual
catch-up traffic/time, and verifies causal-stability GC removes that historical footprint. Defaults
are `n=10000`, `d=100`, and `t={0,100,1000}`; override them with
`RECONCILE_RUNTIME_HISTORY_N`, `RECONCILE_RUNTIME_HISTORY_D`, and
`RECONCILE_RUNTIME_TOMBSTONES`. Larger tombstone sweeps are opt-in because causal-stability
acknowledgment drainage can dominate wall time.

The `state_repair_comparison` target compares three prepared-state repair strategies over the same
sorted `u64 -> u64 digest` manifest. It varies both divergence cardinality and placement while
keeping the key universe aligned: contiguous, 4/16 compact clusters, uniform random, evenly spaced,
and a deterministic `max-spread` stress profile. `max-spread` is not a claimed formal worst case.
Uniform-random runs use configurable deterministic seeds and report min/p50/mean/p90/max bytes.
The synchronized `d=0` control is included; RIBLT uses a 32-byte state-digest equality preflight
before its rateless stream. RBSR prices each enumerated `(key,digest)` symbol at 16 bytes, RIBLT
uses the external `do-riblt` crate with 24-byte coded symbols, and the Merkle baseline uses BLAKE3
with fanout 16. Application payload transfer is excluded, and setup time remains separate from
prepared-state repair time. Defaults are `n=100000`, `d={0,1,10,100,1000,10000}`, and three
uniform-random seeds. Override them with `RECONCILE_STATE_REPAIR_N`,
`RECONCILE_STATE_REPAIR_D`, `RECONCILE_STATE_REPAIR_PROFILES`,
`RECONCILE_STATE_REPAIR_RANDOM_SEEDS`, and `RECONCILE_STATE_REPAIR_SEED_BASE`.


The `riblt_preparation` target isolates the product-cost tradeoff behind the external
`do-riblt 1.0.2` encoder. It compares a fresh on-demand encoder per peer with one
`CachedEncoder` precomputed once and reused across several peers. It also reports full cache
rebuild cost after one update, delete, or insert because the published crate does not expose public
incremental add/remove operations. Cache memory is a structural upper-bound model based on the
public coded-symbol and cache-slot types; total encoder HashMap heap usage is intentionally reported
as opaque rather than guessed. Defaults are `n=100000`, `d=1000`, and
`peers={1,2,4,8}`; override them with `RECONCILE_RIBLT_N`, `RECONCILE_RIBLT_D`,
`RECONCILE_RIBLT_PEERS`, and `RECONCILE_RIBLT_SEED`.


The `state_repair_mutations` target removes the aligned-key assumption from the first state-repair
comparison. Baseline keys are even `u64` values, leaving odd keys for true insertions between
existing rows. It compares random updates/deletes, interleaved inserts, outside-range inserts,
balanced insert/delete, and a mixed case where both replicas mutate autonomously. The Merkle
baseline is a fixed 16-way sparse radix tree over the `u64` key bits, so insertion/deletion affects
only the key's root-to-leaf path instead of shifting positional leaves. Payload transfer remains
excluded. Defaults are `n=100000` and `d={100,1000,10000}`; override them with
`RECONCILE_MUTATION_N`, `RECONCILE_MUTATION_D`, and `RECONCILE_MUTATION_SEED`.

The `cold_start_repair` target prices a reboot with all reconciliation acceleration state
discarded. Phase A starts from canonical rows already resident in memory and reports warm,
one-side-cold and both-side-cold reconstruction. Phase B persists the same current state through
the repository's `FileSnapshot` format, then reports snapshot bytes, load/deserialization,
live-row projection, time-to-ready after rebuilding RBSR/Merkle state, and total repair time.
RIBLT reports both on-demand reconstruction and rebuilding/precomputing a `CachedEncoder`.
Snapshot creation is outside the restart window and filesystem page-cache state is uncontrolled.
The benchmark covers `d=0`, outside-range inserts, and mixed autonomous divergence. Defaults are
`n=100000` and `d={1000,10000}`; override them with `RECONCILE_COLD_N`,
`RECONCILE_COLD_D`, and `RECONCILE_COLD_SEED`.


The `state_repair_sensitivity` target maps the two boundaries exposed by the arbitrary-mutation
benchmark without running a full Cartesian product. `scale` mode varies `n` and `d/n` at
fanout 16 with 16-byte symbols; `fanout` mode sweeps RBSR and key-stable radix Merkle fanout while
measuring RIBLT once per corpus; `symbol` mode repeats representative cases with 16/32/64-byte
wire symbols. Workloads are random updates and outside-range inserts. The radix Merkle fanout is
implemented as a key-stable base-`fanout` hierarchy, so insertions do not shift positional leaves.
Configure with `RECONCILE_SENSITIVITY_MODE`, `RECONCILE_SENSITIVITY_N`,
`RECONCILE_SENSITIVITY_D`, `RECONCILE_SENSITIVITY_FANOUTS`,
`RECONCILE_SENSITIVITY_SYMBOLS`, and `RECONCILE_SENSITIVITY_SEED`.

## Shared experiment artifacts

`devkit::corpus` owns deterministic benchmark inputs; `devkit::experiment` owns schema-v2 JSON
artifacts and rendering. Set `RECONCILE_BENCH_OUTPUT`, `RECONCILE_BENCH_REVISION`, and
`RECONCILE_BENCH_RESOURCE_PROFILE` to retain reproducible run artifacts; existing files are never
overwritten. Artifacts keep observed timings distinct from projected byte/cold-total models, and
leave CPU, peak temporary memory, and transport end-to-end time explicitly unmeasured. Preparation
charges cache build once per multi-peer experiment; cache-memory diagnostics remain human-only.
