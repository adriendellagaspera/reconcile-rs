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
