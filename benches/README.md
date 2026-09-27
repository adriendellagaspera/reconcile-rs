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
sorted `u64 -> u64 digest` manifest. Both replicas contain the same keys and exactly `d` values
differ, which deliberately gives the Merkle baseline an aligned-key workload. It reports discovery
metadata only; application payload transfer is excluded. RBSR uses the repository's counted
protocol driver and prices each enumerated `(key,digest)` symbol at 16 bytes. Rateless IBLT uses
the external `do-riblt` crate and reports its fixed 24-byte coded-symbol payload plus coded-symbol count. The Merkle
baseline uses BLAKE3, fanout 16, and reports hash/request/symbol bytes. Each strategy also reports bootstrap/preparation time separately from prepared-state repair time. Defaults are `n=100000` and
`d={1,10,100,1000,10000}`; override them with `RECONCILE_STATE_REPAIR_N` and
`RECONCILE_STATE_REPAIR_D`.
