# Benchmarks

Benchmarks measure the code that ships in this repository. Results are not versioned documentation.

| target | scope |
|---|---|
| bench | FingerprintTreeMap microbenchmarks |
| system | end-to-end ReplicatedMap behavior |
| protocol | RBSR reconciliation cost |
| history_independent_catchup | RBSR catch-up cost vs superseded mutation history at fixed current state |
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
