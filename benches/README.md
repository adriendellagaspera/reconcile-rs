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
| transport_sensitivity | Project structured repair traces across generic RTT/MTU/loss/handshake profiles |
| netem_trace_validation | Replay RBSR causal flights through NetemTransport to validate the analytical model |
| state_repair_sensitivity | Scale, fanout and symbol-size crossover mapping |
| contention | concurrent write cost |
| snapshot_write_amplification | snapshot write/restart cost |

Run a target with cargo bench --bench <target>. Criterion reports are written under target/criterion/.

Benchmark inputs are deterministic where practical. Compare timings on the same machine/build; do not treat loopback as WAN or component controls as product comparisons. Source files define exact corpus, units, and environment variables. CI only compile-checks benchmark targets.

The `history_independent_catchup` target defaults to `n=100000`, `d=100`, and `h={0,1000,100000}`. Override with `RECONCILE_HISTORY_N`, `RECONCILE_HISTORY_D`, and comma-separated `RECONCILE_HISTORY_OPS`. History construction/normalization happen before timing; it first asserts identical current states and exact protocol-cost traces, then times reconciliation only.

The `runtime_history_independent_catchup` target drives two authoritative `ReplicatedMap` peers through blocked in-memory transport. Live states/final deletions stay fixed while transient insert-delete history grows raw tombstones; it reports raw dated RBSR cost, catch-up, and causal-stability GC. Defaults: `n=10000`, `d=100`, `t={0,100,1000}`; configure with `RECONCILE_RUNTIME_HISTORY_N`, `RECONCILE_RUNTIME_HISTORY_D`, and `RECONCILE_RUNTIME_TOMBSTONES`.

The `state_repair_comparison` target compares prepared-state RBSR, Rateless IBLT, and Merkle over the same sorted `u64 -> u64 digest` manifest. It varies `d` and placement across contiguous, 4/16 clusters, uniform-random, evenly-spaced, and deterministic `max-spread`; the latter is not a formal worst-case claim. Random runs use deterministic seeds and report min/p50/mean/p90/max bytes. `d=0` is included. RIBLT uses a 32-byte equality preflight and 24-byte coded symbols; RBSR prices enumerated rows at 16 bytes; Merkle uses BLAKE3 fanout 16. Payload transfer is excluded and setup remains separate. Defaults: `n=100000`, `d={0,1,10,100,1000,10000}`, three seeds; configure with the `RECONCILE_STATE_REPAIR_*` variables documented in the source.

The `riblt_preparation` target compares a fresh `do-riblt 1.0.2` encoder per peer with one `CachedEncoder` reused across peers. It reports cache rebuild after update/delete/insert because the crate exposes no public incremental add/remove API. Cache memory is a structural upper bound from public types; private HashMap heap usage stays opaque. Defaults: `n=100000`, `d=1000`, `peers={1,2,4,8}`; configure with `RECONCILE_RIBLT_*`.

The `state_repair_mutations` target removes the aligned-key assumption. Baseline keys are even `u64`, leaving odd keys for true interleaved inserts; workloads cover random updates/deletes, interleaved inserts, outside-range inserts, balanced insert/delete, and mixed autonomous changes. Merkle is a key-stable 16-way sparse radix tree, so inserts/deletes affect root-to-key paths rather than shifting positional leaves. Payload transfer is excluded. Defaults: `n=100000`, `d={100,1000,10000}`; configure with `RECONCILE_MUTATION_*`.

The `cold_start_repair` target prices reboot with reconciliation acceleration discarded. Phase A starts from canonical rows in memory; Phase B restores the same state through `FileSnapshot` and reports snapshot bytes, load/deserialization, row projection, rebuild/time-to-ready, and repair. RIBLT reports on-demand and rebuilt/precomputed cache paths. Snapshot creation is outside the restart window and filesystem page-cache state is uncontrolled. Workloads cover `d=0`, outside-range inserts, and mixed divergence. Defaults: `n=100000`, `d={1000,10000}`; configure with `RECONCILE_COLD_*`.

The `state_repair_sensitivity` target maps selected crossovers rather than a full Cartesian product. `scale` varies `n` and `d/n`; `fanout` sweeps RBSR and key-stable radix Merkle fanout; `symbol` repeats representative cases with 16/32/64-byte wire symbols. Workloads are random updates and outside-range inserts. Configure with `RECONCILE_SENSITIVITY_*`.

The `transport_sensitivity` target freezes representative arbitrary-key repair traces and projects them through an explicit analytical network model. Links cover same-host, LAN, regional WAN, intercontinental, high-latency, and constrained asymmetric profiles. Transports model UDP-like application retry, cold/warm TCP-like streams, and cold/resumed QUIC-like streams. Frame overhead and handshake RTTs are model inputs, not wire-accurate claims. It reports MTU packetization, expected retransmission under loss, ordered-stream reordering wait, handshake/propagation/serialization, and CPU separately. RIBLT also reports receiver discovery and sender quiescence using a full-rate one-BDP stop-ACK overshoot envelope. RBSR-native protocol co-design is measured separately; this target keeps transport generic.


The `netem_trace_validation` target validates the finite-flight analytical transport model against
the repository's deterministic `gossip::NetemTransport`. It replays the same RBSR causal flights
used by `transport_sensitivity` with a 16-byte validation header declared as model frame overhead,
so clean-link frame and byte counts must match exactly. Timing is compared at low/high RTT, then
averaged across seeded loss-only and reorder-only lanes; Tokio scheduling and a small loss-detection
margin are reported as residual rather than hidden in the model. This validates the transport
projection layer, not the full `ReplicatedMap` runtime cadence/membership behavior.
