# Benchmarks

Benchmarks measure code that ships in this repository. Comparative algorithm and transport research belongs in [rbsr-research](https://github.com/adriendellagaspera/rbsr-research); measurements and historical conclusions belong in GitHub issues, not versioned documentation.

| target | scope |
|---|---|
| bench | FingerprintTreeMap microbenchmarks |
| system | end-to-end ReplicatedMap behavior |
| protocol | shipped RBSR reconciliation cost |
| history_independent_catchup | RBSR catch-up cost vs superseded mutation history at fixed current state |
| runtime_history_independent_catchup | ReplicatedMap partition catch-up and causal-stability GC |
| membership_scaling | ReplicatedMap membership, peer-routing, durable causal-state, and GC scaling |
| membership_causal_debt | tombstone ACK resend, unreachable-member repair tax, and decommission cost |
| read_replica_fleet | many read replicas against a small authoritative set, including authenticated ingress state |
| membership_churn | sustained authoritative member replacement with ACK coverage rebuilt through real dated traffic |
| contention | concurrent write cost |
| snapshot_write_amplification | paired full-vs-incremental writes, retained storage, segment-chain peaks, and restart cost |
| snapshot_metadata_only | metadata-only membership/ACK persistence under peer decommission churn |

Run a target with `cargo bench --bench <target>`. Criterion reports are written under `target/criterion/`.

Benchmark inputs are deterministic where practical. Compare timings on the same machine/build; do not treat loopback as WAN or component controls as product comparisons. Source files define exact corpora, units, and environment variables. CI only compile-checks benchmark targets.

The `history_independent_catchup` target defaults to `n=100000`, `d=100`, and `h={0,1000,100000}`. Override with `RECONCILE_HISTORY_N`, `RECONCILE_HISTORY_D`, and comma-separated `RECONCILE_HISTORY_OPS`. History construction/normalization happen before timing; it first asserts identical current states and exact protocol-cost traces, then times reconciliation only.

The `runtime_history_independent_catchup` target drives two authoritative `ReplicatedMap` peers through blocked in-memory transport. Live states/final deletions stay fixed while transient insert-delete history grows raw tombstones; it reports raw dated RBSR cost, catch-up, and causal-stability GC. Defaults: `n=10000`, `d=100`, `t={0,100,1000}`; configure with `RECONCILE_RUNTIME_HISTORY_N`, `RECONCILE_RUNTIME_HISTORY_D`, and `RECONCILE_RUNTIME_TOMBSTONES`.

The `membership_scaling` target isolates runtime membership from algorithm research. It measures authoritative/read-replica peer-routing heap, admits 2/10/100/1000 authoritative peers through real dated traffic on `InMemoryNetwork`, counts one full anti-entropy initiation round, and prices persisted membership plus full tombstone-ack matrices through `FileSnapshot`. The largest live point also proves that an expired tombstone remains blocked by unreachable authoritative members until they are decommissioned. A 100k result is never live-measured: the target prints only an explicit `MODEL` extrapolation from the two largest measured points. Configure with `RECONCILE_MEMBERSHIP_COUNTS`, `RECONCILE_MEMBERSHIP_TOMBSTONES`, and `RECONCILE_MEMBERSHIP_MODEL_TARGET`.

The `membership_causal_debt` target measures shipped causal-stability mechanics once membership exists: the 8 KiB-per-round tombstone-ACK resend window, one silent authoritative member against otherwise responsive peers, the bounded RTT-scale repair retries it causes, and `forget_peer` cost as ACK maps grow. Its default live failure scenario is 1,000 authoritative members with 100 retained tombstones; the synthetic decommission sweep extends debt to 1,000 tombstones without changing protocol semantics. Configure with `RECONCILE_CAUSAL_MEMBERS`, `RECONCILE_CAUSAL_TOMBSTONES`, `RECONCILE_CAUSAL_ROUNDS`, `RECONCILE_FAILURE_MEMBERS`, and `RECONCILE_FAILURE_TOMBSTONES`.

The `read_replica_fleet` target measures a topology with many `ReadReplicaMap` instances and a small authoritative set. It reports aggregate read-replica heap, known-authoritative and speculative-probe egress per round, verifies that value-only senders enter neither authoritative `peers` nor causal `members`, and contrasts authenticated vs insecure authoritative ingress heap. The authoritative control deliberately keeps both `max_peers=8` and the independent `max_replay_senders=8` while sweeping up to 1,000 read replicas: the authenticated ingress delta should stop growing with fresh sender cardinality once the replay-sender cap is full, while causal membership remains empty. Configure with `RECONCILE_READ_REPLICA_COUNTS`, `RECONCILE_READ_AUTHORITATIVE_PEERS`, and `RECONCILE_READ_MODEL_TARGET`.

The `membership_churn` target keeps authoritative fleet size constant while replacing members through real authenticated dated traffic. New members preload the same tombstones and rebuild version-correct ACK coverage through the shipped bounded resend cursor; the benchmark reports `forget_peer` and end-to-end replacement latency, traffic per replacement, and initial/final persisted-state size. Defaults are 1,000 authoritative members, 100 replacements, 0/100/1,000 tombstones, and three ACK rounds per newcomer. Configure with `RECONCILE_CHURN_MEMBERS`, `RECONCILE_CHURN_TOMBSTONES`, `RECONCILE_CHURN_REPLACEMENTS`, and `RECONCILE_CHURN_ACK_ROUNDS`.

The `contention` target compares `FingerprintTreeMap` with `BTreeMap` behind the same `parking_lot::RwLock`, using paired trials across writer counts. It reports throughput plus the per-operation cost delta after cancelling the shared lock term. Override the writer sweep with `CONTENTION_WRITERS`; set `CONTENTION_RAW=1` for trial-level rows.

The `snapshot_write_amplification` target is revision-pairable: it observes durable files rather than depending on either persistence layout. It reports changed/new durable bytes per checkpoint, total and peak retained bytes, final and peak delta-segment counts, checkpoint time, and fresh-process restart time. Pair sweeps use `RECONCILE_SNAPSHOT_SIZES` and `RECONCILE_SNAPSHOT_DELTAS`; long-chain sweeps use `RECONCILE_SNAPSHOT_CHAIN_LENGTHS` and `RECONCILE_SNAPSHOT_CHAIN_DELTAS`; set `RECONCILE_SNAPSHOT_TRIALS` for repetitions.

The `snapshot_metadata_only` target isolates causal metadata persistence: it seeds membership plus tombstone ACK state, then removes peers through `forget_peer` without changing entry values. It reports checkpoint bytes/time, retained/peak storage, final/peak segment counts, and restart time. Configure with `RECONCILE_METADATA_MEMBERS`, `RECONCILE_METADATA_BATCHES`, `RECONCILE_METADATA_CHAIN_LENGTHS`, and `RECONCILE_METADATA_TRIALS`.
