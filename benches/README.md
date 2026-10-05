# Benchmarks

RSOS/RBSR-only benchmarks live in [`set-reconciliation`](https://github.com/adriendellagaspera/set-reconciliation): `rsos` owns `micro` and `contention`, while `rbsr` owns `protocol` and `history_independence`. This repository keeps only benchmarks whose measured behavior belongs to the `reconcile` runtime/product boundary.

Benchmarks measure code that ships in this repository. Comparative algorithm research, reproducible experiments, workloads, and the versioned literature survey live in [set-reconciliation](https://github.com/adriendellagaspera/set-reconciliation); runtime/product measurements remain here.

| target | scope |
|---|---|
| replicated_map | ReplicatedMap structure, send/reconcile, RTT, and reconcile-interval lanes |
| system | end-to-end ReplicatedMap behavior |
| runtime_history_independent_catchup | ReplicatedMap partition catch-up and causal-stability GC |
| membership_scaling | ReplicatedMap membership, peer-routing, durable causal-state, and GC scaling |
| membership_causal_debt | tombstone ACK resend, unreachable-member repair tax, and decommission cost |
| read_replica_fleet | many read replicas against a small authoritative set, including authenticated ingress state |
| security_ingress | authenticated MAC/version/replay CPU cost as payload and retained-sender state scale |
| security_amplification | response amplification and bulk-dump concurrency for rejected and valid authenticated ingress |
| fragmentation | UDP application framing, large-message convergence, datagram size, loss/reorder, and reassembly overhead |
| fragment_recovery | Research-only whole-message vs missing-only fragment recovery cost under matched deterministic loss |
| tombstone_gc_skew | one-sided retained tombstone history, reconciliation work, and ACK recovery |
| membership_churn | sustained authoritative member replacement with ACK coverage rebuilt through real dated traffic |
| snapshot_write_amplification | paired full-vs-incremental checkpoint writes, retained storage, segment chains, and restart cost |
| snapshot_metadata_only | metadata-only membership/ACK persistence under peer decommission churn |

Run a target with `cargo bench --bench <target>`. Criterion reports are written under `target/criterion/`.

Benchmark inputs are deterministic where practical. Compare timings on the same machine/build; do not treat loopback as WAN or component controls as product comparisons. Source files define exact corpora, units, and environment variables. CI only compile-checks benchmark targets.

The `runtime_history_independent_catchup` target drives two authoritative `ReplicatedMap` peers through blocked in-memory transport. Live states/final deletions stay fixed while transient insert-delete history grows raw tombstones; it reports raw-store divergence, real catch-up traffic, convergence latency, and causal-stability GC. Defaults: `n=10000`, `d=100`, `t={0,100,1000}`; configure with `RECONCILE_RUNTIME_HISTORY_N`, `RECONCILE_RUNTIME_HISTORY_D`, and `RECONCILE_RUNTIME_TOMBSTONES`.

The `membership_scaling` target isolates runtime membership from algorithm research. It measures authoritative/read-replica peer-routing heap, admits 2/10/100/1000 authoritative peers through real dated traffic on `InMemoryNetwork`, counts one full anti-entropy initiation round, and prices persisted membership plus full tombstone-ack matrices through `FileSnapshot`. The largest live point also proves that an expired tombstone remains blocked by unreachable authoritative members until they are decommissioned. A 100k result is never live-measured: the target prints only an explicit `MODEL` extrapolation from the two largest measured points. Configure with `RECONCILE_MEMBERSHIP_COUNTS`, `RECONCILE_MEMBERSHIP_TOMBSTONES`, and `RECONCILE_MEMBERSHIP_MODEL_TARGET`.

The `membership_causal_debt` target measures shipped causal-stability mechanics once membership exists: the 8 KiB-per-round tombstone-ACK resend window, one silent authoritative member against otherwise responsive peers, the bounded RTT-scale repair retries it causes, and `forget_peer` cost as ACK maps grow. Its default live failure scenario is 1,000 authoritative members with 100 retained tombstones; the synthetic decommission sweep extends debt to 1,000 tombstones without changing protocol semantics. Configure with `RECONCILE_CAUSAL_MEMBERS`, `RECONCILE_CAUSAL_TOMBSTONES`, `RECONCILE_CAUSAL_ROUNDS`, `RECONCILE_FAILURE_MEMBERS`, and `RECONCILE_FAILURE_TOMBSTONES`.

The `read_replica_fleet` target measures a topology with many `ReadReplicaMap` instances and a small authoritative set. It reports aggregate read-replica heap, known-authoritative and speculative-probe egress per round, verifies that value-only senders enter neither authoritative `peers` nor causal `members`, and contrasts authenticated vs insecure authoritative ingress heap. The authoritative control deliberately keeps both `max_peers=8` and the independent `max_replay_senders=8` while sweeping up to 1,000 read replicas: the authenticated ingress delta should stop growing with fresh sender cardinality once the replay-sender cap is full, while causal membership remains empty. Configure with `RECONCILE_READ_REPLICA_COUNTS`, `RECONCILE_READ_AUTHORITATIVE_PEERS`, and `RECONCILE_READ_MODEL_TARGET`.

The `security_ingress` target isolates the authenticated ingress gate. It measures valid and invalid MAC verification, authenticated wrong-version rejection, replay-filter lookup/update cost as retained sender state grows, and the combined auth/version/replay path. Defaults are 5,000 iterations, payloads of 64/1,024/8,192 bytes, and retained-sender populations of 1/128/1,024; configure with `RECONCILE_SECURITY_ITERS`, `RECONCILE_SECURITY_PAYLOADS`, and `RECONCILE_SECURITY_REPLAY_POPULATIONS`.

The `security_amplification` target measures runtime response/input byte amplification for rejected authenticated traffic and valid empty-replica mismatches against a populated authoritative store. It also exercises the shipped concurrent bulk-dump bound and stalled-peer retries. Configure corpus size with `RECONCILE_SECURITY_DATASET`.

The `fragmentation` target cold-syncs one value through deterministic Netem profiles and reports
convergence latency, framed bytes, datagrams, maximum datagram size, thresholds above 1200/1472/8972
bytes, and realized loss. It is revision-pairable: use the same value-size/profile inputs before and
after framing changes. Configure value sizes with `RECONCILE_FRAGMENT_VALUE_SIZES` and the
per-case convergence deadline with `RECONCILE_FRAGMENT_TIMEOUT_MS`.

The `fragment_recovery` target is a research-only isolated-transfer model layered on the shipped
framing sizes; it does not define an ACK/NACK wire format. It compares the #263 whole-message retry
control (receiver keeps already-arrived fragments) with a benchmark-only missing-fragment bitmap
NACK plus completion ACK. Both policies see matched deterministic independent-loss draws for each
data fragment/attempt; the same loss probability also applies to selective-control datagrams. The
whole-retry arm is deliberately given an optimistic one-RTT recovery opportunity, so selective
recovery does not win merely because the production anti-entropy timer is slower. Domain convergence
equals receiver completion by construction in this isolated-transfer model; the separate
`sender_quiescence` metric prices the selective completion ACK. Defaults use a sparse set of
1/4/16/64/~900-frame cases spanning 1/50/150/600 ms RTT and 0/1/5% loss, 1200 B datagrams,
100 Mbit/s symmetric serialization, and 64 deterministic seeds. Configure the budget, bandwidth,
or sample count with `RECONCILE_FRAGMENT_RECOVERY_BUDGET`,
`RECONCILE_FRAGMENT_RECOVERY_BANDWIDTH_BPS`, and
`RECONCILE_FRAGMENT_RECOVERY_TRIALS`. The existing `fragmentation` target remains the production
runtime control and is the validation surface for selected modeled points.

The `tombstone_gc_skew` target compares aligned authoritative peers with a one-sided retained-tombstone history while keeping application-visible state equal, plus a fixed live-divergence control. It reports wire traffic, protocol enumeration, convergence, and bounded tombstone-ACK recovery rounds. The deployment-sized debt term is approximately `delete_rate × effective_unacked_membership_lag`: age timeout alone does not make a tombstone collectible while a causal member has not acknowledged its exact version. Configure with `RECONCILE_GC_SKEW_N`, `RECONCILE_GC_SKEW_DELETIONS`, and `RECONCILE_GC_SKEW_BASE_DIVERGENCE`.

The `membership_churn` target keeps authoritative fleet size constant while replacing members through real authenticated dated traffic. New members preload the same tombstones and rebuild version-correct ACK coverage through the shipped bounded resend cursor; the benchmark reports `forget_peer` and end-to-end replacement latency, traffic per replacement, and initial/final persisted-state size. Defaults are 1,000 authoritative members, 100 replacements, 0/100/1,000 tombstones, and three ACK rounds per newcomer. Configure with `RECONCILE_CHURN_MEMBERS`, `RECONCILE_CHURN_TOMBSTONES`, `RECONCILE_CHURN_REPLACEMENTS`, and `RECONCILE_CHURN_ACK_ROUNDS`.

The `snapshot_write_amplification` target is revision-pairable: it observes durable files rather than depending on either persistence layout. It reports changed/new durable bytes per checkpoint, total retained bytes, base/delta segment counts, checkpoint time, and fresh-process restart time. Pair sweeps use `RECONCILE_SNAPSHOT_SIZES` and `RECONCILE_SNAPSHOT_DELTAS`; long-chain sweeps use `RECONCILE_SNAPSHOT_CHAIN_LENGTHS` and `RECONCILE_SNAPSHOT_CHAIN_DELTAS`; set `RECONCILE_SNAPSHOT_TRIALS` for repetitions.

The `snapshot_metadata_only` target isolates causal metadata persistence: it seeds membership plus tombstone ACK state, then removes peers through `forget_peer` without changing entry values. It reports checkpoint bytes/time, retained/peak storage, final/peak segment counts, and restart time. Configure with `RECONCILE_METADATA_MEMBERS`, `RECONCILE_METADATA_BATCHES`, `RECONCILE_METADATA_CHAIN_LENGTHS`, and `RECONCILE_METADATA_TRIALS`.
