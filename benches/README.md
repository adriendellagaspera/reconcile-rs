# Benchmarks

Runtime and product benchmarks live here; algorithm-only benchmarks live in [set-reconciliation](https://github.com/adriendellagaspera/set-reconciliation).

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
| fragment_recovery | Model-only whole-message vs missing-only fragment recovery cost under matched deterministic loss |
| ordered_stream | Model-only ordered-stream vs multiplexed reliable-stream transport semantics under matched deterministic impairments |
| transport_validation | Matched model validation of selective recovery and independent reliable-stream candidates |
| tombstone_gc_skew | one-sided retained tombstone history, reconciliation work, and ACK recovery |
| membership_churn | sustained authoritative member replacement with ACK coverage rebuilt through real dated traffic |
| snapshot_write_amplification | paired full-vs-incremental checkpoint writes, retained storage, segment chains, and restart cost |
| snapshot_metadata_only | metadata-only membership/ACK persistence under peer decommission churn |

Run with `cargo bench --bench <target>`. Criterion output is under `target/criterion/`; CI compile-checks benchmarks but does not run their measurements. Run matched builds on the same hardware. Loopback measurements are not WAN measurements.

Inputs and environment overrides are defined in each benchmark's source. Where applicable, keep seeds, profiles, dataset sizes, timeout and trial counts identical when comparing revisions. `membership_scaling` distinguishes measured points from printed `MODEL` extrapolations. `fragment_recovery`, `ordered_stream`, and `transport_validation` model transport semantics, **not** measured kernel, TCP, or QUIC implementations; their synthetic recovery/control mechanisms must not be read as runtime wire guarantees.
