# Benchmarks

These targets measure the system runtime or deterministic in-memory transport behavior. Algorithm-only RSOS/RBSR benchmarks belong in [set-reconciliation](https://github.com/adriendellagaspera/set-reconciliation).

Run one target with:

```sh
RUSTFLAGS="--cfg reconcile_internal_testing" cargo bench --bench <target>
```

The internal testing cfg exposes deterministic reconciliation triggers used by several benchmarks; it does not enable additional production features. CI compile-checks benchmark targets but does not execute measurements.

| Targets | Scope |
|---|---|
| replicated_map, system | Per-entry and public-API baselines, batch writes, heap footprint, convergence, gossip, persistence rejoin, and emulated RTT/loss |
| membership_scaling, membership_causal_debt, membership_churn | Membership size, causal ACK metadata, resend/decommission costs, and member replacement |
| read_replica_fleet | Read-only replica fleet memory, egress, and ingress-state overhead |
| snapshot_write_amplification, snapshot_metadata_only | Durable snapshot write cost, retained bytes, and metadata-only changes |
| security_ingress, security_amplification | Authentication/replay cost and end-to-end ingress amplification |
| fragmentation | Runtime framing and reassembly over deterministic in-memory transport emulation |
| tombstone_history_skew | Anti-entropy cost for synthetic persisted states with asymmetric tombstone history; no collection is implemented |

## Interpretation

- Compare revisions on the same hardware, toolchain, build profile, seeds, input sizes, and trial counts.
- Loopback and in-memory emulation are not WAN or kernel-network measurements.
- Synthetic persisted-state inputs are controlled workloads, not forecasts of production state.
- Report workload, environment, and raw output with every result. Do not quote a number without its conditions.
- Some targets are counted experiments rather than Criterion statistical benchmarks.
