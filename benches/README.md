# Benchmarks

This suite records runtime and transport experiments. Algorithm-only reconciliation
benchmarks belong in [set-reconciliation](https://github.com/adriendellagaspera/set-reconciliation).

Run an individual target with:

```sh
cargo bench --bench <target>
```

CI compile-checks benchmark targets; it does not execute measurements.

## Scope

| Targets | What they exercise |
|---|---|
| `replicated_map` | Per-entry `just_insert`, read-replica memory-fill cost, and service reconciliation RTT/cadence; uses internal benchmark seams |
| `system` | Public-API baselines, batched writes, heap footprint, convergence/gossip, persistence rejoin, and emulated RTT/loss |
| `membership_scaling`, `membership_causal_debt`, `membership_churn` | Runtime membership, ACK metadata, and churn costs |
| `read_replica_fleet` | Read-only replica fleet overhead and ingress state |
| `snapshot_write_amplification`, `snapshot_metadata_only` | Durable snapshot writes, retained bytes, and metadata costs |
| `security_ingress`, `security_amplification` | Authentication, replay filtering, and valid/invalid ingress paths |
| `fragmentation` | Runtime framing over deterministic in-memory transport emulation |
| `tombstone_history_skew` | Anti-entropy from synthetic persisted states with asymmetric tombstone history; it does not execute GC |
| `transport_validation` | Analytical comparison of fragment-recovery and ordered-stream semantics |

The shared `fragment_recovery_model.rs` and `ordered_stream_model.rs` files are deterministic
semantic models, not production transport implementations. The model does not measure TCP,
QUIC, kernel scheduling, congestion control, or transport CPU.

## Interpretation

- Compare revisions on the same hardware, with the same toolchain, build profile, seeds, input sizes, and trial counts.
- Loopback measurements are not WAN measurements.
- A benchmark using `InMemoryNetwork` or an emulated transport is not a measurement of kernel UDP behavior.
- Extrapolations are models, not measurements. They must remain labelled as such.
- Report the workload, environment, and raw output with any result. Do not quote a single number without its conditions.

The suite is exploratory and heterogeneous. Passing compilation does not imply that a benchmark is methodologically sound for every comparison. Some targets are counted experiments rather than Criterion statistical benchmarks.
