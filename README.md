# reconcile-rs

Frozen research reference for an experimental Rust implementation of fully replicated key-value storage.

The repository preserves two deliberately incompatible runtime designs:

- Legacy ReplicatedMap: IP-addressed UDP replication, LWW conflict resolution, and RSOS/RBSR range-based anti-entropy.
- SignedReplica: caller-driven datagram actor, allowlisted Ed25519 identities, and a separate v3 persistence format.

The algorithmic RSOS/RBSR work is maintained separately in [set-reconciliation](https://github.com/adriendellagaspera/set-reconciliation). This repository records the system integration, transport, persistence, membership, security, and runtime experiments.

## Important limits

- Every authoritative replica stores the complete dataset. There is no sharding, consensus, transaction support, or linearizable write path.
- Writes propagate asynchronously; same-key conflicts use last-write-wins ordering.
- Physical tombstone collection is not implemented in either runtime path. Tombstones and causal acknowledgement metadata can grow without bound.
- The two runtime paths have incompatible membership, wire, and persistence semantics. They cannot share a cluster or snapshots.
- Snapshot persistence is not a transactional database or write-ahead log.
- This is not a supported database or production dependency. There is no active development, security response commitment, or release roadmap.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the design and invariants, [SIGNED-RUNTIME-v0.md](SIGNED-RUNTIME-v0.md) for the signed actor's distinct semantics, [SECURITY.md](SECURITY.md) for trust boundaries, and [benches/README.md](benches/README.md) for benchmark methodology.

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
