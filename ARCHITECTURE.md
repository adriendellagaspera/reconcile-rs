# Architecture and experimental scope

`reconcile-rs` is an experimental, embedded replication implementation. It is retained as a record of design choices and implementation work, not as a supported runtime.

## Data model

The primary abstraction is a fully replicated key-value map (and set facade). Each authoritative replica stores the complete dataset and serves local reads. Writes propagate asynchronously. Concurrent writes to one key are ordered by a last-write-wins timestamp.

This is eventual-state replication, not a transactional database:

- no sharding or partial dataset ownership;
- no consensus, quorum writes, or linearizable reads;
- no multi-key transactions;
- no application-level conflict resolution beyond timestamp ordering;
- no bounded tombstone retention.

A read replica is a non-authoritative sink. It does not participate in authoritative causal membership.

## Two separate runtime paths

The repository contains two paths that should not be treated as interchangeable:

| Path | Identity and transport | Reconciliation | Persistence |
|---|---|---|---|
| Legacy `ReplicatedMap` | IP-addressed UDP peers | RSOS/RBSR range reconciliation | Legacy snapshot format |
| Signed `SignedReplica<K,V,A>` | Caller-driven datagram substrate with allowlisted Ed25519 `PeerId` identities | Bounded record-by-record round-robin anti-entropy | Incompatible v2 snapshot format |

They have different membership and recovery semantics. There is no mixed cluster mode and no automatic conversion between their persisted formats. The signed actor is opt-in and requires the application to drive probing, receiving, and sending.

## Crate boundaries

- `reconcile` owns the replicated data model, lifecycle, persistence integration, and observability.
- `reconcile-gossip` owns byte-oriented datagram transport, framing, authentication, replay protection, and discovery. It is an implementation detail; applications should normally depend on `reconcile`.
- `lww-register` provides timestamp ordering and LWW entries.
- `rsos` and `rbsr` provide ordered range summaries and range-based reconciliation. Their algorithmic development lives in [set-reconciliation](https://github.com/adriendellagaspera/set-reconciliation).

The persistent ordered tree uses structural sharing and immutable roots. This supports cheap root publication and snapshot reads, but does not make the complete runtime memory-efficient under every workload.

## Persistence and deletion

Persistence is optional. The generic persistence interface exchanges complete state; `FileSnapshot` additionally supports incremental generations. The signed actor uses a different v2 format.

The implementation deliberately disables physical tombstone collection in both runtime paths. Receipt acknowledgements, elapsed retention, decommissioning, and checkpoints are not sufficient proof that stale application data can no longer reappear. Consequently, tombstones and causal acknowledgement metadata are not bounded by a collection policy.

Do not mix snapshots between runtime paths, downgrade formats, or assume that snapshot persistence provides transactional database semantics.

## Network and security

The legacy path uses UDP and supports shared-key authentication, replay protection, framing, and optional payload encryption. The signed path uses explicitly provisioned Ed25519 identities and caller-supplied datagram transport.

Authentication and bounded admission must happen before protocol decoding or allocation of recovery state. These mechanisms reduce specific network risks; they do not make an arbitrary deployment secure by default. Key management, peer authorization, network isolation, and operational recovery remain application responsibilities.

See [SECURITY.md](SECURITY.md) and [SIGNED-RUNTIME-v0.md](SIGNED-RUNTIME-v0.md) for historical implementation details.

## Benchmarks

The benchmark suite contains a mixture of:

- measurements of this implementation on a particular machine;
- deterministic workload experiments;
- analytical or simulated transport models.

Model results are not measurements of kernel UDP, TCP, or QUIC behavior. Loopback results are not WAN results. See [benches/README.md](benches/README.md) for the scope and interpretation rules.
