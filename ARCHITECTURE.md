# Architecture

The runtime is an embedded, **fully replicated** LWW map: each authoritative node holds all entries, serves local reads, propagates writes and repairs divergence through range-based anti-entropy. Deletions remain tombstones until causal stability permits GC. `ReadReplicaMap` is a non-authoritative sink, never a source or causal-stability member.

## 2. Boundaries

```mermaid
graph LR
    gossip["gossip\ntransport and discovery"]
    reconcile["reconcile\nreplica runtime"]
    gossip --> reconcile
```

| component | responsibility |
|---|---|
| `rsos` / `rbsr` (external) | ordered range store and reconciliation; `FingerprintTreeMap` adapter |
| `lww-register` (external) | total timestamp order, LWW entries and `HlcClock` |
| `gossip` | byte-oriented transport, authentication, replay, framing and discovery |
| `reconcile` | public replicated map/set runtime, lifecycle, observability and persistence |

`gossip` does not depend on `lww-register`; deployed network peers remain IP-addressed. Ed25519 peer identity verification is available as a separate opt-in framing primitive, **not** integrated into runtime membership.

| port | owner | adapters |
|---|---|---|
| `Rsos` / `RsosView` | `rsos` / `rbsr` | `FingerprintTreeMap` |
| `Clock` | `lww-register` | `HlcClock` |
| `Persistence` | `reconcile` | in-memory, `FileSnapshot`, external implementations |
| `Transport` | `gossip` | UDP, in-memory, network emulation |
| `Discovery` | `gossip` | random probes, DNS |

Persistent tree nodes share unchanged structure via `Arc`; `ReplicatedMap` and `ReadReplicaMap` publish immutable roots using `ArcSwap`, so reads and snapshots need not hold the writer lock.

## Persistence and recovery

`Persistence::load/save` exchanges a complete `PersistedState` with generic backends. `FileSnapshot` may instead store incremental generations without changing that port's semantics.

- Each durable generation is a consistent cut of entries, membership, tombstone acknowledgements and physical GC deletions. Physical deletion differs from writing a tombstone.
- Each mutation belongs to one generation. Freezing generation `g` isolates it from later writes in `g + 1`; success/failure of `g` cannot retire later writes. Frozen work remains retryable after failure, with at most one generation frozen for publication. Pending mutations in an open generation coalesce.
- `FileSnapshot` uses an immutable base, zero or more ordered immutable deltas, and an atomic authoritative manifest naming the complete contiguous committed chain. Unreferenced segments are ignored.
- Base, delta and manifest formats are versioned and integrity-checked. Recovery validates the manifest, referenced base and every named delta in generation order. Missing, corrupt, duplicated or out-of-order committed segments fail with `InvalidData`; recovery must not silently drop generations or initialize empty state.
- Publication writes and durably syncs data before atomically replacing and durably syncing the manifest. Compaction publishes a fully durable replacement base and manifest **before** deleting obsolete segments. Crashes recover either the previous complete manifest or the new complete manifest, never a partial generation.
- Existing single-file snapshots can be migrated at the first successful incremental checkpoint. Unknown/malformed formats fail closed. Downgrading without an explicitly produced compatible full snapshot is unsupported.
- Materialization policy must keep retained segment count/bytes, recovery work, and pending dirty state bounded; policy thresholds are not durability contracts.

Persisted state is restored **before** network participation.

## Network framing and admission

The normal UDP datagram budget is 1200 bytes inclusive of authentication/version framing. Ingress validation occurs in order: **authentication → strict wire version → topology admission → replay → bounded recovery/reassembly → protocol decode**. Untrusted frames and missing-range controls may not allocate recovery state or mutate domain state before admission.

A frame contains a complete logical message, fragment, or capability-gated selective-recovery control. Support is advertised in the reserved extensible protocol tag; older same-version peers ignore it and use whole-message anti-entropy. Missing-range controls are sent only to peers that advertised support. Receiver and sender state are bounded as described in [`SECURITY.md`](SECURITY.md); ordinary anti-entropy remains the fallback.

## Global invariants

- Authoritative replicas are fully replicated, not sharded; read replicas are sinks.
- Same-key conflicts use deterministic total `Timestamp` order.
- Range equality checks both aggregate cardinality and fingerprint.
- Reconciliation policies are local, not wire-negotiated; non-progressing splits fall back to enumeration.
- Only acknowledged tombstones across active authoritative causal membership may be physically collected.
- Authentication precedes deserialization and protocol handling; wire versions are strictly incompatible.
