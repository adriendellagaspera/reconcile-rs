# Architecture

This document describes repository-wide structure and invariants. Public API semantics belong in
rustdoc; security properties belong in [`SECURITY.md`](SECURITY.md).

## 1. System

`reconcile` is an embedded, fully replicated map. Each authoritative replica stores the complete
dataset. Reads are local; writes are propagated eagerly and repaired by range-based anti-entropy.

Values use last-write-wins ordering. Deletions are tombstones until causal-stability tracking permits
garbage collection. Persistence is optional; a durable backend restores values, tombstones, and
causal-stability state before the node rejoins the protocol.

`ReadReplicaMap` is a non-authoritative, value-only replica. It consumes the same reconciliation
protocol but does not originate authoritative values or participate in tombstone stability.

## 2. Workspace

```mermaid
graph LR
    rsos["rsos\nexternal range store"]
    rbsr["rbsr\nexternal range reconciliation"]
    gossip["gossip\ntransport, auth, discovery"]
    reconcile["reconcile\npublic facade and runtime"]

    rsos -. crates.io .-> rbsr
    rsos -. crates.io .-> reconcile
    rbsr -. crates.io .-> reconcile
    gossip --> reconcile
```

| crate | responsibility |
|---|---|
| `gossip` | datagram transport, wire codec, authentication, replay protection, discovery |
| `reconcile` | replicated maps/sets, key/value bounds, persistence state/ports/adapters, lifecycle, observability |

External domain dependencies are `rsos` (ordered storage and range aggregates) and `rbsr`
(range reconciliation), both maintained in `set-reconciliation`, plus
`lww-register` for LWW entries, timestamps, and clock primitives.

### 2.1 Domain boundary
`rsos`, `rbsr`, and `lww-register` are crates.io dependencies whose standalone invariants and
release gates live in their canonical repositories. `gossip` owns network concerns; `reconcile`
composes those domain crates with runtime adapters.

`gossip` does not depend on `lww-register`: it defines an opaque, currently unused `PeerId` distinct
from the address-based runtime identity; application payloads remain bytes.

### 2.2 Snapshots

`FingerprintTreeMap` is persistent: unchanged nodes are structurally shared through `Arc`.
`ReplicatedMap` and `ReadReplicaMap` publish immutable roots through `ArcSwap`. Point reads and
snapshots therefore do not hold the writer lock.

### 2.3 Incremental persistence generations

`Persistence::load/save` remains the stable generic port: downstream backends receive and return a
complete `PersistedState`. Incremental persistence is an optimization owned by `FileSnapshot`;
it must not change the semantics required of generic persistence implementations.

A durable generation is one coherent cut across the immutable dated-entry root, causal-stability
membership, per-tombstone acknowledgement state, and physical deletion after tombstone GC.

A mutation belongs to exactly one generation. Once generation `g` is frozen for persistence,
later writes are recorded only in `g + 1` and must never be retired by success or failure of
`g`. Multiple changes to the same key within an unfrozen generation coalesce to its final durable
effect. Physical deletion is a distinct delta operation from writing an LWW tombstone.

The in-memory boundary has three logical states: **open** accepts mutations and coalesces the
pending durable delta; **frozen** is immutable work handed to persistence while a new open generation
accepts writes; **committed** is retired only after durable publication succeeds. A failed frozen
generation remains retryable and cannot consume or reorder newer mutations.

At most one generation is frozen for publication at a time. The open generation may continue to
grow behind it, but failed persistence must not create an unbounded queue of immutable generations.

`FileSnapshot` persists generations as an immutable materialized **base**, zero or more immutable
ordered **delta** segments, and one authoritative atomic **manifest**. The manifest names exactly the
base generation and contiguous committed delta range that constitute the last durable state. A
segment not named by the committed manifest is uncommitted garbage and is ignored during recovery.

Every base, delta, and manifest format is explicitly versioned and integrity-checked. Recovery:

1. reads and validates the manifest;
2. validates the referenced base;
3. replays only the contiguous delta sequence named by the manifest, in generation order;
4. rejects missing, corrupt, duplicated, or out-of-order committed segments with
   `InvalidData`; it never silently starts fresh or skips a committed generation.

Publication ordering is durable-data-first: write and sync new segment files, then atomically replace
and durably sync the manifest. Compaction follows the same rule: materialize and sync the replacement
base first, publish a manifest that points to it, and only then garbage-collect superseded files.
A crash at any earlier point therefore recovers the previous committed manifest; a crash after
manifest publication recovers the new complete generation.

Legacy single-file `FileSnapshot` state is migrated explicitly: the first successful incremental
checkpoint may materialize it as the initial base before publishing the incremental manifest.
Unknown future formats or malformed legacy data are rejected. Downgrade to a build that does not
understand the incremental format is unsupported unless an explicit full legacy-compatible
materialization was produced first.

Adaptive materialization is policy, not durability semantics. The implementation may choose delta
or full-base publication from measured cost, but must keep these operational quantities bounded:

- number of committed delta segments;
- total on-disk bytes relative to a fresh full snapshot;
- recovery work/latency;
- retained frozen/open dirty state after repeated persistence failures.

The policy threshold is derived by paired measurements; no fixed delta-count or change-ratio
threshold is part of this architecture contract.

## 3. Runtime boundaries

The main ports are:

| port | owner | adapters |
|---|---|---|
| `Rsos` / `RsosView` | `rsos` / `rbsr` | `FingerprintTreeMap` |
| `Clock` | `lww-register` | `HlcClock` |
| `Persistence` | `reconcile` | in-memory, file snapshot, downstream implementations |
| `Transport` | `gossip` | UDP, in-memory, network-emulation decorator |
| `Discovery` | `gossip` | random probing, DNS |

Authentication and replay validation happen before wire messages reach reconciliation logic.
Malformed or unauthenticated network input must not mutate domain state.

### 3.1 UDP application framing
`gossip` owns one application frame per authenticated datagram: complete logical payload, fragment, or capability-gated selective-recovery control. The default 1200-byte budget includes auth/version overhead, avoiding normal IP fragmentation.
Receive order is auth → version → topology admission → replay → recovery/reassembly → protocol decode; only complete logical payloads reach reconciliation. Missing-range reports therefore cannot allocate or trigger retransmission before the common ingress gate.
Content-derived transfer ids permit fragment reuse across retransmission. New peers advertise selective recovery through the already-reserved opaque protocol tag; older same-version peers ignore that advertisement, and recovery controls are never sent to a peer that has not advertised support. Mixed deployments therefore retain whole-message anti-entropy fallback without weakening the strict outer wire version.
Receiver and sender state are independently count/byte/TTL bounded ([`SECURITY.md`](SECURITY.md)). An idle incomplete transfer can also request missing byte ranges without seeing the terminal fragment: a periodic task scans bounded reassembly state, schedules at most 32 authenticated NACKs per tick, and rate-limits each transfer to one request per 3 seconds after 1 second idle. This task runs for authoritative and read replicas; full anti-entropy remains the fallback. For an identical retained content-addressed transfer, the sender defers redundant whole-message retransmission; the next anti-entropy attempt may send the complete transfer again after a 12-second minimum interval. This guarantees fallback without repeatedly racing the selective-control path.

## 4. Global invariants

- A replica is fully replicated, not sharded.
- Same-key conflicts use the total order defined by `Timestamp`; merge is deterministic.
- Range equality compares both aggregate cardinality and fingerprint.
- Reconciliation policies are local choices; they are not negotiated on the wire.
- A split must make progress. Non-progressing policy output is converted to enumeration.
- Read replicas are sinks: they do not become authoritative sources or causal-stability members.
- Tombstones are collected only after the active authoritative membership has acknowledged them.
- Persisted state is loaded before network participation.
- Authenticated traffic is verified before deserialization and protocol handling.
- Wire versions are strict; incompatible versions do not communicate.

Type-level and function-level forms of these rules are documented next to the APIs that enforce
them.
