# reconcile-rs

[![Crates.io][crates-badge]][crates-url]
[![MIT licensed][mit-badge]][mit-url]
[![Apache licensed][apache-badge]][apache-url]
[![Build Status][actions-badge]][actions-url]
[![Coverage Status][codecov-badge]][codecov-url]
[![Docs Status][docs-badge]][docs-url]

[crates-badge]: https://img.shields.io/crates/v/reconcile.svg
[crates-url]: https://crates.io/crates/reconcile
[mit-badge]: https://img.shields.io/badge/license-MIT-blue.svg
[mit-url]: https://github.com/adriendellagaspera/reconcile-rs/blob/main/LICENSE-MIT
[apache-badge]: https://img.shields.io/badge/license-APACHE-blue.svg
[apache-url]: https://github.com/adriendellagaspera/reconcile-rs/blob/main/LICENSE-APACHE
[actions-badge]: https://github.com/adriendellagaspera/reconcile-rs/actions/workflows/main.yml/badge.svg
[actions-url]: https://github.com/adriendellagaspera/reconcile-rs/actions/workflows/main.yml
[codecov-badge]: https://codecov.io/gh/adriendellagaspera/reconcile-rs/branch/main/graph/badge.svg
[codecov-url]: https://codecov.io/gh/adriendellagaspera/reconcile-rs
[docs-badge]: https://docs.rs/reconcile/badge.svg
[docs-url]: https://docs.rs/reconcile/latest/reconcile/

This crate provides a key-data map structure `FingerprintTreeMap` that can be used together
with the reconciliation `ReplicatedMap`. Different instances can talk together over
UDP to efficiently reconcile their differences.

All the data is available locally on all instances, and the user can be
notified of changes to the collection with an insertion hook.

The protocol allows finding a difference over millions of elements with a limited
number of round-trips. It should also work well to populate an instance from
scratch from other instances.

The intended use case is a scalable Web service with an in-memory, eventually
consistent key-value store. The design enable high performance by avoiding any
latency related to using an external store such as Redis. Durability is
optional and pluggable — see [Persistence](#persistence) — so a node can
recover its state (including tombstones) across a restart instead of rejoining
the cluster as an empty replica.

![Architecture diagram of a scalable Web service using reconcile-rs](img/illustration.png)

In code, this would look like this:

```rust
let mut store = ReplicatedMap::new(Config::new(8080).with_insecure_no_key())
    .await
    .unwrap();
tokio::spawn(store.clone().run(CancellationToken::new()));
// use the reconciliation store as a key-value store in the API
```

## When to use this

`reconcile-rs` is an **embedded, in-memory, eventually-consistent replicated map** — in data-grid
terms, the masterless / AP / gossip corner of an in-memory data grid (the niche of Hazelcast's
*Replicated Map* or Pekko *Distributed Data*, with no mature Rust equivalent). Every instance keeps
the whole dataset in memory and serves reads locally (no network hop); writes propagate
asynchronously and merge last-write-wins.

**Good fit**
- read-heavy access that must be fast and local — no per-read round-trip to Redis/etcd;
- a working set that fits in RAM on every node (full replication = redundancy, not sharding);
- eventual consistency / last-write-wins is acceptable, and same-key write conflicts are rare;
- no separate datastore to operate, and the cluster must keep serving across network partitions.

**Wrong tool for**
- counters, quotas or rate-limits — LWW overwrites, it does not sum;
- ledgers or anything needing strong consistency or transactions;
- datasets larger than a single node's RAM — it is fully replicated, not partitioned;
- collaborative text editing — use a sequence CRDT (Yjs/Automerge-style) instead.

Because every replica holds everything, memory use and write fan-out grow with the dataset and the
node count; see [`POSITIONING.md`](POSITIONING.md) for the detailed positioning and the issue
tracker for current performance limitations.

### Lock-free snapshot reads

The in-memory map is persistent rather than mutated in place. B-tree nodes are structurally shared
behind `Arc`s; a write copy-on-writes only the changed path, while readers keep immutable snapshots
of the old root. `ReplicatedMap` and `ReadReplicaMap` publish the current root through `ArcSwap`, so
a read takes no read lock and an in-flight reader never blocks a writer.

- `get(&key)` returns a zero-copy `ValueRef` pinned to the persistent node found by one tree lookup;
  it remains valid if that key is concurrently overwritten or removed.
- `get_cloned(&key)` performs one lookup and clones the value, avoiding the persistent handle when
  an owned value is preferable.
- `snapshot()` (`value_snapshot()` on `ReplicatedMap`) returns an `O(1)` `Arc` snapshot whose
  `iter()`/`range()` borrow keys and values directly, with no lock held during the scan.

See [`ARCHITECTURE.md`](ARCHITECTURE.md) §2.2 for the snapshot/structural-sharing model and
[`benches/README.md`](benches/README.md) for the current measurement methodology.

## Modelling sets

The most-requested CRDT beyond LWW-Register is an add-wins set (OR-Set). The instinct is to store
the whole set as one value:

```rust
// DON'T: the whole set is one value
store.insert(set_id, my_or_set);
```

Here that is the wrong encoding, and the right one needs no new machinery — each element is its own
key. `ReplicatedSet<K>` is exactly this: a `ReplicatedMap<K, ()>` under a set-shaped API, so a call
site reads as a set operation rather than a raw `()` value peeking through:

```rust
// DO: each element is a key, in a dedicated ReplicatedSet
members.insert((set_id, element));
members.remove(&(set_id, element));
```

Because the store is an ordered map reconciled by range diff, membership-as-keys gives, for free,
what a set-as-value has to build for itself:

| property | as keys | as one value |
|---|---|---|
| diff granularity | two replicas differing by one element exchange one element | re-ships the entire set state on any divergence |
| tombstones | reuses the existing causal-stability GC (issue #109), which already prevents resurrection | needs its own separate element-tombstone GC |
| datagram ceiling | never reached — one element per datagram, regardless of set size | crosses ~65 KB as cardinality grows, then silently stops converging (issue #230) |

**Semantics.** This is last-write-wins *per element* over the HLC total order (see "Conflict
resolution" below), not textbook add-wins. The two differ only when an add and a remove of the
*same* element are genuinely concurrent — the HLC total order picks one deterministic winner instead
of biasing towards the add. For any pair of updates with a causal (happens-before) relationship, and
for concurrent updates to *different* elements, the two are indistinguishable.

**Anti-pattern.** Storing the set as one value re-ships the entire encoded set on every write, so
diff cost and per-datagram size both grow with cardinality — the failure mode documented for
sets-as-values on Riak ([arXiv:1605.06424](https://arxiv.org/abs/1605.06424)): treating a large
collection as a single opaque value defeats the underlying store's own delta-replication and GC, and
growth eventually exceeds a single message. Here that ceiling is the datagram limit (issue #230);
key-encoding avoids it structurally instead of pushing it further out.

More generally: prefer many small keys over few large composite values. The same per-element diff
granularity, tombstone reuse, and datagram-ceiling avoidance apply to any large composite (a map, a
list, a document) that would otherwise be reshipped whole on any change — not just sets. See
[`ARCHITECTURE.md`](ARCHITECTURE.md) §7 for why this is also part of why a pluggable CRDT `Resolve`
seam stays deferred.

**Scope boundary.** Every set sharing one store also shares its anti-entropy cadence, fingerprint
and conflict policy. Independently typed/named collections and partial replication belong to a
higher grid layer; this crate intentionally remains one fully replicated map/set engine.

## Documentation

- [`ARCHITECTURE.md`](ARCHITECTURE.md) — crate/module boundaries, ports, domain types, invariants
  and durable architecture decisions.
- [`SECURITY.md`](SECURITY.md) — the canonical threat model, key management/rotation, replay and
  wire-upgrade security requirements.
- [`POSITIONING.md`](POSITIONING.md) — the niche, competitor context and durable design axes.
- [`benches/README.md`](benches/README.md) — how to reproduce and interpret the benchmark suite.
- [`MIGRATING.md`](MIGRATING.md) — actions required when upgrading across incompatible releases.
- [`CHANGELOG.md`](CHANGELOG.md) — release history.
- [`CONTRIBUTING.md`](CONTRIBUTING.md) — development environment and verification gates.

## MSRV

**1.85**, declared as `rust-version` on all six manifests and checked mechanically by
`clippy::incompatible_msrv` (AGENTS.md §3).

## Security model

A cluster must choose one of two trust modes explicitly:

- `Config::with_cluster_key(key)` authenticates datagrams before deserialization, enables
  per-sender replay protection and derives the keyed RSOS fingerprint lift.
- `Config::with_insecure_no_key()` opts into unauthenticated operation for a trusted underlay.
  Without either choice, construction is rejected.

The optional `encryption` feature adds XChaCha20-Poly1305 payload confidentiality. The trust model
remains one shared cluster secret: there is no per-peer identity or forward secrecy.

Wire versions are strict rather than negotiated, so a wire-format change requires a coordinated
cluster upgrade. Cluster keys can be rotated without disabling authentication through the staged
two-key receive window.

The complete and canonical threat model — including malicious-key-holder limits, replay semantics,
key rotation, `zeroize` scope and metrics-endpoint exposure — is [`SECURITY.md`](SECURITY.md).
Release-specific wire compatibility actions live in [`MIGRATING.md`](MIGRATING.md).

## Persistence

By default a `ReplicatedMap` is held purely in memory: a process restart loses the entire dataset,
**including tombstones**. Losing tombstones is not just a durability problem — it is a correctness
hazard. A node that restarts empty behaves like a brand-new replica, re-learns already-deleted
values from peers, and can resurrect them (the tombstone-resurrection problem of issue #109).

Every store therefore always owns a persistence backend (the `Persistence` trait is mandatory).
What varies is *which* backend is plugged in:

- `InMemoryPersistence` (the default) keeps the latest snapshot in RAM and loses it on restart —
  i.e. the historical behaviour.
- `FileSnapshot` durably stores a single atomically-written snapshot on disk.

Plug in a durable backend between `new()` and `run()`. The previous state — live values,
tombstones, and the issue-#109 causal-stability bookkeeping (membership and per-tombstone
acknowledgments) — is reloaded **before** the node rejoins the gossip protocol, so a restart does
not look like a fresh, empty replica:

```rust
use std::sync::Arc;
use reconcile::{replicated_map::Config, FileSnapshot, ReplicatedMap};

let store = ReplicatedMap::new(Config::new(8080).with_insecure_no_key())
    .await
    .unwrap()
    .with_persistence(Arc::new(FileSnapshot::new("/var/lib/myapp/reconcile.snapshot")));
tokio::spawn(store.clone().run(CancellationToken::new())); // periodically snapshots in the background
```

The backend is pluggable: implement the `Persistence` trait to store snapshots in `redb`, `sled`,
S3, or any other medium.

A failed snapshot write (periodic or `snapshot_now()`) never panics or stops the node — it is
durability breaking silently while the process otherwise looks healthy, so it is surfaced three
ways: a `warn!` log, the `reconcile_persistence_failures_total`/`reconcile_persistence_failures_current`
metrics (see [Observability](#observability)), and `on_persistence_error`, a callback for an
operator's own alerting:

```rust
let store = ReplicatedMap::new(Config::new(8080).with_insecure_no_key())
    .await
    .unwrap()
    .with_persistence(Arc::new(FileSnapshot::new("/var/lib/myapp/reconcile.snapshot")))
    .on_persistence_error(|err| eprintln!("snapshot write failed: {err}"));
```

## Lifecycle and readiness

`run` takes a [`tokio_util::sync::CancellationToken`](https://docs.rs/tokio-util) and returns once
it fires, flushing a final snapshot first — the caller decides what triggers the token (a signal
handler, a shutdown channel, …) and gets a `RunOutcome` reporting whether that final flush
succeeded:

```rust
use tokio_util::sync::CancellationToken;

let shutdown = CancellationToken::new();
let handle = tokio::spawn(store.clone().run(shutdown.clone()));
// ... elsewhere, e.g. on SIGTERM: shutdown.cancel();
let outcome = handle.await.unwrap();
outcome.final_snapshot.expect("final snapshot should succeed");
```

A few more accessors answer questions a production deployment needs that `/metrics` alone can't —
notably telling apart "the process is up" from "this node is actually synchronizing" (a
Kubernetes readiness probe should check the latter; see `examples/k8s/`):

- `sync_state()` — rounds completed, when the last round/snapshot/successful discovery resolution
  happened, and the current peer count, bundled as one `SyncState` snapshot.
- `peers()` / `members()` — the gossip-routing peer set and the causal-stability membership set.
- `local_addr()` — the transport's actual bound address (useful when `Config::port` is `0`).
- `snapshot_now()` — force an out-of-band snapshot instead of waiting for
  `Config::with_snapshot_interval` (default `Some(5 s)`; `None` disables the periodic task
  entirely) to elapse and `Config::with_snapshot_change_threshold` (default `1`) to be met — a
  periodic wakeup only writes once that many changes have landed since the last snapshot, so an
  idle node does zero snapshot IO.

## Observability

The crate is instrumented with [`tracing`](https://docs.rs/tracing): the network engine, the
reconciliation rounds, and the message send/receive paths emit spans and events. As with any
library, `reconcile-rs` does **not** install a subscriber itself — your application does, e.g.
`tracing_subscriber::fmt().init()` (see `examples/demo.rs`).

Runtime metrics (throughput, latency, failure counts, and point-in-time state) are emitted through
the [`metrics`](https://docs.rs/metrics) facade, gated behind opt-in features so the default build
stays lean:

- `metrics` — emit counters, histograms, and gauges. Every name is a public, stable constant in
  `reconcile::metrics` (e.g. `reconcile::metrics::INSERTS_TOTAL`) — no other name set to depend on
  for a dashboard or alert. When this feature is off, every metric call site compiles to a no-op.
  Counters/histograms answer "how much has happened" (`reconcile_inserts_total`,
  `reconcile_updates_received_total`, `reconcile_bytes_sent_total`, `reconcile_send_failures_total`,
  `reconcile_datagrams_dropped_total`, `reconcile_persistence_failures_total`,
  `reconcile_discovery_failures_total`, `reconcile_round_duration_seconds`,
  `reconcile_broadcast_backpressure_total`, …); gauges answer "what is the state right now" — the
  seven an operator pages on:
  - `reconcile_peers_current` — size of the gossip-routing peer set.
  - `reconcile_members_current` — size of the causal-stability membership set.
  - `reconcile_entries_current` — live (non-tombstone) entries.
  - `reconcile_tombstones_current` — outstanding tombstones not yet garbage-collected.
  - `reconcile_bulk_dumps_in_flight` — bulk anti-entropy dumps in flight.
  - `reconcile_broadcasts_in_flight` — write-broadcast tasks in flight (#83, see
    [Write backpressure](#write-backpressure)).
  - `reconcile_persistence_failures_current` — consecutive snapshot failures since the last
    success; `0` while healthy (see [Persistence](#persistence)).
- `metrics-prometheus` — additionally provides `reconcile::prometheus` to install a Prometheus
  recorder and either serve a `/metrics` endpoint or render the exposition text yourself. Binding
  it to `0.0.0.0` (as in the example below, and in the `examples/k8s/` manifests) exposes it on
  every interface — see [`SECURITY.md`](SECURITY.md) for the exposure boundary:

```rust,no_run
# async fn run() -> Result<(), Box<dyn std::error::Error>> {
// Serve a /metrics HTTP endpoint (requires a Tokio runtime):
reconcile::prometheus::serve("0.0.0.0:9000".parse()?).await?;

// ...or install the recorder and render the text yourself through your own HTTP server:
let handle = reconcile::prometheus::install_recorder()?;
let body: String = handle.render();
# let _ = body;
# Ok(())
# }
```

Enable with `cargo build --features metrics-prometheus` (or list `metrics`/`metrics-prometheus`
in your dependency's `features`).

## Operational tuning

The defaults target a small, low-latency cluster. The main controls are:

| concern | configuration |
|---|---|
| background anti-entropy cadence | `reconcile_interval` |
| loss/repair latency | `repair_interval` |
| cross-network cadence/fan-out | `remote_interval`, `remote_fanout` |
| cold-sync pacing | `bulk_send_rate` |
| UDP socket buffering | `recv_buffer_size`, `send_buffer_size` |
| eager-write batching | `coalesce_window` |
| peer/state bounds | `max_peers`, `max_concurrent_bulk_dumps`, `max_concurrent_broadcasts` |
| application value ceiling | `max_value_size` |
| persistence cadence | `snapshot_interval`, `snapshot_change_threshold` |

All runtime-retunable counterparts are documented on `ReplicatedMap`. For measured RTT/loss,
cold-sync and contention behavior, use [`benches/README.md`](benches/README.md) rather than copying
benchmark conclusions into configuration documentation.

## Read replica (`ReadReplicaMap`)

For fleets with many *passive read replicas*, the per-value `Timestamp` a dated `ReplicatedMap`
keeps (for last-write-wins and the issue-#109 tombstone machinery) is pure overhead — a replica that
only consumes values never needs it. `ReadReplicaMap` is a **dateless, read-only replica** that
stores only the value (`State<V>`, ~24 bytes lighter per entry for a small payload) and still
converges with a dated cluster over the **same range-diff protocol, on the same UDP port**.

It stays issue-#109-safe: rather than replacing the timestamped reconciliation hash everywhere (which
would break tombstone causal stability and block GC forever), each dated node maintains an *additional
value-only projection* of its data and answers a read replica's value-only diff against that
projection. A read replica keeps no tombstone bookkeeping, never acknowledges tombstones, and is
never counted as a causal-stability member, so it cannot hold back a dated node's garbage collection.

```rust
use reconcile::{replicated_map::Config, ReadReplicaMap};

// Mirrors a dated cluster reachable at `dated_addr` on the same port.
let read_replica = ReadReplicaMap::<String, String>::new(Config::new(8080).with_insecure_no_key())
    .await
    .unwrap()
    .with_seed(dated_addr);
tokio::spawn(read_replica.clone().run());
// `read_replica.get(&key)` reflects the cluster's current values; deletions appear as `None`.
```

A read replica **always integrates** inbound updates and **never sends authoritative values** — it
is a sink, not a source. The dated↔dated path (and its wire format) is byte-for-byte unchanged.

It also takes the same discovery builders as `ReplicatedMap` — `with_discovery`,
`with_dns_discovery`, `with_discovery_interval` — so it is deployable on Kubernetes the same way
(see the Kubernetes section below). It is simpler there: a read replica holds no causal-stability
membership and no GC gate an absence could wrongly release, so a resolved address is just seeded as
a gossip peer, ages out after 60 s of silence like any other, and every `Discovery` implementation
is accepted regardless of `kind()` — there is no decommissioning step to guard (#30).

It also exposes the same lifecycle/introspection surface as a dated store, where it applies (#30):
`local_addr`, `sync_state` (rounds/`last_round_at`/peer count — no `last_snapshot_at`, since it
never persists, below), `peers`, `seed_peer` (the `&self` counterpart of `with_seed`), and
`set_reconcile_interval` to retune the idle re-initiation cadence live. Two `ReplicatedMap`
accessors have no counterpart, deliberately: `node_id()` (a read replica mints no `Timestamp`s, so
it has no HLC identity) and `members()` (it holds no causal-stability membership at all — see
above — so there is no stronger "GC-gating" peer tier to distinguish from `peers()`).

It never persists: a restart always cold-starts empty and re-syncs from the dated cluster over the
same protocol it uses steady-state. That re-sync is cheap by design (a value-only diff, no
timestamp), so a matching `with_persistence`/snapshot story was not built — the whole point of a
disposable read replica is that losing one costs nothing worse than a resync.

## Multiple geographical locations

A single cluster can span several geographical locations (issue #53). Each location is **just an
address range** — a network (CIDR) that groups co-located nodes. Size it to your topology: a whole
cloud region, an availability zone, or a single subnet. The model is intentionally flat (one CIDR
per location, no rack/host level and no hierarchy), because the CIDR mask already lets you pick the
granularity. Declare every network with `with_net` — **including this node's own**:

```rust
use reconcile::{replicated_map::Config, ReplicatedMap};

let config = Config::new(8080)
    .with_insecure_no_key()
    .with_listen_addr("10.1.0.7".parse().unwrap())
    .with_net("10.1.0.0/16".parse().unwrap())  // this node's network (contains listen_addr)
    .with_net("10.2.0.0/16".parse().unwrap())  // another location
    .with_net("10.3.0.0/16".parse().unwrap()); // and another
let store = ReplicatedMap::<String, String>::new(config).await.unwrap();
```

This node's **local** network is whichever declared net contains its `listen_addr`; all others are
**remote**. (If none contains it, the node logs a loud warning and treats only itself as local, so
every peer is remote.) The gossip is **geography-aware and decentralized** — there are no
relay/gateway nodes to configure or fail over:

- **Discovery** probes one random address in *every* network each round, so peers in all locations
  are auto-discovered (not just within a single flat CIDR).
- **Anti-entropy** sends the full range-diff comparison to *local-network* peers every round (fast
  intra-network convergence, as before) but to *remote* peers only every `remote_interval` rounds and
  to at most `remote_fanout` peers per network — bounding WAN traffic. Tune both with
  `with_remote_interval` / `with_remote_fanout`. Crucially, **repair is decoupled from net
  membership**: a peer learned by actual contact is always reconciled — peers matching no declared
  network fall into an `unclassified` bucket that is repaired on the same throttled cadence — so the
  declared topology only steers *discovery* and the local/remote split, never a peer's eligibility for
  repair.

A peer's network is derived purely from its IP address (`IpNet::contains`), so **the wire format is
unchanged** and a single-network cluster (no extra `with_net`) behaves exactly as before. Live writes
still propagate immediately to all known peers; only the periodic anti-entropy is throttled across
networks. Cross-network tombstone GC is correspondingly slower but remains strictly correct (it never
collects a tombstone before *every* member has acknowledged it).

### Runtime reconfiguration

The topology and gossip knobs can be retuned **live**, without recreating the node (which would
re-bind the socket and lose its identity) — useful for elastic deployments: opening a new region,
decommissioning one, or retuning WAN traffic on the fly. These `&self` methods on `ReplicatedMap`
take effect on the running `run()` loop:

```rust
let _: bool = store.add_net("10.4.0.0/16".parse().unwrap()); // start gossiping with a new location
let _: bool = store.remove_net("10.3.0.0/16".parse().unwrap()); // stop probing a retired one
store.set_nets(&nets).unwrap();                 // replace the whole topology at once
store.set_remote_interval(3);                   // retune cross-network cadence
store.set_remote_fanout(4);                     //   and fan-out
store.set_reconcile_interval(Duration::from_millis(500)); // retune the gossip cadence
store.set_repair_interval(Duration::from_millis(50));     //   and the RTT-scale repair timer (#23)
store.set_tombstone_timeout(Duration::from_secs(120));    // retune tombstone expiry
store.set_coalesce_window(Duration::from_millis(5));      // retune broadcast coalescing (#187)
```

The **local network is re-derived automatically** from the declared nets and the listen address on
every change, so it can never drift out of sync. Topology is per-node and **coordination-free** (no
cluster-wide agreement, no wire tag), and changing it is **safe by construction**: because repair is
decoupled from net membership (above), reshaping the topology can never orphan a known peer from
anti-entropy — the worst case is suboptimal WAN traffic, never silent divergence. Note that nets are
*not* a security boundary (authentication is the cluster key); a declared net only tells the node
which address range to send discovery probes into, so **only declare ranges you operate**. When
migrating a region, prefer `add_net(new)` *before* `remove_net(old)` so discovery keeps the cluster
well-connected throughout. `ReadReplicaMap` exposes the analogous `set_net`/`net` — but only a
single network, not the four other methods above: it runs no cross-network gossip to throttle
(`remote_interval`/`remote_fanout` are ignored, warned about at construction), so there is no
local/remote split for a second declared net to drive (#30).

## Kubernetes (DNS-based discovery)

The default discovery — probing random addresses in the declared networks — does not fit
Kubernetes, where pod IPs are ephemeral and drawn from a large cluster CIDR, so a random probe
almost never lands on a live pod. Instead, point the store at a **headless `Service`**
(`clusterIP: None`): its DNS name resolves to one address record per ready pod, giving every peer in
a single lookup — the canonical StatefulSet pattern, with **no Kubernetes API access and no RBAC**.

```rust
use reconcile::{replicated_map::Config, NodeId, ReplicatedMap};

let config = Config::new(8080)
    .with_insecure_no_key()
    .with_listen_addr(pod_ip)         // bind to the pod IP (downward API: status.podIP)
    .with_node_id(NodeId::new(id));   // stable id derived from the pod name
// Note: no `with_net` — discovery is purely DNS-driven in Kubernetes.
let store = ReplicatedMap::<String, String>::new(config)
    .await
    .unwrap()
    .with_dns_discovery("reconcile-headless.default.svc.cluster.local", 8080);
store.run(CancellationToken::new()).await;
```

While `run()`ning, a background task resolves the name every `with_discovery_interval` (default
5 s) and seeds every returned address as a known peer. A peer's *membership* (which gates tombstone
garbage collection) is **never** granted by DNS — it is still earned through a genuine authenticated
datagram — so an unverified or spoofable address can never block GC. When a pod is deleted it
disappears from DNS; after `with_discovery_miss_threshold` consecutive successful rounds with the
peer absent (default 3, i.e. ~15 s), it is **decommissioned** so its tombstones stop gating GC — but
only immediately if the peer holds no pending, unacknowledged tombstone. If it does, decommissioning
additionally requires the peer to have been continuously absent for
`with_discovery_decommission_floor` (default 10 minutes) — far above any DNS blip or readiness-probe
flap. This closes a resurrection hazard: without the floor, a spoofed resolver or flaky cluster DNS
could decommission a healthy peer, let GC collect a tombstone that peer never acked, and have the
returning peer push the deleted value back. A transient DNS failure is skipped entirely and never
counts as a miss, so a resolver blip cannot decommission a healthy peer. This works alongside the
geography-aware gossip above: declared
networks (if any) still steer the engine's own probing and the local/remote throttle, while DNS
feeds exact peer IPs into the always-reconciled set.

This discovery feeds peers regardless of declared topology, so a discovered peer is always
reconciled even with no `with_net`.

`ReadReplicaMap`/`ReadReplicaSet` take the same three builders (see "Read replica" above) — point
a fleet of passive replicas at the same headless `Service` a dated `ReplicatedMap` cluster uses.

Set the cluster key (`Config::with_cluster_key`, from a Kubernetes `Secret`) on every pod: without
it the cluster runs **unauthenticated** (see the Security model above). A complete, turnkey
Kubernetes example — the env-driven node (`examples/k8s/main.rs`, run with `cargo run --example
k8s`), the manifests (headless `Service`, `StatefulSet`, `ConfigMap`, example `Secret`), the
`Dockerfile`, and a local [kind](https://kind.sigs.k8s.io/) playground — lives under
[`examples/k8s/`](examples/k8s/). It is example/deployment scaffolding only and is excluded from
the published crate.

## FingerprintTreeMap

`FingerprintTreeMap` is the `rsos` crate's ordered, range-summarizable B-tree. It provides normal
ordered-map access plus `O(log n)` rank/select and range-`Aggregate` queries used by RBSR.

Each element is canonically encoded and lifted into a 256-bit BLAKE3-based additive fingerprint;
each node caches its subtree aggregate. The tree is persistent through `Arc` structural sharing,
so cloning a map is an `O(1)` snapshot and a write copy-on-writes only the changed path.

The reconciliation algorithm itself lives in the independent `rbsr` crate and talks to stores
through `RsosView`; it is not coupled to `FingerprintTreeMap` as a concrete backend.

See [`ARCHITECTURE.md`](ARCHITECTURE.md) §2/§5/§6 for structure and invariants,
[`POSITIONING.md`](POSITIONING.md) for the design context, and
[`benches/README.md`](benches/README.md) for current performance measurements.

## ReplicatedMap

`ReplicatedMap` exploits `FingerprintTreeMap`'s range aggregates to conduct a binary-search-like
comparison between two instances' collections; once a difference is found, the corresponding
key-value pairs are exchanged and conflicts resolved.

## Conflict resolution

Conflicts resolve last-write-wins (LWW), keyed on a **Hybrid Logical Clock** (HLC, Kulkarni et al.
2014) rather than a raw wall clock — a naive physical-clock LWW is unsafe: under clock skew the
node with the fastest clock always wins (silently losing causally-newer writes), and on *equal*
timestamps a non-commutative tie-break causes two replicas to keep diverging forever (their
timestamp-inclusive fingerprints never match, so the protocol re-exchanges the pair eternally). The
HLC fixes both: receiving a peer's value advances the local clock past it (no lost update under
bounded skew), and the total order `(physical, logical, node_id)` makes every replica pick the same
survivor — the merge is commutative, associative, idempotent (genuine Strong Eventual Consistency).
LWW still discards one of two *genuinely concurrent* writes by design; recovering both needs version
vectors or a CRDT, out of scope here.

**No conditional write.** Neither `compare_and_swap` nor `insert_if_absent` is exposed, and none is
planned as a cluster-wide primitive: this crate is AP + LWW + no consensus, so a cluster-wide CAS is
unsound by construction — two nodes can each observe the same expected value, each swap, each
broadcast, and LWW then silently keeps only the later-timestamped write, discarding the other (the
exact outcome a CAS exists to prevent). A method named `compare_and_swap` that quietly meant
"node-local only" would be worse than no method at all. What ships instead —
[`ReplicatedMap::update`]/[`upsert`]/[`get_or_insert_with`] — gives an atomicity guarantee only as
strong as *mutating an already-live key*; their rustdoc `# Atomicity` sections spell out exactly how
far each one reaches, including that `upsert`'s and `get_or_insert_with`'s insert-on-absent path is
plain last-write-wins, racy against a second local caller the same way it is against a second node.
A consensus-backed conditional write (a Raft metadata plane, a per-key lease) is out of scope for
this crate.

Each node uses a random `NodeId` by default; set an explicit one with
`Config::with_node_id(NodeId::new(id))` for a stable, reproducible ordering (e.g. in tests) — every
node in a cluster must use a distinct id. `Timestamp`'s type design (why `Hlc`/`PhysicalTime`/
`LogicalCounter`/`NodeId` are each their own newtype) is rationale, not usage — see
[`ARCHITECTURE.md`](ARCHITECTURE.md) §4.

| Benchmark | | Result |
|---|---|---|
| Send 1 insertion, then 1 removal, between two same-size instances | ![](img/perf-send.png) | ~122 µs, flat across N — bounded by local network transmission, not lookup cost. |
| Reconcile 1 insertion, then 1 removal, between two same-size instances | ![](img/perf-reconcile.png) | 240 µs → 640 µs as N goes from 10 to 1,000,000 — the full diff protocol must run to locate the difference. |

**Note:** benchmarked on loopback. A real network adds one round trip on top of the reconcile row and
half of one on top of the send row — measured, not estimated, by `benches/system.rs`'s injected-RTT
lane (`benches/README.md`). At 50 ms RTT that dominates: an anti-entropy convergence goes from 1 ms
to 51 ms. On a *lossy* path, a dropped datagram is retried on `Config::repair_interval`, an
RTT-scale timer (default 150 ms), rather than waiting for the next anti-entropy round
(`Config::reconcile_interval`, 1 s by default) to rediscover it (#23).

## Testing and coverage

CI runs formatting, documentation-structure/budget checks, domain-boundary checks, clippy/build,
nextest, doctests, benchmark compilation, packaging, dependency policy, public-API/semver checks,
coverage and in-diff mutation testing.

For local development, link the repository hooks and let them run the same tiered checks. The exact
commands and why each gate exists are canonical in [`AGENTS.md`](AGENTS.md) §3 and
[`CONTRIBUTING.md`](CONTRIBUTING.md); do not maintain a second copy here.
