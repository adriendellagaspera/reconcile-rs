# Changelog

All notable changes to this project are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); version numbers follow
[Semantic Versioning](https://semver.org/) (pre-1.0: a minor bump can carry breaking changes).

## [Unreleased]

### Changed

- **BREAKING**: `Config::snapshot_interval` is now `Option<Duration>` (#46, re-landing the
  configurability half of `akvize/reconcile-rs#218`, which never merged) — `None` disables
  `ReplicatedMap::run`'s periodic background snapshot task entirely, leaving only an explicit
  `snapshot_now()` call to persist. `Config::with_snapshot_interval` takes the new `Option`
  directly. A new `Config::snapshot_change_threshold` (default `1`) additionally gates *every*
  periodic wakeup, `Some` interval or not: it only writes once that many changes (local writes,
  gossip-applied remote updates, and tombstone GC removals) have landed since the last snapshot,
  so a fully idle node between wakeups does zero snapshot IO — set via
  `Config::with_snapshot_change_threshold`. `snapshot_now()` is unaffected by the threshold; it
  always writes when called. See [MIGRATING.md](MIGRATING.md).
- **BREAKING**: `rbsr::protocol_round`/`protocol_round_with_policy` take a new final `&mut
  rand::rngs::StdRng` argument (#7, migrated from `akvize/reconcile-rs#502`): a session-random
  shift on which block of a SPLIT's fixed-stride cut is undersized, so two sessions over identical
  stores draw different boundaries below the outer range — defense against a Wagner plant crafted
  to land on a deterministic cut point (`ARCHITECTURE.md` §7, "Defense against a correlated false
  SKIP"). Injected, not ambient: `Replica`/`ReadReplicaMap` pass their existing session RNG
  (`src/replica/dispatch.rs`, `src/read_replica_map/write.rs`), unchanged for callers of the
  `reconcile` facade. Split *count* is unaffected by the shift, so wire volume and round count are
  unchanged; only a direct `rbsr` caller's call sites need the new argument. `rbsr` bumped to
  `0.2.0` for it, per AGENTS.md §11's pre-1.0 minor-for-breaking convention.
- **BREAKING**: a comparison round that converges with nothing else to report now gets an explicit
  `ConvergenceAck` reply instead of silence (#23) — `WIRE_VERSION` bumped `2` → `3` accordingly,
  consuming one of the two wire tags #463 reserved for exactly this. **A node running this version
  cannot reconcile with one before it.** Roll out by fully draining and replacing older nodes — do
  not mix versions in one cluster, same as every other `WIRE_VERSION` bump (README "Wire
  versioning"). See [MIGRATING.md](MIGRATING.md).
- **BREAKING**: `ValueRef<'a, V>` is now `ValueRef<K, V>` (#34) — it owns an immutable `Arc`
  snapshot of the backing tree plus the looked-up key instead of a lock guard, so holding one no
  longer risks a deadlock against a concurrent write on the same handle. See
  [MIGRATING.md](MIGRATING.md).
- A configured cluster key now also keys the range-fingerprint lift (#19, migrated from
  `akvize/reconcile-rs#337`): `Replica`/`ReadReplicaMap` derive an independent BLAKE3 subkey from
  `Config::cluster_key` (`ClusterKey::derive_lift_key`) and pass it to `rsos::FingerprintTreeMap`,
  closing the Wagner-grinding gap on `rsos::Fingerprint` (README "Security model") for anyone who is
  not a cluster-key holder. Purely additive at the API and wire-encoding level — no `WIRE_VERSION`
  bump, `Fingerprint` is still 32 bytes either way — but **operationally a coordinated-rollout
  change for an authenticated cluster**: a node upgrading before its peers computes different
  fingerprints for identical data until every node is upgraded (safe — every range just looks
  different meanwhile, nothing is lost — but wasteful). A cluster running
  `Config::with_insecure_no_key()` is unaffected either way; the lift stays unkeyed exactly as
  before.

### Added

- `ReplicatedMap::try_with_discovery` and `replicated_map::NotAuthoritative` (#98): a non-panicking
  twin of `with_discovery`, which keeps its panicking signature (ARCHITECTURE.md §5 invariant 15) —
  a `Speculative` `Discovery::kind()` is a caught-at-startup bug in the impl the developer chose,
  not runtime data a peer or attacker ever influences.
- `Config::max_concurrent_broadcasts`/`with_max_concurrent_broadcasts` (default 1024, #83): bounds
  the number of concurrently in-flight write-broadcast tasks, the egress-side counterpart of
  `Config::max_concurrent_bulk_dumps`; the `reconcile_broadcasts_in_flight` gauge and
  `reconcile_broadcast_backpressure_total` counter (`metrics` feature) expose the depth and every
  rejection/skip. `ReplicatedMap::insert`/`update`/`insert_bulk` keep their infallible signatures —
  at the budget they skip only that call's eager broadcast, recovered by the next anti-entropy
  round or repair retry (#23). `ReplicatedMap::try_insert`/`try_update` are new, additive,
  all-or-nothing counterparts that instead reject with `replicated_map::Backpressure` when the
  budget is exhausted, for a caller that wants to know rather than rely on that backstop — see
  README "Write backpressure".
- `Config::repair_interval`/`with_repair_interval`, and
  `ReplicatedMap`/`ReplicatedSet::set_repair_interval` (#23): an RTT-scale timer (default 150 ms)
  that repairs a comparison round or bulk-transfer datagram lost in flight, decoupled from
  `reconcile_interval`'s background anti-entropy cadence — see README "Reconciliation interval
  floor".
- `ReplicatedMap::snapshot()`/`value_snapshot()` and `ReadReplicaMap::snapshot()` (#34): zero-copy
  `Arc` snapshots of the backing tree for scanning with `rsos`'s existing `iter`/`range`, no lock
  and no lifetime tied to the handle.
- `rsos::LiftKey`, `rsos::lift_keyed`, `rsos::digest_keyed`, and
  `FingerprintTreeMap::with_lift_key` (#19): the keyed-lift primitive above, usable directly by a
  third party driving `rsos`/`rbsr` without the `reconcile` facade.
- `gossip::auth::ClusterKey::derive_lift_key` (#19): the BLAKE3 `derive_key` subkey `reconcile` now
  feeds to `rsos::LiftKey`, independent of the datagram MAC's own use of the same cluster key.
- `ReadReplicaMap::local_addr()`/`sync_state()`/`peers()`/`seed_peer()`/`set_reconcile_interval()`
  (#30): the introspection/lifecycle accessors `ReplicatedMap` already had (#292), closing the gap
  a caller hit trying to build a readiness probe for a read-replica deployment the same way it
  would for a dated one. `sync_state()` returns a `ReadSyncState` (no `last_snapshot_at`: a read
  replica deliberately never persists). No `node_id()`/`members()` counterpart — a read replica
  mints no timestamps and holds no causal-stability membership, so neither concept applies.
- `FingerprintTreeMap::from_sorted_iter`/`from_sorted_iter_keyed` (#51): a bottom-up bulk build
  from already-sorted, duplicate-free input, for a caller that knows its dataset up front (initial
  load, snapshot recovery) and wants to skip both the `O(n log n)` sort `FromIterator::collect()`
  does and the incremental split/rebalance cost of `n` individual `insert` calls. Same resulting
  `aggregate()` as the equivalent serial inserts regardless of the two builds' internal tree shape
  — `Aggregate` is a commutative monoid over the element set.

### Changed

- `rsos::fingerprint_tree_map::Node`'s `children` field is now `Box`-indirected (#47): a leaf —
  the large majority of nodes — no longer carries a full `MAX_CAPACITY + 1`-element array's worth
  of unused inline space just to represent "no children". `Node` is crate-private, so this is not
  a public-API or wire change.

### Fixed

- A receiver-side guard now suppresses re-initiating a full comparison round with a peer whose
  paced bulk transfer might still legitimately be in progress (#85, `akvize/reconcile-rs#178`):
  `start_reconciliation` leaves that peer out of a round's targets while its most recently received
  `EntryUpdate` batch is within `repair_interval` old. Below this fix, a `reconcile_interval` set
  near or under the pacing gap between a holder's datagrams re-issued a full diff mid-transfer on
  every idle-timeout lull, doubling cold-sync traffic — see README "Reconciliation interval floor"
  and `Config::reconcile_interval`/`bulk_send_rate`'s docs, updated to match. No wire-format change.

## [0.4.0] - 2026-08-22

Publishes the two decisions found auditing pre-freeze wire gaps (#382, #463) — both
additive-only-until-this-point, so this is the last release either could land in without costing a
2.0 — and re-baselines the registry snapshot `cargo semver-checks` (#311) diffs against, which
`v0.3.0` and Gate A's (#206) subsequent breaking changes left stale. See
[MIGRATING.md](MIGRATING.md).

### Changed

- **BREAKING**: `rsos::Fingerprint`'s wire encoding is now raw `[u8; 32]` instead of four
  varint-encoded `u64` limbs (#382) — `WIRE_VERSION` bumped `1` → `2` accordingly, since an older
  peer would otherwise silently misdecode the new fixed-width encoding rather than being rejected.
- Wire tags 5 and 6 are now reserved, skippable message slots (#463) — additive, no `WIRE_VERSION`
  bump needed for this part; see README "Wire versioning" for exactly what it does and does not buy.

### Fixed

- `rbsr` bumped to `0.1.1`: its published `0.1.0` manifest still pinned `rsos = "0.3.0"`, and
  `tags.yml` skips republishing a crate whose version is already on the registry — so bumping only
  the dependency pin in this release's first pass left the *published* `rbsr` depending on the old
  `rsos`, while `reconcile` now depends on the new one directly. Two incompatible copies of the
  `Rsos`/`RsosView` traits in one dependency graph failed `reconcile`'s own `cargo publish`
  verification build before any of it reached crates.io. `rbsr`'s own version line stays independent
  of `reconcile`'s (#308) — this bump is solely to force a republish with the corrected pin.

## [0.3.0] - 2026-08-19

`0.2.1` predates the workspace split — this is the first release of the split shape. See
[MIGRATING.md](MIGRATING.md) for the full upgrade path; the highlights:

### Changed

- **BREAKING**: workspace split into five crates — `rsos`, `rbsr`, `lww-register`,
  `reconcile-gossip` (imported as `gossip`) and the `reconcile` facade. See `ARCHITECTURE.md` §2.
- **BREAKING**: `ReconcileStore` renamed to `ReplicatedMap`; `HRTree` renamed to
  `FingerprintTreeMap`.
- **BREAKING**: `Entry`/`State` domain-type refactor (#243); several `just_*` accessors demoted
  (#180).
- **BREAKING**: gossip wire format changed — a `0.3.0` node cannot reconcile with a `0.2.1` node.
- **BREAKING**: on-disk snapshot format now carries a magic + format-version header; a pre-0.3.0
  snapshot is rejected at load time rather than silently misread (`src/snapshot.rs`).

### Added

- `CHANGELOG.md`, `SECURITY.md`, `MIGRATING.md`.
- `rust-version` (MSRV 1.85) declared on all five manifests, plus `docs.rs` build metadata so
  feature-gated items render on the published docs.

## [0.2.1] - 2026-06-12

Last release before the workspace split. See the
[GitHub release notes](https://github.com/Akvize/reconcile-rs/releases/tag/v0.2.1).

[Unreleased]: https://github.com/Akvize/reconcile-rs/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/Akvize/reconcile-rs/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/Akvize/reconcile-rs/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/Akvize/reconcile-rs/releases/tag/v0.2.1
