# Changelog

All notable changes to this project are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); version numbers follow
[Semantic Versioning](https://semver.org/) (pre-1.0: a minor bump can carry breaking changes).

## [Unreleased]

### Changed

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

- `ReplicatedMap::snapshot()`/`value_snapshot()` and `ReadReplicaMap::snapshot()` (#34): zero-copy
  `Arc` snapshots of the backing tree for scanning with `rsos`'s existing `iter`/`range`, no lock
  and no lifetime tied to the handle.
- `rsos::LiftKey`, `rsos::lift_keyed`, `rsos::digest_keyed`, and
  `FingerprintTreeMap::with_lift_key` (#19): the keyed-lift primitive above, usable directly by a
  third party driving `rsos`/`rbsr` without the `reconcile` facade.
- `gossip::auth::ClusterKey::derive_lift_key` (#19): the BLAKE3 `derive_key` subkey `reconcile` now
  feeds to `rsos::LiftKey`, independent of the datagram MAC's own use of the same cluster key.

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
