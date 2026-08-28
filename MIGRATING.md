# Migration guide

## Unreleased

### `Config::with_net`/`with_nets` now return `Result`

`Config::with_net`/`with_nets` (#97, ARCHITECTURE.md §5 invariant 15) used to panic past
`MAX_NETS` declared networks, with a separate fallible `try_with_net` twin for a caller that
wanted to avoid that. The twin is gone; `with_net`/`with_nets` are now the sole, fallible entry
points:

```rust
// Before
let config = Config::new(8080).with_net(net);
// After
let config = Config::new(8080).with_net(net)?;
```

If you were already calling `try_with_net`, drop the `try_` prefix; the signature is unchanged.

### `ReplicatedMap::with_discovery`/`ReplicatedSet::with_discovery` now return `Result`

Both used to panic on a `Speculative` `Discovery::kind()` (#98, ARCHITECTURE.md §5 invariant 15).
There is no `try_with_discovery` twin — `with_discovery` is now the sole, fallible entry point:

```rust
// Before
let store = ReplicatedMap::new(config).await?.with_discovery(discovery);
// After
let store = ReplicatedMap::new(config).await?.with_discovery(discovery)?;
```

`with_dns_discovery` is unaffected (still `-> Self`): `DnsDiscovery::kind()` is unconditionally
`Authoritative`, so it can never hit this error.

### `Config::snapshot_interval` is now `Option<Duration>`

`Config::snapshot_interval` (and `Config::with_snapshot_interval`) changed from `Duration` to
`Option<Duration>` (#46). Action needed if you set it explicitly:

```rust
// Before
Config::new(8080).with_snapshot_interval(Duration::from_secs(30));
// After: `Some` wraps the same value, for the same periodic cadence.
Config::new(8080).with_snapshot_interval(Some(Duration::from_secs(30)));
```

`None` is new: it disables `ReplicatedMap::run`'s periodic background snapshot task entirely —
no wakeup, no snapshot IO, until an explicit `snapshot_now()` call. The default is
`Some(Duration::from_secs(5))`, matching the historical always-on 5 s cadence.

A new `Config::snapshot_change_threshold` (default `1`, set via
`with_snapshot_change_threshold`) additionally gates every periodic wakeup: it only writes once
that many changes (local writes, gossip-applied remote updates, tombstone GC removals) have
landed since the last snapshot. At the default, no action is needed — any single change is
enough to trigger a write, the same as the unconditional-write behavior before this change; only
a fully idle node between wakeups now skips the write. `snapshot_now()` always writes regardless
of this threshold.

### Wire format: a converged comparison round is now acked

`rsos::protocol_round`'s pure-SKIP outcome — every active range a peer sent already matched, so
there was nothing left to send back — now gets an explicit `ConvergenceAck` reply instead of silence
(#23). `WIRE_VERSION` bumped `2` → `3` accordingly, consuming one of the two wire tags #463
reserved for exactly this. **A node running this version cannot reconcile with one before it.**
Roll out by fully draining and replacing older nodes with this version — do not mix versions in
one cluster, same as every other `WIRE_VERSION` bump (README "Wire versioning").

No action needed on the application side: this only affects what crosses the wire between peers,
not any public type or method signature. `Config::repair_interval` (#23's other half, added
alongside this) is what actually benefits — a converged round now clears its pending retry
immediately instead of riding out a bounded, unacknowledged retry cycle.

### `ValueRef` no longer borrows a lock guard

`ReplicatedMap::get`/`ReadReplicaMap::get` return the same `ValueRef<...>` wrapper, but its shape
changed from `ValueRef<'a, V>` to `ValueRef<K, V>` (#34): it now owns an immutable `Arc` snapshot
of the whole backing tree plus the looked-up key, rather than a `parking_lot` lock guard. Action
needed only if you named the type explicitly (e.g. `let v: ValueRef<'_, i32> = store.get(&k)`) —
drop the lifetime argument. `Deref<Target = V>` behavior is unchanged.

One behavioral improvement falls out of this: holding a `ValueRef` no longer risks a deadlock
against a concurrent write on the same handle — the write installs a fresh tree behind a new
`Arc`, and the `ValueRef` still points at whichever tree was live when `get` returned it.

### `with_persistence` now returns `Result` instead of panicking

`ReplicatedMap::with_persistence`/`ReplicatedSet::with_persistence` changed from `Self` to
`Result<Self, replicated_map::PersistenceLoadError>` (#99): loading corrupted (`InvalidData`) or
retry-exhausted persisted state used to panic, now reports it instead. Action needed at every call
site:

```rust
// Before
let store = ReplicatedMap::<K, V>::new(config).await?.with_persistence(backend);
// After
let store = ReplicatedMap::<K, V>::new(config).await?.with_persistence(backend)?;
```

`PersistenceLoadError::Corrupt` wraps the original `io::Error` for an `InvalidData` failure (the
disk state is corrupt or from an incompatible format); `PersistenceLoadError::RetriesExhausted`
wraps it for any other error kind that persisted across every retry. There is no
`try_with_persistence` — this is the one method now, not a fallible twin alongside the old
panicking signature.

### `Authenticator::new`/`with_rotation` now return `Result` instead of panicking

`gossip::auth::Authenticator::new`/`with_rotation` changed from `Self` to
`Result<Self, gossip::auth::EncryptionFeatureDisabled>` (#100): requesting `encrypt = true` without
the crate's `encryption` feature enabled used to panic, now reports it instead. Action needed at
every call site:

```rust
// Before
let auth = Authenticator::new(key, encrypt);
// After
let auth = Authenticator::new(key, encrypt)?;
```

Reached through `reconcile` only via `Config::with_encryption`, which is itself gated on the
`encryption` feature — so a `reconcile` caller can never actually observe this error; it matters
only to a direct `gossip`/`reconcile-gossip` caller choosing `encrypt` at runtime independent of
that feature gate. There is no `try_new`/`try_with_rotation` — this is the one pair of methods now,
not a fallible twin alongside the old panicking signature.

## 0.3.0 to 0.4.0

Both changes below are additive-only-until-the-1.0-wire-freeze decisions (#382, #463) — the last
release either could land in without costing a 2.0 instead (`ARCHITECTURE.md` §5 invariants 1, 14).

### Wire format

`rsos::Fingerprint`'s wire encoding changed from four varint-encoded `u64` limbs to raw `[u8; 32]`
(#382) — `WIRE_VERSION` bumped `1` → `2` accordingly. **A `0.4.0` node cannot reconcile with a
`0.3.0` node.** Roll out by fully draining and replacing `0.3.0` nodes with `0.4.0` nodes — do not
mix versions in one cluster, same as every other `WIRE_VERSION` bump (README "Wire versioning").

Wire tags 5 and 6 are now reserved, skippable slots (#463) — purely additive, no action needed:
a `0.3.0` node already drops an unknown-tag message as `malformed` rather than crashing, and no
message ships on either tag yet. This is forward-compatibility groundwork for a future release, not
something this one exercises.

## 0.2.1 to 0.3.0

`reconcile 0.2.1` predates the workspace split (AGENTS.md §11): it vendors what are now `rsos`,
`rbsr`, `lww-register` and `gossip` directly. `0.3.0` is not wire- or disk-compatible with it —
read this before upgrading a running cluster.

### Renamed types

| 0.2.1 | 0.3.0 |
|---|---|
| `ReconcileStore` | `ReplicatedMap` |
| `HRTree` | `FingerprintTreeMap` |

`Entry`/`State` were refactored (#243) and several `just_*` accessors were demoted (#180) — see
`ARCHITECTURE.md` for the current shape.

### Dependency line

`reconcile` now depends on four workspace crates, all published to crates.io: `rsos`, `rbsr`,
`lww-register` and `gossip` (published under the name `reconcile-gossip` — the plain name was
taken; source still says `use gossip::…`). No action needed if you only depend on `reconcile` —
`cargo add reconcile` pulls them in transitively.

### Wire format

The gossip wire format changed. **A `0.3.0` node cannot reconcile with a `0.2.1` node.** Roll out
by fully draining and replacing `0.2.1` nodes with `0.3.0` nodes — do not mix versions in one
cluster.

### On-disk snapshots

Snapshots now carry an 8-byte header (`RCNL` magic + little-endian `u32` format version,
`src/snapshot.rs`). **A pre-0.3.0 snapshot is rejected at load time**, not silently misread — you
get an `InvalidData` I/O error naming the mismatch. There is no automatic converter: delete the old
snapshot file and let the node re-seed its state from the cluster via anti-entropy, or drain the
data through the `0.2.1` public API before deleting if you need it preserved outside the cluster.
