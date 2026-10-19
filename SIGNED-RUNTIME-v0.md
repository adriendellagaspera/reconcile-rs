# Signed runtime v0 — identity and disk-format break

This release introduces the **opt-in** `reconcile::signed_replica::SignedReplica` actor.

The original UDP `ReplicatedMap` remains an independent legacy IP-keyed runtime. It
does **not** authenticate individual Ed25519 peers and still uses the IP-keyed v1
snapshot schema. Never mix its datagrams, membership or persisted state with the
new signed-only mode. There is no automatic migration or rollback bridge.

## Provisioning and startup

1. Provision a unique **stable** 32-byte Ed25519 secret seed per node, outside
   the application and not derived from the shared cluster MAC key. Keep this
   seed stable across node restarts. Safeguard and back up the secret.
2. Provision exactly the set of authorized **authoritative** `PeerId` public keys
   and a separate reader allowlist. Every authoritative node must agree on
   the full roster. A read-only replica cannot originate dated updates or ACKs.
3. Configure a shared `ClusterKey`; this supplements, but never replaces,
   per-peer signing. Configure bounded signed framing for the physical MTU.
4. Construct `SignedReplica::new(..., Arc<dyn Persistence<K,V,PeerId>>)`, using
   `FileSnapshot` for durable deployments. Snapshot loading completes
   **before ingress starts**. An invalid, incompatible, or corrupted v2
   snapshot is a startup error, never an empty store.
5. The application owns the datagram substrate. Pass its opaque endpoint type
   `A: Clone + Eq` to the runtime; deliver the signed outbound
   `RoutedDatagram<A>` bytes, call `receive(source, bytes, now)` for arrivals,
   send its challenge/ACK responses and periodically call `reconcile(now)`.
   Use `probe(address)` with explicitly provisioned seed endpoints to begin
   return-route validation. Do not interpret an endpoint as a `PeerId`.
6. Call `snapshot_now()` and check the result before planned shutdown.
   Periodic snapshot cadence and graceful persistence flushing are the
   application's responsibility. All signed messages must be sent without
   passing through the historical unsigned UDP receive loop.

## Breaking v0.x on-disk identities

Signed persistence uses **v2** for every affected object: single-file `RCNL`,
materialized `RCNB`, delta `RCND` and manifest `RCNM`. The legacy
IP-keyed **v1** data is not recognized as signed data, including snapshots
with empty member sets.

**Upgrade requires an explicit, coordinated fresh-cluster reset** (or a
separately audited offline conversion): stop *every* node, ensure old
tombstones/deletions cannot resurface, archive old durable data externally,
provision the new authoritative signer roster, initialize fresh signed-v2
storage together, and only then start the signed cluster. Never wipe a
single replica's causal history and let it rejoin others holding older
versions: forgotten tombstone ACKs can permit resurrection.

## Causal membership and GC

The signed mode records every provisioned authoritative signer as a causal
blocker from first startup, **including offline peers**. Only authenticated
signers on returnability-confirmed routes can contribute version-exact
tombstone ACKs. GC is never automatic on disconnection; the application
may explicitly request physical collection only after its retention policy
has elapsed and every current authority ACKs that precise version.

`decommission(PeerId)` is an explicit membership operation, not a timeout.
Persist this change and coordinate removal of the retired key from every
node's trusted roster before restart. Reintroducing a retired replica after
causal history was collected requires a coordinated reset.

Signed read replicas remain outside authoritative membership and cannot
gate GC. A route change does not affect a member ID, replay history or
acknowledgement identity.

## Limitations of this v0 surface

`SignedReplica` is a **caller-driven actor**, not the legacy
`ReplicatedMap::run()` daemon. A host application must provide scheduling,
physical I/O, backpressure, delivery and restart supervision; the runtime
enforces per-datagram framing/reassembly and bounded per-tick reconciliation.
It is not a managed server, a sharded database, a mixed-version bridge or
an arbitrary transport implementation.
