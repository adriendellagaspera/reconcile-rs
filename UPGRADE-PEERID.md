# Signed `PeerId` v2 rollout and upgrade (v0.x)

This release adds an **opt-in** `signed_replica::SignedReplica<K,V,A>` actor. It does **not**
silently change `ReplicatedMap::new`, the legacy UDP `Transport` contract, or the
historical IP-based causal state. The old runtime and the new signed runtime are
**different replication protocols**. They cannot join one cluster, exchange their
wire messages, or share their on-disk causal snapshot format.

## What changes

| Concern | Existing `ReplicatedMap` | Opt-in `SignedReplica` |
|---|---|---|
| Peer authentication | Shared cluster key, *not* an individual signer | Allowlisted Ed25519 `PeerId` **and** inner cluster MAC |
| Transport address | `SocketAddr` / `IpAddr` | Arbitrary caller-owned datagram destination `A: Clone + Eq` |
| Return route | IP-addressed transport | Signed, nonce-challenged exact source route |
| Causal GC membership | Legacy IP identities | Configured **authoritative `PeerId` roster** |
| Tombstone ACK | IP and exact-version token | Verified signer and exact-version token |
| Persistence | v1 IP format | v2 `PeerId` format, including incremental base/delta/manifest |
| Replication | RSOS/RBSR range reconciliation | Bounded, round-robin dated record sweep |
| Value-only replicas | `ReadReplicaMap` | Provisioned `readers` and `read_only` flag |

Neither `PeerId` nor an HLC `NodeId` is a network endpoint. A signed datagram
proves signer possession, but does **not** establish a returnable route, causal
membership, data freshness, or a tombstone acknowledgement on its own. Signed
admission verifies the envelope signature and allowlist, inner MAC/version,
logical peer capacity and per-signer replay **before** route challenge, fragment
allocation, decode and application state.

Configured authoritative members conservatively gate GC **even when disconnected
or never observed**. Inbound traffic may update data or supply ACKs only after
individual signature/role verification and return-route confirmation. Read-only
signers never become causal blockers or supply authoritative tombstone ACKs.
Decommission is explicit and must be coordinated; network silence does not
decommission a member.

## Mandatory coordinated transition

There is deliberately **no automatic v1 IP-to-`PeerId` conversion**, offline
migration command, mixed-version bridge or downgrade rollback. To adopt v2:

1. Stop writes and network participation on **every authoritative replica**,
   including any value-only consumers. Complete an operational backup before
   choosing how to handle existing tombstones and causal history.
2. If the existing data must survive, do **not** reset any node independently.
   Use a separately audited, cluster-wide conversion preserving each entry,
   LWW timestamp, tombstone, authoritative membership and exact ACK version.
   This repository does not ship such a converter.
3. For a **fresh signed cluster**, stop the *entire* fleet and intentionally
   discard the historical data and causal state together, across all nodes.
   Remove old v1 RCNL single-file snapshots and any v1 RCNB/RCND/RCNM
   incremental base/delta/manifest store for **every** node. Do not wipe
   one node and let it rejoin an old cluster: it may forget tombstones and
   resurrect data.
4. Provision a unique, stable Ed25519 private seed for each node (store as a
   secret, never derive from the cluster PSK or an IP). Provision the same
   authoritative public-key roster across the fleet, excluding the local
   signer; configure readers separately. Configure stable clock `NodeId`
   independently of `PeerId`.
5. Configure an application-owned datagram substrate and make it deliver
   `RoutedDatagram<A>` bytes, source address and replies correctly. Perform
   signed `probe` / `receive` return-route handshakes in **both** directions;
   call `reconcile` periodically for anti-entropy and missing-fragment retry.
   The legacy `UdpTransport` is not automatically selected for this actor.
   Returning a `RoutedDatagram<A>` does not mean it was delivered: the caller
   must submit its bytes to the correct substrate destination, handle send
   failures, and feed received datagrams back into the actor.
6. Use `Persistence<K,V,PeerId>` (for example `FileSnapshot`) and **fail
   startup** if its load fails. V2 loads must reject old v1 shapes and corrupt
   or incomplete committed generations with `InvalidData`. Never replace a
   failed load with an empty store. Schedule `snapshot_now` and handle
   persistence errors.
7. Confirm convergence, deleted-key retention, missing-fragment retransmission,
   exact-version ACK coverage, route relocation and restarts on a staging
   cluster before allowing the authoritative group to serve production data.

Changing the provisioned roster is a **causal operation**, not a discovery
timeout. Use explicit `decommission`, durably snapshot the updated roster
and coordinate revocation on all nodes. On restart, the saved authoritative
roster must agree with the configured roster; a mismatch is an error, not
an excuse to discard old durable membership. A full-cluster reset is required
to reinitialize incompatible history intentionally.

## Deployment and security boundaries

- The `SignedReplica` actor always requires the cluster key and signed
  allowlisted datagrams. It cannot silently fall back to legacy unsigned or
  PSK-only ingress.
- The actor currently constructs **MAC-only** inner authentication. Its Ed25519
  signature authenticates identity but **does not encrypt payloads**. Run it over
  a confidential transport or protect the underlying network when data secrecy
  is required; the legacy `with_encryption` switch is not an implicit setting
  for signed mode.
- Return-route challenges bind a signer to a source route but do not grant
  membership. Only a dated protocol message from an authorized full replica
  can mutate authoritative values or provide an ACK. Duplicate, stale or
  unknown-signer datagrams must not allocate fragments, advance HLC state, or
  reopen the signer's replay history on route change.
- Tombstone GC is **not** automatic in the signed actor: operators must enforce
  their deletion-retention period and invoke `collect_if_stable` only after
  all authoritative peers have acknowledged exactly the current version.
  A disconnected authority remains a GC blocker until it rejoins or is
  explicitly decommissioned.
- The new signed actor is an intentionally bounded **baseline**, not a
  drop-in high-throughput successor to RSOS/RBSR. Each `reconcile` call
  currently sends at most one dated entry per confirmed peer, with
  round-robin progress and a cap on fragment work. `SignedConfig::max_frames_per_tick`
  defaults to 8192 (valid range 1–8192); keep it large enough for the
  largest permitted record after signed-MTU fragmentation. A record requiring
  more frames than the configured tick budget is rejected before insertion.
  Convergence of large
  datasets depends strongly on the caller's cadence. Benchmark workload
  size, contact interruptions and time to converge; do not assume legacy
  range-reconciliation throughput or optimize out the causal GC barrier.

## Explicit non-goals

No implicit conversion of `IpAddr` to `PeerId`, no downgrade/rollback
compatibility, no automatic recovery of discarded historical causal state, no
bundled QUIC/radio/optical driver, no automatic IP discovery for signed peers,
and no claims that the cluster PSK individually authenticates a signer.
