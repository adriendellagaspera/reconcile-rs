# Security

Only the latest published release line receives security fixes. Report vulnerabilities privately through GitHub **Security → Report a vulnerability**.

## Trust and confidentiality

Each authoritative replica eventually receives **the entire dataset**. Membership and network reachability are security boundaries.

- `Config::with_cluster_key(key)` enables shared-key authentication, replay protection and a separately keyed range-fingerprint lift. Anyone holding the cluster key can impersonate another holder; there is **no individual peer authentication or forward secrecy** from the PSK.
- `Config::with_insecure_no_key()` disables authentication. Any reachable host can forge or replay updates and tombstones; timestamp bounding is not authentication. Use only on a trusted underlay.
- `with_encryption()`, with the `encryption` feature, provides XChaCha20-Poly1305 payload confidentiality, not peer identity or forward secrecy. Without it, protect confidentiality through the underlay.
- The 256-bit additive range fingerprint uses a keyed lift with a cluster key; this prevents outsider precomputation, **not attacks by an insider** who possesses that key.
- **Opt-in signed-only `SignedReplica`:** `PeerId` is an allowlisted Ed25519 public key with per-datagram proof, inner MAC, signer-keyed replay, nonce-confirmed return route and `PeerId`-keyed durable causal state. It is a separate caller-driven actor, not the legacy `ReplicatedMap` ingress. Signature verification **alone** never grants a route, a dated update or an ACK. Full authoritative signers are provisioned separately from read-only signers; every configured authority remains a GC blocker even while offline.

## Replay and resource admission

Authenticated datagrams carry protected sequence/time metadata. Duplicates, stale sequences and out-of-window timestamps are rejected. Replay state outlives transient peer membership and is capped by `Config::max_replay_senders`. At capacity, known senders retain their replay windows; new senders are dropped until stale state expires. Fresh tracked replay history must never be evicted to admit an unknown sender.

Fragment and selective-recovery state is allocated only **after** authentication, strict version check, topology admission and replay validation. `Config::framing` bounds logical-message size, fragments, per-peer/global retained messages and bytes, expiry, outgoing recovery, missing-range reports and recovery rounds. Transfer content hashes support resumability but are **not** authenticity proofs: assembled contents are checked against the identifier before deserialization, with authority deriving from authenticated datagrams. Invalid recovery controls cannot trigger retransmission; failed recovery falls back to anti-entropy.

## Key management and rotation

Provision keys securely outside source control. Rotate a shared cluster key in fleet-wide phases:

| phase | configuration | sends with | accepts |
|---|---|---|---|
| prepare | `with_cluster_key_rotation(old, new)` | old | old + new |
| switch | `with_cluster_key_rotation(new, old)` | new | new + old |
| retire | `with_cluster_key(new)` | new | new |

Finish each phase across the fleet before proceeding. During a mixed-primary phase, the derived keyed fingerprints differ and may induce redundant reconciliation. The `zeroize` feature clears library-owned key bytes on drop, not caller-owned copies. For the opt-in signed runtime, provision **stable, unique, independently stored Ed25519 seeds** and the explicit public-key roster; never derive signing keys from the PSK or infer `PeerId` from IP. The signed actor uses MAC-only inner authentication and does **not** encrypt payloads; protect data with a confidential substrate when needed.

## Wire compatibility

Wire versions are strict and **not negotiated**: incompatible versions cannot communicate without coordinated upgrades. Legacy selective recovery is capability-advertised using a reserved protocol tag. The opt-in signed `RCPE` envelope is a **distinct signed-only runtime wire protocol**, never negotiated or downgraded to legacy traffic. Causal state persisted for signed peers uses RCNL/RCNB/RCND/RCNM **v2** and is incompatible with legacy IP-state **v1**; loads fail explicitly rather than reset. See [coordinated upgrade guidance](UPGRADE-PEERID.md).

## Operations

Restrict gossip UDP to intended nodes. Use a cluster key unless the network supplies the authentication boundary; enable payload encryption when the network does not supply confidentiality. Size peer, replay-sender, reassembly and recovery bounds for the deployment. Coordinate wire upgrades and key rotation across all nodes. The optional Prometheus HTTP endpoint has no authentication: restrict binding/network access. The new `SignedReplica` is an actor; application code supplies its transport and drives both directions of signed probing and the anti-entropy cadence. Physical tombstone collection is disabled in both runtimes. Receipt ACKs, retention and local checkpoints do not fence stale application history after rollback; tombstones and their ACK metadata remain retained without a time bound. Read-only replicas never count as authorities. Refer to the [signed rollout guide](UPGRADE-PEERID.md).

Authentication/encryption bypass, replay failures, unauthorized state mutation/disclosure outside explicitly insecure mode, and remotely reachable panics are security issues. Shared-key impersonation and UDP source spoofability are limitations of the **legacy runtime**; signed `PeerId` identifies a verified signer, not a transport endpoint. Insecure mode and unencrypted signed payloads are documented limitations, **not** protections.
