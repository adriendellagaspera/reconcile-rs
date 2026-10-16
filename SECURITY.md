# Security

Only the latest published release line receives security fixes. Report vulnerabilities privately through GitHub **Security → Report a vulnerability**.

## Trust and confidentiality

Each authoritative replica eventually receives **the entire dataset**. Membership and network reachability are security boundaries.

- `Config::with_cluster_key(key)` enables shared-key authentication, replay protection and a separately keyed range-fingerprint lift. Anyone holding the cluster key can impersonate another holder; there is **no individual peer authentication or forward secrecy** from the PSK.
- `Config::with_insecure_no_key()` disables authentication. Any reachable host can forge or replay updates and tombstones; timestamp bounding is not authentication. Use only on a trusted underlay.
- `with_encryption()`, with the `encryption` feature, provides XChaCha20-Poly1305 payload confidentiality, not peer identity or forward secrecy. Without it, protect confidentiality through the underlay.
- The 256-bit additive range fingerprint uses a keyed lift with a cluster key; this prevents outsider precomputation, **not attacks by an insider** who possesses that key.
- `PeerId` in the deployed runtime remains an IP-associated label, not authenticated identity. The separate Ed25519 `peer::identity` and signed-envelope primitives require an allowlist but **are not connected to live replica ingress, replay or membership**. Successful envelope signature verification alone does not grant causal membership and does not prevent replay.

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

Finish each phase across the fleet before proceeding. During a mixed-primary phase, the derived keyed fingerprints differ and may induce redundant reconciliation. The `zeroize` feature clears library-owned key bytes on drop, not caller-owned copies. Provision stable Ed25519 seeds securely for any future signed deployment; it is not currently a runtime mode.

## Wire compatibility

Wire versions are strict and **not negotiated**: incompatible versions cannot communicate without coordinated upgrades. Selective recovery is an additive capability advertised through the existing reserved protocol tag; it is never sent to peers that have not advertised support. The opt-in signed `RCPE` envelope is a separate format not recognized by existing runtime receivers. It must not be represented as active or automatically downgraded to unsigned traffic.

## Operations

Restrict gossip UDP to intended nodes. Use a cluster key unless the network supplies the authentication boundary; enable payload encryption when the network does not supply confidentiality. Size peer, replay-sender, reassembly and recovery bounds for the deployment. Coordinate wire upgrades and key rotation across all nodes. The optional Prometheus HTTP endpoint has no authentication: restrict binding/network access.

Authentication/encryption bypass, replay failures, unauthorized state mutation/disclosure outside explicitly insecure mode, and remotely reachable panics are security issues. Shared-key impersonation, UDP source spoofability, absent per-peer trust in the deployed runtime and explicitly insecure mode are documented limitations, **not** protections.
