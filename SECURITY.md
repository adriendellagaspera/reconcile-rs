# Security Policy

## Supported versions

Only the latest published `0.x` release of `reconcile` and its published workspace dependencies
receive security fixes. There is no long-term-support branch before 1.0.

## Reporting a vulnerability

Report suspected vulnerabilities privately through GitHub private vulnerability reporting
(repository **Security → Report a vulnerability**) rather than a public issue.

We aim to acknowledge a report within 5 business days and keep the reporter updated during
investigation and remediation.

## Threat model

`reconcile-rs` is a fully replicated, eventually-consistent system over UDP. Every accepted peer
can eventually receive the whole dataset. Network reachability is therefore a trust boundary, not
just a routing detail.

### Unauthenticated mode

Authentication is disabled only through the explicit
`Config::with_insecure_no_key()` opt-in. Without a cluster key:

- any host able to reach the protocol port can forge updates or tombstones;
- a captured datagram can be replayed;
- speculative discovery can disclose data to a reachable host in a configured network;
- the RSOS fingerprint lift is unkeyed, so a malicious writer can target the additive fingerprint
  construction.

Use this mode only on a trusted underlay that already supplies the required isolation. Construction
without either a cluster key or the explicit insecure opt-in is rejected.

Remote timestamps are still bounded before they influence the local HLC/tombstone-expiry clock, but
that does not authenticate the value: a forged far-future timestamp can still win LWW ordering.

### Shared cluster key

`Config::with_cluster_key(ClusterKey)` enables:

- per-datagram MAC authentication before message deserialization;
- per-sender replay protection using authenticated sequence/stamp metadata;
- a keyed RSOS fingerprint lift derived independently from the same secret.

The default MAC backend is keyed BLAKE3 (`mac-blake3`); `mac-hmac` selects HMAC-SHA256. Every node
in one cluster must use compatible authentication settings.

The shared key proves cluster membership, not peer identity. Any holder can impersonate another
holder, and there is no forward secrecy. Deployments requiring per-peer identity or forward secrecy
must provide it in the surrounding network/security layer.

### Payload confidentiality

With the `encryption` feature,
`Config::with_cluster_key(key).with_encryption()` uses XChaCha20-Poly1305 authenticated encryption
for datagram payloads. Authentication/replay checks and decryption happen before protocol message
handling.

Encryption does not change the trust model: it still uses the shared cluster secret and provides no
per-peer identity or forward secrecy.

### Fingerprint security boundary

Range reconciliation uses a 256-bit additive fingerprint. The cluster key derives a separate keyed
lift key, preventing an attacker who does not know the secret from precomputing a cancelling
fingerprint plant. Per-session cut randomisation also changes refinement boundaries below the outer
range.

A cluster-key holder knows the lift key, so the keyed construction does not defend against a
malicious insider with that secret. The outer range also has no child boundary to randomise. These
are documented residuals of the shared-secret model, not properties provided by the MAC.

## Replay protection

Authenticated modes carry a monotonically increasing sender sequence number and wall-clock stamp
inside the protected region. Receivers reject duplicates, stale/out-of-window sequences and stamps
outside the configured freshness window.

The replay filter is per peer and bounded by the configured peer cap. Forgetting/decommissioning a
peer must not accidentally turn replayed traffic into fresh traffic; changes to peer lifecycle must
preserve that invariant.

## Cluster-key rotation

Provision keys through the deployment's secret source (for example an injected secret or secret
manager), never source control. During rotation every node needs both secrets until the old one is
retired.

Rotate in three cluster-wide phases:

| phase | configuration | sends with | accepts |
|---|---|---|---|
| prepare | `with_cluster_key_rotation(old, new)` | old | old + new |
| switch | `with_cluster_key_rotation(new, old)` | new | new + old |
| retire | `with_cluster_key(new)` | new | new |

Complete each phase across the fleet before starting the next. After retirement, remove the old
secret from the deployment and secret manager.

The primary key also derives the RSOS lift key. During the switch phase, nodes on different
primaries can authenticate each other but compute different fingerprints for equal datasets, so
anti-entropy can temporarily re-diff/re-send unchanged content. Keep the mixed-primary phase short.

The `zeroize` feature wipes `ClusterKey` bytes owned by the library on drop, including both keys
in a rotation window. It cannot wipe the caller's original environment variable, file contents,
secret-manager response buffer, or decrypted application payloads.

## Wire-version compatibility

Every datagram carries a protocol version, protected by MAC/AEAD when keyed. There is no
accepted-version window: peers on different wire versions reject each other's datagrams.

Treat a wire-version change as a coordinated cluster upgrade rather than a rolling mixed-version
deployment. `MIGRATING.md` records release-specific compatibility actions.

## Operational requirements

- Keep the gossip UDP port reachable only by intended cluster participants.
- Use a cluster key unless the surrounding network is explicitly trusted as the authentication
  boundary.
- Prefer the `encryption` feature when payload confidentiality is required and the underlay does
  not already provide it.
- Size `Config::max_peers` for the intended fleet; unknown senders are rejected before
  per-sender state allocation once the cap is reached.
- Use `Config::max_value_size` / fallible writes when applications need synchronous rejection
  before an oversized value reaches the UDP send path.
- Coordinate wire-version upgrades and key-rotation phases cluster-wide.
- The optional Prometheus HTTP endpoint is unauthenticated. Bind it to an internal interface or
  restrict reachability with network policy/firewall rules; the examples use `0.0.0.0:9000` only
  for convenience.

## Scope

In scope: memory-safety bugs, authentication/MAC/AEAD bypass, replay-protection failures,
unauthenticated corruption or exfiltration beyond the documented insecure mode, panics reachable
from untrusted network input, and behavior contradicting this threat model.

Out of scope when behaving as documented:

- unauthenticated operation after explicit `with_insecure_no_key()`;
- lack of per-peer identity or forward secrecy under the shared-key design;
- UDP source-address spoofability as a transport property;
- a malicious holder of the shared cluster key having the authority described above.
