# Security

Only the latest published release line receives security fixes.

Report vulnerabilities privately through GitHub **Security → Report a vulnerability**.

## Trust model

Every authoritative replica can eventually receive the complete dataset. Network membership is
therefore a trust boundary.

Construction requires an explicit mode:

- `Config::with_cluster_key(key)` authenticates datagrams, enables replay protection, and derives
  a separate keyed range-fingerprint lift.
- `Config::with_insecure_no_key()` disables authentication and is appropriate only when the
  surrounding network already provides the required trust boundary.

A shared cluster key proves membership, not peer identity. Any holder can impersonate another
holder. The protocol provides no forward secrecy.

With the `encryption` feature, `with_encryption()` uses XChaCha20-Poly1305 for payload
confidentiality. Encryption does not change the shared-secret trust model.

## Unauthenticated mode

A host that can reach an unauthenticated protocol port can forge or replay updates and tombstones.
Remote timestamp bounding limits clock influence but does not authenticate data.

Use unauthenticated mode only on a trusted underlay.

## Replay protection

Authenticated datagrams carry protected sender sequence and time metadata. Receivers reject
duplicates, stale sequences, and timestamps outside the freshness window. Replay state is retained
independently of transient peer membership.

## Fingerprints

Range reconciliation uses a 256-bit additive fingerprint. A cluster key derives an independent
keyed lift, preventing an outsider from precomputing a cancelling fingerprint for that cluster.

A holder of the cluster key also knows the lift key. The fingerprint mechanism does not protect
against a malicious insider holding the shared secret.

## Key rotation

Provision keys outside source control. Rotate cluster-wide in three phases:

| phase | configuration | sends with | accepts |
|---|---|---|---|
| prepare | `with_cluster_key_rotation(old, new)` | old | old + new |
| switch | `with_cluster_key_rotation(new, old)` | new | new + old |
| retire | `with_cluster_key(new)` | new | new |

Complete each phase across the fleet before starting the next. Nodes using different primary keys
can authenticate during rotation but derive different range fingerprints, causing redundant
anti-entropy until primaries match.

The `zeroize` feature wipes cluster-key bytes owned by the library on drop. It cannot wipe copies
owned by the caller or secret source.

## Wire compatibility

Wire versions are strict and are not negotiated. Nodes with different wire versions reject each
other. A wire-format change therefore requires a coordinated cluster upgrade.

## Operations

- Restrict the gossip UDP port to intended participants.
- Use a cluster key unless the underlay is the authentication boundary.
- Enable `encryption` when payload confidentiality is not supplied elsewhere.
- Set peer and value-size bounds appropriate to the deployment.
- Coordinate key rotation and wire-version changes across the cluster.
- The optional Prometheus HTTP endpoint is unauthenticated; restrict its bind address or network
  reachability.

## Security scope

Security bugs include authentication or encryption bypasses, replay failures, unauthorized data
mutation or disclosure outside explicitly insecure mode, and panics reachable from untrusted
network input.

The documented shared-key authority, lack of per-peer identity/forward secrecy, UDP source-address
spoofability, and explicitly unauthenticated operation are trust-model limitations rather than
additional security guarantees.
