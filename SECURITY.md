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
`PeerId` is a label. `peer::identity` verifies allowlisted Ed25519-signed datagrams, but the
runtime does not yet use this proof: live ingress, replay and causal membership remain IP-keyed.

With the `encryption` feature, `with_encryption()` uses XChaCha20-Poly1305 for payload
confidentiality. Encryption does not change the shared-secret trust model.

## Unauthenticated mode

A host that can reach an unauthenticated protocol port can forge or replay updates and tombstones.
Remote timestamp bounding limits clock influence but does not authenticate data.

Use unauthenticated mode only on a trusted underlay.

## Replay protection

Authenticated datagrams carry protected sender sequence and time metadata. Receivers reject
duplicates, stale sequences, and timestamps outside the freshness window. Replay state is retained
independently of transient peer membership and is bounded by `Config::max_replay_senders`. At that
bound, already-tracked senders keep their replay history and continue through normal checks; a new
authenticated sender is dropped until stale replay state ages past the freshness window and is
purged. Fresh replay state is never evicted to make room.

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

## Fragment reassembly

Fragment state is allocated only after the datagram has passed authentication, wire-version,
topology-peer admission, and replay checks. A forged or replay-rejected datagram therefore cannot
consume reassembly memory.

`Config::framing` bounds incomplete state independently of the protocol data structure:
maximum logical-message size, fragments per message, incomplete transfers per peer, retained bytes
per peer, total retained bytes, and inactivity TTL. When a capacity must be reclaimed, the oldest
eligible incomplete transfer is evicted deterministically; the transfer currently being extended is
never partially evicted to admit its own next fragment. Exact duplicate fragments consume no
additional retained bytes.

The transfer identifier is a content hash used for resumability, not an authentication primitive.
Integrity/authority still comes from the authenticated datagram boundary; after completion, the
reassembled bytes are checked against that content identifier before protocol deserialization.

Selective recovery adds independently bounded sender state only after an authenticated peer has
advertised support. `Config::framing` bounds retained outbound transfers/bytes per peer and globally,
missing ranges per report, recovery rounds per transfer, recovery-state TTL, and capability TTL.
Missing reports and completion acknowledgements pass authentication, version, topology admission and
replay checks before they are parsed; malformed/out-of-range reports cause no retransmission. A lost,
stale, rejected, or exhausted control exchange merely falls back to ordinary anti-entropy.
An idle transfer cannot extend its own retention through recovery requests: the default receiver TTL is 90 seconds and the sender's recoverable-payload TTL is 120 seconds, covering a 30-second contact loss. Periodic NACKs are globally capped at 32 per tick and limited to one per transfer every 3 seconds (after 1 second idle); reports are bounded by the authenticated datagram budget. State expiration and capacity eviction remain authoritative even if retransmission control repeatedly fails.
For capability-advertising peers with a retained outbound transfer, identical full-message retries are suppressed for 12 seconds after the last full send; the next regular anti-entropy attempt beyond that interval may retry the whole message. This is a retry floor, not a guaranteed 12-second resynchronization deadline.

## Wire compatibility

Wire versions are strict and are not negotiated. Nodes with different wire versions reject each
other. Incompatible framing changes require a wire-version bump and coordinated cluster upgrade.
Selective recovery is additive instead: support is advertised inside the pre-existing reserved,
length-prefixed protocol tag that older same-version peers already ignore, and its new outer control
tags are emitted only after that advertisement. An older peer therefore continues using the existing
complete/fragment frames and whole-message retry behavior.

## Operations

- Restrict the gossip UDP port to intended participants.
- Use a cluster key unless the underlay is the authentication boundary.
- Enable `encryption` when payload confidentiality is not supplied elsewhere.
- Set causal-peer, authenticated replay-sender, logical-message, reassembly, and outbound-recovery bounds appropriate to the deployment.
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
