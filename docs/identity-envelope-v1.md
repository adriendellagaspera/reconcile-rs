# Signed peer identity envelope v1

This is an **opt-in framing primitive**, separate from the shipped UDP/IP runtime and its
unchanged inner wire version 4. It does **not** enable signed identity on the live replicas.

Wire layout (105-byte overhead):

`RCPE (4) | outer version (1) | PeerId=Ed25519 public key (32) | signature (64)
| inner length (u32-le, 4) | sealed inner datagram (N)`

The Ed25519 signature uses the pre-existing peer-datagram/v1 domain and signs
`RCPE || outer version || length || inner datagram`. Authorization requires an explicit
allowlist; public-key possession is not cluster membership permission. Strict frame parsing
rejects incorrect magic/version, truncated or trailing bytes and packets over the supplied MTU.
The inner frame retains its own MAC/AEAD, protected sequence/time header and protocol version.
A 1200-byte outer datagram leaves **1095 bytes for the entire inner sealed datagram**.

`open` yields a `VerifiedEnvelope`; `authenticate` independently checks the inner
cluster MAC/AEAD and inner strict wire version. Neither operation performs replay checks,
topology peer admission, causal-membership insertion, or GC acknowledgement.

Migration steps **not** part of this PR: provision stable per-node secrets and allowlist;
explicitly activate signed-only runtime ingress and egress without downgrade; migrate replay
keys only after verified identity and authorized membership; preserve unresolved legacy IP
members in versioned snapshots as tombstone-GC blockers; test all features, underlays, MTUs,
partitions and restart/rekey behavior. Old receivers reject signed frames. A mixed deployment
requires a coordinated rollout or a separately audited bridge (not implicit fallback).
