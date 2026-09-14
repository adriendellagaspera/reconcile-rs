# Security Policy

## Supported versions

Only the latest published `0.x` release of `reconcile` (and its published dependencies `rsos`,
`rbsr`, `lww-register`, `reconcile-gossip`) receives security fixes. There is no long-term-support
branch before `1.0.0` — see the
[`v1.0.0` milestone](https://github.com/Akvize/reconcile-rs/milestone/2) and
[issue #206](https://github.com/Akvize/reconcile-rs/issues/206) for the release plan.

## Reporting a vulnerability

Please report suspected vulnerabilities privately via GitHub's
[private vulnerability reporting](https://github.com/adriendellagaspera/reconcile-rs/security/advisories/new)
(repository Security tab → "Report a vulnerability") rather than a public issue.

We aim to acknowledge a report within 5 business days, and will keep you updated as we investigate
and fix it.

## Cluster-key rotation

Provision cluster keys through the deployment's secret source (for example an environment-injected
secret, a Kubernetes `Secret`, or a secret manager), never source control. During rotation, every
node needs both the old and new secret available until the old key is retired.

Rotate in three cluster-wide phases, completing each phase on every node before starting the next:

| phase | configuration | sends with | accepts |
|---|---|---|---|
| prepare | `with_cluster_key_rotation(old, new)` | old | old + new |
| switch | `with_cluster_key_rotation(new, old)` | new | new + old |
| retire | `with_cluster_key(new)` | new | new |

After the retire phase, remove the old secret from the deployment and secret manager. There is no
key identifier or epoch on the wire: a receiver in the rotation window verifies against its primary
key and then its one fallback key. The same window applies to MAC authentication and, when enabled,
authenticated encryption.

The primary cluster key also derives the keyed RSOS fingerprint lift. Therefore peers on different
primaries can authenticate and exchange values during the switch phase, but equal datasets have
different range fingerprints until the fleet agrees on one primary; anti-entropy may temporarily
re-diff and re-send unchanged data. Keep the mixed-primary phase short. Issue #118 tracks whether a
future multi-summary/key-id design is warranted; it is not required for authentication continuity.

The `zeroize` feature wipes `ClusterKey` bytes owned by the library on drop, including both keys in
a rotation window. It cannot wipe the caller's original environment variable, file contents, or
secret-manager response buffer; those remain the application's responsibility.

## Scope

In scope: memory-safety bugs, authentication/MAC bypass, an unauthenticated node able to corrupt or
exfiltrate data beyond what is already documented below, panics reachable from untrusted network
input, and any behavior contradicting the README's "Security model" section.

Out of scope — documented design choices, not bugs:

- UDP reconciliation is **unauthenticated by default**; a shared cluster key is opt-in and required
  to close this (README "Security model", AGENTS.md §8).
- The cluster key is a single shared secret: no per-peer identity, no forward secrecy (issues #135,
  #136).
- UDP source addresses are spoofable — a property of the transport, not this crate.

See the README's [Security model](README.md#security-model) section for the full threat model.
