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

## Rotating the cluster key

`ClusterKey::with_accepted_key` provides a fixed two-key receive window without changing the wire
format. The key on which the method is called remains the **only** key used to authenticate/encrypt
outgoing datagrams; the additional key is receive-only. Rotate in three deployments, never by
switching every node directly from old-only to new-only:

1. deploy `old.with_accepted_key(new)` everywhere — traffic is still sent with the old key, but
   every node is ready to receive the new one;
2. deploy `new.with_accepted_key(old)` node by node — upgraded and not-yet-upgraded nodes can still
   authenticate each other in both directions;
3. once every node sends with the new key, deploy `new` alone — the old key is then rejected.

Provision both secrets through the same protected channel you use for a single cluster key: an
environment variable injected by the process supervisor, a mounted orchestrator secret, or a
secret-manager/KMS integration. Do not put either secret in source control, images, command-line
arguments, logs, or generated configuration committed to the repository. `ClusterKey`'s `Debug`
implementation redacts both keys, and the optional `zeroize` feature wipes both owned copies on
drop, but neither property protects the caller's original environment/string/file buffer.

The cluster key also derives the keyed RSOS fingerprint lift. During step 2, nodes whose primaries
differ can authenticate and exchange values, but equal datasets intentionally produce different
range fingerprints until every node has switched primary; this can cause transient repeated
anti-entropy work. The data path remains convergent; issue #114 tracks whether this temporary
amplification merits a separate fingerprint-rotation mechanism.

See the README's [Security model](README.md#security-model) section for the full threat model.
