# Security

This repository is an experimental, unmaintained project. It has no guaranteed security response or supported release line. Do not deploy it with sensitive or production data.

The notes below describe the historical implementation; they are not a security audit or deployment recommendation.

## Trust and confidentiality

Each authoritative replica eventually receives the entire dataset. Membership and network reachability are security boundaries.

- `Config::with_cluster_key(key)` enables shared-key authentication and replay protection. Anyone holding the key can impersonate another holder; this is not individual peer authentication or forward secrecy.
- `Config::with_insecure_no_key()` disables authentication. Any reachable host may forge or replay updates and tombstones. Use only for isolated local experiments.
- The optional encryption feature provides payload confidentiality, not peer identity or forward secrecy.
- The opt-in signed `SignedReplica` path uses explicitly allowlisted Ed25519 public keys and a separate membership model. It is not the legacy `ReplicatedMap` ingress and does not make the two runtime paths interoperable.

## Resource admission and replay

The implementation includes replay windows, bounded framing/reassembly state, and admission checks before decoding. These controls have finite configuration limits and implementation-specific behavior. They do not eliminate denial-of-service risk or replace network-level rate limiting and isolation.

## Key handling

Provision keys outside source control. Treat shared cluster keys and Ed25519 private seeds as secrets. The `zeroize` feature only clears library-owned key bytes; it cannot erase caller-owned copies or guarantee that all compiler/runtime copies are removed.

## Reporting

There is no monitored security contact or commitment to respond to vulnerability reports. Do not rely on this repository for security-sensitive deployments.
