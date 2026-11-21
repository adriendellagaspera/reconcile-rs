# reconcile-rs

[![Build](https://github.com/adriendellagaspera/reconcile-rs/actions/workflows/main.yml/badge.svg)](https://github.com/adriendellagaspera/reconcile-rs/actions/workflows/main.yml)
[![Crates.io](https://img.shields.io/crates/v/reconcile.svg)](https://crates.io/crates/reconcile)
[![Documentation](https://docs.rs/reconcile/badge.svg)](https://docs.rs/reconcile/latest/reconcile/)

**Research prototype — no active development, roadmap, or production support.**

This repository records an exploration of fully replicated key-value storage in Rust. It investigates eventual convergence, last-write-wins conflict resolution, range-based anti-entropy, authenticated datagrams, peer membership, and snapshot persistence.

It is kept public as a technical reference. It is not a recommended database, replication framework, or production dependency. The published `0.6.0` crates are historical artifacts; do not assume future releases, compatibility guarantees, or security maintenance.

## What is here

- `reconcile`: the replicated-map/set implementation, persistence, and replica lifecycle.
- `reconcile-gossip`: datagram transport, framing, authentication, replay protection, and discovery. It is an implementation detail of `reconcile`.
- `examples/`: a minimal local experiment.
- `tests/`: behavioral and protocol tests.
- `benches/`: runtime and transport experiments. Some targets are models, not measurements of real network transports.

The algorithm-level RBSR and RSOS implementations are maintained separately in [set-reconciliation](https://github.com/adriendellagaspera/set-reconciliation).

## Try the local demo

With a Rust toolchain installed:

```sh
cargo run --release --example demo -- 8080 127.0.0.1 127.0.0.0/30 100
```

This starts a local UDP node, inserts sample key-value pairs, and reports its map fingerprint. The example disables cluster authentication and is only intended for local experimentation.

## Important limitations

- Every authoritative replica stores the full dataset; there is no sharding.
- Writes are asynchronous and conflict resolution is last-write-wins. There are no transactions, consensus, or strongly consistent writes.
- Eventual convergence depends on peers being able to communicate and continue anti-entropy.
- The legacy `ReplicatedMap` and opt-in signed `SignedReplica` are separate, incompatible runtime paths. Their membership models and snapshot formats cannot be mixed.
- Tombstone collection is disabled. Tombstones and associated causal metadata can grow without bound.
- Persistence uses versioned snapshots; it is not a general-purpose transactional database or a write-ahead log.
- Security depends on correct key provisioning, membership configuration, and network deployment. Read [SECURITY.md](SECURITY.md) before experimenting beyond localhost.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the implementation boundaries and known trade-offs, and [benches/README.md](benches/README.md) before interpreting benchmark results.

## Status

The project is intentionally frozen in practice. Issues and pull requests may not receive a response. Forks are welcome, but this repository does not promise to review contributions or publish further releases.

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
