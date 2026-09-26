# reconcile-rs

[![Crates.io][crates-badge]][crates-url]
[![Build Status][actions-badge]][actions-url]
[![Coverage Status][codecov-badge]][codecov-url]
[![Docs Status][docs-badge]][docs-url]

Embedded, fully replicated, eventually consistent key-value storage for Rust. Each authoritative
node holds the whole dataset and serves reads locally. Writes are sent to peers over UDP;
range-based reconciliation repairs missed updates, and concurrent writes to the same key
resolve by last-write-wins ordering.

Use it when every node can keep the working set in memory and your application can tolerate
eventual consistency. It does not provide sharding or strongly consistent writes. Persistence
is optional; see the [API documentation][docs-url] for the supported configuration and the
[security guide](SECURITY.md) before joining nodes across a network.

## Try it

From a checkout with Rust installed, run the repository's local demo:

```sh
cargo run --release --example demo 8080 127.0.0.1 127.0.0.0/30 100
```

The demo binds a UDP socket, inserts 100 sample key-value pairs and logs the map fingerprint
while the node runs. It explicitly disables cluster authentication for this local example.
The [demo source](examples/demo.rs) shows the `ReplicatedMap` setup, writes and seed handling;
the [Kubernetes example](examples/k8s/) shows a multi-node deployment.

## Add it to a Rust project

```sh
cargo add reconcile
```

Start with the [API documentation][docs-url] for construction, reads, writes and lifecycle.
The [architecture](ARCHITECTURE.md) explains how the replicas reconcile; the
[benchmark guide](benches/README.md) describes the reproducible workload.

Contributions follow [CONTRIBUTING.md](CONTRIBUTING.md). Licensed under either
[MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.

[crates-badge]: https://img.shields.io/crates/v/reconcile.svg
[crates-url]: https://crates.io/crates/reconcile
[actions-badge]: https://github.com/adriendellagaspera/reconcile-rs/actions/workflows/main.yml/badge.svg
[actions-url]: https://github.com/adriendellagaspera/reconcile-rs/actions/workflows/main.yml
[codecov-badge]: https://codecov.io/gh/adriendellagaspera/reconcile-rs/branch/main/graph/badge.svg
[codecov-url]: https://codecov.io/gh/adriendellagaspera/reconcile-rs
[docs-badge]: https://docs.rs/reconcile/badge.svg
[docs-url]: https://docs.rs/reconcile/latest/reconcile/
