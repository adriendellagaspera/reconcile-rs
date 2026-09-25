# reconcile-rs

[![Crates.io][crates-badge]][crates-url]
[![Build Status][actions-badge]][actions-url]
[![Coverage Status][codecov-badge]][codecov-url]
[![Docs Status][docs-badge]][docs-url]

Embedded, fully replicated, eventually consistent key-value storage for Rust. Each node serves local
reads; replicas reconcile over UDP and resolve same-key writes with last-write-wins semantics.

```sh
cargo add reconcile
```

Start with the [API documentation][docs-url] and the runnable examples in
[`examples/`](examples/).

Repository documentation:

- [Architecture](ARCHITECTURE.md)
- [Security](SECURITY.md)
- [Contributing](CONTRIBUTING.md)
- [Benchmarks](benches/README.md)

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.

[crates-badge]: https://img.shields.io/crates/v/reconcile.svg
[crates-url]: https://crates.io/crates/reconcile
[actions-badge]: https://github.com/adriendellagaspera/reconcile-rs/actions/workflows/main.yml/badge.svg
[actions-url]: https://github.com/adriendellagaspera/reconcile-rs/actions/workflows/main.yml
[codecov-badge]: https://codecov.io/gh/adriendellagaspera/reconcile-rs/branch/main/graph/badge.svg
[codecov-url]: https://codecov.io/gh/adriendellagaspera/reconcile-rs
[docs-badge]: https://docs.rs/reconcile/badge.svg
[docs-url]: https://docs.rs/reconcile/latest/reconcile/
