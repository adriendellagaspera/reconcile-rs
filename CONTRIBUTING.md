# Contributing

This repository is not actively developed or maintained. There is no roadmap, release schedule, or commitment to review issues and pull requests.

The code and experiments remain public for inspection, learning, and independent reuse under the repository licenses. If you build on this work, verify the behavior and security properties you need; do not treat the existing test suite as production certification.

For a local change, the basic checks are:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo doc --workspace --all-features --no-deps
```

Tests should assert externally observable behavior and invariants. Keep randomized tests reproducible and avoid fixed network ports.

Algorithm-level reconciliation experiments belong in [set-reconciliation](https://github.com/adriendellagaspera/set-reconciliation).
