#!/usr/bin/env bash
set -Eeuo pipefail

BASE_REF="${1:-origin/main}"

GIT_ROOT=$(git rev-parse --show-toplevel)
cd "$GIT_ROOT"
source ./scripts/lib-changed-paths.sh

export RUSTFLAGS=-Dwarnings RUSTDOCFLAGS=-Dwarnings

if git rev-parse --verify -q "$BASE_REF" >/dev/null; then
    RUST=0
    affects_rust "$BASE_REF" && RUST=1
    DEPS=0
    affects_deps "$BASE_REF" && DEPS=1
else
    echo "run-affected-checks: '$BASE_REF' does not resolve -- running everything" >&2
    RUST=1
    DEPS=1
fi

run() {
    echo "+ $*"
    "$@"
}
skip() {
    echo "run-affected-checks: skipping $1 -- no $2-affecting change against ${BASE_REF}"
}

run ./scripts/check-doc-budget.sh
run ./scripts/check-domain-purity.sh
run ./scripts/check-doc-structure.sh

if [ "$RUST" -eq 1 ]; then
    run cargo fmt --check
    RUSTFLAGS="$RUSTFLAGS --cfg reconcile_internal_testing" run cargo clippy --workspace --all-targets
    RUSTFLAGS="$RUSTFLAGS --cfg reconcile_internal_testing" run cargo clippy --workspace --all-features --all-targets
    run cargo build --workspace
    RUSTFLAGS="$RUSTFLAGS --cfg reconcile_internal_testing" run cargo nextest run --workspace --retries 4 --flaky-result fail
    RUSTFLAGS="$RUSTFLAGS --cfg reconcile_internal_testing" run cargo nextest run --workspace --all-features --retries 4 --flaky-result fail
    RUSTFLAGS="$RUSTFLAGS --cfg reconcile_internal_testing" run cargo test --doc --workspace
    RUSTFLAGS="$RUSTFLAGS --cfg reconcile_internal_testing" run cargo bench --no-run
    run cargo doc --workspace
    run cargo doc --workspace --all-features
    run cargo package --workspace --allow-dirty
    run ./scripts/check-public-api.sh
else
    skip "fmt / clippy / build / nextest / doctest / bench / doc / package / public-api" rust
fi

if [ "$DEPS" -eq 1 ]; then
    run cargo deny check
else
    skip "cargo deny check" deps
fi
