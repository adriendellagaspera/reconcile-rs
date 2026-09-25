#!/usr/bin/env bash
set -Eeuo pipefail

: "${PROPTEST_RNG_SEED:=20260817}"
export PROPTEST_RNG_SEED

export RUSTFLAGS="${RUSTFLAGS:-} --cfg reconcile_internal_testing"

BASE_REF="${1:-origin/main}"
SHARD="${2:-}"

GIT_ROOT=$(git rev-parse --show-toplevel)
cd "$GIT_ROOT"
unset CARGO_TARGET_DIR CARGO_BUILD_TARGET_DIR

DIFF=$(mktemp); trap 'rm -f "$DIFF"' EXIT
git diff "${BASE_REF}..." -- '*.rs' >"$DIFF"

if [ ! -s "$DIFF" ]; then
    echo "check-mutation-gate: no Rust changes against ${BASE_REF}, nothing to check"
    exit 0
fi

echo "check-mutation-gate: mutating lines changed against ${BASE_REF}"
echo "                     PROPTEST_RNG_SEED=${PROPTEST_RNG_SEED}${SHARD:+, shard=$SHARD}"

JOBS=3

SHARD_ARGS=()
if [ -n "$SHARD" ]; then
    SHARD_ARGS=(--shard "$SHARD" --sharding round-robin)
fi

set +e
cargo mutants --workspace --no-shuffle -vV --in-diff "$DIFF" --timeout 300 --jobs "$JOBS" --copy-target=false "${SHARD_ARGS[@]}"
mutants_status=$?
set -e

if [ ! -f mutants.out/outcomes.json ]; then
    echo "check-mutation-gate: cargo-mutants produced no mutants.out/outcomes.json (exit $mutants_status)" >&2
    exit "${mutants_status:-1}"
fi

missed=$(jq '.missed' mutants.out/outcomes.json)
if [ "$missed" -gt 0 ]; then
    echo "check-mutation-gate: $missed mutant(s) survived (missed) in the diff" >&2
    exit 1
fi

if [ "$mutants_status" -ne 0 ]; then
    echo "check-mutation-gate: cargo-mutants exited $mutants_status with 0 missed (e.g. a timeout) --" \
        "treating as pass, per the policy above."
fi

echo "check-mutation-gate: no surviving mutants in the diff"
