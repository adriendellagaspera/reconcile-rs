#!/usr/bin/env bash
# Does a change's *tests* actually detect faults in the code the change touched?
#
# Coverage cannot answer this. Meta's ACH study found 49% of fault-detecting
# generated tests added zero line coverage (arXiv:2501.12862) — a coverage-delta
# gate would have discarded half of the tests that mattered. Mutation does answer
# it: inject a plausible fault into the changed lines, and require the suite to
# fail.
#
# Scope is the diff, not the workspace: a full sweep is ~1400 mutants and hours.
# `--in-diff` on a typical PR is 5-20 mutants, 2-8 minutes.
#
# HERMETICITY IS A PRECONDITION, NOT A NICETY. cargo-mutants assumes the suite is
# deterministic; tests/proptest_*.rs draws a fresh random seed per run unless
# PROPTEST_RNG_SEED is set, which makes the same mutant MISSED on one run and
# caught on the next. Pin it, or this gate reports noise.
set -Eeuo pipefail

: "${PROPTEST_RNG_SEED:=20260817}"
export PROPTEST_RNG_SEED

# #330: `all_features = true` in .cargo/mutants.toml no longer implies the `reconcile::testing` /
# `RangeAggregate::for_testing` seam -- internal-testing stopped being a Cargo feature, so the
# integration-test oracles cargo-mutants runs need this `--cfg` set explicitly, same as every other
# `--all-features` build in main.yml after that migration.
export RUSTFLAGS="${RUSTFLAGS:-} --cfg reconcile_internal_testing"

BASE_REF="${1:-origin/main}"
# Optional "i/n" (0-based, matching mutants.yml's `nightly` job's own convention) to run only one
# shard of the in-diff mutant set -- see the --shard block below for why pr-diff needs this.
SHARD="${2:-}"

GIT_ROOT=$(git rev-parse --show-toplevel)
cd "$GIT_ROOT"
# Each cargo-mutants worker must build into its own scratch target directory.
# Do not let either the script or the caller force all workers into a shared target.
unset CARGO_TARGET_DIR CARGO_BUILD_TARGET_DIR

DIFF=$(mktemp); trap 'rm -f "$DIFF"' EXIT
git diff "${BASE_REF}..." -- '*.rs' >"$DIFF"

if [ ! -s "$DIFF" ]; then
    echo "check-mutation-gate: no Rust changes against ${BASE_REF}, nothing to check"
    exit 0
fi

echo "check-mutation-gate: mutating lines changed against ${BASE_REF}"
echo "                     PROPTEST_RNG_SEED=${PROPTEST_RNG_SEED}${SHARD:+, shard=$SHARD}"

# #438: historical parallel runs reported inconsistent MISSED/CAUGHT verdicts.
# The repository then had two concrete isolation defects: all workers inherited one
# CARGO_TARGET_DIR, and several tests competed for fixed/probed-and-released UDP ports.
# Both are removed; repeated post-isolation jobs=1/jobs=3 campaigns are the evidence
# required before this value is raised.
JOBS=3

# --copy-target=false overrides .cargo/mutants.toml's `copy_target = true`.
# Keep it disabled with parallel workers: every cargo-mutants worker must build in
# its own scratch target rather than copying or reusing a target being mutated elsewhere.
#
# --workspace is load-bearing: without it cargo-mutants scopes to the invoking package
# only (the root `reconcile` crate), so a diff touching rsos/rbsr/lww-register/gossip
# would silently match zero mutants there instead of gating them. Verified empirically —
# `cargo mutants --in-diff` on a diff to lww-register/src/clock.rs reports "No mutants to
# filter" without --workspace, and finds the mutant there.
#
# cargo-mutants' own exit code is non-zero for *any* mutant that isn't caught or unviable --
# that includes TIMEOUT, not just MISSED. .cargo/mutants.toml already documents the intended
# policy ("a mutant that breaks convergence can hang rather than fail... timeouts count as
# caught") but that comment only describes what a human reading the nightly sweep should
# conclude; nothing enforced it here. #427's fingerprint_tree_map split surfaced the gap: a
# handful of `i += 1`-style loop counters mutated to `i *= 1` (starting from `i == 0`) hang
# forever by construction -- no test, however thorough, can turn a genuine infinite loop into
# a finite assertion, so treating a TIMEOUT exactly like a MISSED here would make this gate
# permanently unpassable for that code shape. Score on `missed` specifically instead of the
# raw exit code, so a hang still counts as a detected fault (matching the mutants.toml
# comment) while an actual survivor still fails the gate.
#
# SHARD_ARGS: a large mechanical split (#427/#452) can put 200+ mutants in one diff -- git diff
# shows moved code as changed regardless of whether any logic in it actually did (AGENTS.md §10's
# "a rule enforced by eye" problem, here applied to cargo-mutants' own scope). At --jobs 1 (the
# the then-sequential gate) that serialized into a 2+ hour run, twice cut off mid-run by the CI
# runner with zero mutants missed either time -- wasted compute, not a real gate failure. Splitting
# the *same* mutant set across parallel CI shards still caps each runner's wall-clock even now that
# each shard can use several isolated cargo-mutants workers. Empty on a normal PR (5-20 mutants):
# one shard gets everything, so sharding remains a no-op for the common case.
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
