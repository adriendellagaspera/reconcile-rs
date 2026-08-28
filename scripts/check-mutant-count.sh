#!/usr/bin/env bash
# .cargo/mutants.toml's exclude_globs/exclude_re/skip_calls exist to skip mutants this repo has
# decided not to test (test-only seams, provably-equivalent mutants, mutants that hang rather than
# fail -- see that file's own comments for the per-pattern reasoning). This script proves those
# exclusions still exclude *something real*: it re-lists mutants with and without this file's
# config and asserts the unconfigured count is strictly greater than the configured one -- if it
# isn't, some exclude_globs/exclude_re/skip_calls pattern has gone stale (a moved or renamed file,
# a refactored call site) and no longer matches any real mutation site.
#
# It does not track or assert either count's absolute value. That total moves with every commit
# that adds or removes mutable code anywhere in the workspace, unrelated to whether the exclusions
# themselves are still live -- pinning it in a comment would fail (and merge-conflict across every
# branch touching mutable code in parallel) on every such commit, not only when an exclusion
# actually goes stale.
#
# Test quality itself -- whether a mutant is missed by the test suite -- is a different question,
# checked by `mutants.yml`'s diff-scoped `pr-diff` job (`check-mutation-gate.sh`), not this script.
#
# `--list` is pure mutant-site discovery -- syntactic, no build -- so both invocations are
# sub-second even cold (measured: ~0.4s each on this workspace).
set -Eeuo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR/.."

command -v cargo-mutants >/dev/null || { echo "check-mutant-count: cargo-mutants is required" >&2; exit 1; }

configured=$(cargo mutants --list --workspace --all-features | wc -l)
unconfigured=$(cargo mutants --list --workspace --all-features --no-config | wc -l)

if [ "$unconfigured" -le "$configured" ]; then
    echo "check-mutant-count: .cargo/mutants.toml's exclusions no longer exclude anything ($configured mutants with the config, $unconfigured without) -- check for a stale exclude_globs/exclude_re/skip_calls pattern (a moved or renamed file, a refactored call site)." >&2
    exit 1
fi

echo "check-mutant-count: exclusions still exclude $((unconfigured - configured)) mutant(s) ($configured configured, $unconfigured unconfigured)"
