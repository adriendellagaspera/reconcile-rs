#!/usr/bin/env bash
# .cargo/mutants.toml's exclude_re entries exist to skip mutants this repo has decided not to test
# (test-only seams, provably-equivalent mutants, mutants that hang rather than fail -- see that
# file's own comments for the per-pattern reasoning). This script proves EACH one still excludes
# something real: it lists mutants with `--no-config` (ignoring this file entirely) and checks
# every individual exclude_re entry against that list, one at a time -- failing by name on any
# entry that matches zero mutants.
#
# exclude_globs is not checked here: cargo-mutants never visits `benches/**`/`examples/**` at all,
# config or not (they're not `[lib]` targets, so they're outside its default mutation universe) --
# `--no-config`'s listing has zero matches for them regardless of whether the setting is doing
# anything, so there's no meaningful signal to check it against.
#
# An earlier version of this check only asserted the *aggregate* mutant count dropped when the
# config was applied. That is a weaker property than it looks: as long as *some* entries are still
# live, the aggregate count keeps dropping even while other entries have gone completely stale (a
# moved/renamed file, a refactored call site no longer matched by a line:col regex) and are
# silently excluding nothing. Six entries were found dead or near-dead this way, coexisting with
# fourteen live ones the whole time -- the aggregate check never caught it. Per-entry checking is
# the only way to prove *every* exclusion, not just the aggregate, is still doing something.
#
# Test quality itself -- whether a mutant is missed by the test suite -- is a different question,
# checked by `mutants.yml`'s diff-scoped `pr-diff` job (`check-mutation-gate.sh`), not this script.
# Nor does this script re-verify *why* a still-matching exclusion is justified (a claimed hang
# really still hangs, a claimed equivalent mutant really is equivalent) -- only that cargo-mutants
# still generates at least one mutant the pattern applies to. That narrower, more expensive
# question needs actually running the mutant, which for some of the patterns below is not safe to
# do unattended (see their own comments in .cargo/mutants.toml on unbounded memory growth risking
# an OOM/runner kill rather than a clean timeout) -- a manual, deliberate check, not this script's.
#
# `--list` is pure mutant-site discovery -- syntactic, no build -- so it's sub-second even cold
# (measured: ~0.4s on this workspace). `skip_calls` is not checked here: it suppresses mutating
# call *arguments*, which doesn't leave a distinguishable trace in `--list` output the way a whole
# excluded mutant site does -- verify it by hand if `.cargo/mutants.toml` ever changes it.
set -Eeuo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR/.."

command -v cargo-mutants >/dev/null || { echo "check-mutant-count: cargo-mutants is required" >&2; exit 1; }

unconfigured=$(cargo mutants --list --workspace --all-features --no-config)
unconfigured_count=$(wc -l <<<"$unconfigured")

result=$(python3 -c '
import re
import sys
import tomllib

with open(".cargo/mutants.toml", "rb") as f:
    config = tomllib.load(f)

lines = sys.stdin.read().splitlines()
dead = []

for pattern in config.get("exclude_re", []):
    regex = re.compile(pattern)
    if not any(regex.search(line) for line in lines):
        dead.append(f"exclude_re entry matches no mutant: {pattern!r}")

for entry in dead:
    print(entry)
n_re = len(config.get("exclude_re", []))
print(f"{n_re} exclude_re entries checked", file=sys.stderr)
' <<<"$unconfigured")

status=0
while IFS= read -r line; do
    [ -z "$line" ] && continue
    echo "check-mutant-count: $line -- a stale pattern (a moved/renamed file, a refactored call site) excluding nothing" >&2
    status=1
done <<<"$result"

configured_count=$(cargo mutants --list --workspace --all-features | wc -l)

if [ "$status" -ne 0 ]; then
    echo >&2
    echo "check-mutant-count: fix or remove the dead entries above in .cargo/mutants.toml's exclude_re." >&2
    exit 1
fi

echo "check-mutant-count: every exclude_re entry still matches at least one mutant ($configured_count configured, $unconfigured_count unconfigured)"
