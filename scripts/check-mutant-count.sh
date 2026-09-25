#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR/.."

command -v cargo-mutants >/dev/null || { echo "check-mutant-count: cargo-mutants is required" >&2; exit 1; }

unconfigured=$(cargo mutants --list --workspace --all-features --no-config --colors=never)
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

configured_count=$(cargo mutants --list --workspace --all-features --colors=never | wc -l)

if [ "$status" -ne 0 ]; then
    echo >&2
    echo "check-mutant-count: fix or remove the dead entries above in .cargo/mutants.toml's exclude_re." >&2
    exit 1
fi

echo "check-mutant-count: every exclude_re entry still matches at least one mutant ($configured_count configured, $unconfigured_count unconfigured)"
