#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR/.."

CRATES=(rsos rbsr lww-register gossip .)
declare -A PKG_NAME=(
    [rsos]=rsos
    [rbsr]=rbsr
    [lww-register]=lww-register
    [gossip]=reconcile-gossip
    [.]=reconcile
)

BLESS=0
if [ "${1:-}" = "--bless" ]; then
    BLESS=1
fi

if ! command -v cargo-public-api >/dev/null 2>&1; then
    echo "check-public-api: cargo-public-api not installed — 'cargo install cargo-public-api' (AGENTS.md §2)" >&2
    exit 1
fi

mkdir -p public-api

status=0
reconcile_api=""
for dir in "${CRATES[@]}"; do
    pkg="${PKG_NAME[$dir]}"
    snapshot="public-api/${pkg}.txt"
    echo "check-public-api: rendering $pkg" >&2
    if ! current=$(cargo public-api -p "$pkg" --simplified 2>/tmp/check-public-api.$$.log); then
        cat /tmp/check-public-api.$$.log >&2
        rm -f /tmp/check-public-api.$$.log
        echo "check-public-api: 'cargo public-api -p $pkg' failed (see above)" >&2
        status=1
        continue
    fi
    rm -f /tmp/check-public-api.$$.log

    if [ "$pkg" = "reconcile" ]; then
        reconcile_api="$current"
    fi

    if [ "$BLESS" -eq 1 ]; then
        printf '%s\n' "$current" >"$snapshot"
        continue
    fi

    if [ ! -f "$snapshot" ]; then
        echo "check-public-api: no snapshot for $pkg at $snapshot — run './scripts/check-public-api.sh --bless'" >&2
        status=1
        continue
    fi

    if ! diff -u "$snapshot" <(printf '%s\n' "$current"); then
        echo "check-public-api: $pkg's public API changed — if this is deliberate, run" >&2
        echo "'./scripts/check-public-api.sh --bless' and commit the updated $snapshot" >&2
        status=1
    fi
done

rule3_hit=0
if [ -n "$reconcile_api" ] && grep -qE '(^|[^A-Za-z0-9_])rbsr::' <<<"$reconcile_api"; then
    echo "check-public-api: reconcile's public API signatures name an rbsr:: symbol:" >&2
    grep -E '(^|[^A-Za-z0-9_])rbsr::' <<<"$reconcile_api" >&2
    rule3_hit=1
fi

reconcile_json="target/doc/reconcile.json"
if [ -f "$reconcile_json" ]; then
    if reexport_hits=$(python3 -c '
import json, sys
with open(sys.argv[1]) as f:
    doc = json.load(f)
for item in doc.get("index", {}).values():
    src = item.get("inner", {}).get("use", {}).get("source", "")
    if src == "rbsr" or src.startswith("rbsr::"):
        print(src)
' "$reconcile_json" 2>/dev/null) && [ -n "$reexport_hits" ]; then
        echo "check-public-api: reconcile re-exports rbsr symbol(s) directly:" >&2
        echo "$reexport_hits" >&2
        rule3_hit=1
    fi
fi

if [ "$rule3_hit" -eq 1 ]; then
    echo "rbsr is deliberately 0.x (AGENTS.md §11, #308) — route this through a reconcile-owned" >&2
    echo "type, or reopen the version-line decision." >&2
    status=1
fi

if [ "$BLESS" -eq 1 ] && [ "$status" -eq 0 ]; then
    echo "check-public-api: snapshots regenerated under public-api/ — review the diff before committing" >&2
fi

exit "$status"
