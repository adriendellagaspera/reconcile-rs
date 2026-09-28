#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR/.."

status=0

# RSOS/RBSR purity is enforced in their canonical repository.
# This gate now checks only local workspace dependency edges against ARCHITECTURE.md.

graph_status=0
if [ -f ARCHITECTURE.md ]; then
    documented=$(
        awk '
            /^## /         { insection = ($0 ~ /^## 2\./); next }
            !insection     { next }
            /^```mermaid/ { inblock = 1; next }
            /^```/         { inblock = 0 }
            !inblock       { next }
            match($0, /^[[:space:]]*[A-Za-z_][A-Za-z0-9_]*\["/) {
                id = $0; sub(/^[[:space:]]*/, "", id); sub(/\[.*$/, "", id)
                label = $0; sub(/^[^"]*"/, "", label); sub(/\\n.*$/, "", label); sub(/".*$/, "", label)
                sub(/[[:space:]].*$/, "", label)
                name[id] = label; next
            }
            match($0, /-->/) {
                from = $0; sub(/^[[:space:]]*/, "", from); sub(/[[:space:]]*-->.*$/, "", from)
                to   = $0; sub(/^.*-->[[:space:]]*/, "", to); sub(/[[:space:]].*$/, "", to)
                if (from in name && to in name) print name[to] "|" name[from]
            }
        ' ARCHITECTURE.md | sort -u
    )
    actual=$(
        for m in Cargo.toml */Cargo.toml; do
            [ -f "$m" ] || continue
            crate=$(sed -nE 's/^name[[:space:]]*=[[:space:]]*"([^"]+)".*/\1/p' "$m" | head -1)
            [ -n "$crate" ] || continue
            [ "$crate" = "reconcile-gossip" ] && crate=gossip
            awk '/^\[dependencies\]/,/^\[[^d]/' "$m" |
                sed -nE 's/^([A-Za-z0-9_-]+)[[:space:]]*=.*path[[:space:]]*=.*/\1/p' |
                while read -r dep; do echo "$crate|$dep"; done
        done | sort -u
    )
    while IFS= read -r edge; do
        [ -n "$edge" ] || continue
        grep -qxF "$edge" <<<"$documented" ||
            { echo "check-domain-purity: the architecture dependency graph does not draw ${edge%|*} --> depends on --> ${edge#*|}" >&2; graph_status=1; }
    done <<<"$actual"
    while IFS= read -r edge; do
        [ -n "$edge" ] || continue
        grep -qxF "$edge" <<<"$actual" ||
            { echo "check-domain-purity: the architecture dependency graph draws ${edge%|*} depending on ${edge#*|}, which no manifest declares" >&2; graph_status=1; }
    done <<<"$documented"
fi

if [ "$graph_status" -ne 0 ]; then
    echo >&2
    echo "the architecture dependency graph and the workspace manifests disagree. The manifests are" >&2
    echo "ground truth: fix the diagram, or fix the dependency if the diagram was the intent." >&2
    status=1
fi

exit "$status"
