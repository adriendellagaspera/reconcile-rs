#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR/.."

DOMAIN_FILES=(
    lww-register/src/lib.rs
    lww-register/src/bounds.rs
    lww-register/src/clock.rs
    lww-register/src/entry.rs
    lww-register/src/persistence.rs
)

FORBIDDEN='^\s*use\s+(tokio|bincode|chrono|ipnet|mio|reqwest|hyper|std::net)\b'

NET_VALUE_TYPES='(IpAddr|Ipv4Addr|Ipv6Addr|SocketAddr|SocketAddrV4|SocketAddrV6|AddrParseError)'
ALLOWED="^[0-9]+:[[:space:]]*use[[:space:]]+std::net::(\{[[:space:]]*)?(${NET_VALUE_TYPES}([[:space:]]*,[[:space:]]*)?)+[[:space:]]*\}?[[:space:]]*;[[:space:]]*$"

status=0
for f in "${DOMAIN_FILES[@]}"; do
    if [ ! -f "$f" ]; then
        echo "check-domain-purity: $f listed but missing — update the script" >&2
        status=1
        continue
    fi
    if hits=$(grep -nE "$FORBIDDEN" "$f" | grep -vE "$ALLOWED"); then
        echo "check-domain-purity: infrastructure import(s) in domain module $f:" >&2
        echo "$hits" >&2
        status=1
    fi
done

if [ "$status" -ne 0 ]; then
    echo >&2
    echo "Domain modules must stay infrastructure-free." >&2
    echo "Route the dependency through a port/adapter instead, or move the code out of the domain." >&2
    echo >&2
fi

manifest_status=0

STANDALONE_MANIFESTS=(
    rsos/Cargo.toml
    rbsr/Cargo.toml
    lww-register/Cargo.toml
)

FORBIDDEN_DEPS='tokio|bincode|chrono|ipnet|mio|reqwest|hyper|socket2|async-trait'

for m in "${STANDALONE_MANIFESTS[@]}"; do
    if [ ! -f "$m" ]; then
        echo "check-domain-purity: $m listed but missing — update the script" >&2
        manifest_status=1
        continue
    fi
    crate_dir=$(dirname "$m")
    if ! awk -v forb="^(${FORBIDDEN_DEPS})\$" \
             -v crate="$crate_dir" '
        { line = $0; sub(/#.*$/, "", line) }
        line ~ /^[[:space:]]*\[/ {
            hdr = line
            sub(/^[[:space:]]*\[+/, "", hdr)
            sub(/\]+.*$/, "", hdr)
            gsub(/[[:space:]"'"'"']/, "", hdr)
            insec = (hdr ~ /(^|\.)(dependencies|dev-dependencies|build-dependencies)$/)
            if (hdr ~ /(^|\.)(dependencies|dev-dependencies|build-dependencies)\.[A-Za-z0-9_.-]+$/) {
                nm = hdr
                sub(/^.*dependencies\./, "", nm)
                if (nm ~ forb) {
                    printf "  line %d: [%s]\n", NR, hdr; bad = 1
                }
            }
            next
        }
        insec && line ~ /=/ {
            key = line
            sub(/=.*$/, "", key)
            gsub(/[[:space:]"'"'"']/, "", key)
            sub(/\..*$/, "", key)
            if (key ~ forb) {
                printf "  line %d: %s\n", NR, key; bad = 1
            }
            if (match(line, /package[[:space:]]*=[[:space:]]*"[^"]+"/)) {
                pkg = substr(line, RSTART, RLENGTH)
                sub(/^[^"]*"/, "", pkg)
                sub(/".*$/, "", pkg)
                if (pkg ~ forb) {
                    printf "  line %d: %s (renamed dependency)\n", NR, pkg; bad = 1
                }
            }
        }
        END { exit bad ? 1 : 0 }
    ' "$m"; then
        echo "check-domain-purity: forbidden infrastructure dependency in $m (see above)" >&2
        manifest_status=1
    fi
done

if [ "$manifest_status" -ne 0 ]; then
    echo >&2
    echo "rsos/rbsr/lww-register must stay standalone: no async runtime, socket, wire codec or" >&2
    echo "wall clock in their manifests. Put the adapter" >&2
    echo "in gossip or reconcile instead." >&2
    status=1
fi

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
