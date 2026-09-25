#!/usr/bin/env bash

changed_paths() {
    local base="$1" target="${2:-}"
    if [ -z "$target" ]; then
        git diff --name-only "$base"
        git ls-files --others --exclude-standard
    else
        git diff --name-only "${base}...${target}"
    fi | sort -u
}

_is_rust_path() {
    case "$1" in
        *.rs | */Cargo.toml | Cargo.toml | Cargo.lock | rust-toolchain* | .github/workflows/main.yml) return 0 ;;
        *) return 1 ;;
    esac
}

_is_deps_path() {
    case "$1" in
        */Cargo.toml | Cargo.toml | Cargo.lock | deny.toml | .github/workflows/main.yml) return 0 ;;
        *) return 1 ;;
    esac
}

_any_path_matches() {
    local matcher="$1" base="$2" target="${3:-}" path
    while IFS= read -r path; do
        [ -z "$path" ] && continue
        "$matcher" "$path" && return 0
    done < <(changed_paths "$base" "$target")
    return 1
}

affects_rust() { _any_path_matches _is_rust_path "$1" "${2:-}"; }
affects_deps() { _any_path_matches _is_deps_path "$1" "${2:-}"; }
