#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR/.."

LABELS_FILE=.github/labels.tsv
REPO=${REPO:-adriendellagaspera/reconcile-rs}

apply=false
prune=false
for arg in "$@"; do
    case "$arg" in
        --apply) apply=true ;;
        --prune) prune=true ;;
        *)
            echo "sync-labels: unknown argument '$arg' (expected --apply and/or --prune)" >&2
            exit 2
            ;;
    esac
done

if [ ! -f "$LABELS_FILE" ]; then
    echo "sync-labels: $LABELS_FILE is missing" >&2
    exit 1
fi

if ! command -v gh >/dev/null 2>&1; then
    echo "sync-labels: the GitHub CLI (gh) is required" >&2
    exit 1
fi

existing=$(gh label list --repo "$REPO" --limit 200 --json name --jq '.[].name')

MIGRATIONS=(
    "bug=C-bug"
    "documentation=C-docs"
    "enhancement=C-feature"
)

renamed=0
for row in "${MIGRATIONS[@]}"; do
    old=${row%%=*}
    new=${row#*=}
    grep -Fxq "$old" <<<"$existing" || continue
    if grep -Fxq "$new" <<<"$existing"; then
        printf '  SKIP    %-28s -> %s already exists\n' "$old" "$new"
        continue
    fi
    renamed=$((renamed + 1))
    printf '  RENAME  %-28s -> %s\n' "$old" "$new"
    if $apply; then
        gh label edit "$old" --repo "$REPO" --name "$new" >/dev/null
    fi
    existing=$(printf '%s\n' "$existing" | sed "s|^${old}$|${new}|")
done

declare -A wanted=()
status=0
created=0
updated=0

while IFS=$'\t' read -r name color description || [ -n "$name" ]; do
    case "$name" in ''|'#'*) continue ;; esac

    if [ -z "$color" ] || [ -z "$description" ]; then
        echo "sync-labels: '$name' is missing a colour or a description (expected 3 tab-separated fields)" >&2
        status=1
        continue
    fi
    if ! [[ "$color" =~ ^[0-9a-fA-F]{6}$ ]]; then
        echo "sync-labels: '$name' has colour '$color', expected 6 hex digits with no leading #" >&2
        status=1
        continue
    fi

    wanted["$name"]=1

    if grep -Fxq "$name" <<<"$existing"; then
        updated=$((updated + 1))
        printf '  UPDATE  %-28s #%s\n' "$name" "$color"
        if $apply; then
            gh label edit "$name" --repo "$REPO" --color "$color" --description "$description" >/dev/null
        fi
    else
        created=$((created + 1))
        printf '  CREATE  %-28s #%s\n' "$name" "$color"
        if $apply; then
            gh label create "$name" --repo "$REPO" --color "$color" --description "$description" >/dev/null
        fi
    fi
done <"$LABELS_FILE"

if [ "$status" -ne 0 ]; then
    echo >&2
    echo "sync-labels: $LABELS_FILE has malformed rows; nothing was applied" >&2
    exit "$status"
fi

deleted=0
while IFS= read -r name; do
    [ -n "$name" ] || continue
    if [ -z "${wanted[$name]+set}" ]; then
        deleted=$((deleted + 1))
        printf '  DELETE  %-28s (absent from %s)\n' "$name" "$LABELS_FILE"
        if $apply && $prune; then
            gh label delete "$name" --repo "$REPO" --yes >/dev/null
        fi
    fi
done <<<"$existing"

echo
printf '  %d to rename, %d to create, %d to update, %d not in the file\n' \
    "$renamed" "$created" "$updated" "$deleted"
if ! $apply; then
    echo "  dry run — re-run with --apply (and --prune to act on the DELETE lines)"
elif [ "$deleted" -gt 0 ] && ! $prune; then
    echo "  the DELETE lines were left alone — pass --prune to act on them"
fi
