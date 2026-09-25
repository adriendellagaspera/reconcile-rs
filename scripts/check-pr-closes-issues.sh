#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=scripts/lib-closing-refs.sh
source "$SCRIPT_DIR/lib-closing-refs.sh"

REPO="${GITHUB_REPOSITORY:-adriendellagaspera/reconcile-rs}"
BODY="${PR_BODY:-}"

mapfile -t all_refs < <(grep -oE '#[0-9]+' <<<"$BODY" | tr -d '#' | sort -u)
mapfile -t closing_refs < <(grep -oPi "$CLOSING_RE" <<<"$BODY" | grep -oE '[0-9]+' | sort -u)
mapfile -t nonclosing_refs < <(grep -oPi "$NONCLOSING_RE" <<<"$BODY" | grep -oE '[0-9]+' | sort -u)

status=0
for n in "${all_refs[@]:-}"; do
    [ -z "$n" ] && continue
    is_in "$n" "${closing_refs[@]:-}" && continue
    is_in "$n" "${nonclosing_refs[@]:-}" && continue
    state=$(gh issue view "$n" --repo "$REPO" --json state -q .state 2>/dev/null || echo "")
    if [ "$state" = "OPEN" ]; then
        echo "check-pr-closes-issues: #$n is open and mentioned without stating intent -- add 'Closes #$n' if this PR resolves it, or 'relates to #$n' if it doesn't" >&2
        status=1
    fi
done

for n in "${closing_refs[@]:-}"; do
    [ -z "$n" ] && continue
    [ "$(gh issue view "$n" --repo "$REPO" --json state -q .state 2>/dev/null || echo "")" = "OPEN" ] || continue
    body=$(gh issue view "$n" --repo "$REPO" --json body -q .body 2>/dev/null || echo "")
    open_boxes=$(grep -cE '^[[:space:]]*[-*][[:space:]]+\[ \]' <<<"$body" || true)
    if [ "${open_boxes:-0}" -gt 0 ]; then
        echo "check-pr-closes-issues: this PR closes #$n, which still has $open_boxes unticked box(es) -- tick what this PR delivers, and split or re-home what it does not, before merging" >&2
        status=1
    fi
done

exit "$status"
