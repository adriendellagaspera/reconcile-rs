#!/usr/bin/env bash
set -Eeuo pipefail

REPO="${GITHUB_REPOSITORY:-adriendellagaspera/reconcile-rs}"
BASE="${1:-origin/main}"

if [ "$(git rev-parse --is-shallow-repository)" = "true" ]; then
    echo "check-closed-issue-shas: shallow clone -- fetch full history first (checkout: fetch-depth: 0)" >&2
    exit 1
fi

status=0
count=0

while IFS= read -r issue; do
    number=$(jq -r '.number' <<<"$issue")
    url=$(jq -r '.url' <<<"$issue")
    body=$(jq -r '.body' <<<"$issue")

    shas=$(grep -oE '`[0-9a-f]{7,40}`' <<<"$body" | tr -d '`' | sort -u || true)

    for sha in $shas; do
        count=$((count + 1))
        if ! git cat-file -e "${sha}^{commit}" 2>/dev/null; then
            continue # not a commit this clone knows about -- prose, not a citation
        fi
        if ! git merge-base --is-ancestor "$sha" "$BASE" 2>/dev/null; then
            echo "check-closed-issue-shas: issue #$number ($url) cites $sha, which is not an ancestor of $BASE" >&2
            status=1
        fi
    done
done < <(gh issue list --repo "$REPO" --state closed --limit 500 --json number,url,body | jq -c '.[]')

echo "check-closed-issue-shas: checked $count cited SHA(s) across closed issues"
exit "$status"
