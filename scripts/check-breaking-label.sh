#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR/.."

BASE=${BASE_SHA:?BASE_SHA is required: the base commit of the pull request}
HEAD=${HEAD_SHA:?HEAD_SHA is required: the head commit of the pull request}

diff=$(git diff --no-color "$BASE...$HEAD" -- 'public-api/*.txt')

if [ -z "$diff" ]; then
    echo "check-breaking-label: no public-API snapshot change in this PR"
    exit 0
fi

removed=$(grep -c '^-[^-]' <<<"$diff" || true)
added=$(grep -c '^+[^+]' <<<"$diff" || true)

echo "check-breaking-label: public-API snapshot diff — $added added, $removed removed/changed"

real_removed_output=$(python3 -c '
import re, sys

BOUND_RUN = re.compile(r"\b[A-Za-z_][\w:]*(?:\s\+\s[A-Za-z_][\w:]*)+\b")

def normalize(body):
    return BOUND_RUN.sub(lambda m: " + ".join(sorted(m.group(0).split(" + "))), body)

lines = sys.stdin.read().split("\n")
i, n = 0, len(lines)
real_removed = []
while i < n:
    line = lines[i]
    if line.startswith(("--- ", "+++ ", "@@", "diff --git", "index ")):
        i += 1
        continue
    if line.startswith("-") and not line.startswith("---"):
        rem = []
        j = i
        while j < n and lines[j].startswith("-") and not lines[j].startswith("---"):
            rem.append(lines[j])
            j += 1
        add = []
        k = j
        while k < n and lines[k].startswith("+") and not lines[k].startswith("+++"):
            add.append(lines[k])
            k += 1
        if len(rem) == len(add) and rem and all(
            normalize(r[1:]) == normalize(a[1:]) for r, a in zip(rem, add)
        ):
            i = k
            continue
        real_removed.extend(rem)
        i = k
        continue
    i += 1
print(len(real_removed))
for line in real_removed:
    print(line)
' <<<"$diff")

real_removed=$(head -n1 <<<"$real_removed_output")
real_removed_lines=$(tail -n+2 <<<"$real_removed_output")

if [ "$removed" -gt "$real_removed" ]; then
    echo "check-breaking-label: $((removed - real_removed)) of those are trait-bound reorderings" \
        "only (Rust's \`A + B\` conjunctions are unordered) — not counted as removed"
fi

if [ "$real_removed" -eq 0 ]; then
    echo "check-breaking-label: additive only, no M-breaking required"
    exit 0
fi

if [ -n "${PR_LABELS:-}" ]; then
    labels="$PR_LABELS"
else
    command -v gh >/dev/null || {
        echo "check-breaking-label: gh is required, or set PR_LABELS" >&2
        exit 1
    }
    labels=$(gh pr view "${PR_NUMBER:?PR_NUMBER is required}" --repo "${GITHUB_REPOSITORY:-adriendellagaspera/reconcile-rs}" \
        --json labels --jq '.labels[].name')
fi

if grep -qxF 'M-breaking' <<<"$labels"; then
    echo "check-breaking-label: non-additive diff, M-breaking applied"
    exit 0
fi

echo >&2
sed 's/^/  /' <<<"$real_removed_lines" >&2
echo >&2
echo "check-breaking-label: the lines above leave the public API in this PR, and it carries no" >&2
echo "M-breaking. Either apply the label for a non-additive change requiring a major version" >&2
echo "entry, .github/labels.tsv), or keep the item and deprecate it additively instead." >&2
exit 1
