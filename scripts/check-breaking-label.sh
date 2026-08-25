#!/usr/bin/env bash
# `M-breaking` against the public-API diff a PR actually carries (#15).
#
# `.github/labels.tsv` documents `M-breaking` as "Non-additive: needs a major version and a
# migration-guide entry", and until now nothing checked that against reality: a PR could drop a
# public item with no label anywhere and nothing noticed. #15 framed three open questions; the
# answers this script implements, and why:
#
#   1. Which diff.  Neither `cargo public-api diff ..` (which does its own git checkout of each
#      commit and refuses a dirty tree) nor a second rustdoc-JSON snapshot under `public-api/`.
#      The committed `public-api/*.txt` snapshots ARE the old API, one item per line, sorted and
#      stable -- so `git diff base...head -- public-api/*.txt` classifies directly: a removed line
#      is an item dropped or a signature changed (a changed signature renders as one `-` plus one
#      `+`), and an added-only diff is additive. No new artifact for `--bless` to keep in step, no
#      `nightly`, no `cargo-public-api`, no git-state dance -- seconds, on a checkout CI already has.
#
#   2. Where it binds.  Not in `check-public-api.sh`: that script reads only the working tree, and
#      reading a label needs `gh` plus a token. Its own workflow instead, on the same pattern as
#      `pr-issue-references.yml`, with `labeled`/`unlabeled` in the trigger list so applying the
#      label re-runs the check rather than leaving a stale red behind.
#
#   3. One direction only.  A non-additive diff with no `M-breaking` fails. The converse -- the
#      label present on an additive-only diff -- is deliberately NOT checked, and not out of
#      caution: `M-breaking` covers more than the Rust API. `v0.4.0` shipped `rsos::Fingerprint`'s
#      wire encoding as raw `[u8; 32]` with `WIRE_VERSION` 1 -> 2 (CHANGELOG), a genuine break that
#      no API text render shows. Checking that direction would cry wolf on exactly the changes the
#      label exists for -- the failure `check-doc-issue-claims.sh`'s header measured and refused.
#
# A snapshot re-render that reformats existing lines (a `cargo-public-api` upgrade, say) reads as
# non-additive here. That is the intended bias: it is loud at the one moment a human is already
# looking at the blessed diff, and the remedy is one label or one sentence, not a silent pass.
#
# One reformatting is filtered rather than left loud: a trait-bound conjunction (`A + B`) is an
# *unordered* set in Rust -- `dyn Fn(..) + Send + Sync` and `dyn Fn(..) + Sync + Send` are the same
# bound, never a different one. `cargo-public-api` renders such a run in whatever order rustdoc's
# JSON backend emits it, which is nightly-version-dependent and carries no semver weight (#66: a
# nightly bump alone reordered `ReadReplicaMap`'s synthesized `Send`/`Sync`/`Unpin`/... impls, ten
# lines, zero content change). Left unfiltered, every such nightly drift would demand a false
# `M-breaking` on a diff that removed nothing -- exactly the "cry wolf" failure mode §3's rationale
# above already rejects for the opposite direction. A `-`/`+` block is trusted as reordering-only
# only when it is a clean, equal-length swap (no line added or dropped) *and* every pair is
# identical once each bound-conjunction run within it is sorted; anything looser -- an added or
# removed line, a bound whose token set actually changed -- still counts as removed, unfiltered.
#
# Fixture mode: set PR_LABELS to a newline-separated label list to exercise the rule without `gh`.
set -Eeuo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR/.."

BASE=${BASE_SHA:?BASE_SHA is required: the base commit of the pull request}
HEAD=${HEAD_SHA:?HEAD_SHA is required: the head commit of the pull request}

# `...` (three dots) so the comparison is against the merge base, not the tip of a base branch that
# has moved on: commits landing on `main` after the PR branched are not this PR's diff.
diff=$(git diff --no-color "$BASE...$HEAD" -- 'public-api/*.txt')

if [ -z "$diff" ]; then
    echo "check-breaking-label: no public-API snapshot change in this PR"
    exit 0
fi

# `^-[^-]` skips the `--- a/...` file header. A snapshot line is a rendered Rust item (`pub fn`,
# `impl`, ...), so none can begin with `-` and be mistaken for a diff marker.
removed=$(grep -c '^-[^-]' <<<"$diff" || true)
added=$(grep -c '^+[^+]' <<<"$diff" || true)

echo "check-breaking-label: public-API snapshot diff — $added added, $removed removed/changed"

# Walks the diff in order, grouping each maximal run of `-` lines with the `+` run immediately
# following it. Filters that pair out (never touching `real_removed`) only when the two runs are
# the same length and every line matches once its bound-conjunction runs (`A + B + C`, no other
# punctuation in a token) are sorted -- an unordered-set comparison, not a text one. A `-` run with
# no matching `+` run, a length mismatch, or a pair that still differs after sorting is real,
# unfiltered. Prints the real-removed lines so the failure message below never shows reordering
# noise a human would have to mentally filter back out.
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
echo "M-breaking. Either apply the label (non-additive: needs a major version and a MIGRATING.md" >&2
echo "entry, .github/labels.tsv), or keep the item and deprecate it additively instead." >&2
exit 1
