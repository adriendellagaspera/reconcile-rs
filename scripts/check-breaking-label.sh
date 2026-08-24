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

if [ "$removed" -eq 0 ]; then
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
grep '^-[^-]' <<<"$diff" | sed 's/^/  /' >&2
echo >&2
echo "check-breaking-label: the lines above leave the public API in this PR, and it carries no" >&2
echo "M-breaking. Either apply the label (non-additive: needs a major version and a MIGRATING.md" >&2
echo "entry, .github/labels.tsv), or keep the item and deprecate it additively instead." >&2
exit 1
