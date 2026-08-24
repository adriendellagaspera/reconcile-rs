#!/usr/bin/env bash
# Enforces size budgets on this repo's prose docs: the agent instruction files
# (cumulative), and SOTA.md (its own, separate cap).
#
# AGENTS.md + CLAUDE.md are budgeted *cumulatively*, not per-file, because that is how
# the two are consumed: CLAUDE.md opens with `@AGENTS.md`, which imports the whole file
# verbatim, so any reader — human or agent — gets the sum. Budgeting them separately
# would let the pair grow without bound while each file looked disciplined.
#
# Why a budget at all: these two files are read in full, every time, before any work
# happens. Past that length they stop being read closely, which is the failure mode
# AGENTS.md's own preamble names — "This file states rules, not rationale" — and the
# reason it links out instead of explaining in place. The cap turns that intent into a
# failing command (AGENTS.md §10) rather than a habit someone has to remember.
#
# SOTA.md is a different kind of doc — a durable reference, not read in full every
# session (AGENTS.md §9) — so it gets its own cap rather than joining the sum above;
# mixing the two would either gate a reference doc on a read-every-time budget it was
# never meant to fit, or let read-every-time prose hide inside a much larger allowance.
# It is budgeted at all because "durable, not updated for routine changes" has still
# proven not to mean "never grows": PROGRESS.md's retirement redistributed prose here
# with nothing tracking the result (978 -> 1021 lines in that one change). The cap
# exists so that keeps being a decision made in the open, not untracked drift.
#
# But that failure mode is *prose* drift, and until #472 the budget was one number over a
# file that is 39% glossary, bibliography and index. Those sections are supposed to grow
# monotonically as the literature is surveyed, so a whole-file cap taxes them for the
# prose's sins: the first thing it blocks is adding a reference, which is the last thing
# this repository should make expensive. #472 hit exactly that — the cap fired on a merge
# and the cheapest way out would have been to drop a citation.
#
# So the cap is scoped to what it was always about: §1–§2, everything above the reference
# half. §3 (glossary), §4 (bibliography) and §5 (index) are *reported every run but never
# capped* — that keeps "growth is fine, silent growth is not" without ever standing
# between a reader and a citation.
#
# When either budget fires, the fix is to *move* prose, not to compress it into denser
# prose:
#   - rationale, measurements and worked examples  -> CONTRIBUTING.md or ARCHITECTURE.md
#   - a rule someone must remember and apply by eye -> a script wired into a hook and CI
# Raising a cap is a deliberate decision, not the default remedy: change the constant
# below and say why in the commit.
set -Eeuo pipefail

AGENTS_MAX_LINES=200
# §1–§2 only. See the header: the reference half is reported, never capped.
SOTA_PROSE_MAX_LINES=700

# The heading that opens the reference half. Load-bearing: if it is renamed, this script
# fails loudly rather than silently budgeting the whole file again.

# Resolve the repo root from the script's own location (not `git rev-parse`, since the
# pre-commit hook runs this against a bare `git checkout-index` copy with no `.git`).
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR/.."

AGENTS_FILES=(
    AGENTS.md
    CLAUDE.md
)

status=0

check_missing() {
    local f=$1
    if [ ! -f "$f" ]; then
        echo "check-doc-budget: $f listed but missing — update the script" >&2
        status=1
        return 1
    fi
    return 0
}

echo "AGENTS.md + CLAUDE.md (cumulative, read in full every session):"
total=0
for f in "${AGENTS_FILES[@]}"; do
    check_missing "$f" || continue
    n=$(wc -l <"$f")
    printf '  %-12s %4d\n' "$f" "$n"
    total=$((total + n))
done

if [ "$status" -ne 0 ]; then
    exit "$status"
fi

printf '  %-12s %4d / %d\n' "total" "$total" "$AGENTS_MAX_LINES"

if [ "$total" -gt "$AGENTS_MAX_LINES" ]; then
    echo >&2
    echo "check-doc-budget: AGENTS.md + CLAUDE.md are $total lines, over the $AGENTS_MAX_LINES-line budget" >&2
    echo "by $((total - AGENTS_MAX_LINES))." >&2
    echo >&2
    echo "Move prose out rather than compressing it: rationale and worked examples belong in" >&2
    echo "CONTRIBUTING.md or ARCHITECTURE.md, and a rule enforced by eye belongs in a script" >&2
    echo "wired into a hook and CI (AGENTS.md §10)." >&2
    echo >&2
    status=1
fi

echo
echo "SOTA.md (durable positioning, capped in full):"
if check_missing SOTA.md; then
    total=$(wc -l <SOTA.md)
    # No prose/reference split any more: the glossary, bibliography and index (§3-§5) had no
    # consumer in this repository -- nothing outside SOTA.md itself cited them -- and moved out
    # with the rest of the literature material. What is left is §1-§2, all of it capped.
    prose=$total
    printf '  %-22s %4d / %d\n' "§1-§2 prose" "$prose" "$SOTA_PROSE_MAX_LINES"
    if [ "$prose" -gt "$SOTA_PROSE_MAX_LINES" ]; then
        echo >&2
        echo "check-doc-budget: SOTA.md's §1-§2 prose is $prose lines, over the" >&2
        echo "$SOTA_PROSE_MAX_LINES-line budget by $((prose - SOTA_PROSE_MAX_LINES))." >&2
        echo >&2
        echo "Move it rather than compressing it: a measurement or worked example belongs in" >&2
        echo "ARCHITECTURE.md or a test's module docs, a live status belongs in the tracker" >&2
        echo "(AGENTS.md §1/§9). Raising the constant is a deliberate decision: change it above" >&2
        echo "and say why in the commit." >&2
        echo >&2
        status=1
    fi
fi

exit "$status"
