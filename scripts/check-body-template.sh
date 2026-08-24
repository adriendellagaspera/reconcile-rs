#!/usr/bin/env bash
# PR and issue bodies against the templates they were opened from (#9).
#
# `.github/pull_request_template.md` and `.github/ISSUE_TEMPLATE/issue.md` both state AGENTS.md §9
# in an HTML comment, and a comment is not a gate: a body could drop the mandatory sections, invent
# its own, or rename the table's rows, and CI stayed green. §10 names that exact shape -- a rule a
# human must remember and enforce by eye -- as the kind that belongs in a workflow.
#
# What is checked, and nothing else:
#
#   1. every `##` heading in the body is one the template has
#   2. the PR arm additionally requires its one mandatory heading; the issue arm does not, because
#      `ISSUE_TEMPLATE/issue.md`'s own comment says to delete a section that does not apply. So the
#      issue arm is subset-in-template-order, exactly as #9 specifies -- it does not invent a
#      "`## Problem` is mandatory" rule the template never stated
#   3. the leading table's row labels, in order: a `Why` renamed to `Motivation` is drift that a
#      heading check alone misses
#
# Deliberately NOT checked (#9's rejected-checks table): body length, prose-to-table ratio,
# restatement across documents. "Too verbose" is not mechanically decidable, and a gate on it would
# punish a body that genuinely needs to explain. §10 is about rules a command can decide.
#
# Only `##` counts. Deeper headings are free: the templates fix the *shape* of a body, not how its
# sections are organised inside, and `###` under a template section is elaboration rather than
# drift.
#
# CI only, both arms: the subject is the GitHub body, which no local hook has -- the same reason
# `check-pr-closes-issues.sh` is CI-only (AGENTS.md §3).
#
# Grandfathering. Every issue opened before this gate predates the rule, and editing one later must
# not fail on a template it never saw. The issue arm therefore skips anything created before
# TEMPLATE_SINCE, the same bounded-window shape `check-issue-triage.sh`'s rule 7 uses and for the
# same reason: a gate that is red on arrival for the backlog teaches people to ignore it.
set -Eeuo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR/.."

TEMPLATE_SINCE=${TEMPLATE_SINCE:-2026-08-24}

arm=${1:?usage: check-body-template.sh (pr|issue)}

case "$arm" in
pr)
    body=${PR_BODY:?PR_BODY is required}
    allowed=(Verification)
    mandatory=(Verification)
    table_rows=(Issue Change Why)
    template=.github/pull_request_template.md
    ;;
issue)
    body=${ISSUE_BODY:?ISSUE_BODY is required}
    created=${ISSUE_CREATED_AT:-}
    # ISO-8601 sorts lexicographically, so a date-only cutoff compares directly against a full
    # timestamp -- no date arithmetic, nothing to get wrong across `date` implementations.
    if [ -n "$created" ] && [ "$created" \< "$TEMPLATE_SINCE" ]; then
        echo "check-body-template: issue created $created, before $TEMPLATE_SINCE -- predates the gate, not checked"
        exit 0
    fi
    allowed=(Problem Fix Acceptance)
    mandatory=()
    table_rows=(Where What)
    template=.github/ISSUE_TEMPLATE/issue.md
    ;;
*)
    echo "check-body-template: unknown arm '$arm' (expected 'pr' or 'issue')" >&2
    exit 1
    ;;
esac

# A fenced block may legitimately contain a line starting with `## ` (a shell comment, a diff of
# this very template), and an HTML comment may carry one the author meant to leave inert -- the
# templates ship with such comments. Neither is a heading, so both come out before scanning.
strip_noise() {
    awk '
        /^[ \t]*```/ { fenced = !fenced; next }
        fenced { next }
        { line = $0
          while (match(line, /<!--/)) {
              before = substr(line, 1, RSTART - 1)
              rest = substr(line, RSTART + 4)
              if (match(rest, /-->/)) { line = before substr(rest, RSTART + 3); continue }
              in_comment = 1; line = before; break
          }
          if (in_comment && match($0, /-->/)) { in_comment = 0; line = substr($0, RSTART + 3) }
          else if (in_comment) next
          print line
        }
    '
}

clean=$(strip_noise <<<"$body")

status=0
fail() {
    echo "check-body-template: $1" >&2
    status=1
}

# --- headings ------------------------------------------------------------------------------
mapfile -t headings < <(grep -E '^## +\S' <<<"$clean" | sed -E 's/^## +//; s/[[:space:]]+$//' || true)

in_list() {
    local needle=$1 item
    shift
    for item in "$@"; do [ "$item" = "$needle" ] && return 0; done
    return 1
}

for h in "${headings[@]}"; do
    in_list "$h" "${allowed[@]}" ||
        fail "'## $h' is not a section of $template -- the template's sections are: ${allowed[*]}"
done

for m in "${mandatory[@]:-}"; do
    [ -n "$m" ] || continue
    in_list "$m" "${headings[@]:-}" || fail "'## $m' is missing -- $template requires it"
done

# Subset is not enough on its own: sections in a scrambled order read as a different document.
# Compared against `allowed` filtered to what the body actually has, so a deleted section is fine.
expected_order=()
for a in "${allowed[@]}"; do
    in_list "$a" "${headings[@]:-}" && expected_order+=("$a")
done
if [ "${headings[*]:-}" != "${expected_order[*]:-}" ]; then
    fail "sections are out of template order: got '${headings[*]:-}', expected '${expected_order[*]:-}'"
fi

# --- the leading table's row labels --------------------------------------------------------
# The first table of the body (the PR arm's Issue/Change/Why, the issue arm's Where/What under
# `## Problem`). A body that deleted the section holding it has no table to check.
mapfile -t labels < <(
    grep -E '^\|' <<<"$clean" |
        grep -vE '^\|[[:space:]]*-+' |
        sed -E 's/^\|[[:space:]]*//; s/[[:space:]]*\|.*$//' |
        grep -vE '^$' || true
)

if [ "${#labels[@]}" -gt 0 ]; then
    got=("${labels[@]:0:${#table_rows[@]}}")
    if [ "${got[*]}" != "${table_rows[*]}" ]; then
        fail "the leading table's rows are '${got[*]}', not $template's '${table_rows[*]}'"
    fi
elif [ "$arm" = pr ]; then
    fail "no leading table -- $template opens with one, rows: ${table_rows[*]}"
fi

if [ "$status" -eq 0 ]; then
    echo "check-body-template: $arm body conforms to $template"
else
    echo >&2
    echo "The templates fix the shape of a body; this gate makes them binding (#9). Structure only --" >&2
    echo "never length: see the script's header for what is deliberately not checked." >&2
fi

exit "$status"
