#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR/.."

REPO=${REPO:-adriendellagaspera/reconcile-rs}
LABELS_FILE=.github/labels.tsv

BLOCKER_KEYWORDS='gated on|blocked by|blocked on|parked on|waits on|waiting on|unparked when'

BOXES_SINCE=${TRIAGE_BOXES_SINCE:-2026-08-15}

GRACE_MINUTES=${TRIAGE_GRACE_MINUTES:-60}
grace_cutoff=$(date -u -d "${GRACE_MINUTES} minutes ago" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null ||
    date -u -v-"${GRACE_MINUTES}"M +%Y-%m-%dT%H:%M:%SZ)

if ! command -v jq >/dev/null 2>&1; then
    echo "check-issue-triage: jq is required" >&2
    exit 1
fi
if [ -z "${ISSUES_JSON:-}" ] && ! command -v gh >/dev/null 2>&1; then
    echo "check-issue-triage: the GitHub CLI (gh) is required, or set ISSUES_JSON" >&2
    exit 1
fi

known=$(grep -v '^#' "$LABELS_FILE" | grep -v '^$' | cut -f1)

if [ -n "${ISSUES_JSON:-}" ]; then
    issues=$(cat "$ISSUES_JSON")
else
    issues=$(gh issue list --repo "$REPO" --state open --limit 500 \
        --json number,title,labels,body,createdAt)
fi

violations=0
report() {
    violations=$((violations + 1))
    printf '  #%-5s %s\n' "$1" "$2"
}

notes=0
note() {
    notes=$((notes + 1))
    printf '  #%-5s · %s\n' "$1" "$2"
}

while IFS="$(printf '\t')" read -r number kinds areas statuses needs_triage unknown blocked_ok fresh title; do
    [ -n "$number" ] || continue

    if [ "$fresh" = "fresh" ]; then
        say=note
    else
        say=report
    fi

    if [ "$statuses" -ne 1 ]; then
        $say "$number" "$statuses S- labels, expected exactly 1 — $title"
        continue
    fi

    if [ "$needs_triage" = "needs-triage" ]; then
        [ "$kinds" -eq 0 ] ||
            $say "$number" "S-needs-triage carries $kinds C- label(s), expected 0 — $title"
        [ "$areas" -eq 0 ] ||
            $say "$number" "S-needs-triage carries $areas A- label(s), expected 0 — $title"
    else
        [ "$kinds" -eq 1 ] || $say "$number" "$kinds C- labels, expected exactly 1 — $title"
        [ "$areas" -ge 1 ] || $say "$number" "no A- label — $title"
    fi

    [ "$unknown" -eq 0 ] || $say "$number" "$unknown label(s) absent from $LABELS_FILE — $title"

    case "$blocked_ok" in
        ok) ;;
        bare) $say "$number" "S-blocked names #NNN but not as 'blocked by #NNN' — rule 6 cannot read it — $title" ;;
        *)  $say "$number" "S-blocked with no #NNN blocker in the body — $title" ;;
    esac
done < <(
    jq -r --arg known "$known" --arg kw "$BLOCKER_KEYWORDS" --arg grace "$grace_cutoff" '
        ($known | split("\n")) as $known |
        .[] |
        [ .number,
          ([.labels[].name | select(startswith("C-"))] | length),
          ([.labels[].name | select(startswith("A-"))] | length),
          ([.labels[].name | select(startswith("S-"))] | length),
          (if ([.labels[].name] | index("S-needs-triage")) then "needs-triage" else "-" end),
          ([.labels[].name | select(. as $n | $known | index($n) | not)] | length),
          (if ([.labels[].name] | index("S-blocked"))
           then (if ((.body // "") | test("(?i)(?:" + $kw + ")[ \t:*_`]*#[0-9]+")) then "ok"
                 elif ((.body // "") | test("#[0-9]+")) then "bare"
                 else "missing" end)
           else "ok" end),
          (if (.createdAt // "") >= $grace then "fresh" else "-" end),
          .title
        ] | @tsv
    ' <<<"$issues"
)

declare -A REF_STATE
ref_state() {
    local n=$1
    if [ -z "${REF_STATE[$n]:-}" ]; then
        REF_STATE[$n]=$(gh issue view "$n" --repo "$REPO" --json state --jq .state 2>/dev/null || echo UNKNOWN)
    fi
    printf '%s' "${REF_STATE[$n]}"
}

if command -v gh >/dev/null 2>&1; then
    while IFS=$'\t' read -r number which refs title; do
        [ -n "$number" ] || continue
        [ "$refs" = "-" ] && continue

        open_left=0
        closed_refs=""
        unresolved=""
        for r in $refs; do
            case "$(ref_state "$r")" in
                CLOSED)  closed_refs="$closed_refs #$r" ;;
                UNKNOWN) unresolved="$unresolved #$r" ;;
                *)       open_left=$((open_left + 1)) ;;
            esac
        done

        [ -z "$unresolved" ] || note "$number" "$which references$unresolved, which resolve to no issue in $REPO — state unknown, rule 6 skipped — $title"
        [ -z "$unresolved" ] || continue

        [ -n "$closed_refs" ] || continue

        if [ "$which" = "S-blocked" ]; then
            [ "$open_left" -eq 0 ] || continue
            report "$number" "S-blocked, but every issue it references is closed —$closed_refs — $title"
        else
            report "$number" "S-parked on$closed_refs, which is closed — the stated gate is met — $title"
        fi
    done < <(
        jq -r --arg kw "$BLOCKER_KEYWORDS" '
            def refs_all: [ (.body // "") | scan("#([0-9]+)") | .[0] ] | unique;
            def refs_annotated: [ (.body // "")
                | scan("(?i)(?:" + $kw + ")[ \t:*_`]*((?:#[0-9]+(?:[ \t]*(?:,|and|&|/)[ \t]*)?)+)")
                | .[0] | scan("#([0-9]+)") | .[0] ] | unique;
            def field: join(" ") | if . == "" then "-" else . end;
            .[]
            | . as $i
            | ([$i.labels[].name]) as $l
            | if ($l | index("S-blocked")) then
                  [ $i.number, "S-blocked",
                    (($i | refs_annotated) as $named
                     | (if ($named | length) > 0 then $named else ($i | refs_all) end) | field),
                    $i.title ]
              elif ($l | index("S-parked")) then
                  [ $i.number, "S-parked", (($i | refs_annotated) | field), $i.title ]
              else empty end
            | @tsv
        ' <<<"$issues"
    )
fi

if [ -n "${CLOSED_ISSUES_JSON:-}" ]; then
    closed=$(cat "$CLOSED_ISSUES_JSON")
elif [ -z "${ISSUES_JSON:-}" ] && command -v gh >/dev/null 2>&1; then
    closed=$(gh issue list --repo "$REPO" --state closed --limit 500 \
        --json number,title,body,closedAt,stateReason)
else
    closed=""
fi

if [ -n "$closed" ]; then
    while IFS=$'\t' read -r number open_boxes title; do
        [ -n "$number" ] || continue
        report "$number" "closed with $open_boxes unticked acceptance box(es) — split it, or tick them — $title"
    done < <(
        jq -r --arg since "$BOXES_SINCE" '
            .[]
            | . as $i
            | select(((.stateReason // "") | ascii_upcase) != "NOT_PLANNED")
            | select((.closedAt // "") >= $since)
            | ([ (.body // "") | scan("(?m)^[ \t]*[-*][ \t]+\\[[ ]\\]") ] | length) as $open
            | select($open > 0)
            | [ $i.number, $open, $i.title ] | @tsv
        ' <<<"$closed"
    )

    historical=$(jq --arg since "$BOXES_SINCE" '
        [ .[] | select(((.stateReason // "") | ascii_upcase) != "NOT_PLANNED")
              | select((.closedAt // "") < $since)
              | select(([ (.body // "") | scan("(?m)^[ \t]*[-*][ \t]+\\[[ ]\\]") ] | length) > 0) ]
        | length' <<<"$closed")
    [ "$historical" -eq 0 ] ||
        echo "  · $historical issue(s) closed before $BOXES_SINCE carry unticked boxes — predate the rule, not gated"
fi

if [ -z "${ISSUES_JSON:-}" ] && command -v gh >/dev/null 2>&1; then
    parents=$(gh api "repos/$REPO/issues?state=open&per_page=100" --paginate 2>/dev/null \
        | jq -s 'add // [] | map(select(.pull_request == null))' 2>/dev/null || echo '[]')

    if [ "$(jq '[.[] | select(has("sub_issues_summary"))] | length' <<<"$parents")" -eq 0 ]; then
        echo "  · sub_issues_summary absent from the REST payload — rule 8 skipped, not passed"
    else
        while IFS=$'\t' read -r number tracking total title; do
            [ -n "$number" ] || continue
            if [ "$tracking" = "tracking" ]; then
                report "$number" "C-tracking-issue open with all $total sub-issues closed — it carries no work of its own — $title"
            else
                note "$number" "open with all $total sub-issues closed — check whether anything of its own is left — $title"
            fi
        done < <(
            jq -r '
                .[]
                | . as $i
                | (.sub_issues_summary // {}) as $s
                | select(($s.total // 0) > 0 and ($s.completed // 0) == $s.total)
                | [ $i.number,
                    (if ([$i.labels[].name] | index("C-tracking-issue")) then "tracking" else "-" end),
                    $s.total,
                    $i.title ] | @tsv
            ' <<<"$parents"
        )
    fi
fi

total=$(jq 'length' <<<"$issues")
untriaged=$(jq '[.[] | select([.labels[].name] | index("S-needs-triage"))] | length' <<<"$issues")

echo
printf '  %d open issues, %d awaiting triage, %d violation(s), %d note(s)\n' \
    "$total" "$untriaged" "$violations" "$notes"

if [ -n "${TRIAGE_SLA_DAYS:-}" ] && [ "$untriaged" -gt 0 ]; then
    cutoff=$(date -u -d "${TRIAGE_SLA_DAYS} days ago" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null ||
        date -u -v-"${TRIAGE_SLA_DAYS}"d +%Y-%m-%dT%H:%M:%SZ)
    stale=$(jq -r --arg cutoff "$cutoff" '
        [.[] | select(([.labels[].name] | index("S-needs-triage")) and .createdAt < $cutoff)]
        | map("#\(.number) \(.title)") | .[]' <<<"$issues")
    if [ -n "$stale" ]; then
        echo >&2
        echo "  awaiting triage for more than ${TRIAGE_SLA_DAYS} days:" >&2
        echo "$stale" | sed 's/^/    /' >&2
        violations=$((violations + $(wc -l <<<"$stale")))
    fi
fi

if [ "$violations" -gt 0 ]; then
    echo >&2
    echo "check-issue-triage: $violations issue(s) do not satisfy .github/labels.tsv's invariants." >&2
    echo "Fix the labels, or change the taxonomy in that file and say why in the commit." >&2
    exit 1
fi
