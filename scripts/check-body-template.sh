#!/usr/bin/env bash
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

expected_order=()
for a in "${allowed[@]}"; do
    in_list "$a" "${headings[@]:-}" && expected_order+=("$a")
done
if [ "${headings[*]:-}" != "${expected_order[*]:-}" ]; then
    fail "sections are out of template order: got '${headings[*]:-}', expected '${expected_order[*]:-}'"
fi

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
