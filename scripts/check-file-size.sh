#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR/.."

PROD_WARN=280
PROD_FAIL=400
TEST_WARN=400
TEST_FAIL=600

EXCEPTIONS=()

is_exception() {
    local f=$1 e
    for e in "${EXCEPTIONS[@]}"; do
        [ "$f" = "$e" ] && return 0
    done
    return 1
}

status=0
scanned=0
warned=0
declare -A exception_lines # path -> line count, filled in as each is found on disk below

while IFS= read -r -d '' f; do
    rel=${f#./}
    case "$rel" in
    */benches/* | benches/* | */examples/* | examples/*) continue ;;
    */tests.rs | */tests/*.rs | tests/*.rs) category=test ;;
    *) category=prod ;;
    esac

    scanned=$((scanned + 1))
    n=$(wc -l <"$f")
    if [ "$category" = prod ]; then
        warn=$PROD_WARN
        fail=$PROD_FAIL
    else
        warn=$TEST_WARN
        fail=$TEST_FAIL
    fi

    if is_exception "$rel"; then
        exception_lines["$rel"]=$n
        if [ "$n" -le "$fail" ]; then
            echo "check-file-size: $rel is $n lines ($category), no longer over the $fail-line budget" \
                "-- remove it from EXCEPTIONS in scripts/check-file-size.sh" >&2
            status=1
        fi
        continue
    fi

    if [ "$n" -gt "$fail" ]; then
        echo "check-file-size: $rel is $n lines ($category), over the $fail-line hard-fail budget" >&2
        status=1
    elif [ "$n" -gt "$warn" ]; then
        echo "check-file-size: $rel is $n lines ($category), over the $warn-line warning budget (fails at $fail)"
        warned=$((warned + 1))
    fi
done < <(find . -name '*.rs' -not -path './target/*' -print0)

for e in "${EXCEPTIONS[@]}"; do
    if [ -z "${exception_lines[$e]:-}" ]; then
        echo "check-file-size: EXCEPTIONS lists '$e', which no longer exists" \
            "-- fix or remove the entry in scripts/check-file-size.sh" >&2
        status=1
    fi
done

echo "check-file-size: ${#EXCEPTIONS[@]} files whitelisted (over hard-fail, grandfathered in EXCEPTIONS):"
for e in "${EXCEPTIONS[@]}"; do
    printf '  %-42s %s\n' "$e" "${exception_lines[$e]:-MISSING} lines"
done

if [ "$status" -eq 0 ]; then
    echo "check-file-size: $scanned files scanned, $warned over warning budget, none over hard-fail outside the whitelist above"
else
    echo >&2
    echo "Split the file rather than raising FAIL. If it genuinely cannot" >&2
    echo "decompose further, add it to EXCEPTIONS in scripts/check-file-size.sh and say why in" >&2
    echo "the commit." >&2
fi

exit "$status"
