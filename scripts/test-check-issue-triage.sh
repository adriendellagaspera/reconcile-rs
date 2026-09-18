#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CHECK="$SCRIPT_DIR/check-issue-triage.sh"

run_case() {
    local expected=$1
    local needle=$2
    local payload=$3
    local fixture
    fixture=$(mktemp)
    trap 'rm -f "$fixture"' RETURN
    printf '%s\n' "$payload" >"$fixture"

    set +e
    output=$(ISSUES_JSON="$fixture" TRIAGE_GRACE_MINUTES=0 "$CHECK" 2>&1)
    status=$?
    set -e

    if [ "$expected" = pass ]; then
        if [ "$status" -ne 0 ]; then
            printf 'expected pass, got %s:\n%s\n' "$status" "$output" >&2
            return 1
        fi
    else
        if [ "$status" -eq 0 ]; then
            printf 'expected failure, got success:\n%s\n' "$output" >&2
            return 1
        fi
        grep -Fq "$needle" <<<"$output" || {
            printf 'expected diagnostic %q, got:\n%s\n' "$needle" "$output" >&2
            return 1
        }
    fi
}

created='2026-01-01T00:00:00Z'

run_case pass "" "[{
  \"number\": 1,
  \"title\": \"pure needs triage\",
  \"body\": \"\",
  \"createdAt\": \"$created\",
  \"labels\": [{\"name\": \"S-needs-triage\"}]
}]"

run_case fail "S-needs-triage carries 1 C- label(s), expected 0" "[{
  \"number\": 2,
  \"title\": \"classified but still needs triage\",
  \"body\": \"\",
  \"createdAt\": \"$created\",
  \"labels\": [
    {\"name\": \"S-needs-triage\"},
    {\"name\": \"C-cleanup\"},
    {\"name\": \"A-meta\"}
  ]
}]"

run_case fail "0 C- labels, expected exactly 1" "[{
  \"number\": 3,
  \"title\": \"ready without classification\",
  \"body\": \"\",
  \"createdAt\": \"$created\",
  \"labels\": [{\"name\": \"S-ready\"}]
}]"

echo "check-issue-triage fixtures: ok"
