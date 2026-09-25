#!/usr/bin/env bash
CLOSING_RE='(?i)\b(close[sd]?|fix(e[sd])?|resolve[sd]?)\b:?[[:space:]]*#([0-9]+)'
NONCLOSING_RE='(?i)\b(relates?\s+to|see|tracks?|blocked\s+by|part\s+of|ref(erences?)?)\b:?[[:space:]]*#([0-9]+)'

is_in() {
    local needle="$1"
    shift
    for x in "$@"; do
        [ "$x" = "$needle" ] && return 0
    done
    return 1
}
