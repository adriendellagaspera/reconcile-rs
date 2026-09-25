#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR/.."

status=0
while IFS= read -r -d '' f; do
    echo "check-test-file-naming: $f -- split test modules are named 'tests.rs' (mod tests;)," >&2
    echo "  not '<name>_tests.rs'. Rename the file and its 'mod' declaration to match." >&2
    status=1
done < <(find . -path './target' -prune -o -path '*/src/*_tests.rs' -print0)

if [ "$status" -eq 0 ]; then
    echo "check-test-file-naming: no '<name>_tests.rs' files under src/"
fi

exit "$status"
