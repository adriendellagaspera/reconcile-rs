#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR/.."

declare -A LIMITS=(
  [README.md]=80
  [ARCHITECTURE.md]=150
  [SECURITY.md]=120
  [CONTRIBUTING.md]=100
  [AGENTS.md]=100
  [CLAUDE.md]=10
  [benches/README.md]=100
)

status=0
for file in "${!LIMITS[@]}"; do
  if [ ! -f "$file" ]; then
    echo "check-doc-budget: missing $file" >&2
    status=1
    continue
  fi
  count=$(wc -l <"$file")
  limit=${LIMITS[$file]}
  printf '%-24s %4d / %d\n' "$file" "$count" "$limit"
  if [ "$count" -gt "$limit" ]; then
    echo "check-doc-budget: $file exceeds its line budget" >&2
    status=1
  fi
done

exit "$status"
