#!/usr/bin/env bash
set -Eeuo pipefail

GIT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$GIT_ROOT"

CRATE_SRC_DIRS=(src rsos/src rbsr/src lww-register/src gossip/src)
TOP_N=15

RCA_OUT=$(mktemp -d)
JSCPD_OUT=$(mktemp -d)
trap 'rm -rf "$RCA_OUT" "$JSCPD_OUT"' EXIT

echo "## Cognitive complexity (rust-code-analysis)"
echo
RCA_PATH_ARGS=()
for dir in "${CRATE_SRC_DIRS[@]}"; do
    RCA_PATH_ARGS+=(-p "$dir")
done
rust-code-analysis-cli -m -O json -o "$RCA_OUT" "${RCA_PATH_ARGS[@]}" -j "$(nproc)" >/dev/null

echo "Top $TOP_N functions by cognitive complexity (higher = harder to hold in your head reading"
echo "top to bottom -- Mozilla's rust-code-analysis definition, not cyclomatic path count):"
echo
echo '| complexity | function |'
echo '|---|---|'
find "$RCA_OUT" -name '*.json' -print0 |
    xargs -0 -I{} jq -r '.. | objects | select(.kind? == "function") | "\(.metrics.cognitive.sum)\t\(.name)"' {} |
    sort -t $'\t' -k1 -rn |
    awk -F'\t' -v n="$TOP_N" 'NR <= n {printf "| %s | `%s` |\n", $1, $2}'

echo
echo "## Duplication (jscpd, >=10 lines / >=50 tokens)"
echo
jscpd --min-lines 10 --min-tokens 50 --reporters json --silent -o "$JSCPD_OUT" \
    --ignore '**/target/**' "${CRATE_SRC_DIRS[@]}" >/dev/null

jq -r '
  .statistics.total as $t
  | "- \($t.clones) clone pairs across \($t.sources) files",
    "- \($t.duplicatedLines) duplicated lines / \($t.lines) total (\($t.percentage)%)"
' "$JSCPD_OUT/jscpd-report.json"
