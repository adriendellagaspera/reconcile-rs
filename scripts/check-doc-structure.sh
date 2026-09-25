#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR/.."

status=0
fail() { echo "check-doc-structure: $*" >&2; status=1; }

mapfile -t DOCS < <(find . -type f -name '*.md' -not -path './target/*' -not -path './.git/*' | sort)

declare -A ANCHOR_CACHE
anchors_for() {
  local file=$1 headings ids
  [[ -v ANCHOR_CACHE[$file] ]] && return 0
  headings=$(sed -nE 's/^#{1,6}[[:space:]]+(.*)$/\1/p' "$file" |
    sed -E 's/[`*_]//g; s/\[([^]]*)\]\([^)]*\)/\1/g' |
    sed -E 's/[^[:alnum:][:space:]_-]//g' | tr '[:upper:]' '[:lower:]' |
    sed -E 's/[[:space:]]+/-/g; s/^-+|-+$//g') || true
  ids=$(grep -oE '<a[[:space:]]+id="[^"]+"' "$file" | sed -E 's/.*id="([^"]+)".*/\1/') || true
  ANCHOR_CACHE[$file]=$'\n'"$headings"$'\n'"$ids"$'\n'
}

for doc in "${DOCS[@]}"; do
  dir=$(dirname "$doc")
  while IFS= read -r target; do
    case "$target" in http://*|https://*|mailto:*|"") continue ;; esac
    file_part=${target%%#*}
    frag=''
    [[ "$target" == *'#'* ]] && frag=${target#*#}
    if [ -n "$file_part" ]; then
      resolved="$dir/$file_part"
      [ -e "$resolved" ] || { fail "$doc: missing link target $target"; continue; }
    else
      resolved="$doc"
    fi
    if [ -n "$frag" ] && [[ "$resolved" == *.md ]]; then
      anchors_for "$resolved"
      [[ ${ANCHOR_CACHE[$resolved]} == *$'\n'"$frag"$'\n'* ]] || fail "$doc: missing anchor $target"
    fi
  done < <(grep -oE '\]\([^)[:space:]]+\)' "$doc" | sed -E 's/^\]\(//; s/\)$//')

  while IFS= read -r ref; do
    [[ "$ref" == */* && "$ref" =~ \.[a-z]+$ ]] || continue
    case "$ref" in *.com/*|*.org/*|*.io/*|*.net/*|*.dev/*|target/*|*'*'*|*'{'*) continue ;; esac
    [ -e "$dir/$ref" ] || [ -e "$ref" ] || fail "$doc: missing path $ref"
  done < <(grep -oE '`[A-Za-z0-9_][A-Za-z0-9_./*{}-]*`' "$doc" | tr -d '`')
done

CURRENT_DOCS=(
  README.md ARCHITECTURE.md SECURITY.md CONTRIBUTING.md AGENTS.md CLAUDE.md
  benches/README.md examples/README.md examples/k8s/README.md examples/k8s/kind/README.md
  gossip/README.md lww-register/README.md rbsr/README.md rsos/README.md public-api/README.md
)
for doc in "${CURRENT_DOCS[@]}"; do
  if grep -nE '#[0-9]+' "$doc" >/dev/null; then fail "$doc: tracker references do not belong in current-state documentation"; fi
done

rust_refs=$(grep -rnE '^[[:space:]]*//[/!]?.*(#[0-9]+|ARCHITECTURE\.md|AGENTS\.md|POSITIONING\.md|MIGRATING\.md|CHANGELOG\.md|README\.md)' --include='*.rs' --exclude-dir=target . || true)
if [ -n "$rust_refs" ]; then
  echo "$rust_refs" >&2
  fail 'Rust comments must be self-contained and must not cite tracker issues or repository prose'
fi

config_refs=$(grep -rnE '^[[:space:]]*#([^!]|$).*(#[0-9]+|POSITIONING\.md|MIGRATING\.md|CHANGELOG\.md)' \
  --include='*.sh' --include='*.toml' --include='*.yml' --include='*.yaml' \
  --exclude='check-doc-structure.sh' --exclude-dir=target . || true)
if [ -n "$config_refs" ]; then
  echo "$config_refs" >&2
  fail 'Configuration comments must describe current local constraints, not tracker history'
fi
for old in CHANGELOG.md MIGRATING.md POSITIONING.md; do
  [ ! -e "$old" ] || fail "$old is historical documentation"
done

exit "$status"
