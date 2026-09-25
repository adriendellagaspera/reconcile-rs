#!/usr/bin/env bash
set -euo pipefail

[ "${CLAUDE_CODE_REMOTE:-}" = "true" ] || exit 0

GIT_ROOT=$(git rev-parse --show-toplevel)
for hook in pre-commit pre-push; do
    link="$GIT_ROOT/.git/hooks/$hook"
    if [ -L "$link" ] && [ "$(readlink "$link")" = "../../$hook" ]; then
        continue
    fi
    ln -sf "../../$hook" "$link"
    echo "session-start: linked $hook"
done

if command -v cargo-deny >/dev/null 2>&1; then
    echo "session-start: $(cargo deny --version) already installed"
else
    echo "session-start: installing cargo-deny (AGENTS.md §3's last gate line) ..."
    cargo install cargo-deny --locked
    echo "session-start: $(cargo deny --version) ready"
fi

if command -v cargo-nextest >/dev/null 2>&1; then
    echo "session-start: $(cargo nextest --version | head -1) already installed"
else
    echo "session-start: installing cargo-nextest (the pre-push tier, AGENTS.md §3) ..."
    cargo install cargo-nextest --locked
    echo "session-start: $(cargo nextest --version | head -1) ready"
fi

MUTANTS_VERSION=27.1.0
if command -v cargo-mutants >/dev/null 2>&1 && [ "$(cargo mutants --version | awk '{print $2}')" = "$MUTANTS_VERSION" ]; then
    echo "session-start: cargo-mutants $MUTANTS_VERSION already installed"
else
    echo "session-start: installing cargo-mutants $MUTANTS_VERSION (repo-gates' check-mutant-count.sh) ..."
    cargo install cargo-mutants --locked --version "$MUTANTS_VERSION"
    echo "session-start: $(cargo mutants --version) ready"
fi

GITLEAKS_VERSION=8.21.2
if command -v gitleaks >/dev/null 2>&1 && [ "$(gitleaks version)" = "$GITLEAKS_VERSION" ]; then
    echo "session-start: gitleaks $GITLEAKS_VERSION already installed"
else
    echo "session-start: installing gitleaks $GITLEAKS_VERSION (pre-commit tier's secret scan) ..."
    TMP=$(mktemp -d)
    curl -sSL "https://github.com/gitleaks/gitleaks/releases/download/v${GITLEAKS_VERSION}/gitleaks_${GITLEAKS_VERSION}_linux_x64.tar.gz" \
        | tar xz -C "$TMP" gitleaks
    mkdir -p "$HOME/.local/bin"
    install -m 0755 "$TMP/gitleaks" "$HOME/.local/bin/gitleaks"
    rm -rf "$TMP"
    echo "session-start: $(gitleaks version) ready"
fi
