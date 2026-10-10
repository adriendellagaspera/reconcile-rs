#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")/.."

expected=$(python3 - <<'PY'
import tomllib
with open('Cargo.lock', 'rb') as source:
    lock = tomllib.load(source)
print(next(package['version'] for package in lock['package'] if package['name'] == 'wasm-bindgen'))
PY
)
if [ "$(wasm-bindgen --version)" != "wasm-bindgen $expected" ]; then
  echo "Install wasm-bindgen-cli $expected to match Cargo.lock" >&2
  exit 1
fi
CARGO_PROFILE_RELEASE_DEBUG=0 cargo build --locked --release -p reconcile-swarm-web --target wasm32-unknown-unknown
mkdir -p target/swarm-pages/pkg
cp examples/swarm/{index.html,app.js,knowledge.js,api.js,worker-client.js,worker.js} target/swarm-pages/
printf "export const apiBase = '';\nexport const simulationMode = 'wasm';\n" > target/swarm-pages/config.js
wasm-bindgen --target web --no-typescript --out-dir target/swarm-pages/pkg target/wasm32-unknown-unknown/release/reconcile_swarm_web.wasm
touch target/swarm-pages/.nojekyll
