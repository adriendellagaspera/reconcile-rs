#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
python3 - <<'PYTHON'
import pathlib
import re
import tomllib

source = pathlib.Path("src/replica/gc.rs").read_text()
closed = re.search(r"fn has_collection_proof\(\)\s*->\s*bool\s*\{\s*false\s*\}", source)
if not closed:
    raise SystemExit("check-gc-fence: collection has no reviewed history-fencing implementation; keep the gate closed or explicitly replace this safety policy and its equivalent-mutant exclusions")
expected = [
    r"replicated_map/write\.rs:.*replace ReplicatedMap.*clear_expired_tombstones with \(\)",
    r"signed_replica/causal\.rs:.*replace SignedReplica.*collect_if_stable -> bool with false",
    r"replica/gc\.rs:.*replace Replica.*gc_collect -> Option.* with None",
]
with open(".cargo/mutants.toml", "rb") as stream:
    exclusions = tomllib.load(stream)["exclude_re"]
if any(pattern not in exclusions for pattern in expected):
    raise SystemExit("check-gc-fence: equivalent-mutant policy and closed history gate disagree")
print("check-gc-fence: physical collection closed; only equivalent closed-gate replacements excluded")
PYTHON
