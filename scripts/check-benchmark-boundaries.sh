#!/usr/bin/env bash
set -euo pipefail

python3 - <<'PY'
from pathlib import Path
import re

benchmark_forbidden = re.compile(
    r"\brbsr::|\buse\s+rbsr\b|\bextern\s+crate\s+rbsr\b|\bdevkit::"
)
test_forbidden = re.compile(
    r"\b(?:initial_ranges|protocol_round|protocol_round_with_policy)\b|\bdevkit::"
)

violations = []
for root, pattern in ((Path("benches"), benchmark_forbidden), (Path("tests"), test_forbidden)):
    for path in sorted(root.rglob("*.rs")):
        for number, line in enumerate(path.read_text().splitlines(), 1):
            stripped = line.lstrip()
            if stripped.startswith("//"):
                continue
            if pattern.search(line):
                violations.append(f"{path}:{number}: {line.strip()}")

if violations:
    print(
        "reconcile-rs owns runtime/product behavior at its benchmark and test boundaries. "
        "Direct RBSR protocol drivers and research devkit support belong in the standalone/research repositories.",
        file=__import__("sys").stderr,
    )
    for violation in violations:
        print(f"  - {violation}", file=__import__("sys").stderr)
    raise SystemExit(1)

print("runtime benchmark/test ownership boundary: ok")
PY
