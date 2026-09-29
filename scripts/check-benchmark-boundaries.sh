#!/usr/bin/env bash
set -euo pipefail

python3 - <<'PY'
from pathlib import Path
import re

forbidden = re.compile(
    r"\brbsr::|\buse\s+rbsr\b|\bextern\s+crate\s+rbsr\b|\bdevkit::"
)

violations = []
for path in sorted(Path("benches").glob("*.rs")):
    for number, line in enumerate(path.read_text().splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//"):
            continue
        if forbidden.search(line):
            violations.append(f"{path}:{number}: {line.strip()}")

if violations:
    print(
        "reconcile-rs benchmarks own runtime/product behavior. "
        "Direct RBSR drivers and research devkit support belong upstream/research.",
        file=__import__("sys").stderr,
    )
    for violation in violations:
        print(f"  - {violation}", file=__import__("sys").stderr)
    raise SystemExit(1)

print("runtime benchmark ownership boundary: ok")
PY
