---
description: Writing or modifying tests
globs: ["tests/**/*.rs", "**/tests/**/*.rs", "**/src/**/tests.rs"]
---

Tests assert behavior or invariants, not implementation accidents.

- Prefer properties such as round-trip, convergence, ordering, and idempotence.
- Do not accept generated snapshots without reviewing their semantics.
- Do not add tests whose only assertion is that code did not panic.
- Keep randomized tests deterministic and avoid fixed network ports.

The mutation workflow checks whether changed tests detect faults.
