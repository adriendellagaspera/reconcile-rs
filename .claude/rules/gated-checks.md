---
description: Repository verification
globs: ["**/*"]
---

Use targeted commands while developing. Do not replay the repository's full verification matrix
manually: pre-commit, pre-push, and GitHub Actions own it.

When a gate fails, diagnose that failure directly. Regeneration commands such as
./scripts/check-public-api.sh --bless are authoring steps, not verification.
