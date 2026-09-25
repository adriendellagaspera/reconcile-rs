---
description: Searching this repository
globs: ["**/*"]
---

Start with file-level search (rg -l or rg -c) and narrow before reading whole files.

Use explicit globs when generated or lock files are irrelevant. For structural Rust queries, prefer
syntax-aware search once a regular expression would approximate the language.
