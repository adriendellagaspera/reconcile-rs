# AGENTS.md

Repository rules for humans and coding agents.

## Documentation

Versioned documentation describes only the current tree.

- One fact has one authoritative home; other surfaces link to it.
- Public API contracts belong in rustdoc.
- Repository-wide structure and invariants belong in `ARCHITECTURE.md`.
- Threat model and operational security belong in `SECURITY.md`.
- Developer workflow belongs in `CONTRIBUTING.md`.
- Benchmark mechanics belong beside the benchmark and in `benches/README.md`.
- Releases, chronology, decisions, investigations, and measurements belong in Git/GitHub, not
  current-state documentation.

Comments explain non-obvious local contracts or constraints. Do not narrate the change that produced
the code, cite tracker items as permanent rationale, or restate another documentation surface.

## Workflow

Install the repository hooks from `CONTRIBUTING.md`. They validate staged and pushed trees;
GitHub Actions owns the complete verification matrix. Use targeted commands for development and
diagnosis, not as a second copy of the gate sequence.

`--workspace`, not `--all`, is the workspace-wide Cargo scope.

## Code

- Every crate root forbids unsafe code.
- Model domain and wire concepts with dedicated types; validation belongs to the type that owns the
  invariant.
- Keep `rsos`, `rbsr`, and `lww-register` infrastructure-free as defined by
  `ARCHITECTURE.md`.
- `gossip` owns transport/authentication/discovery; `reconcile` composes adapters and domain.
- Repository-only test seams use `cfg(reconcile_internal_testing)`, not a Cargo feature.
- New behavior requires tests. Prefer properties over implementation literals and keep randomized
  tests deterministic.

## Changes

Keep PRs focused and follow the repository templates. A human-visible rule that can be checked
mechanically should be encoded in a gate rather than duplicated as prose.

For public API changes, regenerate `public-api/*.txt` with
`./scripts/check-public-api.sh --bless`.

Publishing is performed by `.github/workflows/tags.yml` from `v*` tags. Do not publish workspace
crates manually.
