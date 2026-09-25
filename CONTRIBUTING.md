# Contributing

Use the Rust version declared by the workspace manifests. A standard Cargo toolchain is sufficient
for normal development; the repository also provides a Dev Container.

## Setup

Install the repository hooks once:

```sh
ln -sf ../../pre-commit .git/hooks/pre-commit
ln -sf ../../pre-push .git/hooks/pre-push
```

The hooks validate the exact staged/pushed tree. GitHub Actions is the source of truth for the full
verification matrix; do not maintain a second command list in documentation.

Use targeted commands while developing when they help diagnose or exercise the code you are
changing. Commit and push normally to run the repository gates.

## Tests

Tests should assert behavior or invariants rather than implementation snapshots. Keep randomized
tests reproducible and do not bind fixed network ports.

Public-API behavior belongs in `tests/`; narrow internal invariants belong next to the code.
Property tests are preferred when the property is stronger than a finite set of examples.

## Public API changes

The committed `public-api/*.txt` files are generated snapshots. After an intentional public API
change, regenerate them with:

```sh
./scripts/check-public-api.sh --bless
```

Do not edit snapshots by hand.

## Benchmarks

Benchmark targets and their interpretation rules are documented in
[`benches/README.md`](benches/README.md). Benchmark results are not repository documentation;
record measurements in the work item or review that needs them.

## Pull requests

Keep a change focused. Update current-state documentation in the same change when its contract
changes. Release history and implementation chronology belong to Git tags, releases, commits, and
the issue tracker rather than versioned prose.
