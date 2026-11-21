# Benchmarks

These benchmarks document experiments on the runtime and its transport/recovery mechanisms. Algorithm-only reconciliation benchmarks live in [set-reconciliation](https://github.com/adriendellagaspera/set-reconciliation).

Run an individual target with:

```sh
cargo bench --bench <target>
```

Criterion output is written under `target/criterion/`. CI compile-checks the benchmark targets; it does not run measurements.

## Interpretation

- Compare revisions on the same hardware, with the same toolchain, build profile, seeds, input sizes, and trial counts.
- Loopback measurements are not WAN measurements.
- A benchmark that exercises the in-memory network is not a measurement of kernel UDP behavior.
- `fragment_recovery`, `ordered_stream`, and `transport_validation` are transport models. They do not measure real TCP or QUIC implementations.
- `membership_scaling` distinguishes measured points from printed model extrapolations.
- Report the workload, environment, and raw output with any result. Do not quote a single number without its conditions.

The suite is exploratory and heterogeneous. Passing compilation does not imply that a benchmark is methodologically sound for every comparison.
