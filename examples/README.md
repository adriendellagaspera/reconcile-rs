# Examples

- `demo.rs`: a minimal local UDP experiment.
- `k8s/`: a historical Kubernetes deployment experiment.

Run the local demo with:

```sh
cargo run --release --example demo -- 8080 127.0.0.1 127.0.0.0/30 100
```

The demo disables cluster authentication. Use it only on localhost or an isolated test network. The Kubernetes example is not a production deployment recipe.
