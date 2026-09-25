# Examples

- demo.rs: minimal local example.
- k8s/: Kubernetes deployment example.

Run the local demo with:

~~~sh
cargo run --release --example demo 8080 127.0.0.1 127.0.0.0/30 100000
~~~

See k8s/README.md for the Kubernetes example.
