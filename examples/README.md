# Examples

- demo.rs: minimal local UDP example.
- swarm.rs: three-replica runtime lab with a browser UI.
- k8s/: Kubernetes deployment example.

Run the UDP demo with:

~~~sh
cargo run --release --example demo 8080 127.0.0.1 127.0.0.0/30 100000
~~~

See k8s/README.md for Kubernetes deployment.

## Runtime lab

~~~sh
cargo run --release --example swarm
~~~

Open `http://127.0.0.1:8088`; `--port 0` chooses an available loopback port.
The lab runs three independent authoritative `ReplicatedMap<String, String>` instances.
Each owns its HLC, immutable RSOS tree and protocol tasks. There is no master or application
simulation. The directory and build target retain the `swarm` name for hosting compatibility.

The presenter advances three actions:

1. Insert three keys on A. Wait for actual dated-entry agreement across all replicas.
2. Cut both links to C, write `alpha` on A and `charlie` on C to `m/status`.
   A and B continue exchanging; C remains locally writable. The writes are causally independent.
3. Restore C's links and seed peer discovery. No application writes occur on reconnect.
   Wait for exact dated-entry agreement; the greatest HLC/node stamp wins the same-key conflict.

A button becomes available from observed snapshot readiness, not a timed animation. Reset destroys
all three stores and starts fresh. Periodic reconciliation continues after convergence.

### What is measured

Values, stamps, full-range and fixed subrange count/fingerprint summaries come from real RSOS
snapshots. Each replica is captured once per poll; the three snapshots are successive observations,
not a globally atomic cut. Convergence compares complete dated entries, not shortened fingerprints.
The inspected ranges are illustrative queries, not a trace of RBSR's selected protocol splits.

Per-link counters record actual in-memory transport ingress bytes/datagrams in both directions.
They include protocol overhead and are not acknowledgements. The activity panel records explicit
actions and sampled state changes; it can miss intermediate states. Round counts come from each
replica's `sync_state`. There are no invented packet animations or protocol-specific message labels.

RSOS is embedded storage, not remote infrastructure: its persistent `FingerprintTreeMap` provides
ordered entries and range aggregates. reconcile-rs composes that backend with HLC/LWW semantics,
RBSR range reconciliation, gossip messaging and runtime lifecycle. The UI does not inspect internal
tree node allocation or demonstrate structural-sharing performance.

This explains eventual consistency and deterministic conflict resolution. It does not establish
consensus, exclusive assignment, durability, physical radio behavior, or bandwidth/scaling results.
LWW discards the losing same-key value. There are three keys and no persistence; reloading loses state.

[Presentation script and discussion prompts](swarm/PRESENTATION.md).

## Static browser build

Install `wasm32-unknown-unknown` and `wasm-bindgen-cli` matching `Cargo.lock`, then run:

~~~sh
rustup target add wasm32-unknown-unknown
./scripts/build-swarm-web.sh
~~~

Serve `target/swarm-pages` over HTTP(S). Relative module paths support a Pages subdirectory.
Every visitor gets a dedicated worker with independent real Rust replicas and authenticated
in-memory datagrams; there is no communication between visitors or API server. Browser event-loop
scheduling can delay timers. The browser runtime reports UDP binding and DNS as `Unsupported`.
The native example uses Tokio and serves the same UI, with one shared cluster per server.

The `Runtime demo` workflow validates the WASM build and browser behavior, then publishes Pages
from `main` or `demo/swarm-vertical-slice`; `demo/swarm-wasm` validates without publishing.
Select **Pages → Source → GitHub Actions** and permit the publishing branch in the Pages environment.

Frontend/worker tests use `node --test examples/swarm/*.test.mjs`. After building, run
`PLAYWRIGHT_MODULE=/path/to/playwright node examples/swarm/browser/check.cjs` to exercise isolated
writes, dated convergence, RSOS range agreement, reset, independent visitors and mobile layout.
