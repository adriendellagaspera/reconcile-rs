# Examples

- demo.rs: minimal local example.
- swarm.rs: two real replicas with local fog of war, partition/heal and seeded packet loss.
- k8s/: Kubernetes deployment example.

Run the local demo with:

~~~sh
cargo run --release --example demo 8080 127.0.0.1 127.0.0.0/30 100000
~~~

See k8s/README.md for the Kubernetes example.

Run the visual vertical slice with:

~~~sh
cargo run --release --example swarm -- --loss 35 --port 8088
~~~

Open `http://127.0.0.1:8088`. No Internet or external assets are used at runtime. The first
Cargo build requires downloading dependencies; build before the interview and run
`./target/release/examples/swarm` offline. The example binds HTTP only on loopback; replica
datagrams use `InMemoryNetwork`, authenticated with a fixed demo-only key.

`swarm/cluster.rs` owns independent `ReplicatedMap`s, deterministic application observations,
and a delivery-time partition gate inside the existing seeded `NetemTransport`.
`swarm/http.rs` serves the embedded `swarm/index.html` and snapshots. `swarm/tests.rs` verifies
isolation, lossy repair, concurrent LWW writes and reset via the same runtime used by the UI.

Local map cells, contacts and sector knowledge come from the selected replica. Vehicle selection
markers are application metadata. Ground truth is synthetic. Divergence counts keys with unequal
dated entries (including missing entries); agreement is identical keys divided by the union of
keys. Counters distinguish netem loss from partition drops and delivered wire traffic; they do
not claim to measure repair-only bytes or RBSR ranges. Loss is chosen at launch; reset recreates
both replicas, transport pumps and counters. Pause affects only the automatic control sequence.

The script starts partitioned, expands both local observations, selects the uninformed node,
heals the network and waits for actual convergence. It does not copy data between replicas.
Scenario observations and loss streams are seeded/repeatable; HLC wall time and runtime scheduling
mean exact timestamps, packet counts and convergence time are not a bit-for-bit replay.

This is the initial two-node proof, before extending to a moving swarm and a 2–4 minute scenario.
Discussion points for Bring Something Awesome: full-replication capacity, LWW versus sensor fusion,
HLC versus observation time, indefinite partitions, reconciliation versus merge semantics,
wire overhead under loss, tombstones and causal-stability GC, returning stale nodes, authentication
and replay protection. These are not additional simulation features.
