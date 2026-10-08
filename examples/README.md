# Examples

- demo.rs: minimal local UDP example.
- swarm.rs: local visual swarm with real replicas, fog of war and lossy partitions.
- k8s/: Kubernetes deployment example.

Run the UDP demo with:

~~~sh
cargo run --release --example demo 8080 127.0.0.1 127.0.0.0/30 100000
~~~

See k8s/README.md for the Kubernetes example.

## Visual swarm

~~~sh
cargo run --release --example swarm
~~~

Open `http://127.0.0.1:8088` and click **Run Helsing demo**. The default is 12 nodes and 35%
packet loss. Options: `--nodes 2..20`, `--loss 0..100`, `--port PORT`, `--speed 1..20`, `--mtu BYTES`.
For example, `cargo run --release --example swarm -- --nodes 2 --loss 50` is a minimal cluster.
Speed accelerates application time only; the emulated 160 ms RTT and protocol timers stay real.

No Internet or external assets are used at runtime. Build before the interview, then run
`./target/release/examples/swarm` offline. The server binds HTTP only on loopback; replica
traffic uses `InMemoryNetwork`, authenticated with a fixed demo-only key. Each replica has a
unique stable node id and its own map, HLC, protocol loops and transport endpoint.

The in-memory substrate uses an explicit 16 KiB datagram budget. This is not an MTU-safe UDP
configuration. `--mtu 1200` exercises the library’s default application fragmentation; under high
loss, large fragmented refinement worksets can make repair much slower. The UI displays the
actual configured budget, and the script never declares convergence before entries agree.

### Scenario and controls

The script lasts at least 150 simulated seconds, with up to 90 more seconds for final repair:

1. 0–30 s: connected exploration of two offshore patrol areas.
2. 30–60 s: partition into west/east groups; both explore previously unknown terrain.
3. 60–120 s: a contact appears near the first western vehicle; nearby vehicles observe it locally.
4. From 120 s: heal connectivity and freeze observations while the real protocol repairs state.
5. From 150 s: finish only once all dated entries agree. At 240 s, stop the script if repair is
   still pending; the network remains healed and anti-entropy continues.

Manual controls provide play/pause, reset, local scans, contact reveal, partition and healing.
Manual topology/observation controls leave the scripted sequence. Pause freezes application motion,
observations and scenario time; reconciliation continues. Reset recreates all replicas, transport
pumps and demo traffic counters. Ground truth and cluster views allow clicking vehicles; local
knowledge shows only the selected vehicle and data actually present in its snapshot.

### Implementation and measurements

- `swarm/world.rs`: deterministic terrain, cosmetic patrols and synthetic contact truth.
- `swarm/cluster.rs`: independent replicas, sensor writes and backend-controlled scenario.
- `swarm/transport.rs`: delivery-time partition gate inside the existing seeded `NetemTransport`.
- `swarm/http.rs`: bounded local HTTP server serving embedded `swarm/index.html` and `swarm/app.js`.
- `swarm/tests.rs`: isolation, lossy convergence, LWW resolution, reset and the scripted swarm.
- `swarm/telemetry.rs`: optional collection of library metric counters without an external exporter.

The built-in anti-entropy sweep is idle-driven. An application timer also invokes the public
`start_reconciliation` API every two seconds, staggered per node, so sustained multi-peer traffic
cannot starve fresh comparisons. Merge semantics, wire encoding and repair remain library-owned.
No snapshot is ever copied into another replica by the simulation or UI.

Divergence counts union keys whose dated entries differ on any replica, including missing entries
and LWW timestamps. Agreement is identical keys divided by union keys. Group agreement uses the
same definition within each west/east cohort. The actual topology is two stars with one bridge;
partitioning disables the bridge. Flashes correspond to delivered traffic on those routes.

Counters distinguish netem loss, partition drops and delivered replica wire traffic. Rate is a
UI sample of the byte counter. HTTP snapshot traffic is excluded. These are not repair-only bytes,
RBSR range counts or bandwidth caps. Round counts come from each replica’s public `sync_state`.
With `--features metrics`, `/state` also includes library process counters (not reset per scenario).
Contact age uses simulation time, distinct from HLC ordering.
Scenario observations and loss streams are repeatable; wall-clock HLC stamps, gossip target order
and runtime scheduling prevent bit-for-bit replay of packet counts or convergence time.

BSA discussion points: full-replication capacity, LWW versus sensor fusion, HLC versus observation
time, indefinite partitions, reconciliation versus merge semantics, loss/overhead trade-offs,
tombstones and causal-stability GC, returning stale nodes, authentication and replay protection.
