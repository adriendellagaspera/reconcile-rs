# Examples

- demo.rs: minimal local UDP example.
- swarm.rs: local visual swarm with independent replicas and intermittent pairwise links.
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

Open `http://127.0.0.1:8088` and click **Run Helsing demo**. The default is 12 drones,
one command-center peer, and 35% seeded packet loss. Options: `--nodes 2..20` (drones,
excluding the center), `--loss 0..100`, `--port PORT`, `--speed 1..20`, `--mtu BYTES`.
Speed accelerates application time only; emulated 160 ms RTT and protocol timers stay real.

Build before the interview, then run `./target/release/examples/swarm` offline. No Internet or
external assets are used at runtime. HTTP binds only on loopback. Authenticated replica traffic
uses `InMemoryNetwork` and a fixed demo-only key. Each peer has its own map, HLC, protocol loops,
transport endpoint and stable node id. The command center is an ordinary authoritative replica
that originates no sensor observations; it can exchange data, but fleet operation never requires it.

The explicit default 16 KiB in-memory datagram budget is displayed in the UI. This is not an
MTU-safe UDP configuration. `--mtu 1200` exercises default application framing; under high loss,
repair of large refinement worksets can exceed the scripted interview window. Completion always
requires actual dated-entry equality; the simulation never copies snapshots between replicas.

### Views and knowledge

- **Onboard knowledge**: only the selected drone's snapshot, plus its own navigation position.
- **Command center / local synthesis**: only the center's snapshot, plus its fixed own location.
- **Simulator / ground truth**: actual terrain, positions, contact and failure states.
- **Simulator / pair connectivity**: actual allowed links; flashes are delivered datagrams.

The application stores static terrain, per-drone coverage reports, each drone's latest vehicle
report, and the latest contact observation per sensor. Separate contact keys preserve reports from
different sensors; this is a bounded latest-report model, not an unbounded observation history.
All observers use the same local projection: contact synthesis selects the latest observation by
application observation time, breaks ties by source id, and exposes other source reports. It is
not a sensor-fusion estimate and does not average asynchronous positions.

Vehicle and contact reports progressively gray and fade according to observation time, not
receipt time or HLC ordering. Vehicle reports are stale after 30 simulated seconds; contacts after
45. Stale positions have dashed outlines. Rings illustrate growing uncertainty, not calibrated
covariance or confidence. Terrain does not age. Visual expiry never deletes replicated data.
A silent vehicle is shown as stale/unknown, not diagnosed as failed. Failure states and global
metrics are explicitly simulator information, separate from local knowledge.

### Connectivity and failures

Every pair has an independent symmetric availability decision. Distance, a synthetic weather front,
a manual pair cut, or an offline/stopped endpoint can block it. No route privileges G1, G7 or the
center. Positions update connectivity as drones move; graph components and their agreement are
computed from the actual links, rather than fixed west/east memberships. The model is synthetic,
not an acoustic propagation simulation. Packet loss uses per-directed-link seeded streams;
latency/loss settings are uniform. Link range uses map units, not meters or guaranteed bandwidth.

Simulator controls select any peer (including the center), cut/restore its modem, halt/resume it,
change communication range, toggle the weather front, or cut one pair. Removing a manual cut
still requires reachable, online endpoints and clear weather. **Restore all peers & links** resumes
all peers, reconnects their modems, clears cuts/weather and extends range to cover the whole map.

Modem disconnection leaves navigation, sensing and local writes running. Halt suspends navigation,
sensors and the peer's protocol tasks. Resume restarts the protocol using retained in-memory state;
this does not demonstrate disk persistence, process-crash recovery or replacement after storage loss.
Returning peers recover missed reports and contribute their isolated discoveries through normal
anti-entropy. Already delivered knowledge survives loss of its originating peer.

### Script and timing

The script runs for at least 150 simulated seconds, with a repair deadline at 240 seconds:

1. 0–30 s: explore with distance-based peer links.
2. 30–60 s: a weather front cuts cross-channel links; the command center loses its modem.
3. 60–90 s: G1's modem disconnects; a contact appears near it; local discoveries continue.
4. 90–120 s: G1 reconnects; another drone halts (G7 with the default fleet size).
5. From 120 s: resume all peers, heal links and freeze new sensor writes for exact repair.
6. From 150 s: finish only when every dated entry agrees. At 240 s stop scripted time even if
   repair remains pending; the restored network and anti-entropy continue running.

Manual topology/observation controls leave the script. Play/pause freezes application motion,
observations and scenario time; reconciliation continues. Reset recreates every peer, transport
pump and traffic counter. Clicking a drone/center in a simulator view opens its local perspective.

### Implementation and measurements

- `swarm/world.rs`: deterministic terrain, patrols and contact truth.
- `swarm/cluster.rs`: replica lifecycle, local sensor writes and scripted events.
- `swarm/transport.rs`: pairwise delivery/ingress gates, topology and traffic accounting.
- `swarm/knowledge.js`: shared snapshot-only knowledge synthesis and freshness projection.
- `swarm/http.rs`: bounded local HTTP server and embedded browser assets.
- `swarm/tests.rs`, `swarm/knowledge.test.mjs`: protocol scenarios and local projection properties.
- `swarm/telemetry.rs`: optional library metric collection without an external exporter.

A staggered application timer invokes public `start_reconciliation` every two real seconds;
sustained traffic must not starve the library's idle-driven comparison sweep. Merge semantics,
wire encoding and repair remain library-owned. Delivery checks topology after netem delay; ingress
rechecks queued datagrams before consumption. Seeding reachable neighbors after topology changes
restores gossip-routing candidates without decommissioning isolated causal-stability members.

Divergence counts union keys with differing dated entries on any replica, including absent keys
and LWW timestamps. Agreement includes offline/halted peers and the center. Component agreement
uses the same definition inside each current connected component; connectivity alone does not
imply convergence. The UI uses one snapshot per peer per sample, not an atomic distributed cut.

Counters distinguish seeded loss drops, unavailable-link drops, and delivered replica wire traffic.
Wire bytes include protocol overhead, exclude HTTP, and are not repair-only bytes or bandwidth
caps. Delivered means admitted to the in-memory fabric, not necessarily consumed by an endpoint.
Round counts come from public `sync_state`. With `--features metrics`, `/state` includes process
counters, which reset does not clear. Wall-clock HLC stamps and runtime scheduling prevent
bit-for-bit replay of traffic counts or convergence times despite repeatable geometry and seeds.

Long-offline replica removal, deletion/tombstone GC, permanent storage loss, real sensor fusion
and exclusive mission assignment require additional application policies; the demo does not
claim those semantics from LWW convergence alone.
