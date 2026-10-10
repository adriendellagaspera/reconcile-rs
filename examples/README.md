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

Open `http://127.0.0.1:8088`; exploration starts automatically in the CC perspective. The default is 12 drones,
one command-center peer, 35% seeded packet loss, and 10 kbit/s per glider in each direction. Options: `--nodes 2..20` (drones,
excluding the center), `--loss 0..100`, `--port PORT`, `--speed 1..20`, `--mtu BYTES`, `--bandwidth-kbps 0..1000` (0 disables the cap).
Speed accelerates application time only; emulated 160 ms RTT and protocol timers stay real.

Build before the interview, then run `./target/release/examples/swarm` offline. No Internet or
external assets are used at runtime. HTTP binds only on loopback. Authenticated replica traffic
uses `InMemoryNetwork` and a fixed demo-only key. Each peer has its own map, HLC, protocol loops,
transport endpoint and stable node id. The command center is an ordinary authoritative replica
that originates no sensor observations; it can exchange data, but fleet operation never requires it.

The default 1200-byte datagram budget exercises UDP application framing. Each glider has a
shared 10,000 bit/s (1,250 byte/s) TX budget across all neighbors, and a separate aggregate RX
budget of the same size. Two byte-fair TX classes reserve half the glider's capacity for CC
when both classes have queued packets; either class borrows the full rate when the other is idle.
Each class reserves 8 KiB of admission space (or one configured datagram if larger),
including its packet in service, with at most 64 datagrams per class. Fairness is bounded
by a packet's serialization time; priority changes neither reconciliation nor merge semantics.
CC has independent 10 kbit/s TX lanes to each configured glider, with 16 KiB (or one configured datagram if larger) of queue space per
lane, and unlimited aggregate RX. Fleet operation does not depend on CC availability.
TX serializes packets before seeded loss and latency, including protocol bytes, retries and
configured-peer probes. Excess packets drop like UDP. Glider RX polices aggregate ingress with
at most one datagram of burst; concurrent senders can still overload a glider. This is destination
scheduling and finite capacity, not application-aware scheduling or acoustic physics. HTTP and
sensor-local writes consume none of this budget. Speed never accelerates bandwidth or protocol
time; at high application speed fresh reports can outpace the channel.

The bandwidth panel shows per-peer TX/RX rates, queued bytes, the slowest lane's nominal drain time and saturation
drops separately from seeded packet loss. Connected peers can remain divergent, and orders can
expire before delivery. No contact or order receives implicit priority. LWW retains the latest state
per key, so superseded intermediate reports need not ever be seen remotely. Anti-entropy may
continue after scripted time stops; the demo never claims a fixed convergence deadline. Use
`--bandwidth-kbps 0 --mtu 16384` to compare with an unconstrained in-memory transport.

### Interaction

Click a visible glider on the map or a locally known fleet report to enter its onboard view.
Click the fixed station to return to CC. The inspector's order menu immediately issues the chosen
scan/hold/patrol order from CC, addressed to that selected drone. An issued order does not appear
in the drone's local knowledge until it arrives. Contacts, visible source observations, links and surveyed terrain tiles can also be selected.
Their inspector and hover hints use the same local data; unknown objects have no click targets.
Scroll or pinch to zoom, drag to pan, and double-click to reset or enlarge the map.
Escape clears object selection, then returns to CC. Secondary knowledge lists fold into the rail.
The only persistent button pauses motion; replica traffic and command delivery continue.

Use the perspective menu to enter ground truth when exploring drones unknown to the current
observer. Simulation diagnostics and topology/scenario controls are collapsed and available only
there. The scripted outage sequence lives in that panel's scenario menu. Reset resumes manual
exploration. Local perspectives hide all global replica states, global counters and global topology;
unknown fleet members are absent rather than populated from simulator truth.

### Views and knowledge

- **Local knowledge**: the selected peer's snapshot, plus its own navigation position.
  CC uses exactly the same projection. Its fixed station is on the central island.
- **Simulator / ground truth**: actual terrain, positions, typed contacts and failure states.
Ground truth includes actual allowed links, with a **Show connections** toggle. Local views
show only direct ingress seen by that observer; silence after five real seconds makes links dashed.
Endpoints use last known reports; unknown endpoints remain listed. CC position is fixed mission configuration.

The application stores static terrain, per-drone coverage reports, each drone's latest vehicle
report, and the latest observation per contact and sensor. Separate contact keys preserve reports from
different sensors; this is a bounded latest-report model, not an unbounded observation history.
All observers use the same local projection: contact synthesis selects the latest observation for each contact by
application observation time, breaks ties by source id, and exposes other source reports. It is
not a sensor-fusion estimate and does not average asynchronous positions.

Vehicle and contact reports progressively gray and fade according to observation time, not
receipt time or HLC ordering. Vehicle reports are stale after 30 simulated seconds; contacts after
45. Stale positions have dashed outlines. Rings illustrate growing uncertainty, not calibrated
covariance or confidence. Terrain does not age. Visual expiry never deletes replicated data.
A silent vehicle is shown as stale/unknown, not diagnosed as failed. Failure states and global
metrics are explicitly simulator information, separate from local knowledge.

### Orders, contacts and coastline

The CC writes a latest desired order per drone: **Scan now**, **Hold position**, or **Resume patrol**.
A drone processes only orders in its own replica, writes an acknowledgement once per sequence,
and rejects orders older than 120 simulated seconds. Acknowledgements become visible to the CC
only through replication. A modem-offline drone can execute an already received order; a halted
drone waits for resume. Indirect paths can deliver orders even when the direct CC link is cut.
Hold freezes navigation while sensing and protocol traffic continue. New orders supersede older
ones: this is desired-state control, not a durable queue or a safety-critical command protocol.
Pausing also pauses order deadlines, while allowing delivered orders to execute.

Five seeded mobile contacts represent a civil vessel, hostile vessel, whale, sperm whale and
an ocean front; the scripted discovery adds another hostile contact. Reset repeats their initial
layout. Nature is synthetic sensor ground truth, not a classifier or threat assessment. The ocean
front contact is separate from the topology's drifting storm zone. Each sensor retains its
latest report per contact, and each local view synthesizes each identity separately.
Terrain tiles include an 8×8 land mask for a finer irregular coastline and islands. Local views
render only masks present in their replica; ground truth renders every tile.

### Connectivity and failures

Every pair has an independent symmetric availability decision. Distance, drifting storm and human-jamming zones,
a manual pair cut, or an offline/stopped endpoint can block it. No route requires G1, G7 or the
center. Zones obstruct segments that intersect their circular footprint; crossing the map midpoint
has no special meaning. Storm drift follows application time; the jammer is stationary. Zones are
visible and selectable only in ground truth, where their inspector can clear them. Positions update connectivity as drones move; graph components and their agreement are
computed from the actual links, rather than fixed west/east memberships. The model is synthetic,
not an acoustic propagation simulation. Packet loss uses per-directed-link seeded streams;
latency/loss settings are uniform. Link range uses map units, not meters or guaranteed bandwidth.

Simulator controls select any peer (including the center), cut/restore its modem, halt/resume it,
change communication range, toggle storm or human jamming, or cut one pair. Removing a manual cut
still requires reachable, online endpoints and clear weather. **Restore all peers & links** resumes
all peers, reconnects their modems, clears cuts/disruptions and extends range to cover the whole map.

Modem disconnection leaves navigation, sensing and local writes running. Halt suspends navigation,
sensors and the peer's protocol tasks. Resume restarts the protocol using retained in-memory state;
this does not demonstrate disk persistence, process-crash recovery or replacement after storage loss.
Returning peers recover missed reports and contribute their isolated discoveries through normal
anti-entropy. Already delivered knowledge survives loss of its originating peer.

### Script and timing

The script runs for at least 150 simulated seconds, with a repair deadline at 240 seconds:

1. 0–30 s: explore with distance-based peer links.
2. 30–60 s: a drifting storm obstructs intersecting links; the command center loses its modem.
3. 60–90 s: G1's modem disconnects; a contact appears near it; local discoveries continue.
4. 90–120 s: G1 reconnects; another drone halts (G7 with the default fleet size).
5. From 120 s: resume all peers, heal links and freeze new sensor writes for exact repair.
6. From 150 s: finish only when every dated entry agrees. At 240 s stop scripted time even if
   repair remains pending; the restored network and anti-entropy continue running.

Manual topology/observation controls leave the script. Pause freezes application motion,
observations and scenario time; reconciliation and order processing continue. Reset recreates every peer, transport
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
Wire bytes include protocol overhead, exclude HTTP, and are not repair-only bytes. Per-peer TX counts offered wire bytes before netem loss;
RX counts bytes admitted to protocol consumption. Per-sample rates can burst on packet boundaries. Delivered means admitted to the in-memory fabric, not necessarily consumed by an endpoint.
Round counts come from public `sync_state`. With `--features metrics`, `/state` includes process
counters, which reset does not clear. Wall-clock HLC stamps and runtime scheduling prevent
bit-for-bit replay of traffic counts or convergence times despite repeatable geometry and seeds.

Long-offline replica removal, deletion/tombstone GC, permanent storage loss, real sensor fusion
and exclusive mission assignment require additional application policies; the demo does not
claim those semantics from LWW convergence alone.
