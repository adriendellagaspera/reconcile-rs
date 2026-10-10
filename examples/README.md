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

Open `http://127.0.0.1:8088`. The flood-response mission starts on station with six aircraft
and an ordinary response-center replica. Defaults: 5% seeded packet loss, 1200-byte datagrams,
250 kbit/s aggregate aircraft TX/RX, and independent capped CC outbound lanes. Options:
`--nodes 2..20`, `--loss 0..100`, `--port PORT`, `--speed 1..20`, `--mtu BYTES`,
`--bandwidth-kbps 0..1000` (0 disables shaping). Protocol timers and bandwidth stay in real time.
The browser starts the scripted exercise at ×10; the native server starts manual exploration at ×1.

No Internet or external assets are used at runtime. Native HTTP binds on loopback. Each aircraft
owns a real map, HLC, protocol tasks and authenticated in-memory transport endpoint. The shared
terrestrial map is rendered from fixed mission geography outside every replicated store.

### Mission and physical assumptions

A synthetic 32×20 km river valley contains flooded settlements, a damaged bridge and a blocked
road. The region is context, not exhaustively inspected ground. Camera observations require a
100 m footprint radius. RGB sensors produce possible-person cues; odd-numbered sensor indices
simulate verification on a subsequent local measurement. Ground-truth objects, classifications
and actual aircraft intentions appear only in the explicit simulator perspective.

Aircraft cruise at a bounded 20 m/s (72 km/h) with 60 min nominal endurance and a 15 min reserve.
They return when the remaining time cannot cover the reserve plus the straight-line return leg,
or after 45 km of illustrated on-station flight. Initial deployment transit is excluded; aircraft
start on station. Wind, bank/turn dynamics, image classification and rescue dispatch are not
modeled. Holding/verification/monitoring is drawn stationary as a kinematic loiter approximation.
Camera footprints do not imply that whole sectors have been inspected.

### Local decisions and evidence

`poi/{id}/{sensor}` preserves each sensor's latest observation. `poi-first/{id}/{sensor}` keeps
immutable first discoveries. A verified confirmation or dismissal outranks an unverified cue;
among verified reports, observation time wins, then lower sensor index. This is application
projection over separate LWW entries, not sensor fusion or a new library merge policy.
Reception never changes the original observation time. No classifier or real track association
is implemented; POI identities are supplied by the simulator.

Each aircraft chooses actions from its own replica. A possible-person cue can redirect aircraft 2
for verification and aircraft 3 for nearby observation. Aircraft 4 verifies its local false-positive cue. Confirmation lets aircraft 1 monitor the
location; receipt of a dismissal cancels the corresponding verification. Surroundings are drawn
as a small orbit. These roles are deterministic demonstration capabilities, not optimal task
allocation. `intent/{aircraft}` advertises the chosen action and POI; it is not an exclusive lock.
Partitions can cause duplicate work. Plans do not read other replicas or simulator object truth.

The CC produces no observations. Its received findings can be inspected by selecting Center.
Locally unknown findings have no markers or hit targets. Own navigation is available immediately;
other aircraft positions and intentions require received reports. Fixed terrain and the known
fleet roster are mission configuration. Simulator phase, uplink incident and collapsed diagnostics
are explicitly global exercise information. Older reports retain their age across relays.

### Script and interaction

The exercise interrupts the CC modem after 20 simulated seconds and cuts cross-group links
between aircraft 1–3 and the remaining aircraft. Available within-group exchanges continue.
At five simulated minutes, all peers and links are restored. At fifteen minutes, movement and
sensor writes freeze for exact anti-entropy repair. The mission clock stops only when dated entries
agree; no wall-clock convergence deadline is promised. Radio availability is synthetic, not an
RF propagation model. There is no GNSS jamming, flight-control recovery or jamming mitigation.

Select an aircraft or Center to compare local knowledge. Select a finding to read its evidence
and frame it on the map. Ground truth exposes actual objects and positions separately. Links are
hidden by default; local links reflect only direct ingress and last-known endpoint positions.
Drag/pinch/scroll to navigate. On phones the map fills the workspace and an accessible bottom
sheet opens the inspector; Overview fits the entire region and Follow selected recenters it.
Pause stops movement and sensor time while network traffic continues. Restart rebuilds all peers.
Manual modem/heal controls leave scripted timing. UI clock choices are ×1, ×10, ×30 and ×60.

### Transport and semantics

Aircraft share bounded TX/RX budgets across neighbors. CC has independent outbound lanes and
unlimited aggregate reception. Traffic shaping accounts for protocol overhead; packets can drop
from queue overflow, loss, or unavailable links. Connected replicas may still diverge. Increasing
the application clock does not accelerate networking and can outpace reconciliation.

Desired orders and immutable issued/acknowledged journals remain supported by the engine;
only the addressed drone executes a received desired order. History is not a replay queue.
Exclusive task assignment, bulk imagery transfer, durable recovery, retention/GC and real sensor
fusion need separate policies. Latest source reports are bounded per POI; journals need retention
for unbounded missions. The in-memory fleet loses state when reloaded.

Protocol fixtures also retain the coastal/acoustic geometry to exercise bounded multi-hop repair,
source retention, order delivery and observer isolation independently of the flood mission.
The browser verifies the flood runtime and snapshot projection, plus the library convergence probe.

## Swarm on GitHub Pages

GitHub Pages serves the UI and the compiled Rust fleet. Each visitor owns an independent
simulation in a dedicated Web Worker: the same replicas, authenticated datagrams, network limits,
orders and reconciliation as the native example. No API server or external runtime assets are needed.
There is no communication between visitors; reloading starts a new fleet with no persistent state.
Browser scheduling, including background-tab throttling, can delay protocol timers.

Install the `wasm32-unknown-unknown` Rust target and `wasm-bindgen-cli` matching the version in
`Cargo.lock`, then build:

~~~sh
rustup target add wasm32-unknown-unknown
./scripts/build-swarm-web.sh
~~~

Serve `target/swarm-pages` with any static HTTP server. Module workers require HTTP(S), rather
than opening `index.html` directly. The build generates browser configuration and relative module
paths, so the same artifact works under the repository's Pages subdirectory.

In repository settings, select **Pages → Source → GitHub Actions** and allow the publishing branch
in the `github-pages` environment. The `Swarm demo` workflow builds and verifies the WASM fleet
in Chromium, then deploys from `main` or `demo/swarm-vertical-slice`; `demo/swarm-wasm` is validation-only. It publishes this repository's Pages root.

The browser adapter keeps timers cancellable and runs replica tasks on the worker event loop;
Tokio remains the native runtime. Browser callers supply an in-memory or custom transport:
UDP binding and DNS discovery return `Unsupported`. The demo does not use filesystem persistence.

Frontend and worker request properties are checked with `node --test examples/swarm/*.test.mjs`.
After building, run `PLAYWRIGHT_MODULE=/path/to/playwright node examples/swarm/browser/check.cjs`
to verify real convergence, flood discoveries and local reactions, independent visitors,
observer isolation and mobile layout in a real browser.

### Optional shared native server

The native HTTP UI is still available through `cargo run --release --example swarm`.
For a shared HTTPS simulation, use `examples/swarm/deploy/compose.yaml` and its `.env.example`:
point `SWARM_DOMAIN` at the server and configure `SWARM_UI_ORIGIN` for the UI's origin.
Caddy publishes ports 80/443; the Rust listener stays on loopback inside the engine namespace.
A separately served UI can select `simulationMode = 'http'` and set `apiBase` in `config.js`.
Orders and reset then affect all visitors to that server. This is an unauthenticated demonstration,
with no persistent state, rather than an operational command center.
