# Examples

- demo.rs: minimal local UDP example.
- swarm.rs: interactive three-replica playground and architecture page.
- k8s/: Kubernetes deployment example.

Run the UDP demo with:

~~~sh
cargo run --release --example demo 8080 127.0.0.1 127.0.0.0/30 100000
~~~

See k8s/README.md for Kubernetes deployment.

## Replica playground

~~~sh
cargo run --release --example swarm
~~~

Open `http://127.0.0.1:8088`; `--port 0` chooses an available loopback port.
Three independent authoritative `ReplicatedMap<u32, String>` instances own their HLC,
immutable RSOS trees and protocol tasks. The directory/build target retains the `swarm` name.

The controls accept key intervals `[from, to)` within 0–1024 and values up to 256 UTF-8 bytes.
Choose any replica and write or delete a range. A one-key interval writes a single key.
Load 128 keys inserts a common value on the selected replica. Live writes update one selected
key per second, cycling through the chosen range with a changing value suffix.

Each replica can disconnect without stopping its local runtime or writes. Each link can also
be cut independently; available paths through other replicas can still reconcile data.
Connect all restores peers and links; Reset rebuilds all stores and stops live writes.
Outside editable fields, A/B/C select a replica, W writes, D deletes and Space toggles its
connection. Native button keyboard activation is preserved.
Same-key conflicts use the greatest HLC/node stamp. Range deletion removes locally live values
by broadcasting tombstones; deleting an empty range acknowledges zero changed keys.

Commands and polls are serialized. Each successful action has a monotonically increasing revision
and explicit acknowledgement; errors remain visible across polling. Control elements preserve DOM
identity across refreshes. The frontend has no scripted stages or convergence-dependent action gates.

### Observations

Each tile and table row queries a real fixed 32-key RSOS range. It shows raw dated-entry count and
fingerprint, including tombstones. Blue means stored entries, amber means a differing summary,
and hatching means the range contains only tombstones. Click a tile to select its key range.
Replica tables show the first six entries in the selected interval, including tombstones, with
writer identity and full HLC stamp on hover. Counts distinguish live values from tombstones.

Each replica is captured once per poll. These are successive local snapshots, not a globally
atomic cluster cut. Agreement compares complete dated entries; shortened fingerprints are visual
aids. The fixed inspected ranges are not a trace of RBSR's actual chosen splits.
Round counts use `sync_state`. Link counters measure transport ingress, including protocol
overhead; they are not acknowledgements. The recent-action list contains successful user commands.

RSOS is embedded ordered storage, not a central server. reconcile-rs composes its persistent tree
and aggregates with HLC/LWW semantics, RBSR reconciliation, gossip messaging and runtime lifecycle.
The playground uses authenticated in-memory datagrams. It does not demonstrate disk persistence,
consensus, exclusive ownership, physical networking or benchmark scaling results.

`architecture.html` contains conceptual diagrams for the cluster, runtime/write path, persistent
RSOS roots and aggregates, RBSR range comparison, network admission, tombstone GC and optional
persistence. Library capabilities beyond the playground are identified explicitly.

[Presentation and experiment prompts](swarm/PRESENTATION.md).

## Static browser build

Install `wasm32-unknown-unknown` and `wasm-bindgen-cli` matching `Cargo.lock`, then run:

~~~sh
rustup target add wasm32-unknown-unknown
./scripts/build-swarm-web.sh
~~~

Static module/worker/WASM references include a commit version to prevent mixed cached deployments.
Serve `target/swarm-pages` over HTTP(S). Relative module paths support a Pages subdirectory.
Each visitor gets a dedicated worker with independent real Rust replicas; visitors do not exchange
state. Reloading loses data. Browser scheduling can delay timers. UDP binding and DNS return
`Unsupported` in this browser adapter. Native Tokio serves the same UI over loopback HTTP,
with one shared cluster per server. The architecture page is available in both hosting modes.

The `Runtime demo` workflow validates the WASM build and browser behavior, then publishes Pages
from `main` or `demo/swarm-vertical-slice`; `demo/swarm-wasm` validates without publishing.
Select **Pages → Source → GitHub Actions** and permit the publishing branch in the Pages environment.

Frontend/worker tests use `node --test examples/swarm/*.test.mjs`. After building, run
`PLAYWRIGHT_MODULE=/path/to/playwright node examples/swarm/browser/check.cjs` to verify free writes,
offline replicas, conflicting ranges, tombstones/reinsertion, stable controls, persistent feedback,
independent visitors, reset and the playground/architecture layouts.
