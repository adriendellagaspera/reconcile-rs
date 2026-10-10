use std::collections::BTreeSet;
use std::io;
use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;

use gossip::netem::{Impairments, Link, Netem, NetemTransport, Probability, Rtt, Seed};
use reconcile::clock::NodeId;
use reconcile::{
    replicated_map::{Config, FramingConfig},
    ClusterKey, InMemoryNetwork, ReplicatedMap,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::bandwidth::{BandwidthTransport, Counters};
use super::transport::{address, Network, PartitionGate};
use super::world::{
    terrain, terrain_detail, Observation, OrderAction, PeerState, Point, World, COMMAND_POSITION,
    HEIGHT, WIDTH,
};

pub struct Cluster {
    pub nodes: Vec<ReplicatedMap<String, Observation>>,
    network: Arc<Network>,
    losses: Vec<Impairments>,
    cancel: CancellationToken,
    runtimes: Vec<Runtime>,
    pub loss: f64,
    pub datagram_budget: usize,
    pub bandwidth_kbps: usize,
    bandwidth: Vec<Arc<Counters>>,
    pub world: World,
    next_order: u64,
    handled_orders: Vec<u64>,
}

struct Runtime {
    cancel: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
}

impl Runtime {
    fn stop(&self) {
        self.cancel.cancel();
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl Cluster {
    #[cfg(test)]
    pub fn new(loss: f64, size: usize) -> io::Result<Self> {
        Self::with_limits(loss, size, 16 * 1024, 0)
    }

    pub fn with_limits(
        loss: f64,
        size: usize,
        datagram_budget: usize,
        bandwidth_kbps: usize,
    ) -> io::Result<Self> {
        if bandwidth_kbps > 1000 {
            return Err(io::Error::other(
                "bandwidth must be between 0 and 1000 kbit/s",
            ));
        }
        if !(2..=20).contains(&size) {
            return Err(io::Error::other("node count must be between 2 and 20"));
        }
        let fabric = InMemoryNetwork::new();
        let world = World::new(size);
        let mut positions = world.positions.clone();
        positions.push(COMMAND_POSITION);
        let network = Arc::new(Network::new(positions));
        let cancel = CancellationToken::new();
        let mut nodes = Vec::new();
        let mut losses = Vec::new();
        let mut runtimes = Vec::new();
        let mut bandwidth = Vec::new();
        for id in 0..=size {
            let addr = address(id);
            let gate = PartitionGate {
                inner: fabric.bind(addr),
                source: id,
                network: network.clone(),
            };
            let transport = NetemTransport::new(
                Arc::new(gate),
                Netem::uniform(
                    Link::at(Rtt::from_millis(160.0)).with_loss(Probability::percent(loss)),
                    Seed::new(42 + id as u64),
                ),
            );
            losses.push(transport.impairments());
            let config = Config::default()
                .with_port(addr.port())
                .with_listen_addr(addr.ip())
                .with_net("127.0.0.0/27".parse().unwrap())
                .map_err(io::Error::other)?
                .with_cluster_key(ClusterKey::new([0x42; 32]))
                .with_framing(
                    FramingConfig::default().with_datagram_payload_budget(datagram_budget),
                )
                .with_node_id(NodeId::new(id as u64 + 1))
                .with_repair_interval(Duration::from_millis(350))
                .with_reconcile_interval(Duration::from_secs(2));
            let counters = Arc::new(Counters::default());
            let transport = if id == size {
                BandwidthTransport::command_center(
                    Arc::new(transport),
                    bandwidth_kbps,
                    datagram_budget,
                    counters.clone(),
                    (0..size).map(address).collect(),
                )
            } else {
                BandwidthTransport::glider(
                    Arc::new(transport),
                    bandwidth_kbps,
                    datagram_budget,
                    counters.clone(),
                    Some(address(size)),
                )
            };
            bandwidth.push(counters);
            let node = ReplicatedMap::new_with_transport(config, Arc::new(transport))
                .map_err(io::Error::other)?;
            runtimes.push(Self::spawn_runtime(&node, id, cancel.child_token()));
            nodes.push(node);
        }
        let mut cluster = Self {
            nodes,
            network,
            losses,
            cancel,
            runtimes,
            loss,
            datagram_budget,
            bandwidth_kbps,
            bandwidth,
            world,
            next_order: 0,
            handled_orders: vec![0; size],
        };
        cluster.refresh_network();
        cluster.observe();
        Ok(cluster)
    }

    fn spawn_runtime(
        node: &ReplicatedMap<String, Observation>,
        id: usize,
        cancel: CancellationToken,
    ) -> Runtime {
        let running = node.clone();
        let token = cancel.clone();
        let run = tokio::spawn(async move {
            running.run(token).await;
        });
        let rounds = node.clone();
        let token = cancel.clone();
        let sweep = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(id as u64 * 80)).await;
            let mut timer = tokio::time::interval(Duration::from_secs(2));
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = timer.tick() => rounds.start_reconciliation().await,
                }
            }
        });
        Runtime {
            cancel,
            tasks: vec![run, sweep],
        }
    }

    pub fn center(&self) -> usize {
        self.world.positions.len()
    }

    fn refresh_network(&self) {
        {
            let mut topology = self.network.topology.write();
            topology.positions[..self.center()].copy_from_slice(&self.world.positions);
            topology.storm_position = Point {
                x: 16.0 + (self.world.seconds() / 35.0).sin() * 5.0,
                y: 10.0 + (self.world.seconds() / 50.0).cos() * 3.0,
            };
        }
        // Newly reachable pairs may have aged out of gossip discovery during isolation.
        for (id, node) in self.nodes.iter().enumerate() {
            for peer in 0..self.nodes.len() {
                if self.network.allowed(id, peer) {
                    node.seed_peer(address(peer).ip());
                }
            }
        }
    }

    pub fn partition(&self, enabled: bool) {
        self.network.topology.write().storm = enabled;
        self.refresh_network();
    }

    pub fn toggle_jammer(&self) {
        let mut topology = self.network.topology.write();
        topology.jammer = !topology.jammer;
        drop(topology);
        self.refresh_network();
    }

    pub fn set_range(&self, range: f64) -> io::Result<()> {
        if !range.is_finite() || !(1.0..=40.0).contains(&range) {
            return Err(io::Error::other("range must be between 1 and 40 map units"));
        }
        self.network.topology.write().range = range;
        self.refresh_network();
        Ok(())
    }

    pub fn toggle_link(&self, a: usize, b: usize) -> io::Result<()> {
        if a >= self.nodes.len() || b >= self.nodes.len() || a == b {
            return Err(io::Error::other("invalid peer pair"));
        }
        let mut topology = self.network.topology.write();
        let blocked = !topology.blocked[a * self.nodes.len() + b];
        topology.blocked[a * self.nodes.len() + b] = blocked;
        topology.blocked[b * self.nodes.len() + a] = blocked;
        drop(topology);
        self.refresh_network();
        Ok(())
    }

    pub fn set_peer(&mut self, id: usize, state: PeerState) -> io::Result<()> {
        if id >= self.nodes.len() {
            return Err(io::Error::other("invalid peer"));
        }
        let previous = self.network.topology.read().peers[id];
        self.network.topology.write().peers[id] = state;
        if state == PeerState::Stopped {
            self.runtimes[id].stop();
        } else if previous == PeerState::Stopped {
            self.runtimes[id] = Self::spawn_runtime(&self.nodes[id], id, self.cancel.child_token());
        }
        self.refresh_network();
        Ok(())
    }

    pub fn heal(&mut self) {
        for id in 0..self.nodes.len() {
            self.set_peer(id, PeerState::Active).unwrap();
        }
        {
            let mut topology = self.network.topology.write();
            topology.storm = false;
            topology.jammer = false;
            topology.blocked.fill(false);
            topology.range = 40.0;
        }
        self.refresh_network();
    }

    pub fn start_demo(&mut self) {
        self.world.scripted = true;
        self.world.playing = true;
        self.partition(false);
    }

    pub fn issue_order(&mut self, recipient: usize, action: OrderAction) -> io::Result<()> {
        if recipient >= self.center() {
            return Err(io::Error::other("orders require a drone recipient"));
        }
        if self.network.topology.read().peers[self.center()] == PeerState::Stopped {
            return Err(io::Error::other("command center is halted"));
        }
        self.next_order += 1;
        self.nodes[self.center()].insert(
            format!("order/{recipient:02}"),
            Observation::Order {
                recipient,
                sequence: self.next_order,
                issued: self.world.ticks,
                expires: self.world.ticks + 240,
                action,
            },
        );
        Ok(())
    }

    fn process_orders(&mut self) {
        for id in 0..self.center() {
            if self.network.topology.read().peers[id] == PeerState::Stopped {
                continue;
            }
            let snapshot = self.nodes[id].snapshot();
            let Some(Observation::Order {
                sequence,
                expires,
                action,
                ..
            }) = snapshot
                .get(&format!("order/{id:02}"))
                .and_then(|entry| entry.value())
                .cloned()
            else {
                continue;
            };
            if sequence <= self.handled_orders[id] {
                continue;
            }
            self.handled_orders[id] = sequence;
            let applied = self.world.ticks <= expires;
            if applied {
                match action {
                    OrderAction::Hold => self.world.held[id] = true,
                    OrderAction::Patrol => self.world.held[id] = false,
                    OrderAction::Scan => self.observe_peer(id, true),
                }
            }
            self.nodes[id].insert(
                format!("order-ack/{id:02}"),
                Observation::Acknowledgement {
                    recipient: id,
                    sequence,
                    received: self.world.ticks,
                    applied,
                },
            );
        }
    }

    pub fn advance(&mut self) {
        self.process_orders();
        if !self.world.playing {
            return;
        }
        self.world.ticks += 1;
        if self.world.scripted {
            match self.world.ticks {
                60 => {
                    self.partition(true);
                    self.set_peer(self.center(), PeerState::Offline).unwrap();
                }
                120 => {
                    self.set_peer(0, PeerState::Offline).unwrap();
                    self.world.reveal_contact();
                }
                180 => {
                    self.set_peer(0, PeerState::Active).unwrap();
                    self.set_peer(self.center() / 2, PeerState::Stopped)
                        .unwrap();
                }
                240 => self.heal(),
                _ => (),
            }
        }
        // Stop new writes while repairing; exact convergence needs a quiescent interval.
        if !self.world.scripted || self.world.ticks < 240 {
            let peers = self.network.topology.read().peers.clone();
            self.world.move_active(&peers);
            self.refresh_network();
            self.observe();
        }
        if self.world.scripted
            && self.world.ticks >= 300
            && (self.world.ticks >= 480 || self.divergent_keys() == 0)
        {
            self.world.playing = false;
        }
    }

    pub fn observe(&mut self) {
        for id in 0..self.center() {
            self.observe_peer(id, false);
        }
    }

    fn observe_peer(&mut self, id: usize, force: bool) {
        if self.network.topology.read().peers[id] == PeerState::Stopped {
            return;
        }
        let position = &self.world.positions[id];
        let cx = position.x as usize;
        let cy = position.y as usize;
        let radius = 3;
        let mut updates = Vec::new();
        for y in cy.saturating_sub(radius)..=(cy + radius).min(HEIGHT - 1) {
            for x in cx.saturating_sub(radius)..=(cx + radius).min(WIDTH - 1) {
                if (x as f64 + 0.5 - position.x).hypot(y as f64 + 0.5 - position.y) > radius as f64
                {
                    continue;
                }
                let key = format!("map/{x:02}/{y:02}");
                if !self.nodes[id].contains_key(&key) {
                    updates.push((
                        key,
                        Observation::Terrain {
                            x,
                            y,
                            land: terrain(x, y),
                            detail: terrain_detail(x, y),
                        },
                    ));
                }
            }
        }
        let sector = (cy / 5) * (WIDTH / 4) + cx / 4;
        let key = format!("sector/{sector:02}/{id:02}");
        if !self.nodes[id].contains_key(&key) {
            updates.push((
                key,
                Observation::Sector {
                    id: sector,
                    scanned: self.world.ticks,
                },
            ));
        }
        if force || self.world.ticks % 4 == 0 {
            updates.push((
                format!("vehicle/{id:02}"),
                Observation::Vehicle {
                    position: *position,
                    seen: self.world.ticks,
                    source: id,
                    battery: 100 - (self.world.ticks / 30).min(70) as u8,
                },
            ));
        }
        for contact in self
            .world
            .contacts()
            .into_iter()
            .filter(|c| c.position.distance(*position) <= 4.0)
        {
            if force || self.world.ticks % 2 == 0 {
                updates.push((
                    format!("contact/{:02}/{id:02}", contact.id),
                    Observation::Contact {
                        id: contact.id,
                        kind: contact.kind,
                        position: contact.position,
                        seen: self.world.ticks,
                        source: id,
                    },
                ));
            }
        }
        self.nodes[id].insert_bulk(&updates);
    }

    pub fn divergent_keys(&self) -> usize {
        let snapshots: Vec<_> = self.nodes.iter().map(|n| n.snapshot()).collect();
        let keys: BTreeSet<_> = snapshots
            .iter()
            .flat_map(|s| s.iter().map(|(k, _)| k.clone()))
            .collect();
        keys.iter()
            .filter(|k| {
                snapshots[1..]
                    .iter()
                    .any(|s| s.get(*k) != snapshots[0].get(*k))
            })
            .count()
    }

    pub fn state(&self) -> serde_json::Value {
        // Capture each replica once: the UI and dated-entry comparisons share this cut.
        let snapshots: Vec<_> = self.nodes.iter().map(|n| n.snapshot()).collect();
        let keys: BTreeSet<_> = snapshots
            .iter()
            .flat_map(|s| s.iter().map(|(k, _)| k.clone()))
            .collect();
        let divergent = keys
            .iter()
            .filter(|k| {
                snapshots[1..]
                    .iter()
                    .any(|s| s.get(*k) != snapshots[0].get(*k))
            })
            .count();
        let entries: Vec<Vec<_>> = snapshots
            .iter()
            .map(|s| {
                s.iter()
                    .filter_map(|(k, e)| e.value().map(|v| (k.clone(), v.clone())))
                    .collect()
            })
            .collect();
        let components = self.network.components();
        let group_agreement: Vec<_> = components
            .iter()
            .map(|members| {
                let union: BTreeSet<_> = members
                    .iter()
                    .flat_map(|&n| snapshots[n].iter().map(|(k, _)| k.clone()))
                    .collect();
                let same = union
                    .iter()
                    .filter(|k| {
                        members[1..]
                            .iter()
                            .all(|&n| snapshots[n].get(*k) == snapshots[members[0]].get(*k))
                    })
                    .count();
                serde_json::json!({ "members": members, "keys": union.len(), "same": same })
            })
            .collect();
        let topology = self.network.topology.read();
        let positions = topology.positions.clone();
        let peer_states = topology.peers.clone();
        let storm = topology.storm;
        let range = topology.range;
        let disruptions = topology.disruptions();
        drop(topology);
        serde_json::json!({
            "partitioned": components.len() > 1, "loss": self.loss,
            "center": self.center(), "peer_states": peer_states, "storm": storm, "range": range,
            "datagram_budget": self.datagram_budget,
            "bandwidth_kbps": self.bandwidth_kbps, "queue_capacity": super::bandwidth::QUEUE_BYTES.max(2 * self.datagram_budget),
            "bandwidth": self.bandwidth.iter().map(|stats| stats.snapshot()).collect::<Vec<_>>(),
            "nodes": entries, "union_keys": keys.len(), "divergent_keys": divergent,
            "loss_offered": self.losses.iter().map(Impairments::offered).sum::<u64>(),
            "loss_dropped": self.losses.iter().map(Impairments::dropped).sum::<u64>(),
            "partition_dropped": self.network.blocked_drops.load(Ordering::Relaxed),
            "delivered_bytes": self.network.bytes.iter().map(|b| b.load(Ordering::Relaxed)).sum::<u64>(),
            "delivered_datagrams": self.network.datagrams.load(Ordering::Relaxed),
            "links": self.network.links(), "groups": group_agreement,
            "direct_peers": (0..self.nodes.len()).map(|id| self.network.direct_peers(id)).collect::<Vec<_>>(),
            "command_position": COMMAND_POSITION, "disruptions": disruptions,
            "truth_contacts": self.world.contacts(),
            "truth_detail": (0..HEIGHT).flat_map(|y| (0..WIDTH).map(move |x| terrain_detail(x,y))).collect::<Vec<_>>(),
            "positions": positions, "truth_contact": self.world.contact(),
            "truth_map": (0..HEIGHT).flat_map(|y| (0..WIDTH).map(move |x| terrain(x, y))).collect::<Vec<_>>(),
            "ticks": self.world.ticks, "seconds": self.world.seconds(), "playing": self.world.playing,
            "scripted": self.world.scripted, "phase": self.world.phase(divergent == 0),
            "library_metrics": super::telemetry::snapshot(),
            "rounds_total": self.nodes.iter().map(|n| n.sync_state().rounds).sum::<u64>(),
        })
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        self.cancel.cancel();
        for runtime in &self.runtimes {
            runtime.stop();
        }
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
