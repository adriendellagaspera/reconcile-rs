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

use super::transport::{address, Network, PartitionGate};
use super::world::{terrain, Observation, World, HEIGHT, WIDTH};

pub struct Cluster {
    pub nodes: Vec<ReplicatedMap<String, Observation>>,
    network: Arc<Network>,
    losses: Vec<Impairments>,
    cancel: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
    pub loss: f64,
    pub datagram_budget: usize,
    pub world: World,
}

impl Cluster {
    #[cfg(test)]
    pub fn new(loss: f64, size: usize) -> io::Result<Self> {
        Self::with_datagram_budget(loss, size, 16 * 1024)
    }

    pub fn with_datagram_budget(
        loss: f64,
        size: usize,
        datagram_budget: usize,
    ) -> io::Result<Self> {
        if !(2..=20).contains(&size) {
            return Err(io::Error::other("node count must be between 2 and 20"));
        }
        let fabric = InMemoryNetwork::new();
        let network = Arc::new(Network::new(size));
        let cancel = CancellationToken::new();
        let mut nodes = Vec::new();
        let mut losses = Vec::new();
        let mut tasks = Vec::new();
        for id in 0..size {
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
            let node = ReplicatedMap::new_with_transport(config, Arc::new(transport))
                .map_err(io::Error::other)?;
            for peer in 0..size {
                if network.route(id, peer) {
                    node.seed_peer(address(peer).ip());
                }
            }
            let running_node = node.clone();
            let token = cancel.clone();
            tasks.push(tokio::spawn(async move {
                running_node.run(token).await;
            }));
            // The built-in sweep is idle-driven. Sustained multi-peer traffic must not starve
            // anti-entropy, so the application also schedules the public round API.
            let round_node = node.clone();
            let token = cancel.clone();
            tasks.push(tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(id as u64 * 80)).await;
                let mut rounds = tokio::time::interval(Duration::from_secs(2));
                rounds.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = token.cancelled() => break,
                        _ = rounds.tick() => round_node.start_reconciliation().await,
                    }
                }
            }));
            nodes.push(node);
        }
        let mut cluster = Self {
            nodes,
            network,
            losses,
            cancel,
            tasks,
            loss,
            datagram_budget,
            world: World::new(size),
        };
        cluster.observe();
        Ok(cluster)
    }

    pub fn partition(&self, enabled: bool) {
        self.network.partitioned.store(enabled, Ordering::Relaxed);
        // Re-seed on healing if routing peers were aged out during isolation.
        if !enabled {
            for (id, node) in self.nodes.iter().enumerate() {
                for peer in 0..self.nodes.len() {
                    if self.network.route(id, peer) {
                        node.seed_peer(address(peer).ip());
                    }
                }
            }
        }
    }

    pub fn start_demo(&mut self) {
        self.world.scripted = true;
        self.world.playing = true;
        self.partition(false);
    }

    pub fn advance(&mut self) {
        if !self.world.playing {
            return;
        }
        self.world.ticks += 1;
        if self.world.scripted {
            match self.world.ticks {
                60 => self.partition(true),
                120 => self.world.reveal_contact(),
                240 => self.partition(false),
                _ => (),
            }
        }
        // Stop new writes while repairing; exact convergence needs a quiescent interval.
        if !self.world.scripted || self.world.ticks < 240 {
            self.world.move_vehicles();
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
        for (id, position) in self.world.positions.iter().enumerate() {
            let cx = position.x as usize;
            let cy = position.y as usize;
            let radius = 3;
            let mut updates = Vec::new();
            for y in cy.saturating_sub(radius)..=(cy + radius).min(HEIGHT - 1) {
                for x in cx.saturating_sub(radius)..=(cx + radius).min(WIDTH - 1) {
                    if (x as f64 + 0.5 - position.x).hypot(y as f64 + 0.5 - position.y)
                        > radius as f64
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
                            },
                        ));
                    }
                }
            }
            let sector = (cy / 5) * (WIDTH / 4) + cx / 4;
            let key = format!("sector/{sector:02}");
            if !self.nodes[id].contains_key(&key) {
                updates.push((
                    key,
                    Observation::Sector {
                        id: sector,
                        scanned: self.world.ticks,
                    },
                ));
            }
            if let Some(contact) = self
                .world
                .contact()
                .filter(|p| p.distance(*position) <= 4.0)
            {
                // One update per simulated second; observations are not fusion estimates.
                if self.world.ticks % 2 == 0 {
                    updates.push((
                        "contact/01".into(),
                        Observation::Contact {
                            position: contact,
                            seen: self.world.ticks,
                            source: id,
                        },
                    ));
                }
            }
            self.nodes[id].insert_bulk(&updates);
        }
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
        let group_agreement: Vec<_> = (0..2)
            .map(|group| {
                let members: Vec<_> = (0..self.nodes.len())
                    .filter(|&n| self.network.group(n) == group)
                    .collect();
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
                serde_json::json!({"group": group, "keys": union.len(), "same": same})
            })
            .collect();
        serde_json::json!({
            "partitioned": self.network.partitioned.load(Ordering::Relaxed), "loss": self.loss,
            "datagram_budget": self.datagram_budget,
            "nodes": entries, "union_keys": keys.len(), "divergent_keys": divergent,
            "loss_offered": self.losses.iter().map(Impairments::offered).sum::<u64>(),
            "loss_dropped": self.losses.iter().map(Impairments::dropped).sum::<u64>(),
            "partition_dropped": self.network.partition_drops.load(Ordering::Relaxed),
            "delivered_bytes": self.network.bytes.iter().map(|b| b.load(Ordering::Relaxed)).sum::<u64>(),
            "delivered_datagrams": self.network.datagrams.load(Ordering::Relaxed),
            "links": self.network.links(), "groups": group_agreement,
            "positions": self.world.positions, "truth_contact": self.world.contact(),
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
        for task in &self.tasks {
            task.abort();
        }
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
