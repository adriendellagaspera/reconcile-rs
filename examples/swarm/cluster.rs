use std::collections::BTreeSet;
use std::io;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;

use gossip::netem::{Impairments, Link, Netem, NetemTransport, Probability, Rtt, Seed};
use reconcile::{
    async_trait, replicated_map::Config, ClusterKey, InMemoryNetwork, InMemoryTransport,
    ReplicatedMap, Transport,
};
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub const WIDTH: usize = 32;
pub const HEIGHT: usize = 20;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Observation {
    Terrain {
        x: usize,
        y: usize,
        land: bool,
    },
    Contact {
        x: usize,
        y: usize,
        seen: u64,
        source: usize,
    },
    Sector {
        id: usize,
        scanned: u64,
    },
}

pub fn terrain(x: usize, y: usize) -> bool {
    let x = x as f64;
    let y = y as f64;
    x < 3.0 + (y * 0.5).sin() * 1.8
        || ((x - 15.0) / 4.0).powi(2) + ((y - 8.0) / 3.0).powi(2) < 1.0
        || ((x - 26.0) / 3.0).powi(2) + ((y - 15.0) / 2.0).powi(2) < 1.0
}

#[derive(Default)]
struct Traffic {
    partition_drops: AtomicU64,
    delivered_bytes: AtomicU64,
    delivered_datagrams: AtomicU64,
}

// Inside Netem: a delayed packet is checked against topology at delivery time.
struct PartitionGate {
    inner: InMemoryTransport,
    partitioned: Arc<AtomicBool>,
    traffic: Arc<Traffic>,
}

#[async_trait]
impl Transport for PartitionGate {
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.inner.recv_from(buf).await
    }

    async fn send_to(&self, buf: &[u8], dst: &SocketAddr) -> io::Result<usize> {
        if self.partitioned.load(Ordering::Relaxed) {
            self.traffic.partition_drops.fetch_add(1, Ordering::Relaxed);
            return Ok(buf.len());
        }
        // Count only traffic routed to an actual endpoint, excluding discovery probes.
        if ["127.0.0.1", "127.0.0.2"]
            .iter()
            .any(|ip| dst.ip().to_string() == *ip)
        {
            self.traffic
                .delivered_bytes
                .fetch_add(buf.len() as u64, Ordering::Relaxed);
            self.traffic
                .delivered_datagrams
                .fetch_add(1, Ordering::Relaxed);
        }
        self.inner.send_to(buf, dst).await
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}

pub struct Cluster {
    pub nodes: Vec<ReplicatedMap<String, Observation>>,
    partitioned: Arc<AtomicBool>,
    traffic: Arc<Traffic>,
    losses: Vec<Impairments>,
    cancel: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
    pub loss: f64,
    observations: u64,
}

impl Cluster {
    pub fn new(loss: f64) -> io::Result<Self> {
        let network = InMemoryNetwork::new();
        let partitioned = Arc::new(AtomicBool::new(true));
        let traffic = Arc::new(Traffic::default());
        let cancel = CancellationToken::new();
        let mut nodes = Vec::new();
        let mut losses = Vec::new();
        let mut tasks = Vec::new();
        for id in 0..2 {
            let addr: SocketAddr = format!("127.0.0.{}:9000", id + 1).parse().unwrap();
            let gate = PartitionGate {
                inner: network.bind(addr),
                partitioned: partitioned.clone(),
                traffic: traffic.clone(),
            };
            let transport = NetemTransport::new(
                Arc::new(gate),
                Netem::uniform(
                    Link::at(Rtt::from_millis(160.0)).with_loss(Probability::percent(loss)),
                    Seed::new(42 + id),
                ),
            );
            losses.push(transport.impairments());
            let config = Config::default()
                .with_port(addr.port())
                .with_listen_addr(addr.ip())
                .with_net("127.0.0.0/30".parse().unwrap())
                .map_err(io::Error::other)?
                .with_cluster_key(ClusterKey::new([0x42; 32]))
                .with_reconcile_interval(Duration::from_millis(250));
            let node = ReplicatedMap::new_with_transport(config, Arc::new(transport))
                .map_err(io::Error::other)?;
            node.seed_peer(format!("127.0.0.{}", 2 - id).parse().unwrap());
            let running_node = node.clone();
            let token = cancel.clone();
            tasks.push(tokio::spawn(async move {
                running_node.run(token).await;
            }));
            nodes.push(node);
        }
        let mut cluster = Self {
            nodes,
            partitioned,
            traffic,
            losses,
            cancel,
            tasks,
            loss,
            observations: 0,
        };
        cluster.observe();
        Ok(cluster)
    }

    pub fn partition(&self, enabled: bool) {
        self.partitioned.store(enabled, Ordering::Relaxed);
        if !enabled {
            for id in 0..2 {
                self.nodes[id].seed_peer(format!("127.0.0.{}", 2 - id).parse().unwrap());
            }
        }
    }

    pub fn observe(&mut self) {
        self.observations += 1;
        for id in 0..2 {
            let cx = if id == 0 { 7 } else { 24 };
            let cy = if id == 0 { 6 } else { 12 };
            let radius = (self.observations + 1).min(6) as usize;
            for y in cy - radius..=(cy + radius).min(HEIGHT - 1) {
                for x in cx - radius..=(cx + radius).min(WIDTH - 1) {
                    if (x as isize - cx as isize).pow(2) + (y as isize - cy as isize).pow(2)
                        > (radius * radius) as isize
                    {
                        continue;
                    }
                    let key = format!("map/{x:02}/{y:02}");
                    if !self.nodes[id].contains_key(&key) {
                        self.nodes[id].insert(
                            key,
                            Observation::Terrain {
                                x,
                                y,
                                land: terrain(x, y),
                            },
                        );
                    }
                }
            }
            self.nodes[id].insert(
                format!("sector/{id}"),
                Observation::Sector {
                    id,
                    scanned: self.observations,
                },
            );
        }
        // A contact is observed only by A. B must learn it through the protocol.
        self.nodes[0].insert(
            "contact/01".into(),
            Observation::Contact {
                x: 10,
                y: 7,
                seen: self.observations,
                source: 0,
            },
        );
    }

    pub fn state(&self) -> serde_json::Value {
        // One immutable dated snapshot per node; comparisons include LWW timestamps.
        let snapshots: Vec<_> = self.nodes.iter().map(|n| n.snapshot()).collect();
        let keys: BTreeSet<_> = snapshots
            .iter()
            .flat_map(|s| s.iter().map(|(k, _)| k.clone()))
            .collect();
        let divergent = keys
            .iter()
            .filter(|k| snapshots[0].get(*k) != snapshots[1].get(*k))
            .count();
        let entries: Vec<Vec<_>> = snapshots
            .iter()
            .map(|s| {
                s.iter()
                    .filter_map(|(k, e)| e.value().map(|v| (k.clone(), v.clone())))
                    .collect()
            })
            .collect();
        serde_json::json!({
            "partitioned": self.partitioned.load(Ordering::Relaxed), "loss": self.loss,
            "nodes": entries, "union_keys": keys.len(), "divergent_keys": divergent,
            "loss_offered": self.losses.iter().map(Impairments::offered).sum::<u64>(),
            "loss_dropped": self.losses.iter().map(Impairments::dropped).sum::<u64>(),
            "partition_dropped": self.traffic.partition_drops.load(Ordering::Relaxed),
            "delivered_bytes": self.traffic.delivered_bytes.load(Ordering::Relaxed),
            "delivered_datagrams": self.traffic.delivered_datagrams.load(Ordering::Relaxed),
            "observation_step": self.observations,
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
