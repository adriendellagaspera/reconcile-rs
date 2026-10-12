#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use reconcile::runtime as tokio;

use super::transport::{address, Network, PartitionGate, SIZE};
use reconcile::{
    replicated_map::{Config, FramingConfig},
    ClusterKey, InMemoryNetwork, NodeId, ReplicatedMap,
};
use std::io;
use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub const KEY_LIMIT: u32 = 1024;

pub struct Cluster {
    pub nodes: Vec<ReplicatedMap<u32, String>>,
    network: Arc<Network>,
    cancel: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
    pub revision: u64,
    pub last_action: String,
}
impl Cluster {
    pub fn new() -> io::Result<Self> {
        let fabric = InMemoryNetwork::new();
        let network = Arc::new(Network::new());
        let cancel = CancellationToken::new();
        let mut nodes = Vec::new();
        let mut tasks = Vec::new();
        for id in 0..SIZE {
            let addr = address(id);
            let config = Config::default()
                .with_port(addr.port())
                .with_listen_addr(addr.ip())
                .with_net("127.0.0.0/29".parse().unwrap())
                .map_err(io::Error::other)?
                .with_cluster_key(ClusterKey::new([0x42; 32]))
                .with_node_id(NodeId::new(id as u64 + 1))
                .with_framing(FramingConfig::default().with_datagram_payload_budget(1200))
                .with_repair_interval(Duration::from_millis(350))
                .with_reconcile_interval(Duration::from_millis(800));
            let gate = PartitionGate {
                inner: fabric.bind(addr),
                source: id,
                network: network.clone(),
            };
            let node = ReplicatedMap::new_with_transport(config, Arc::new(gate))
                .map_err(io::Error::other)?;
            let running = node.clone();
            let token = cancel.child_token();
            tasks.push(tokio::spawn(async move {
                running.run(token).await;
            }));
            // Application-owned cadence invokes the real anti-entropy API.
            let rounds = node.clone();
            let token = cancel.child_token();
            tasks.push(tokio::spawn(async move {
                let mut timer = tokio::time::interval(Duration::from_millis(800));
                timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = token.cancelled() => break,
                        _ = timer.tick() => rounds.start_reconciliation().await,
                    }
                }
            }));
            nodes.push(node);
        }
        let cluster = Self {
            nodes,
            network,
            cancel,
            tasks,
            revision: 0,
            last_action: "Ready".into(),
        };
        cluster.seed();
        Ok(cluster)
    }
    fn seed(&self) {
        for (id, node) in self.nodes.iter().enumerate() {
            for peer in 0..SIZE {
                if self.network.allowed(id, peer) {
                    node.seed_peer(address(peer).ip());
                }
            }
        }
    }
    pub fn connect(&self, id: usize, online: bool) {
        self.network.online[id].store(online, Ordering::Relaxed);
        self.seed();
    }
    pub fn link(&self, a: usize, b: usize, open: bool) {
        self.network.blocked[a * SIZE + b].store(!open, Ordering::Relaxed);
        self.network.blocked[b * SIZE + a].store(!open, Ordering::Relaxed);
        self.seed();
    }
    pub fn online(&self, id: usize) -> bool {
        self.network.online[id].load(Ordering::Relaxed)
    }
    pub fn acknowledge(&mut self, message: String) {
        self.revision += 1;
        self.last_action = message;
    }
    pub fn state(&self) -> serde_json::Value {
        let snapshots: Vec<_> = self.nodes.iter().map(|node| node.snapshot()).collect();
        let equal = snapshots[1..]
            .iter()
            .all(|s| s.iter().eq(snapshots[0].iter()));
        let nodes: Vec<_> = snapshots.iter().enumerate().map(|(id, s)| {
            let summary = |aggregate: rsos::Aggregate| serde_json::json!({
                "count": aggregate.size(), "fingerprint": aggregate.fingerprint().to_string()
            });
            let entries: Vec<_> = s.iter().map(|(key, entry)| serde_json::json!({
                "key": key, "value": entry.value(),
                "stamp": {"physical": entry.stamp.physical().millis(),
                    "logical": entry.stamp.logical().get(), "node": entry.stamp.node_id().get()},
            })).collect();
            let live = s.iter().filter(|(_, e)| !e.is_tombstone()).count();
            let bins: Vec<_> = (0..32).map(|i| summary(s.aggregate(i * 32..(i + 1) * 32))).collect();
            serde_json::json!({"entries": entries, "root": summary(s.aggregate(..)),
                "live": live, "tombstones": s.len() - live, "online": self.online(id),
                "bins": bins, "rounds": self.nodes[id].sync_state().rounds})
        }).collect();
        let links: Vec<_> = (0..SIZE)
            .flat_map(|a| (0..SIZE).filter(move |&b| b != a).map(move |b| (a, b)))
            .map(|(a, b)| {
                serde_json::json!({"a": a, "b": b, "connected": self.network.allowed(a,b),
                "datagrams": self.network.received[a*SIZE+b].load(Ordering::Relaxed),
                "bytes": self.network.bytes[a*SIZE+b].load(Ordering::Relaxed),
                "open": !self.network.blocked[a*SIZE+b].load(Ordering::Relaxed)})
            })
            .collect();
        let divergent = (0..KEY_LIMIT)
            .filter(|key| {
                snapshots[1..]
                    .iter()
                    .any(|s| s.get(key) != snapshots[0].get(key))
            })
            .count();
        serde_json::json!({"revision": self.revision, "last_action": self.last_action,
            "nodes": nodes, "links": links, "equal": equal, "divergent": divergent,
            "key_limit": KEY_LIMIT, "dropped": self.network.dropped.load(Ordering::Relaxed)})
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
