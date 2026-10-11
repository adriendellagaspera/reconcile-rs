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

pub const KEYS: [&str; 3] = ["a/config", "m/status", "z/version"];

pub struct Cluster {
    pub nodes: Vec<ReplicatedMap<String, String>>,
    network: Arc<Network>,
    cancel: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
    pub stage: usize,
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
            stage: 0,
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
    pub fn next(&mut self) -> io::Result<()> {
        // Readiness is actual dated-entry equality, never an animation deadline.
        let state = self.state();
        if !state["ready"].as_bool().unwrap() {
            return Err(io::Error::other("wait for observed convergence"));
        }
        match self.stage {
            0 => self.nodes[0].insert_bulk(&[
                (KEYS[0].into(), "fixed".into()),
                (KEYS[1].into(), "ready".into()),
                (KEYS[2].into(), "1".into()),
            ]),
            1 => {
                self.network.isolated.store(true, Ordering::Relaxed);
                self.nodes[0].insert(KEYS[1].into(), "alpha".into());
                self.nodes[2].insert(KEYS[1].into(), "charlie".into());
            }
            2 => {
                self.network.isolated.store(false, Ordering::Relaxed);
                self.seed();
            }
            _ => return Err(io::Error::other("exercise complete; reset to repeat")),
        }
        self.stage += 1;
        Ok(())
    }
    pub fn state(&self) -> serde_json::Value {
        let snapshots: Vec<_> = self.nodes.iter().map(|node| node.snapshot()).collect();
        let equal = snapshots[1..]
            .iter()
            .all(|s| s.iter().eq(snapshots[0].iter()));
        let ab_equal = snapshots[0].iter().eq(snapshots[1].iter());
        let isolated = self.network.isolated.load(Ordering::Relaxed);
        let nodes: Vec<_> = snapshots.iter().enumerate().map(|(id, s)| {
            let summary = |aggregate: rsos::Aggregate| serde_json::json!({
                "count": aggregate.size(), "fingerprint": aggregate.fingerprint().to_string()
            });
            let entries: Vec<_> = s.iter().map(|(key, entry)| serde_json::json!({
                "key": key, "value": entry.value(),
                "stamp": {"physical": entry.stamp.physical().millis(),
                    "logical": entry.stamp.logical().get(), "node": entry.stamp.node_id().get()},
            })).collect();
            serde_json::json!({"entries": entries, "root": summary(s.aggregate(..)),
                "ranges": [summary(s.aggregate(.."m".to_string())),
                    summary(s.aggregate("m".to_string().."z".to_string())),
                    summary(s.aggregate("z".to_string()..))],
                "rounds": self.nodes[id].sync_state().rounds})
        }).collect();
        let links: Vec<_> = (0..SIZE)
            .flat_map(|a| (0..SIZE).filter(move |&b| b != a).map(move |b| (a, b)))
            .map(|(a, b)| {
                serde_json::json!({"a": a, "b": b, "connected": self.network.allowed(a,b),
                "datagrams": self.network.received[a*SIZE+b].load(Ordering::Relaxed),
                "bytes": self.network.bytes[a*SIZE+b].load(Ordering::Relaxed)})
            })
            .collect();
        serde_json::json!({"stage": self.stage, "nodes": nodes, "links": links,
            "isolated": isolated, "equal": equal,
            "ready": if self.stage == 2 { ab_equal && !equal } else { equal },
            "dropped": self.network.dropped.load(Ordering::Relaxed)})
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
