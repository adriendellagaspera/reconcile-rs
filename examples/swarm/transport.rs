use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use parking_lot::RwLock;
use reconcile::{async_trait, InMemoryTransport, Transport};
use serde::Serialize;

use super::world::{PeerState, Point};

pub fn address(id: usize) -> SocketAddr {
    SocketAddr::from((Ipv4Addr::new(127, 0, 0, (id + 1) as u8), 9000))
}

pub const DEFAULT_RANGE: f64 = 13.0;

pub struct Topology {
    pub positions: Vec<Point>,
    pub peers: Vec<PeerState>,
    pub blocked: Vec<bool>,
    pub range: f64,
    pub storm: bool,
}

impl Topology {
    fn reason(&self, a: usize, b: usize) -> Option<&'static str> {
        if a == b {
            return Some("self");
        }
        if self.peers[a] == PeerState::Stopped || self.peers[b] == PeerState::Stopped {
            return Some("stopped");
        }
        if self.peers[a] == PeerState::Offline || self.peers[b] == PeerState::Offline {
            return Some("modem offline");
        }
        if self.blocked[a * self.peers.len() + b] {
            return Some("manual cut");
        }
        if self.positions[a].distance(self.positions[b]) > self.range {
            return Some("out of range");
        }
        // A synthetic weather front obstructs paths crossing the central channel.
        // It models pairwise link outages, not an acoustic propagation model.
        if self.storm && ((self.positions[a].x < 16.0) != (self.positions[b].x < 16.0)) {
            return Some("weather front");
        }
        None
    }
}

#[derive(Serialize)]
pub struct PairLink {
    pub a: usize,
    pub b: usize,
    pub enabled: bool,
    pub manual_cut: bool,
    pub reason: Option<&'static str>,
    pub distance: f64,
    pub bytes: u64,
}

pub struct Network {
    pub topology: RwLock<Topology>,
    pub blocked_drops: AtomicU64,
    pub bytes: Vec<AtomicU64>,
    pub datagrams: AtomicU64,
    pub size: usize,
}

impl Network {
    pub fn new(positions: Vec<Point>) -> Self {
        let size = positions.len();
        Self {
            topology: RwLock::new(Topology {
                positions,
                peers: vec![PeerState::Active; size],
                blocked: vec![false; size * size],
                range: DEFAULT_RANGE,
                storm: false,
            }),
            blocked_drops: AtomicU64::new(0),
            bytes: (0..size * size).map(|_| AtomicU64::new(0)).collect(),
            datagrams: AtomicU64::new(0),
            size,
        }
    }

    pub fn allowed(&self, a: usize, b: usize) -> bool {
        self.topology.read().reason(a, b).is_none()
    }

    pub fn links(&self) -> Vec<PairLink> {
        let topology = self.topology.read();
        (0..self.size)
            .flat_map(|a| ((a + 1)..self.size).map(move |b| (a, b)))
            .map(|(a, b)| {
                let reason = topology.reason(a, b);
                PairLink {
                    a,
                    b,
                    enabled: reason.is_none(),
                    manual_cut: topology.blocked[a * self.size + b],
                    reason,
                    distance: topology.positions[a].distance(topology.positions[b]),
                    bytes: self.bytes[a * self.size + b].load(Ordering::Relaxed)
                        + self.bytes[b * self.size + a].load(Ordering::Relaxed),
                }
            })
            .collect()
    }

    pub fn components(&self) -> Vec<Vec<usize>> {
        let topology = self.topology.read();
        let mut seen = vec![false; self.size];
        let mut components = Vec::new();
        for root in 0..self.size {
            if seen[root] {
                continue;
            }
            let mut members = vec![root];
            seen[root] = true;
            let mut cursor = 0;
            while cursor < members.len() {
                for (peer, visited) in seen.iter_mut().enumerate() {
                    if !*visited && topology.reason(members[cursor], peer).is_none() {
                        *visited = true;
                        members.push(peer);
                    }
                }
                cursor += 1;
            }
            components.push(members);
        }
        components
    }
}

// Inside Netem: availability is checked when a delayed packet reaches the fabric.
// Already queued ingress is rechecked before a restarted node can consume it.
pub struct PartitionGate {
    pub inner: InMemoryTransport,
    pub source: usize,
    pub network: Arc<Network>,
}

#[async_trait]
impl Transport for PartitionGate {
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        loop {
            let (size, source) = self.inner.recv_from(buf).await?;
            if let Some(id) = (0..self.network.size).find(|&id| address(id) == source) {
                if self.network.allowed(id, self.source) {
                    return Ok((size, source));
                }
                self.network.blocked_drops.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    async fn send_to(&self, buf: &[u8], destination: &SocketAddr) -> io::Result<usize> {
        let Some(id) = (0..self.network.size).find(|&id| address(id) == *destination) else {
            return Ok(buf.len()); // Discovery probes to unbound addresses, like UDP.
        };
        if !self.network.allowed(self.source, id) {
            self.network.blocked_drops.fetch_add(1, Ordering::Relaxed);
            return Ok(buf.len());
        }
        self.inner.send_to(buf, destination).await?;
        self.network.bytes[self.source * self.network.size + id]
            .fetch_add(buf.len() as u64, Ordering::Relaxed);
        self.network.datagrams.fetch_add(1, Ordering::Relaxed);
        Ok(buf.len())
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}
