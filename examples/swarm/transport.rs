use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Instant;

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
    pub storm_position: Point,
    pub jammer: bool,
}

#[derive(Serialize)]
pub struct Disruption {
    pub kind: &'static str,
    pub position: Point,
    pub radius: f64,
}

impl Disruption {
    fn intersects(&self, a: Point, b: Point) -> bool {
        let dx = b.x - a.x;
        let dy = b.y - a.y;
        let length_squared = dx * dx + dy * dy;
        let t = if length_squared == 0.0 {
            0.0
        } else {
            (((self.position.x - a.x) * dx + (self.position.y - a.y) * dy) / length_squared)
                .clamp(0.0, 1.0)
        };
        self.position.distance(Point {
            x: a.x + t * dx,
            y: a.y + t * dy,
        }) <= self.radius
    }
}

impl Topology {
    pub fn disruptions(&self) -> Vec<Disruption> {
        let mut zones = Vec::new();
        if self.storm {
            zones.push(Disruption {
                kind: "storm",
                position: self.storm_position,
                radius: 4.0,
            });
        }
        if self.jammer {
            zones.push(Disruption {
                kind: "human jamming",
                position: Point { x: 24.0, y: 12.0 },
                radius: 3.0,
            });
        }
        zones
    }

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
        for zone in self.disruptions() {
            if zone.intersects(self.positions[a], self.positions[b]) {
                return Some(zone.kind);
            }
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

#[derive(Clone, Copy)]
struct Receipt {
    datagrams: u64,
    last: Instant,
}

#[derive(Serialize)]
pub struct DirectPeer {
    pub peer: usize,
    pub datagrams: u64,
    pub age_ms: u128,
}

pub struct Network {
    pub topology: RwLock<Topology>,
    pub blocked_drops: AtomicU64,
    pub bytes: Vec<AtomicU64>,
    pub datagrams: AtomicU64,
    pub size: usize,
    receipts: RwLock<Vec<Option<Receipt>>>,
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
                storm_position: Point { x: 16.0, y: 13.0 },
                jammer: false,
            }),
            blocked_drops: AtomicU64::new(0),
            bytes: (0..size * size).map(|_| AtomicU64::new(0)).collect(),
            datagrams: AtomicU64::new(0),
            size,
            receipts: RwLock::new(vec![None; size * size]),
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

    fn received(&self, source: usize, observer: usize) {
        let mut receipts = self.receipts.write();
        let previous = receipts[source * self.size + observer];
        receipts[source * self.size + observer] = Some(Receipt {
            datagrams: previous.map_or(1, |receipt| receipt.datagrams + 1),
            last: Instant::now(),
        });
    }

    pub fn direct_peers(&self, observer: usize) -> Vec<DirectPeer> {
        let receipts = self.receipts.read();
        (0..self.size)
            .filter_map(|peer| {
                receipts[peer * self.size + observer].map(|receipt| DirectPeer {
                    peer,
                    datagrams: receipt.datagrams,
                    age_ms: receipt.last.elapsed().as_millis(),
                })
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
                    self.network.received(id, self.source);
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn disruption_blocks_intersecting_paths_not_map_halves() {
        let zone = Disruption {
            kind: "storm",
            position: Point { x: 16.0, y: 10.0 },
            radius: 3.0,
        };
        assert!(zone.intersects(Point { x: 10.0, y: 10.0 }, Point { x: 22.0, y: 10.0 }));
        assert!(zone.intersects(Point { x: 14.0, y: 8.0 }, Point { x: 14.0, y: 12.0 }));
        assert!(!zone.intersects(Point { x: 10.0, y: 2.0 }, Point { x: 22.0, y: 2.0 }));
        assert!(!zone.intersects(Point { x: 10.0, y: 10.0 }, Point { x: 12.0, y: 10.0 }));
    }
}
