use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

use reconcile::{async_trait, InMemoryTransport, Transport};

pub fn address(id: usize) -> SocketAddr {
    SocketAddr::from((Ipv4Addr::new(127, 0, 0, (id + 1) as u8), 9000))
}

pub struct Network {
    pub partitioned: AtomicBool,
    pub partition_drops: AtomicU64,
    pub bytes: Vec<AtomicU64>,
    pub datagrams: AtomicU64,
    pub size: usize,
}

impl Network {
    pub fn new(size: usize) -> Self {
        Self {
            partitioned: AtomicBool::new(true),
            partition_drops: AtomicU64::new(0),
            bytes: (0..size * size).map(|_| AtomicU64::new(0)).collect(),
            datagrams: AtomicU64::new(0),
            size,
        }
    }

    pub fn group(&self, id: usize) -> usize {
        usize::from(id >= self.size / 2)
    }

    pub fn allowed(&self, source: usize, destination: usize) -> bool {
        self.route(source, destination)
            && (!self.partitioned.load(Ordering::Relaxed)
                || self.group(source) == self.group(destination))
    }

    pub fn route(&self, a: usize, b: usize) -> bool {
        if a == b {
            return false;
        }
        let (a, b) = (a.min(b), a.max(b));
        let split = self.size / 2;
        if self.group(a) == self.group(b) {
            a == if a < split { 0 } else { split }
        } else {
            a == 0 && b == split
        }
    }

    pub fn links(&self) -> Vec<serde_json::Value> {
        (0..self.size)
            .flat_map(|a| {
                ((a + 1)..self.size).map(move |b| {
                    serde_json::json!({"a": a, "b": b, "enabled": self.allowed(a, b),
                "bytes": self.bytes[a * self.size + b].load(Ordering::Relaxed)
                    + self.bytes[b * self.size + a].load(Ordering::Relaxed)})
                })
            })
            .collect()
    }
}

// Inside Netem: topology is checked when a delayed packet reaches the fabric.
// Datagrams already admitted before a partition may still be consumed afterward.
pub struct PartitionGate {
    pub inner: InMemoryTransport,
    pub source: usize,
    pub network: Arc<Network>,
}

#[async_trait]
impl Transport for PartitionGate {
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.inner.recv_from(buf).await
    }

    async fn send_to(&self, buf: &[u8], destination: &SocketAddr) -> io::Result<usize> {
        let Some(id) = (0..self.network.size).find(|&id| address(id) == *destination) else {
            return Ok(buf.len()); // Discovery probes to unbound addresses, like UDP.
        };
        if !self.network.route(self.source, id) {
            return Ok(buf.len());
        }
        if !self.network.allowed(self.source, id) {
            self.network.partition_drops.fetch_add(1, Ordering::Relaxed);
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
