use reconcile::{async_trait, InMemoryTransport, Transport};
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

pub const SIZE: usize = 3;
pub fn address(id: usize) -> SocketAddr {
    SocketAddr::from((Ipv4Addr::new(127, 0, 0, (id + 1) as u8), 9000))
}

pub struct Network {
    pub isolated: AtomicBool,
    pub received: Vec<AtomicU64>,
    pub bytes: Vec<AtomicU64>,
    pub dropped: AtomicU64,
}
impl Network {
    pub fn new() -> Self {
        Self {
            isolated: AtomicBool::new(false),
            received: (0..SIZE * SIZE).map(|_| AtomicU64::new(0)).collect(),
            bytes: (0..SIZE * SIZE).map(|_| AtomicU64::new(0)).collect(),
            dropped: AtomicU64::new(0),
        }
    }
    pub fn allowed(&self, a: usize, b: usize) -> bool {
        a != b && !(self.isolated.load(Ordering::Relaxed) && (a == 2 || b == 2))
    }
}

// The cut is checked at send and receive, including packets queued before isolation.
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
            if let Some(id) = (0..SIZE).find(|&id| address(id) == source) {
                if self.network.allowed(id, self.source) {
                    let pair = id * SIZE + self.source;
                    self.network.received[pair].fetch_add(1, Ordering::Relaxed);
                    self.network.bytes[pair].fetch_add(size as u64, Ordering::Relaxed);
                    return Ok((size, source));
                }
                self.network.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    async fn send_to(&self, buf: &[u8], destination: &SocketAddr) -> io::Result<usize> {
        let Some(id) = (0..SIZE).find(|&id| address(id) == *destination) else {
            return Ok(buf.len());
        };
        if self.network.allowed(self.source, id) {
            self.inner.send_to(buf, destination).await
        } else {
            self.network.dropped.fetch_add(1, Ordering::Relaxed);
            Ok(buf.len())
        }
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}
