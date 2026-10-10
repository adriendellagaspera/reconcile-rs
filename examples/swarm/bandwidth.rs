use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use reconcile::{async_trait, Transport};
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::Instant;

pub const QUEUE_BYTES: usize = 16 * 1024;

#[derive(Default)]
pub struct Counters {
    tx: AtomicU64,
    rx: AtomicU64,
    queued: Mutex<usize>,
    lane_queued: Mutex<Vec<usize>>,
    tx_drops: AtomicU64,
    rx_drops: AtomicU64,
    waiting: AtomicU64,
}

#[derive(Serialize)]
pub struct BandwidthSnapshot {
    pub tx_bytes: u64,
    pub rx_bytes: u64,
    pub queued_bytes: usize,
    pub max_lane_queued_bytes: usize,
    pub tx_dropped: u64,
    pub rx_dropped: u64,
    pub waiting_ms: u64,
}

impl Counters {
    pub fn snapshot(&self) -> BandwidthSnapshot {
        BandwidthSnapshot {
            tx_bytes: self.tx.load(Ordering::Relaxed),
            rx_bytes: self.rx.load(Ordering::Relaxed),
            queued_bytes: *self.queued.lock(),
            max_lane_queued_bytes: self.lane_queued.lock().iter().copied().max().unwrap_or(0),
            tx_dropped: self.tx_drops.load(Ordering::Relaxed),
            rx_dropped: self.rx_drops.load(Ordering::Relaxed),
            waiting_ms: self.waiting.load(Ordering::Relaxed),
        }
    }
}

// Ingress policing permits at most one datagram of burst credit. Credits are
// shared across all senders at a glider. The shore station has no aggregate RX cap.
struct Bucket {
    credit: f64,
    last: Instant,
    burst: usize,
}

impl Bucket {
    fn admit(&mut self, size: usize, now: Instant, rate: usize) -> bool {
        self.credit = (self.credit + now.duration_since(self.last).as_secs_f64() * rate as f64)
            .min(self.burst as f64);
        self.last = now;
        if size as f64 > self.credit {
            return false;
        }
        self.credit -= size as f64;
        true
    }
}

struct Packet {
    bytes: Vec<u8>,
    destination: SocketAddr,
}

#[derive(Default)]
struct Pending {
    packets: [VecDeque<Packet>; 2],
    bytes: [usize; 2],
    datagrams: [usize; 2],
    served: [usize; 2],
}

impl Pending {
    fn pop(&mut self) -> Option<(usize, Packet)> {
        let class = match (self.packets[0].is_empty(), self.packets[1].is_empty()) {
            (true, true) => return None,
            (false, true) => {
                self.served[1] = self.served[0];
                0
            }
            (true, false) => {
                self.served[0] = self.served[1];
                1
            }
            (false, false) => usize::from(self.served[0] > self.served[1]),
        };
        let packet = self.packets[class].pop_front()?;
        self.served[class] += packet.bytes.len();
        Some((class, packet))
    }
}

struct Lane {
    pending: Mutex<Pending>,
    ready: Notify,
    priority: Option<SocketAddr>,
    capacity: usize,
}

pub struct BandwidthTransport<T: Transport> {
    inner: Arc<T>,
    lanes: Vec<Arc<Lane>>,
    destinations: Option<HashMap<SocketAddr, usize>>,
    pumps: Vec<JoinHandle<()>>,
    counters: Arc<Counters>,
    rate: usize,
    ingress: Mutex<Bucket>,
}

impl<T: Transport> BandwidthTransport<T> {
    #[cfg(test)]
    pub fn new(inner: Arc<T>, kbps: usize, mtu: usize, counters: Arc<Counters>) -> Self {
        Self::glider(inner, kbps, mtu, counters, None)
    }

    pub fn glider(
        inner: Arc<T>,
        kbps: usize,
        mtu: usize,
        counters: Arc<Counters>,
        center: Option<SocketAddr>,
    ) -> Self {
        Self::build(inner, kbps, mtu, counters, None, center)
    }

    pub fn command_center(
        inner: Arc<T>,
        kbps: usize,
        mtu: usize,
        counters: Arc<Counters>,
        peers: Vec<SocketAddr>,
    ) -> Self {
        Self::build(inner, kbps, mtu, counters, Some(peers), None)
    }

    fn build(
        inner: Arc<T>,
        kbps: usize,
        mtu: usize,
        counters: Arc<Counters>,
        peers: Option<Vec<SocketAddr>>,
        priority: Option<SocketAddr>,
    ) -> Self {
        let rate = kbps * 1000 / 8;
        let destinations = peers.as_ref().map(|peers| {
            peers
                .iter()
                .enumerate()
                .map(|(id, addr)| (*addr, id))
                .collect()
        });
        let mut lanes = Vec::new();
        let mut pumps = Vec::new();
        let lane_count = peers.as_ref().map_or(1, Vec::len);
        *counters.lane_queued.lock() = vec![0; lane_count];
        for lane_id in 0..lane_count {
            let lane = Arc::new(Lane {
                pending: Mutex::new(Pending::default()),
                ready: Notify::new(),
                priority,
                capacity: if priority.is_some() {
                    (QUEUE_BYTES / 2).max(mtu)
                } else {
                    QUEUE_BYTES.max(mtu)
                },
            });
            let queue = lane.clone();
            let sending = inner.clone();
            let stats = counters.clone();
            pumps.push(tokio::spawn(async move {
                loop {
                    let next = queue.pending.lock().pop();
                    let Some((class, packet)) = next else {
                        queue.ready.notified().await;
                        continue;
                    };
                    let size = packet.bytes.len();
                    if rate > 0 {
                        let duration = Duration::from_secs_f64(size as f64 / rate as f64);
                        tokio::time::sleep(duration).await;
                        stats
                            .waiting
                            .fetch_add(duration.as_millis() as u64, Ordering::Relaxed);
                    }
                    stats.tx.fetch_add(size as u64, Ordering::Relaxed);
                    let _ = sending.send_to(&packet.bytes, &packet.destination).await;
                    {
                        let mut pending = queue.pending.lock();
                        pending.bytes[class] -= size;
                        pending.datagrams[class] -= 1;
                    }
                    *stats.queued.lock() -= size;
                    stats.lane_queued.lock()[lane_id] -= size;
                }
            }));
            lanes.push(lane);
        }
        Self {
            inner,
            lanes,
            destinations,
            pumps,
            counters,
            rate: if peers.is_some() { 0 } else { rate },
            ingress: Mutex::new(Bucket {
                credit: 0.0,
                last: Instant::now(),
                burst: mtu,
            }),
        }
    }
}

#[async_trait]
impl<T: Transport> Transport for BandwidthTransport<T> {
    async fn send_to(&self, buf: &[u8], destination: &SocketAddr) -> io::Result<usize> {
        let lane_index = match &self.destinations {
            Some(destinations) => match destinations.get(destination) {
                Some(index) => *index,
                None => return Ok(buf.len()), // Only configured peers get bounded CC lanes.
            },
            None => 0,
        };
        let lane = &self.lanes[lane_index];
        let class = usize::from(lane.priority.is_some_and(|center| center != *destination));
        let mut pending = lane.pending.lock();
        // Separate admission reservations prevent fleet traffic from occupying CC space.
        // Bytes include the packet currently on the wire; capacity is always bounded.
        if buf.len() > lane.capacity.saturating_sub(pending.bytes[class])
            || pending.datagrams[class] >= 64
        {
            self.counters.tx_drops.fetch_add(1, Ordering::Relaxed);
            return Ok(buf.len());
        }
        pending.bytes[class] += buf.len();
        pending.datagrams[class] += 1;
        *self.counters.queued.lock() += buf.len();
        self.counters.lane_queued.lock()[lane_index] += buf.len();
        pending.packets[class].push_back(Packet {
            bytes: buf.to_vec(),
            destination: *destination,
        });
        drop(pending);
        lane.ready.notify_one();
        Ok(buf.len())
    }

    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        loop {
            let (size, source) = self.inner.recv_from(buf).await?;
            if self.rate == 0 || self.ingress.lock().admit(size, Instant::now(), self.rate) {
                self.counters.rx.fetch_add(size as u64, Ordering::Relaxed);
                return Ok((size, source));
            }
            self.counters.rx_drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}

impl<T: Transport> Drop for BandwidthTransport<T> {
    fn drop(&mut self) {
        for pump in &self.pumps {
            pump.abort();
        }
    }
}

#[cfg(test)]
#[path = "bandwidth/tests.rs"]
mod tests;
