use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use reconcile::{async_trait, Transport};
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;

pub const QUEUE_BYTES: usize = 16 * 1024;

#[derive(Default)]
pub struct Counters {
    tx: AtomicU64,
    rx: AtomicU64,
    queued: Mutex<usize>,
    tx_drops: AtomicU64,
    rx_drops: AtomicU64,
    waiting: AtomicU64,
}

#[derive(Serialize)]
pub struct BandwidthSnapshot {
    pub tx_bytes: u64,
    pub rx_bytes: u64,
    pub queued_bytes: usize,
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
            tx_dropped: self.tx_drops.load(Ordering::Relaxed),
            rx_dropped: self.rx_drops.load(Ordering::Relaxed),
            waiting_ms: self.waiting.load(Ordering::Relaxed),
        }
    }
}

// Ingress policing permits at most one datagram of burst credit. Credits are
// shared across all senders, so a many-to-one CC fan-in cannot multiply its rate.
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

pub struct BandwidthTransport<T: Transport> {
    inner: Arc<T>,
    outgoing: mpsc::Sender<Packet>,
    pump: JoinHandle<()>,
    counters: Arc<Counters>,
    rate: usize,
    ingress: Mutex<Bucket>,
}

impl<T: Transport> BandwidthTransport<T> {
    pub fn new(inner: Arc<T>, kbps: usize, mtu: usize, counters: Arc<Counters>) -> Self {
        let rate = kbps * 1000 / 8;
        let (outgoing, mut incoming) = mpsc::channel::<Packet>(64);
        let sending = inner.clone();
        let stats = counters.clone();
        let pump = tokio::spawn(async move {
            while let Some(packet) = incoming.recv().await {
                let size = packet.bytes.len();
                // A fresh serialization delay for each packet: no catch-up burst
                // after scheduler stalls, and no parallel per-neighbor budgets.
                if rate > 0 {
                    let duration = Duration::from_secs_f64(size as f64 / rate as f64);
                    tokio::time::sleep(duration).await;
                    stats
                        .waiting
                        .fetch_add(duration.as_millis() as u64, Ordering::Relaxed);
                }
                stats.tx.fetch_add(size as u64, Ordering::Relaxed);
                let _ = sending.send_to(&packet.bytes, &packet.destination).await;
                *stats.queued.lock() -= size;
            }
        });
        Self {
            inner,
            outgoing,
            pump,
            counters,
            rate,
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
        let mut queued = self.counters.queued.lock();
        if buf.len() > QUEUE_BYTES.saturating_sub(*queued) {
            self.counters.tx_drops.fetch_add(1, Ordering::Relaxed);
            return Ok(buf.len());
        }
        *queued += buf.len();
        let packet = Packet {
            bytes: buf.to_vec(),
            destination: *destination,
        };
        if self.outgoing.try_send(packet).is_err() {
            *queued -= buf.len();
            self.counters.tx_drops.fetch_add(1, Ordering::Relaxed);
        }
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
        self.pump.abort();
    }
}

#[cfg(test)]
#[path = "bandwidth/tests.rs"]
mod tests;
