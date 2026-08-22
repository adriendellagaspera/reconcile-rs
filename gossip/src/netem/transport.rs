// Copyright 2026 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The [`Transport`] decorator itself and its per-node delivery pump. Parameters
//! (`Link`/`Netem`/`Seed`) live in the parent module.

use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::SeedableRng;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::{stream_seed, Netem};
use crate::Transport;

/// Tokio's timer resolution: [`tokio::time::sleep`] rounds up to the next millisecond. The pump
/// therefore sleeps to within this much of a delivery and yields out the remainder — without it the
/// 0.1 ms lane would measure the timer wheel rather than the link.
const TIMER_RESOLUTION: Duration = Duration::from_millis(1);

/// A datagram in flight, ordered by when it is due and, for a tie, by send order.
struct Pending {
    due: Instant,
    seq: u64,
    destination: SocketAddr,
    bytes: Vec<u8>,
}

impl Ord for Pending {
    fn cmp(&self, other: &Pending) -> Ordering {
        self.due.cmp(&other.due).then(self.seq.cmp(&other.seq))
    }
}

impl PartialOrd for Pending {
    fn partial_cmp(&self, other: &Pending) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Pending {
    fn eq(&self, other: &Pending) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Pending {}

/// The in-flight queue, shared between [`NetemTransport::send_to`] and the pump task.
#[derive(Default)]
struct InFlight {
    /// `Reverse`, so the [`BinaryHeap`] pops the *earliest* due datagram.
    queue: Mutex<BinaryHeap<Reverse<Pending>>>,
    wake: Notify,
}

/// What the model did to this node's outbound traffic — the instrument's own accounting, so a lane
/// can assert it got the loss rate it asked for instead of trusting it.
#[derive(Clone, Debug, Default)]
pub struct Impairments {
    offered: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
}

impl Impairments {
    /// Datagrams handed to [`Transport::send_to`].
    pub fn offered(&self) -> u64 {
        self.offered.load(AtomicOrdering::Relaxed)
    }

    /// Datagrams the loss model swallowed.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(AtomicOrdering::Relaxed)
    }

    /// The realized loss fraction, or `0.0` before anything was sent.
    pub fn loss_fraction(&self) -> f64 {
        let offered = self.offered();
        if offered == 0 {
            0.0
        } else {
            self.dropped() as f64 / offered as f64
        }
    }
}

/// A [`Transport`] decorator that delays, drops and reorders what the wrapped one sends.
///
/// Construct inside a Tokio runtime: it spawns the per-node delivery pump, which is aborted when
/// the transport drops.
pub struct NetemTransport<T> {
    inner: Arc<T>,
    netem: Netem,
    local: SocketAddr,
    /// One PRNG per directed link, created on first use and advanced in send order.
    streams: Mutex<HashMap<SocketAddr, StdRng>>,
    in_flight: Arc<InFlight>,
    seq: AtomicU64,
    impairments: Impairments,
    pump: JoinHandle<()>,
}

impl<T: Transport> NetemTransport<T> {
    /// Wrap `inner` in `netem`.
    ///
    /// # Panics
    ///
    /// If called outside a Tokio runtime, or if `inner` has no local address (neither is reachable
    /// from the benchmarks, which bind an `InMemoryTransport` first).
    pub fn new(inner: Arc<T>, netem: Netem) -> NetemTransport<T> {
        let local = inner
            .local_addr()
            .expect("a netem-wrapped transport must already be bound");
        let in_flight = Arc::new(InFlight::default());
        let pump = tokio::spawn(pump(Arc::clone(&inner), Arc::clone(&in_flight)));
        NetemTransport {
            inner,
            netem,
            local,
            streams: Mutex::new(HashMap::new()),
            in_flight,
            seq: AtomicU64::new(0),
            impairments: Impairments::default(),
            pump,
        }
    }

    /// What the model has done to this node's outbound traffic so far.
    pub fn impairments(&self) -> Impairments {
        self.impairments.clone()
    }
}

impl<T> Drop for NetemTransport<T> {
    fn drop(&mut self) {
        // The pump owns an `Arc` of the wrapped transport and outlives nothing: a benchmark that
        // rebuilds its cluster once per iteration would otherwise accumulate one live task per
        // node per sample.
        self.pump.abort();
    }
}

#[async_trait::async_trait]
impl<T: Transport> Transport for NetemTransport<T> {
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        // Reception is untouched: impairment is applied once per datagram, on the sending side,
        // where the link's direction is known from the destination.
        self.inner.recv_from(buf).await
    }

    async fn send_to(&self, buf: &[u8], destination: &SocketAddr) -> io::Result<usize> {
        let link = self.netem.link_to(destination);
        let delay = {
            let mut streams = self.streams.lock();
            let stream = streams.entry(*destination).or_insert_with(|| {
                StdRng::seed_from_u64(stream_seed(self.netem.seed, self.local, *destination))
            });
            link.draw(stream)
        };
        self.impairments
            .offered
            .fetch_add(1, AtomicOrdering::Relaxed);
        let Some(delay) = delay else {
            // A dropped datagram is a successful send: UDP gives the sender no other answer, and
            // that indistinguishability is exactly what the protocol has to survive.
            self.impairments
                .dropped
                .fetch_add(1, AtomicOrdering::Relaxed);
            return Ok(buf.len());
        };
        self.in_flight.queue.lock().push(Reverse(Pending {
            due: Instant::now() + delay,
            seq: self.seq.fetch_add(1, AtomicOrdering::Relaxed),
            destination: *destination,
            bytes: buf.to_vec(),
        }));
        self.in_flight.wake.notify_one();
        Ok(buf.len())
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}

/// What the pump does next, decided under the queue lock and acted on outside it.
enum Step {
    Deliver(Pending),
    WaitUntil(Instant),
    Idle,
}

/// Deliver queued datagrams in due order, one node's worth.
async fn pump<T: Transport>(inner: Arc<T>, in_flight: Arc<InFlight>) {
    loop {
        let step = {
            let mut queue = in_flight.queue.lock();
            match queue.peek().map(|Reverse(head)| head.due) {
                Some(due) if due <= Instant::now() => {
                    Step::Deliver(queue.pop().expect("just peeked").0)
                }
                Some(due) => Step::WaitUntil(due),
                None => Step::Idle,
            }
        };
        match step {
            // A send error is what a real one is here: the datagram is gone, and the protocol is
            // required to tolerate that (`Replica::run` logs and counts, never fails).
            Step::Deliver(datagram) => {
                let _ = inner.send_to(&datagram.bytes, &datagram.destination).await;
            }
            Step::Idle => in_flight.wake.notified().await,
            Step::WaitUntil(due) => wait_until(due, &in_flight.wake).await,
        }
    }
}

/// Wait for `due`, or for a nearer datagram to arrive.
///
/// Sleeps to within [`TIMER_RESOLUTION`] and yields out the rest: tokio rounds a sleep up to the
/// next millisecond, which is five times the entire one-way delay of the 0.1 ms lane.
async fn wait_until(due: Instant, wake: &Notify) {
    if let Some(coarse) = due.checked_sub(TIMER_RESOLUTION) {
        if coarse > Instant::now() {
            tokio::select! {
                _ = tokio::time::sleep_until(coarse) => {}
                // Something nearer-due may have been queued behind us; re-decide.
                _ = wake.notified() => return,
            }
        }
    }
    while Instant::now() < due {
        tokio::task::yield_now().await;
    }
}
