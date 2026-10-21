// Copyright 2026 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The [`Transport`] decorator itself and its per-node delivery pump. Parameters
//! (`Link`/`IpLinkPolicy`/`Seed`) live in the parent module.

use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::hash::Hash;
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

use super::{IpLinkPolicy, LinkPolicy};
use crate::Transport;

/// Tokio's timer resolution: [`tokio::time::sleep`] rounds up to the next millisecond. The pump
/// therefore sleeps to within this much of a delivery and yields out the remainder — without it the
/// 0.1 ms lane would measure the timer wheel rather than the link.
const TIMER_RESOLUTION: Duration = Duration::from_millis(1);

/// A datagram in flight, ordered by when it is due and, for a tie, by send order.
#[derive(Debug)]
struct Pending<A> {
    due: Instant,
    seq: u64,
    destination: A,
    bytes: Vec<u8>,
}

impl<A> Ord for Pending<A> {
    fn cmp(&self, other: &Pending<A>) -> Ordering {
        self.due.cmp(&other.due).then(self.seq.cmp(&other.seq))
    }
}

impl<A> PartialOrd for Pending<A> {
    fn partial_cmp(&self, other: &Pending<A>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<A> PartialEq for Pending<A> {
    fn eq(&self, other: &Pending<A>) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<A> Eq for Pending<A> {}

/// The in-flight queue, shared between [`EmulatedTransport::send_to`] and the pump task.
struct InFlight<A> {
    /// `Reverse`, so the [`BinaryHeap`] pops the *earliest* due datagram.
    queue: Mutex<BinaryHeap<Reverse<Pending<A>>>>,
    wake: Notify,
}

impl<A> Default for InFlight<A> {
    fn default() -> Self {
        Self {
            queue: Mutex::new(BinaryHeap::new()),
            wake: Notify::new(),
        }
    }
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
/// the transport drops. `IpLinkPolicy` preserves IP policy and seeded streams by default. Custom
/// [`LinkPolicy`] implementations select link conditions and reproducible seeds for opaque
/// endpoints. Pending messages use unbounded queues, as does the in-memory transport.
pub struct EmulatedTransport<T, A = SocketAddr, P = IpLinkPolicy> {
    inner: Arc<T>,
    policy: P,
    local: A,
    /// One PRNG per directed link, created on first use and advanced in send order.
    streams: Mutex<HashMap<A, StdRng>>,
    in_flight: Arc<InFlight<A>>,
    seq: AtomicU64,
    impairments: Impairments,
    pump: JoinHandle<()>,
}

impl<T, A, P> EmulatedTransport<T, A, P>
where
    T: Transport<A>,
    A: Clone + Eq + Hash + Send + Sync + 'static,
    P: LinkPolicy<A>,
{
    /// Wrap `inner` with network conditions selected by `policy`.
    ///
    /// # Panics
    ///
    /// If called outside a Tokio runtime, or if `inner` has no local address (neither is reachable
    /// from the benchmarks, which bind an `InMemoryTransport` first).
    pub fn new(inner: Arc<T>, policy: P) -> Self {
        let local = inner
            .local_addr()
            .expect("an emulated transport must already be bound");
        let in_flight = Arc::new(InFlight::default());
        let pump = tokio::spawn(pump(Arc::clone(&inner), Arc::clone(&in_flight)));
        EmulatedTransport {
            inner,
            policy,
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

impl<T, A, P> Drop for EmulatedTransport<T, A, P> {
    fn drop(&mut self) {
        // The pump owns an `Arc` of the wrapped transport and outlives nothing: a benchmark that
        // rebuilds its cluster once per iteration would otherwise accumulate one live task per
        // node per sample.
        self.pump.abort();
    }
}

#[async_trait::async_trait]
impl<T, A, P> Transport<A> for EmulatedTransport<T, A, P>
where
    T: Transport<A>,
    A: Clone + Eq + Hash + Send + Sync + 'static,
    P: LinkPolicy<A>,
{
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, A)> {
        // Reception is untouched: impairment is applied once per datagram, on the sending side,
        // where the link's direction is known from the destination.
        self.inner.recv_from(buf).await
    }

    async fn send_to(&self, buf: &[u8], destination: &A) -> io::Result<usize> {
        let (link, seed) = self.policy.link(&self.local, destination);
        let delay = {
            let mut streams = self.streams.lock();
            let stream = streams
                .entry(destination.clone())
                .or_insert_with(|| StdRng::seed_from_u64(seed.get()));
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
            destination: destination.clone(),
            bytes: buf.to_vec(),
        }));
        self.in_flight.wake.notify_one();
        Ok(buf.len())
    }

    fn local_addr(&self) -> io::Result<A> {
        self.inner.local_addr()
    }
}

/// What the pump does next, decided under the queue lock and acted on outside it.
enum Step<A> {
    Deliver(Pending<A>),
    WaitUntil(Instant),
    Idle,
}

/// Deliver queued datagrams in due order, one node's worth.
async fn pump<T, A>(inner: Arc<T>, in_flight: Arc<InFlight<A>>)
where
    T: Transport<A>,
    A: Send + Sync + 'static,
{
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
        if coarse_wait_worthwhile(coarse, Instant::now()) {
            tokio::select! {
                _ = tokio::time::sleep_until(coarse) => {}
                // Something nearer-due may have been queued behind us; re-decide.
                _ = wake.notified() => return,
            }
        }
    }
    while still_waiting(Instant::now(), due) {
        tokio::task::yield_now().await;
    }
}

/// Whether a coarse sleep to `coarse` is still worth taking from `now`. At `coarse == now` it is
/// not: the immediate fall-through to the spin loop above reaches the same wait with one less
/// `select!`/timer round-trip.
fn coarse_wait_worthwhile(coarse: Instant, now: Instant) -> bool {
    coarse > now
}

/// Whether the spin loop above should keep yielding: `now` strictly before `due`, so it stops as
/// soon as `due` is reached rather than looping one extra time at the exact tie.
fn still_waiting(now: Instant, due: Instant) -> bool {
    now < due
}

#[cfg(test)]
mod tests;
