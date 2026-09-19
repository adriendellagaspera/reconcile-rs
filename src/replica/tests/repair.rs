// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! #23: a comparison round dropped in flight must be repaired on `repair_interval` -- an
//! RTT-scale timer -- not left to `reconcile_interval`'s background sweep to rediscover.

use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::clock::{ManualClock, NodeId};
use crate::entry::Entry;
use crate::replica::Replica;
use crate::replicated_map::Config;
use crate::transport::{InMemoryNetwork, Transport};

/// Drops exactly the first `remaining` outbound datagrams *addressed to `target`*, then passes
/// every later one (to `target` or anyone else) through unmodified -- a deterministic stand-in
/// for "the network lost this node's first attempt to reach this one peer".
/// `.claude/rules/tests.md` requires determinism, which rules out a probabilistic `netem` lane
/// for pinning down "exactly one datagram, gone" -- and matching on `target` specifically (rather
/// than "whichever send happens first") is what makes this immune to `start_reconciliation`'s
/// `targets: HashSet<IpAddr>` iterating in an unspecified order: a discovery probe's send to some
/// other, unrelated address must never be the one consumed instead of the real peer's.
struct DropTo<T> {
    inner: T,
    target: IpAddr,
    remaining: AtomicUsize,
}

#[async_trait::async_trait]
impl<T: Transport> Transport for DropTo<T> {
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.inner.recv_from(buf).await
    }

    async fn send_to(&self, buf: &[u8], dst: &SocketAddr) -> io::Result<usize> {
        if dst.ip() == self.target {
            let dropped = self
                .remaining
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
                .is_ok();
            if dropped {
                // Pretend it was sent: the caller sees an ordinary successful send, exactly like
                // a real UDP datagram that leaves the host and is dropped in flight downstream.
                return Ok(buf.len());
            }
        }
        self.inner.send_to(buf, dst).await
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}

fn value_of(eng: &Replica<u32, u32>, key: u32) -> Option<u32> {
    eng.map
        .load_full()
        .get(&key)
        .and_then(|e| e.value().copied())
}

/// The core #23 regression. `reconcile_interval` is starved to an hour, mirroring
/// `pending_dump_requeue.rs`'s isolating idiom for #516: convergence within this test's real
/// deadline can only come from the repair timer, never from the idle-timeout fallback.
///
/// `a`'s only outbound datagram to `b` before this test seeds any data is `run()`'s own startup
/// `start_reconciliation` round -- `DropTo` drops exactly that one, regardless of whether `a`
/// also probes some other address first (`start_reconciliation`'s `targets` is a `HashSet`, so
/// iteration order is unspecified). `b` is never told about `a` (no seeded peer, and default-net
/// `RandomProbe` finding this exact loopback address by chance is negligible), so `b` cannot
/// independently discover `a` and mask the drop with its own unrelated traffic: only `a`'s own
/// repaired retry can converge this.
#[tokio::test]
async fn a_lost_round_initiation_is_repaired_within_repair_interval() {
    let net = InMemoryNetwork::new();
    let port = crate::replica::tests::next_ephemeral_test_port();
    let a_ip: IpAddr = "127.0.0.20".parse().unwrap();
    let b_ip: IpAddr = "127.0.0.21".parse().unwrap();
    let cfg = |ip: IpAddr| {
        Config::default()
            .with_listen_addr(ip)
            .with_port(port)
            .with_reconcile_interval(Duration::from_secs(3600))
            .with_repair_interval(Duration::from_millis(30))
            .with_insecure_no_key()
    };
    let a: Replica<u32, u32> = Replica::new_with_transport(
        cfg(a_ip),
        Arc::new(DropTo {
            inner: net.bind(SocketAddr::new(a_ip, port)),
            target: b_ip,
            remaining: AtomicUsize::new(1),
        }),
        Arc::new(ManualClock::new(NodeId::new(1))),
    );
    let b: Replica<u32, u32> = Replica::new_with_transport(
        cfg(b_ip),
        Arc::new(net.bind(SocketAddr::new(b_ip, port))),
        Arc::new(ManualClock::new(NodeId::new(2))),
    );
    a.peers.write().insert(b_ip, Instant::now());

    a.just_insert(1, Entry::present(a.clock_now(), 42));

    let ta = tokio::spawn(a.clone().run());
    let tb = tokio::spawn(b.clone().run());

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut converged = false;
    while Instant::now() < deadline {
        if value_of(&b, 1) == Some(42) {
            converged = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    ta.abort();
    tb.abort();
    assert!(
        converged,
        "a comparison round dropped in flight was not repaired well inside repair_interval -- #23"
    );
}

/// The complementary #23 case: a comparison round that finds nothing to report converges via a
/// real `Message::ConvergenceAck` (`dispatch.rs`), not by riding out a bounded, unacknowledged
/// retry. `repair_interval` is starved to an hour -- long enough that exhausting
/// `MAX_REPAIR_ATTEMPTS` retries alone would take hours, nowhere near this test's real deadline --
/// so `a`'s entry clearing here can only mean a real ack arrived, not a give-up. `b` is never told
/// about `a` (mirrors the sibling test above), so `b`'s own reciprocal `start_reconciliation`
/// traffic cannot independently clear `a`'s entry and mask whether the ack path actually fired.
///
/// The round is triggered by calling `start_reconciliation` directly, before either engine's
/// receive loop is even spawned: that registers the pending repair synchronously, with no race to
/// observe an in-between state (the full round trip is fast enough, being in-memory, that polling
/// for a transient "pending" state after spawning both loops is not reliable -- it can already
/// have cleared by the first poll).
#[tokio::test]
async fn a_converged_round_is_acked_without_riding_out_a_retry() {
    let net = InMemoryNetwork::new();
    let port = crate::replica::tests::next_ephemeral_test_port();
    let a_ip: IpAddr = "127.0.0.24".parse().unwrap();
    let b_ip: IpAddr = "127.0.0.25".parse().unwrap();
    let cfg = |ip: IpAddr| {
        Config::default()
            .with_listen_addr(ip)
            .with_port(port)
            .with_reconcile_interval(Duration::from_secs(3600))
            .with_repair_interval(Duration::from_secs(3600))
            .with_insecure_no_key()
    };
    let a: Replica<u32, u32> = Replica::new_with_transport(
        cfg(a_ip),
        Arc::new(net.bind(SocketAddr::new(a_ip, port))),
        Arc::new(ManualClock::new(NodeId::new(1))),
    );
    let b: Replica<u32, u32> = Replica::new_with_transport(
        cfg(b_ip),
        Arc::new(net.bind(SocketAddr::new(b_ip, port))),
        Arc::new(ManualClock::new(NodeId::new(2))),
    );
    a.peers.write().insert(b_ip, Instant::now());

    let mut send_buf = Vec::new();
    a.start_reconciliation(&mut send_buf).await;
    assert!(
        a.pending_repairs.read().contains_key(&b_ip),
        "a's manually triggered round must have registered a pending repair before this test can \
         check how it clears"
    );

    let ta = tokio::spawn(a.clone().run());
    let tb = tokio::spawn(b.clone().run());

    // Checked for b_ip specifically, not overall emptiness: start_reconciliation's own discovery
    // probing (`self.probe.discover()`) also calls note_pending_repair for probe addresses that
    // are not b_ip and, being probes into an address nobody bound in this test's InMemoryNetwork,
    // never get a reply -- their entries would keep pending_repairs non-empty for the full
    // repair_interval regardless of whether b_ip's own entry cleared correctly.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut cleared = false;
    while Instant::now() < deadline {
        if !a.pending_repairs.read().contains_key(&b_ip) {
            cleared = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    ta.abort();
    tb.abort();
    assert!(
        cleared,
        "a converged comparison round must clear pending_repairs via a real ConvergenceAck, not by \
         waiting on repair_interval's multi-hour retry bound -- #23"
    );
}
