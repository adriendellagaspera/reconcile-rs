// Copyright 2026 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! #85: a peer whose paced bulk transfer to us might still legitimately be in progress must be
//! left out of `start_reconciliation`'s targets -- re-initiating a full comparison mid-transfer
//! only re-diffs and re-sends ranges the peer is already (legitimately) sending, doubling traffic
//! (akvize/reconcile-rs#178) instead of converging any faster.

use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::clock::{ManualClock, NodeId};
use crate::replica::Replica;
use crate::replicated_map::Config;
use crate::transport::{InMemoryNetwork, InMemoryTransport, Transport};

/// Counts outbound datagrams addressed to `target`'s IP, passing every send through to `inner`
/// unmodified -- a deterministic stand-in for "did `start_reconciliation` actually contact this
/// peer", mirroring `repair.rs`'s `DropTo`.
struct CountSendsTo {
    inner: InMemoryTransport,
    target: IpAddr,
    count: AtomicUsize,
}

#[async_trait::async_trait]
impl Transport for CountSendsTo {
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.inner.recv_from(buf).await
    }

    async fn send_to(&self, buf: &[u8], dst: &SocketAddr) -> io::Result<usize> {
        if dst.ip() == self.target {
            self.count.fetch_add(1, Ordering::AcqRel);
        }
        self.inner.send_to(buf, dst).await
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}

/// `a`, wired with a `CountSendsTo` transport so the test can observe whether `peer_ip` was
/// actually contacted, plus `peer_ip` already seeded as a known local peer (default nets fall back
/// to `127.0.0.1/8`, so every `127.0.0.x` address used here is local -- contacted every round,
/// unconditionally, never gated behind the random cross-network fanout).
fn engine_with_counter(
    a_ip: IpAddr,
    peer_ip: IpAddr,
    port: u16,
    net: &InMemoryNetwork,
    repair_interval: Duration,
) -> (Replica<u32, u32>, Arc<CountSendsTo>) {
    let counter = Arc::new(CountSendsTo {
        inner: net.bind(SocketAddr::new(a_ip, port)),
        target: peer_ip,
        count: AtomicUsize::new(0),
    });
    let a: Replica<u32, u32> = Replica::new_with_transport(
        Config::default()
            .with_listen_addr(a_ip)
            .with_port(port)
            .with_repair_interval(repair_interval)
            .with_insecure_no_key(),
        Arc::clone(&counter) as Arc<dyn Transport>,
        Arc::new(ManualClock::new(NodeId::new(1))),
    );
    a.peers.write().insert(peer_ip, Instant::now());
    (a, counter)
}

/// The core #85 regression: a peer whose dated bulk-update batch just landed within
/// `repair_interval` must not be sent a fresh full comparison.
#[tokio::test]
async fn a_peer_with_a_recent_bulk_update_is_excluded_from_the_round() {
    let net = InMemoryNetwork::new();
    let port = crate::replica::tests::next_ephemeral_test_port();
    let a_ip: IpAddr = "127.0.0.40".parse().unwrap();
    let peer_ip: IpAddr = "127.0.0.41".parse().unwrap();
    let (a, counter) = engine_with_counter(a_ip, peer_ip, port, &net, Duration::from_secs(3600));

    a.note_bulk_update_received(peer_ip);
    a.start_reconciliation(&mut Vec::new()).await;

    assert_eq!(
        counter.count.load(Ordering::Acquire),
        0,
        "a peer whose bulk update just landed within repair_interval must be left out of this \
         round's targets -- #85"
    );
}

/// The control case for the regression above: with no recently received bulk update at all, the
/// same known local peer *is* contacted -- proving the exclusion above is actually caused by the
/// guard, not by some unrelated reason `peer_ip` never gets targeted.
#[tokio::test]
async fn a_peer_with_no_recent_bulk_update_is_still_included_in_the_round() {
    let net = InMemoryNetwork::new();
    let port = crate::replica::tests::next_ephemeral_test_port();
    let a_ip: IpAddr = "127.0.0.42".parse().unwrap();
    let peer_ip: IpAddr = "127.0.0.43".parse().unwrap();
    let (a, counter) = engine_with_counter(a_ip, peer_ip, port, &net, Duration::from_secs(3600));

    a.start_reconciliation(&mut Vec::new()).await;

    assert!(
        counter.count.load(Ordering::Acquire) >= 1,
        "a known local peer with no recent bulk-update activity must still be contacted every \
         round"
    );
}

/// The guard's window is bounded, not permanent: once `repair_interval` has actually elapsed with
/// no further bulk-update activity from `peer_ip`, the next round must contact it again --
/// otherwise a genuinely finished transfer would leave the peer stuck out of every future round.
#[tokio::test]
async fn the_exclusion_lapses_once_repair_interval_elapses_with_no_further_update() {
    let net = InMemoryNetwork::new();
    let port = crate::replica::tests::next_ephemeral_test_port();
    let a_ip: IpAddr = "127.0.0.44".parse().unwrap();
    let peer_ip: IpAddr = "127.0.0.45".parse().unwrap();
    let (a, counter) = engine_with_counter(a_ip, peer_ip, port, &net, Duration::from_millis(20));

    a.note_bulk_update_received(peer_ip);
    a.start_reconciliation(&mut Vec::new()).await;
    assert_eq!(
        counter.count.load(Ordering::Acquire),
        0,
        "immediately after the bulk update, the peer must still be excluded"
    );

    tokio::time::sleep(Duration::from_millis(40)).await;
    a.start_reconciliation(&mut Vec::new()).await;
    assert!(
        counter.count.load(Ordering::Acquire) >= 1,
        "once repair_interval has elapsed with no further update, the peer must be contacted \
         again -- the guard must not wedge it out of reconciliation forever"
    );
}
