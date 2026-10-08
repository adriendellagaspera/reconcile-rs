// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. You may not use this file except according to those terms.

//! Runtime Netem controls for selective fragment recovery and mixed-capability fallback.

use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use gossip::netem::{Link, Netem, NetemTransport, Probability, Rtt, Seed};
use reconcile::{
    replicated_map::Config, InMemoryNetwork, InMemoryTransport, ReplicatedMap, Transport,
};
use tokio_util::sync::CancellationToken;

const PORT: u16 = 9_283;

struct Counted<T> {
    inner: T,
    bytes: Arc<AtomicUsize>,
    largest: Arc<AtomicUsize>,
}

#[async_trait]
impl<T: Transport> Transport for Counted<T> {
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.inner.recv_from(buf).await
    }

    async fn send_to(&self, buf: &[u8], dst: &SocketAddr) -> io::Result<usize> {
        let sent = self.inner.send_to(buf, dst).await?;
        self.bytes.fetch_add(sent, Ordering::Relaxed);
        self.largest.fetch_max(sent, Ordering::Relaxed);
        Ok(sent)
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}

fn config(ip: IpAddr, selective: bool) -> Config {
    let mut config = Config::new(PORT)
        .with_listen_addr(ip)
        .with_reconcile_interval(Duration::from_millis(30))
        .with_repair_interval(Duration::from_millis(100))
        .with_insecure_no_key();
    config.framing = config.framing.with_selective_recovery(selective);
    config
}

type CountedEndpoint = Arc<Counted<NetemTransport<InMemoryTransport>>>;
type EndpointStats = (
    CountedEndpoint,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
    gossip::netem::Impairments,
);

fn endpoint(network: &InMemoryNetwork, addr: SocketAddr, link: Link, seed: u64) -> EndpointStats {
    let netem = NetemTransport::new(
        Arc::new(network.bind(addr)),
        Netem::uniform(link, Seed::new(seed)),
    );
    let impairments = netem.impairments();
    let bytes = Arc::new(AtomicUsize::new(0));
    let largest = Arc::new(AtomicUsize::new(0));
    (
        Arc::new(Counted {
            inner: netem,
            bytes: Arc::clone(&bytes),
            largest: Arc::clone(&largest),
        }),
        bytes,
        largest,
        impairments,
    )
}

async fn exercise(
    selective_source: bool,
    selective_destination: bool,
    rtt_ms: f64,
    loss: f64,
) -> usize {
    let network = InMemoryNetwork::new();
    let source_addr: SocketAddr = "127.28.3.1:9283".parse().unwrap();
    let destination_addr: SocketAddr = "127.28.3.2:9283".parse().unwrap();
    let reorder = if loss == 0.0 { 0.0 } else { 5.0 };
    let link = Link::at(Rtt::from_millis(rtt_ms))
        .with_loss(Probability::percent(loss))
        .with_reorder(Probability::percent(reorder));
    let (source_transport, source_bytes, source_max, source_impairments) =
        endpoint(&network, source_addr, link, 0x28301);
    let (destination_transport, destination_bytes, destination_max, destination_impairments) =
        endpoint(&network, destination_addr, link, 0x28302);
    let source = ReplicatedMap::<u64, Vec<u8>>::new_with_transport(
        config(source_addr.ip(), selective_source),
        source_transport,
    )
    .unwrap();
    let destination = ReplicatedMap::<u64, Vec<u8>>::new_with_transport(
        config(destination_addr.ip(), selective_destination),
        destination_transport,
    )
    .unwrap();
    destination.seed_peer(source_addr.ip());

    let source_task = tokio::spawn(source.clone().run(CancellationToken::new()));
    let destination_task = tokio::spawn(destination.clone().run(CancellationToken::new()));
    // The selective candidate requires a capability exchange before outbound payload
    // retention is possible. Compare like-for-like post-negotiation bulk transfers rather
    // than racing the capability handshake against the first bulk message.
    tokio::time::sleep(Duration::from_millis((rtt_ms * 6.0).max(500.0) as u64)).await;
    source_bytes.store(0, Ordering::Relaxed);
    destination_bytes.store(0, Ordering::Relaxed);
    source.load_bulk(&[(42, vec![0x5a; 128 * 1024])]);
    let target = source.fingerprint(..);
    tokio::time::timeout(Duration::from_secs(45), async {
        while destination.fingerprint(..) != target {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("Netem fragmented transfer must converge");

    let bytes = source_bytes.load(Ordering::Relaxed) + destination_bytes.load(Ordering::Relaxed);
    assert!(source_max.load(Ordering::Relaxed) <= 1200);
    assert!(destination_max.load(Ordering::Relaxed) <= 1200);
    if loss > 0.0 {
        assert!(source_impairments.dropped() + destination_impairments.dropped() > 0);
    }
    source_task.abort();
    destination_task.abort();
    bytes
}

#[tokio::test(flavor = "multi_thread")]
async fn selective_recovery_converges_under_loss_reorder_and_high_rtt() {
    let clean = exercise(true, true, 50.0, 0.0).await;
    let wan = exercise(true, true, 150.0, 5.0).await;
    let high_rtt = exercise(true, true, 600.0, 5.0).await;
    assert!(clean > 0 && wan > 0 && high_rtt > 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn disabled_peer_retains_legacy_recovery_under_loss() {
    let bytes = exercise(true, false, 150.0, 5.0).await;
    assert!(bytes > 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn selective_recovery_has_negligible_clean_path_wire_overhead() {
    let selective = exercise(true, true, 50.0, 0.0).await;
    let legacy = exercise(false, false, 50.0, 0.0).await;
    assert!(
        selective <= legacy + legacy / 20,
        "clean link selective={selective} legacy={legacy} exceeds 5% wire overhead"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn selective_recovery_reduces_wire_under_matched_netem_loss() {
    let selective = exercise(true, true, 150.0, 5.0).await;
    let legacy = exercise(false, false, 150.0, 5.0).await;
    assert!(
        selective < legacy,
        "selective recovery must reduce modeled-wire submissions on this matched loss profile: selective={selective}, legacy={legacy}"
    );
}

/// Cut both directions after the sender has offered ~80% of a 128 KiB transfer.
struct ContactGate {
    armed: std::sync::atomic::AtomicBool,
    paused: std::sync::atomic::AtomicBool,
    forwarded_data_bytes: AtomicUsize,
}

impl ContactGate {
    fn new() -> Self {
        Self {
            armed: std::sync::atomic::AtomicBool::new(false),
            paused: std::sync::atomic::AtomicBool::new(false),
            forwarded_data_bytes: AtomicUsize::new(0),
        }
    }
}

struct Partitioned<T> {
    inner: Arc<T>,
    gate: Arc<ContactGate>,
    source: bool,
}

#[async_trait]
impl<T: Transport> Transport for Partitioned<T> {
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.inner.recv_from(buf).await
    }

    async fn send_to(&self, buf: &[u8], dst: &SocketAddr) -> io::Result<usize> {
        if self.gate.paused.load(Ordering::Acquire) {
            return Ok(buf.len());
        }
        if self.source && self.gate.armed.load(Ordering::Acquire) && buf.len() > 500 {
            let previous = self
                .gate
                .forwarded_data_bytes
                .fetch_add(buf.len(), Ordering::AcqRel);
            if previous >= 100_000 {
                self.gate.paused.store(true, Ordering::Release);
                return Ok(buf.len());
            }
        }
        self.inner.send_to(buf, dst).await
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}

/// The long-contact-gap experiment runs separately from every ordinary CI lane because it
/// deliberately advances 30 seconds of real wall clock per candidate.
async fn interrupted_transfer(selective: bool) -> usize {
    let network = InMemoryNetwork::new();
    let a_addr: SocketAddr = "127.28.9.1:9283".parse().unwrap();
    let b_addr: SocketAddr = "127.28.9.2:9283".parse().unwrap();
    let link = Link::at(Rtt::from_millis(150.0)).with_loss(Probability::percent(5.0));
    let (a_transport, a_bytes, _, _) = endpoint(&network, a_addr, link, 0x28391);
    let (b_transport, b_bytes, _, _) = endpoint(&network, b_addr, link, 0x28392);
    let gate = Arc::new(ContactGate::new());
    let a = ReplicatedMap::<u64, Vec<u8>>::new_with_transport(
        config(a_addr.ip(), selective),
        Arc::new(Partitioned {
            inner: a_transport,
            gate: Arc::clone(&gate),
            source: true,
        }),
    )
    .unwrap();
    let b = ReplicatedMap::<u64, Vec<u8>>::new_with_transport(
        config(b_addr.ip(), selective),
        Arc::new(Partitioned {
            inner: b_transport,
            gate: Arc::clone(&gate),
            source: false,
        }),
    )
    .unwrap();
    b.seed_peer(a_addr.ip());
    let ta = tokio::spawn(a.clone().run(CancellationToken::new()));
    let tb = tokio::spawn(b.clone().run(CancellationToken::new()));
    // Exchange capability before the first large payload, then arm the automatic partition.
    tokio::time::sleep(Duration::from_secs(2)).await;
    gate.armed.store(true, Ordering::Release);
    a.load_bulk(&[(1, vec![0x51; 128 * 1024])]);
    let target = a.fingerprint(..);
    tokio::time::timeout(Duration::from_secs(10), async {
        while !gate.paused.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the first transfer must reach the 80% contact interruption");
    assert_ne!(
        b.fingerprint(..),
        target,
        "partial transfer must remain incomplete"
    );
    // Allow previously queued datagrams to reach their destination; the link remains partitioned.
    tokio::time::sleep(Duration::from_secs(30)).await;
    assert_ne!(b.fingerprint(..), target);
    let pre_resume = a_bytes.load(Ordering::Relaxed) + b_bytes.load(Ordering::Relaxed);
    gate.armed.store(false, Ordering::Release);
    gate.paused.store(false, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(30), async {
        while b.fingerprint(..) != target {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("interrupted transfer must converge after reconnection");
    let post_resume =
        a_bytes.load(Ordering::Relaxed) + b_bytes.load(Ordering::Relaxed) - pre_resume;
    ta.abort();
    tb.abort();
    post_resume
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "explicit 30-second partition benchmark; run as a dedicated release acceptance gate"]
async fn selective_recovery_reduces_post_resume_wire_after_thirty_second_partition() {
    let selective = interrupted_transfer(true).await;
    let legacy = interrupted_transfer(false).await;
    eprintln!("post-resume wire: selective={selective} legacy={legacy}");
    assert!(
        selective < legacy,
        "the 30-second retained-progress path must reduce post-resume wire bytes"
    );
}
