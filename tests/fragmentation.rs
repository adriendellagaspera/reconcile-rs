// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! End-to-end application-framing regressions over the deterministic Netem transport.

use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use gossip::netem::{Link, Netem, NetemTransport, Probability, Rtt, Seed};
use reconcile::{
    replicated_map::{Config, DEFAULT_DATAGRAM_PAYLOAD_BUDGET},
    InMemoryNetwork, InMemoryTransport, ReadReplicaMap, ReplicatedMap, Transport,
};
use tokio_util::sync::CancellationToken;

const PORT: u16 = 9_263;
const PATIENCE: Duration = Duration::from_secs(15);

struct RecordingTransport<T> {
    inner: T,
    max_datagram: Arc<AtomicUsize>,
}

#[async_trait]
impl<T: Transport> Transport for RecordingTransport<T> {
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.inner.recv_from(buf).await
    }

    async fn send_to(&self, buf: &[u8], dst: &SocketAddr) -> io::Result<usize> {
        self.max_datagram.fetch_max(buf.len(), Ordering::Relaxed);
        self.inner.send_to(buf, dst).await
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}

fn config(ip: IpAddr) -> Config {
    Config::new(PORT)
        .with_listen_addr(ip)
        .with_reconcile_interval(Duration::from_millis(20))
        .with_repair_interval(Duration::from_millis(50))
        .with_insecure_no_key()
}

fn endpoint(
    network: &InMemoryNetwork,
    addr: SocketAddr,
    link: Link,
    seed: u64,
) -> (
    Arc<RecordingTransport<NetemTransport<InMemoryTransport>>>,
    Arc<AtomicUsize>,
    gossip::netem::Impairments,
) {
    let netem = NetemTransport::new(
        Arc::new(network.bind(addr)),
        Netem::uniform(link, Seed::new(seed)),
    );
    let impairments = netem.impairments();
    let max_datagram = Arc::new(AtomicUsize::new(0));
    (
        Arc::new(RecordingTransport {
            inner: netem,
            max_datagram: Arc::clone(&max_datagram),
        }),
        max_datagram,
        impairments,
    )
}

async fn wait_for_fingerprint(store: &ReplicatedMap<u64, Vec<u8>>, target: rsos::Fingerprint) {
    tokio::time::timeout(PATIENCE, async {
        while store.fingerprint(..) != target {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("fragmented transfer must converge");
}

#[tokio::test(flavor = "multi_thread")]
async fn large_value_converges_under_fragment_loss_without_oversized_datagrams() {
    let network = InMemoryNetwork::new();
    let a_addr: SocketAddr = "127.26.0.1:9263".parse().unwrap();
    let b_addr: SocketAddr = "127.26.0.2:9263".parse().unwrap();
    let lossy = Link::at(Rtt::from_millis(2.0)).with_loss(Probability::percent(10.0));
    let (a_transport, a_max, a_impairments) = endpoint(&network, a_addr, lossy, 0x2631);
    let (b_transport, b_max, b_impairments) = endpoint(&network, b_addr, lossy, 0x2632);

    let a = ReplicatedMap::<u64, Vec<u8>>::new_with_transport(config(a_addr.ip()), a_transport)
        .expect("source config");
    let b = ReplicatedMap::<u64, Vec<u8>>::new_with_transport(config(b_addr.ip()), b_transport)
        .expect("destination config");
    a.load_bulk(&[(7, vec![0x5a; 128 * 1024])]);
    let target = a.fingerprint(..);
    b.seed_peer(a_addr.ip());

    let ta = tokio::spawn(a.clone().run(CancellationToken::new()));
    let tb = tokio::spawn(b.clone().run(CancellationToken::new()));
    wait_for_fingerprint(&b, target).await;

    assert!(
        a_impairments.dropped() + b_impairments.dropped() > 0,
        "the configured lossy link must actually drop a fragment/control datagram"
    );
    assert!(
        a_max.load(Ordering::Relaxed) <= DEFAULT_DATAGRAM_PAYLOAD_BUDGET
            && b_max.load(Ordering::Relaxed) <= DEFAULT_DATAGRAM_PAYLOAD_BUDGET,
        "all emitted datagrams must stay within the configured MTU-safe budget"
    );
    ta.abort();
    tb.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn large_value_converges_when_fragments_are_reordered() {
    let network = InMemoryNetwork::new();
    let a_addr: SocketAddr = "127.26.1.1:9263".parse().unwrap();
    let b_addr: SocketAddr = "127.26.1.2:9263".parse().unwrap();
    let reordered = Link::at(Rtt::from_millis(4.0)).with_reorder(Probability::percent(50.0));
    let (a_transport, a_max, _) = endpoint(&network, a_addr, reordered, 0x2633);
    let (b_transport, b_max, _) = endpoint(&network, b_addr, reordered, 0x2634);

    let a = ReplicatedMap::<u64, Vec<u8>>::new_with_transport(config(a_addr.ip()), a_transport)
        .expect("source config");
    let b = ReplicatedMap::<u64, Vec<u8>>::new_with_transport(config(b_addr.ip()), b_transport)
        .expect("destination config");
    a.load_bulk(&[(9, vec![0xa5; 128 * 1024])]);
    let target = a.fingerprint(..);
    b.seed_peer(a_addr.ip());

    let ta = tokio::spawn(a.clone().run(CancellationToken::new()));
    let tb = tokio::spawn(b.clone().run(CancellationToken::new()));
    wait_for_fingerprint(&b, target).await;

    assert!(
        a_max.load(Ordering::Relaxed) <= DEFAULT_DATAGRAM_PAYLOAD_BUDGET
            && b_max.load(Ordering::Relaxed) <= DEFAULT_DATAGRAM_PAYLOAD_BUDGET
    );
    ta.abort();
    tb.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn multiple_fragmented_transfers_converge_under_reordering() {
    let network = InMemoryNetwork::new();
    let a_addr: SocketAddr = "127.26.3.1:9263".parse().unwrap();
    let b_addr: SocketAddr = "127.26.3.2:9263".parse().unwrap();
    let reordered = Link::at(Rtt::from_millis(6.0))
        .with_jitter(Duration::from_millis(3))
        .with_reorder(Probability::percent(50.0));
    let (a_transport, a_max, _) = endpoint(&network, a_addr, reordered, 0x2637);
    let (b_transport, b_max, _) = endpoint(&network, b_addr, reordered, 0x2638);

    let a = ReplicatedMap::<u64, Vec<u8>>::new_with_transport(config(a_addr.ip()), a_transport)
        .expect("source config");
    let b = ReplicatedMap::<u64, Vec<u8>>::new_with_transport(config(b_addr.ip()), b_transport)
        .expect("destination config");
    a.load_bulk(&[(10, vec![0x10; 96 * 1024]), (11, vec![0x11; 96 * 1024])]);
    let target = a.fingerprint(..);
    b.seed_peer(a_addr.ip());

    let ta = tokio::spawn(a.clone().run(CancellationToken::new()));
    let tb = tokio::spawn(b.clone().run(CancellationToken::new()));
    wait_for_fingerprint(&b, target).await;

    assert_eq!(b.get(&10).map(|value| value.len()), Some(96 * 1024));
    assert_eq!(b.get(&11).map(|value| value.len()), Some(96 * 1024));
    assert!(
        a_max.load(Ordering::Relaxed) <= DEFAULT_DATAGRAM_PAYLOAD_BUDGET
            && b_max.load(Ordering::Relaxed) <= DEFAULT_DATAGRAM_PAYLOAD_BUDGET
    );

    ta.abort();
    tb.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn read_replica_handles_mixed_small_and_fragmented_value_only_traffic() {
    let network = InMemoryNetwork::new();
    let source_addr: SocketAddr = "127.26.2.1:9263".parse().unwrap();
    let read_addr: SocketAddr = "127.26.2.2:9263".parse().unwrap();
    let link = Link::at(Rtt::from_millis(2.0)).with_reorder(Probability::percent(25.0));
    let (source_transport, source_max, _) = endpoint(&network, source_addr, link, 0x2635);
    let (read_transport, read_max, _) = endpoint(&network, read_addr, link, 0x2636);

    let source = ReplicatedMap::<u64, Vec<u8>>::new_with_transport(
        config(source_addr.ip()),
        source_transport,
    )
    .expect("source config");
    source.load_bulk(&[
        (1, b"small".to_vec()),
        (2, vec![0x42; 96 * 1024]),
        (3, b"tail".to_vec()),
    ]);
    let target = source.value_fingerprint(..);

    let read =
        ReadReplicaMap::<u64, Vec<u8>>::new_with_transport(config(read_addr.ip()), read_transport)
            .expect("read-replica config")
            .with_seed(source_addr.ip());

    let source_task = tokio::spawn(source.clone().run(CancellationToken::new()));
    let read_task = tokio::spawn(read.clone().run());
    tokio::time::timeout(PATIENCE, async {
        while read.value_fingerprint(..) != target {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("value-only fragmented transfer must converge");

    assert_eq!(read.get_cloned(&1), Some(b"small".to_vec()));
    assert_eq!(read.get_cloned(&3), Some(b"tail".to_vec()));
    assert_eq!(
        read.get_cloned(&2).map(|value| value.len()),
        Some(96 * 1024)
    );
    assert!(
        source_max.load(Ordering::Relaxed) <= DEFAULT_DATAGRAM_PAYLOAD_BUDGET
            && read_max.load(Ordering::Relaxed) <= DEFAULT_DATAGRAM_PAYLOAD_BUDGET
    );

    source_task.abort();
    read_task.abort();
}
