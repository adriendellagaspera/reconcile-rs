// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// End-to-end ingress amplification probe for #204.
//
// Contrasts rejected authenticated first flights with two accepted mismatch requests against a
// populated authoritative store:
// - invalid MAC: rejected before version/replay/decode, no response;
// - authenticated malformed payload: clears auth/version/replay, fails decode, no response;
// - replay of that authenticated malformed payload: rejected by replay state, no response;
// - empty read replica: valid value-only mismatch that may request the full projection;
// - empty authoritative replica: valid dated mismatch that may request the full dated store.
//
// The last two are not attacks by themselves: they are the expensive valid paths a compromised
// authenticated peer can select. The benchmark reports response/input byte amplification and keeps
// the runtime's existing bulk-send pacing/budgets intact.
//
// Run:
//   cargo bench --bench security_amplification
//
// Override:
//   RECONCILE_SECURITY_DATASET=20000

use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use reconcile::{
    replicated_map::Config, ClusterKey, InMemoryNetwork, InMemoryTransport, NodeId, ReadReplicaMap,
    ReplicatedMap, Transport,
};
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

const PORT: u16 = 9_877;
const CENTRAL_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 82, 0, 1));
const REQUESTER_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 82, 0, 2));
const ATTACKER_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 82, 0, 3));
const LONG_INTERVAL: Duration = Duration::from_secs(60 * 60);
const WAIT_TIMEOUT: Duration = Duration::from_secs(10);
const QUIET_FOR: Duration = Duration::from_millis(50);
const KEY: [u8; 32] = [0x5a; 32];

#[derive(Clone, Default)]
struct Traffic {
    rx_bytes: Arc<AtomicU64>,
    rx_datagrams: Arc<AtomicU64>,
    tx_bytes: Arc<AtomicU64>,
    tx_datagrams: Arc<AtomicU64>,
    tx_by_peer: Arc<Mutex<HashMap<IpAddr, (u64, u64)>>>,
}

impl Traffic {
    fn reset(&self) {
        self.rx_bytes.store(0, Ordering::Relaxed);
        self.rx_datagrams.store(0, Ordering::Relaxed);
        self.tx_bytes.store(0, Ordering::Relaxed);
        self.tx_datagrams.store(0, Ordering::Relaxed);
        self.tx_by_peer
            .lock()
            .expect("traffic mutex poisoned")
            .clear();
    }

    fn record_tx(&self, peer: IpAddr, bytes: usize) {
        self.tx_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        self.tx_datagrams.fetch_add(1, Ordering::Relaxed);
        let mut guard = self.tx_by_peer.lock().expect("traffic mutex poisoned");
        let entry = guard.entry(peer).or_default();
        entry.0 += bytes as u64;
        entry.1 += 1;
    }

    fn to_peer(&self, peer: IpAddr) -> (u64, u64) {
        self.tx_by_peer
            .lock()
            .expect("traffic mutex poisoned")
            .get(&peer)
            .copied()
            .unwrap_or_default()
    }
}

struct CountingTransport {
    inner: InMemoryTransport,
    traffic: Traffic,
}

#[async_trait]
impl Transport for CountingTransport {
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let (size, peer) = self.inner.recv_from(buf).await?;
        self.traffic
            .rx_bytes
            .fetch_add(size as u64, Ordering::Relaxed);
        self.traffic.rx_datagrams.fetch_add(1, Ordering::Relaxed);
        Ok((size, peer))
    }

    async fn send_to(&self, buf: &[u8], dst: &SocketAddr) -> io::Result<usize> {
        let sent = self.inner.send_to(buf, dst).await?;
        self.traffic.record_tx(dst.ip(), sent);
        Ok(sent)
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}

fn cluster_key() -> ClusterKey {
    ClusterKey::new(KEY)
}

fn config(addr: IpAddr, node_id: u64) -> Config {
    Config::new(PORT)
        .with_listen_addr(addr)
        .with_node_id(NodeId::new(node_id))
        .with_max_peers(8)
        .with_max_replay_senders(8)
        .with_reconcile_interval(LONG_INTERVAL)
        .with_repair_interval(LONG_INTERVAL)
        .with_snapshot_interval(None)
        .with_cluster_key(cluster_key())
}

async fn wait_until(mut predicate: impl FnMut() -> bool, what: &str) {
    tokio::time::timeout(WAIT_TIMEOUT, async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

async fn wait_for_quiet(traffic: &Traffic) {
    let mut last = traffic.tx_bytes.load(Ordering::Relaxed);
    let mut stable_since = Instant::now();
    loop {
        tokio::time::sleep(Duration::from_millis(5)).await;
        let now = traffic.tx_bytes.load(Ordering::Relaxed);
        if now == last {
            if stable_since.elapsed() >= QUIET_FOR {
                return;
            }
        } else {
            last = now;
            stable_since = Instant::now();
        }
        assert!(stable_since.elapsed() < WAIT_TIMEOUT);
    }
}

struct Central {
    store: ReplicatedMap<u64, u64>,
    traffic: Traffic,
    shutdown: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

async fn central(network: &InMemoryNetwork, n: usize) -> Central {
    let traffic = Traffic::default();
    let store = ReplicatedMap::<u64, u64>::new_with_transport(
        config(CENTRAL_IP, 1),
        Arc::new(CountingTransport {
            inner: network.bind(SocketAddr::new(CENTRAL_IP, PORT)),
            traffic: traffic.clone(),
        }),
    )
    .expect("valid central store");
    let corpus: Vec<_> = (0..n as u64)
        .map(|key| (key, key.wrapping_mul(2_654_435_761)))
        .collect();
    store.load_bulk(&corpus);

    let shutdown = CancellationToken::new();
    let run_store = store.clone();
    let run_shutdown = shutdown.clone();
    let task = tokio::spawn(async move {
        let _ = run_store.run(run_shutdown).await;
    });
    wait_until(
        || store.sync_state().rounds >= 1,
        "central startup reconciliation round",
    )
    .await;

    Central {
        store,
        traffic,
        shutdown,
        task,
    }
}

async fn stop(central: Central) {
    central.shutdown.cancel();
    central.task.await.expect("central run task");
}

async fn rejected_case(kind: &str, datagrams: &[Vec<u8>], n: usize) {
    let network = InMemoryNetwork::new();
    let central = central(&network, n).await;
    let attacker = network.bind(SocketAddr::new(ATTACKER_IP, PORT));

    central.traffic.reset();
    let started = Instant::now();
    let input_bytes: u64 = datagrams.iter().map(|frame| frame.len() as u64).sum();
    for frame in datagrams {
        attacker
            .send_to(frame, &SocketAddr::new(CENTRAL_IP, PORT))
            .await
            .expect("inject datagram");
    }
    wait_until(
        || central.traffic.rx_datagrams.load(Ordering::Relaxed) >= datagrams.len() as u64,
        "central to receive injected datagrams",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    let (output_bytes, output_datagrams) = central.traffic.to_peer(ATTACKER_IP);
    println!(
        "[security-amplification] kind={kind},dataset={n},input_bytes={input_bytes},input_dgrams={},output_bytes={output_bytes},output_dgrams={output_datagrams},amplification={:.3},elapsed_ms={:.3}",
        datagrams.len(),
        if input_bytes == 0 { 0.0 } else { output_bytes as f64 / input_bytes as f64 },
        started.elapsed().as_secs_f64() * 1_000.0,
    );
    assert_eq!(output_bytes, 0, "rejected/malformed input must not amplify");
    stop(central).await;
}

async fn read_replica_case(n: usize) {
    let network = InMemoryNetwork::new();
    let central = central(&network, n).await;
    let requester_traffic = Traffic::default();
    let requester = ReadReplicaMap::<u64, u64>::new_with_transport(
        config(REQUESTER_IP, 2),
        Arc::new(CountingTransport {
            inner: network.bind(SocketAddr::new(REQUESTER_IP, PORT)),
            traffic: requester_traffic.clone(),
        }),
    )
    .expect("valid read replica");
    requester.seed_peer(CENTRAL_IP);

    central.traffic.reset();
    requester_traffic.reset();
    let started = Instant::now();
    requester.start_reconciliation().await;
    wait_until(
        || central.traffic.rx_datagrams.load(Ordering::Relaxed) >= 1,
        "central to receive read-replica request",
    )
    .await;
    wait_for_quiet(&central.traffic).await;

    let (input_bytes, input_dgrams) = requester_traffic.to_peer(CENTRAL_IP);
    let (output_bytes, output_dgrams) = central.traffic.to_peer(REQUESTER_IP);
    println!(
        "[security-amplification] kind=valid-read-replica,dataset={n},input_bytes={input_bytes},input_dgrams={input_dgrams},output_bytes={output_bytes},output_dgrams={output_dgrams},amplification={:.3},elapsed_ms={:.3}",
        output_bytes as f64 / input_bytes.max(1) as f64,
        started.elapsed().as_secs_f64() * 1_000.0,
    );
    stop(central).await;
}

async fn authoritative_case(n: usize) {
    let network = InMemoryNetwork::new();
    let central = central(&network, n).await;
    let requester_traffic = Traffic::default();
    let requester = ReplicatedMap::<u64, u64>::new_with_transport(
        config(REQUESTER_IP, 2),
        Arc::new(CountingTransport {
            inner: network.bind(SocketAddr::new(REQUESTER_IP, PORT)),
            traffic: requester_traffic.clone(),
        }),
    )
    .expect("valid authoritative requester");
    requester.seed_peer(CENTRAL_IP);

    central.traffic.reset();
    requester_traffic.reset();
    let started = Instant::now();
    requester.start_reconciliation().await;
    wait_until(
        || central.traffic.rx_datagrams.load(Ordering::Relaxed) >= 1,
        "central to receive authoritative request",
    )
    .await;
    wait_for_quiet(&central.traffic).await;

    let (input_bytes, input_dgrams) = requester_traffic.to_peer(CENTRAL_IP);
    let (output_bytes, output_dgrams) = central.traffic.to_peer(REQUESTER_IP);
    println!(
        "[security-amplification] kind=valid-authoritative,dataset={n},input_bytes={input_bytes},input_dgrams={input_dgrams},output_bytes={output_bytes},output_dgrams={output_dgrams},amplification={:.3},elapsed_ms={:.3}",
        output_bytes as f64 / input_bytes.max(1) as f64,
        started.elapsed().as_secs_f64() * 1_000.0,
    );
    stop(central).await;
}

fn main() {
    let n = std::env::var("RECONCILE_SECURITY_DATASET")
        .unwrap_or_else(|_| "20000".to_owned())
        .parse::<usize>()
        .expect("RECONCILE_SECURITY_DATASET must be an integer");
    assert!(n > 0);

    let runtime = Runtime::new().expect("Tokio runtime");
    runtime.block_on(async {
        let wrong = gossip::auth::Authenticator::new(
            Some(gossip::auth::ClusterKey::new([0x99; 32])),
            false,
        )
        .expect("MAC mode")
        .seal(
            gossip::replay::Seq::new(1),
            gossip::replay::Stamp::new(chrono::Utc::now().timestamp_millis().max(0) as u64),
            b"bad-mac",
        );
        rejected_case("bad-mac", &[wrong], n).await;

        let auth = gossip::auth::Authenticator::new(Some(cluster_key()), false).expect("MAC mode");
        let counter = gossip::replay::SenderCounter::new();
        let malformed = auth.seal(counter.next_seq(), counter.next_stamp(), b"not-a-message");
        rejected_case("authenticated-malformed", &[malformed.clone()], n).await;
        rejected_case("replayed-malformed", &[malformed.clone(), malformed], n).await;

        read_replica_case(n).await;
        authoritative_case(n).await;
    });
}
