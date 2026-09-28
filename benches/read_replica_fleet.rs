// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// Product/runtime probe for read-replica fleet shape.
//
// Measures shipped ReadReplicaMap behavior only:
// - aggregate heap and round egress for many read replicas tracking a small authoritative set;
// - authoritative peers/members after value-only read-replica traffic;
// - bounded authenticated per-sender ingress state by contrasting keyed vs insecure controls.
//
// Run:
//   cargo bench --bench read_replica_fleet
//
// Overrides:
//   RECONCILE_READ_REPLICA_COUNTS=1,10,100,1000
//   RECONCILE_READ_AUTHORITATIVE_PEERS=3
//   RECONCILE_READ_MODEL_TARGET=100000

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::{HashMap, HashSet};
use std::future::pending;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use reconcile::{
    replicated_map::Config, ClusterKey, InMemoryNetwork, InMemoryTransport, NodeId, ReadReplicaMap,
    ReplicatedMap, Transport,
};
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

const PORT: u16 = 9_874;
const CENTRAL_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 254, 0, 1));
const LONG_INTERVAL: Duration = Duration::from_secs(60 * 60);
const WAIT_TIMEOUT: Duration = Duration::from_secs(20);
const SETTLE: Duration = Duration::from_millis(25);
const MAX_PEERS_CONTROL: usize = 8;
const MAX_REPLAY_SENDERS_CONTROL: usize = 8;

struct CountingAllocator;
static LIVE_BYTES: AtomicI64 = AtomicI64::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            LIVE_BYTES.fetch_add(layout.size() as i64, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        LIVE_BYTES.fetch_sub(layout.size() as i64, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = System.realloc(ptr, layout, new_size);
        if !new_ptr.is_null() {
            LIVE_BYTES.fetch_add(new_size as i64 - layout.size() as i64, Ordering::Relaxed);
        }
        new_ptr
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn live_bytes() -> i64 {
    LIVE_BYTES.load(Ordering::Relaxed)
}

#[derive(Clone, Default)]
struct Traffic {
    tx_bytes: Arc<AtomicU64>,
    tx_datagrams: Arc<AtomicU64>,
    rx_bytes: Arc<AtomicU64>,
    rx_datagrams: Arc<AtomicU64>,
    tx_by_peer: Arc<Mutex<HashMap<IpAddr, (u64, u64)>>>,
}

impl Traffic {
    fn reset(&self) {
        self.tx_bytes.store(0, Ordering::Relaxed);
        self.tx_datagrams.store(0, Ordering::Relaxed);
        self.rx_bytes.store(0, Ordering::Relaxed);
        self.rx_datagrams.store(0, Ordering::Relaxed);
        self.tx_by_peer
            .lock()
            .expect("traffic mutex poisoned")
            .clear();
    }

    fn record_tx(&self, dst: IpAddr, bytes: usize) {
        self.tx_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        self.tx_datagrams.fetch_add(1, Ordering::Relaxed);
        let mut guard = self.tx_by_peer.lock().expect("traffic mutex poisoned");
        let entry = guard.entry(dst).or_default();
        entry.0 += bytes as u64;
        entry.1 += 1;
    }

    fn classify_tx(&self, known: &HashSet<IpAddr>) -> (u64, u64, u64, u64) {
        self.tx_by_peer
            .lock()
            .expect("traffic mutex poisoned")
            .iter()
            .fold(
                (0, 0, 0, 0),
                |(known_bytes, known_dgrams, probe_bytes, probe_dgrams),
                 (peer, (bytes, dgrams))| {
                    if known.contains(peer) {
                        (
                            known_bytes + bytes,
                            known_dgrams + dgrams,
                            probe_bytes,
                            probe_dgrams,
                        )
                    } else {
                        (
                            known_bytes,
                            known_dgrams,
                            probe_bytes + bytes,
                            probe_dgrams + dgrams,
                        )
                    }
                },
            )
    }
}

struct NullCountingTransport {
    local: SocketAddr,
    traffic: Traffic,
}

#[async_trait]
impl Transport for NullCountingTransport {
    async fn recv_from(&self, _buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        pending().await
    }

    async fn send_to(&self, buf: &[u8], dst: &SocketAddr) -> io::Result<usize> {
        self.traffic.record_tx(dst.ip(), buf.len());
        Ok(buf.len())
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local)
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

#[derive(Clone, Copy)]
struct FleetPoint {
    replicas: usize,
    heap_bytes: i64,
    known_tx_bytes: u64,
    known_tx_datagrams: u64,
    probe_tx_bytes: u64,
    probe_tx_datagrams: u64,
}

#[derive(Clone, Copy)]
struct IngressPoint {
    replicas: usize,
    insecure_heap_delta: i64,
    authenticated_heap_delta: i64,
}

fn cluster_key() -> ClusterKey {
    ClusterKey::new([0x5a; 32])
}

fn env_list(name: &str, default: &str) -> Vec<usize> {
    let mut values: Vec<_> = std::env::var(name)
        .unwrap_or_else(|_| default.to_owned())
        .split(',')
        .map(|raw| {
            raw.trim()
                .parse::<usize>()
                .unwrap_or_else(|_| panic!("{name}: {raw:?}"))
        })
        .collect();
    values.sort_unstable();
    values.dedup();
    assert!(!values.is_empty());
    values
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |raw| {
        raw.parse::<usize>()
            .unwrap_or_else(|_| panic!("{name} must be an integer"))
    })
}

fn read_ip(index: usize) -> IpAddr {
    assert!(index < 0x00bf_fffe);
    IpAddr::V4(Ipv4Addr::from(0x7f40_0001u32 + index as u32))
}

fn authoritative_ip(index: usize) -> IpAddr {
    assert!(index < 240);
    IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10 + index as u8))
}

fn read_config(addr: IpAddr, max_peers: usize, authenticated: bool) -> Config {
    let config = Config::new(PORT)
        .with_listen_addr(addr)
        .with_net("10.0.0.0/8".parse().expect("valid probe net"))
        .expect("one network fits")
        .with_max_peers(max_peers)
        .with_reconcile_interval(LONG_INTERVAL)
        .with_snapshot_interval(None);
    if authenticated {
        config.with_cluster_key(cluster_key())
    } else {
        config.with_insecure_no_key()
    }
}

fn authoritative_config(authenticated: bool) -> Config {
    let config = Config::new(PORT)
        .with_listen_addr(CENTRAL_IP)
        .with_net("127.0.0.0/8".parse().expect("valid local net"))
        .expect("one network fits")
        .with_node_id(NodeId::new(1))
        .with_max_peers(MAX_PEERS_CONTROL)
        .with_max_replay_senders(MAX_REPLAY_SENDERS_CONTROL)
        .with_reconcile_interval(LONG_INTERVAL)
        .with_repair_interval(LONG_INTERVAL)
        .with_snapshot_interval(None);
    if authenticated {
        config.with_cluster_key(cluster_key())
    } else {
        config.with_insecure_no_key()
    }
}

fn fleet_point(runtime: &Runtime, replicas: usize, authoritative_peers: usize) -> FleetPoint {
    let traffic = Traffic::default();
    let known: HashSet<_> = (0..authoritative_peers).map(authoritative_ip).collect();

    let before = live_bytes();
    let mut fleet = Vec::with_capacity(replicas);
    for index in 0..replicas {
        let addr = read_ip(index);
        let replica = ReadReplicaMap::<u64, u64>::new_with_transport(
            read_config(addr, authoritative_peers.saturating_add(8), true),
            Arc::new(NullCountingTransport {
                local: SocketAddr::new(addr, PORT),
                traffic: traffic.clone(),
            }),
        )
        .expect("valid read-replica control");
        for peer in &known {
            replica.seed_peer(*peer);
        }
        fleet.push(replica);
    }
    let heap_bytes = live_bytes() - before;

    traffic.reset();
    runtime.block_on(async {
        for replica in &fleet {
            replica.start_reconciliation().await;
        }
    });
    let (known_tx_bytes, known_tx_datagrams, probe_tx_bytes, probe_tx_datagrams) =
        traffic.classify_tx(&known);

    assert_eq!(
        known_tx_datagrams,
        replicas.saturating_mul(authoritative_peers) as u64
    );
    assert_eq!(probe_tx_datagrams, replicas as u64);

    std::hint::black_box(&fleet);

    FleetPoint {
        replicas,
        heap_bytes,
        known_tx_bytes,
        known_tx_datagrams,
        probe_tx_bytes,
        probe_tx_datagrams,
    }
}

async fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool, what: &str) {
    tokio::time::timeout(timeout, async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

async fn ingress_heap_delta(replicas: usize, authenticated: bool) -> i64 {
    let network = InMemoryNetwork::new();
    let central_traffic = Traffic::default();
    let central = ReplicatedMap::<u64, u64>::new_with_transport(
        authoritative_config(authenticated),
        Arc::new(CountingTransport {
            inner: network.bind(SocketAddr::new(CENTRAL_IP, PORT)),
            traffic: central_traffic.clone(),
        }),
    )
    .expect("valid authoritative ingress control");

    let mut fleet = Vec::with_capacity(replicas);
    for index in 0..replicas {
        let addr = read_ip(index);
        let replica = ReadReplicaMap::<u64, u64>::new_with_transport(
            read_config(addr, 4, authenticated),
            Arc::new(network.bind(SocketAddr::new(addr, PORT))),
        )
        .expect("valid live read replica");
        replica.seed_peer(CENTRAL_IP);
        fleet.push(replica);
    }

    let shutdown = CancellationToken::new();
    let run_store = central.clone();
    let run_shutdown = shutdown.clone();
    let run_task = tokio::spawn(async move {
        let _ = run_store.run(run_shutdown).await;
    });
    wait_until(
        WAIT_TIMEOUT,
        || central.sync_state().rounds >= 1,
        "authoritative startup round",
    )
    .await;
    tokio::time::sleep(SETTLE).await;

    central_traffic.reset();
    let before = live_bytes();
    for replica in &fleet {
        replica.start_reconciliation().await;
    }
    wait_until(
        WAIT_TIMEOUT,
        || central_traffic.rx_datagrams.load(Ordering::Relaxed) >= replicas as u64,
        "all value-only read-replica datagrams",
    )
    .await;
    tokio::time::sleep(SETTLE).await;
    let heap_delta = live_bytes() - before;

    assert!(
        central.members().is_empty(),
        "read replicas must never enter causal membership"
    );
    assert!(
        central.peers().is_empty(),
        "value-only read replicas must never enter authoritative gossip routing"
    );

    shutdown.cancel();
    run_task.await.expect("authoritative run task");
    std::hint::black_box(&fleet);

    heap_delta
}

fn ingress_point(runtime: &Runtime, replicas: usize) -> IngressPoint {
    let insecure_heap_delta = runtime.block_on(ingress_heap_delta(replicas, false));
    let authenticated_heap_delta = runtime.block_on(ingress_heap_delta(replicas, true));
    IngressPoint {
        replicas,
        insecure_heap_delta,
        authenticated_heap_delta,
    }
}

fn extrapolate(points: &[(usize, f64)], target: usize) -> Option<(usize, usize, f64)> {
    if points.len() < 2 {
        return None;
    }
    let (n1, v1) = points[points.len() - 2];
    let (n2, v2) = points[points.len() - 1];
    if n2 <= n1 || target < n2 {
        return None;
    }
    let slope = (v2 - v1) / (n2 - n1) as f64;
    Some((n1, n2, v2 + slope * (target - n2) as f64))
}

fn report_model(label: &str, points: &[(usize, f64)], target: usize, unit: &str) {
    if let Some((n1, n2, estimate)) = extrapolate(points, target) {
        println!(
            "[read-fleet-model] MODEL target={target},metric={label},estimate={estimate:.3} {unit},based_on={n1},{n2},method=linear-last-two"
        );
    }
}

fn main() {
    let counts = env_list("RECONCILE_READ_REPLICA_COUNTS", "1,10,100,1000");
    let authoritative_peers = env_usize("RECONCILE_READ_AUTHORITATIVE_PEERS", 3);
    let model_target = env_usize("RECONCILE_READ_MODEL_TARGET", 100_000);
    assert!(authoritative_peers > 0);

    let runtime = Runtime::new().expect("Tokio runtime");

    let mut fleet_points = Vec::new();
    for &replicas in &counts {
        let point = fleet_point(&runtime, replicas, authoritative_peers);
        println!(
            "[read-fleet] replicas={},authoritative_peers={authoritative_peers},heap_bytes={},known_tx_bytes={},known_tx_dgrams={},probe_tx_bytes={},probe_tx_dgrams={}",
            point.replicas,
            point.heap_bytes,
            point.known_tx_bytes,
            point.known_tx_datagrams,
            point.probe_tx_bytes,
            point.probe_tx_datagrams
        );
        fleet_points.push(point);
    }

    let mut ingress_points = Vec::new();
    for &replicas in &counts {
        let point = ingress_point(&runtime, replicas);
        let auth_extra = point
            .authenticated_heap_delta
            .saturating_sub(point.insecure_heap_delta);
        println!(
            "[read-ingress] replicas={},authoritative_max_peers={},authoritative_max_replay_senders={},insecure_heap_delta={},authenticated_heap_delta={},auth_extra_heap={auth_extra}",
            point.replicas,
            MAX_PEERS_CONTROL,
            MAX_REPLAY_SENDERS_CONTROL,
            point.insecure_heap_delta,
            point.authenticated_heap_delta,
        );
        ingress_points.push(point);
    }

    report_model(
        "read_replica_fleet_heap",
        &fleet_points
            .iter()
            .map(|p| (p.replicas, p.heap_bytes as f64))
            .collect::<Vec<_>>(),
        model_target,
        "bytes",
    );
    report_model(
        "read_replica_known_round_egress",
        &fleet_points
            .iter()
            .map(|p| (p.replicas, p.known_tx_bytes as f64))
            .collect::<Vec<_>>(),
        model_target,
        "bytes",
    );
    let max_auth_extra_heap = ingress_points
        .iter()
        .map(|p| {
            p.authenticated_heap_delta
                .saturating_sub(p.insecure_heap_delta)
        })
        .max()
        .unwrap_or(0);
    println!(
        "[read-ingress-bound] max_replay_senders={MAX_REPLAY_SENDERS_CONTROL},max_observed_auth_extra_heap={max_auth_extra_heap} bytes"
    );
}
