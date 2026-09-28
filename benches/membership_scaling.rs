// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// Product/runtime scaling probe for ReplicatedMap membership and causal-stability metadata.
//
// This does not compare reconciliation algorithms. It measures shipped runtime bookkeeping:
// peer-routing heap, real authoritative-member admission/background traffic, and durable
// membership/tombstone-ack metadata. Live runs stop at the configured maximum (1,000 by default);
// the 100k point is only a linear model from the two largest measured points and is labelled MODEL.
//
// Run:
//   cargo bench --bench membership_scaling
// Overrides:
//   RECONCILE_MEMBERSHIP_COUNTS=2,10,100,1000
//   RECONCILE_MEMBERSHIP_TOMBSTONES=0,10,100
//   RECONCILE_MEMBERSHIP_MODEL_TARGET=100000

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::hint::black_box;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use reconcile::{
    replicated_map::Config, Entry, FileSnapshot, Hlc, InMemoryNetwork, InMemoryTransport,
    LogicalCounter, NodeId, PersistedState, Persistence, PhysicalTime, ReadReplicaMap,
    ReplicatedMap, Timestamp, Transport,
};
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

const PORT: u16 = 9_872;
const CENTRAL_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 250, 0, 1));
const LONG_INTERVAL: Duration = Duration::from_secs(60 * 60);
const WAIT_TIMEOUT: Duration = Duration::from_secs(20);
const GC_BLOCK_WINDOW: Duration = Duration::from_millis(1_100);
const GC_WAIT_TIMEOUT: Duration = Duration::from_secs(3);
const GC_KEY: u64 = u64::MAX;

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
    rx_bytes: Arc<AtomicU64>,
    rx_datagrams: Arc<AtomicU64>,
    tx_bytes: Arc<AtomicU64>,
    tx_datagrams: Arc<AtomicU64>,
}

#[derive(Clone, Copy, Default)]
struct TrafficSnapshot {
    rx_bytes: u64,
    rx_datagrams: u64,
    tx_bytes: u64,
    tx_datagrams: u64,
}

impl Traffic {
    fn reset(&self) {
        self.rx_bytes.store(0, Ordering::Relaxed);
        self.rx_datagrams.store(0, Ordering::Relaxed);
        self.tx_bytes.store(0, Ordering::Relaxed);
        self.tx_datagrams.store(0, Ordering::Relaxed);
    }

    fn snapshot(&self) -> TrafficSnapshot {
        TrafficSnapshot {
            rx_bytes: self.rx_bytes.load(Ordering::Relaxed),
            rx_datagrams: self.rx_datagrams.load(Ordering::Relaxed),
            tx_bytes: self.tx_bytes.load(Ordering::Relaxed),
            tx_datagrams: self.tx_datagrams.load(Ordering::Relaxed),
        }
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
        self.traffic
            .tx_bytes
            .fetch_add(sent as u64, Ordering::Relaxed);
        self.traffic.tx_datagrams.fetch_add(1, Ordering::Relaxed);
        Ok(sent)
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}

#[derive(Clone, Copy)]
struct RoutingPoint {
    members: usize,
    authoritative_heap: i64,
    read_replica_heap: i64,
}

#[derive(Clone, Copy)]
struct DurablePoint {
    members: usize,
    tombstones: usize,
    heap_bytes: i64,
    file_bytes: u64,
    save_ms: f64,
    load_ms: f64,
}

#[derive(Clone, Copy)]
struct LivePoint {
    members: usize,
    admission_ms: f64,
    admission: TrafficSnapshot,
    round_ms: f64,
    round: TrafficSnapshot,
}

fn env_list(name: &str, default: &str) -> Vec<usize> {
    let mut values: Vec<_> = std::env::var(name)
        .unwrap_or_else(|_| default.to_owned())
        .split(',')
        .map(|raw| {
            raw.trim()
                .parse::<usize>()
                .unwrap_or_else(|_| panic!("{name} contains a non-integer value: {raw:?}"))
        })
        .collect();
    values.sort_unstable();
    values.dedup();
    assert!(!values.is_empty(), "{name} must contain at least one value");
    values
}

fn model_target() -> usize {
    std::env::var("RECONCILE_MEMBERSHIP_MODEL_TARGET").map_or(100_000, |raw| {
        raw.parse::<usize>()
            .expect("RECONCILE_MEMBERSHIP_MODEL_TARGET must be an integer")
    })
}

fn peer_ip(index: usize) -> IpAddr {
    assert!(
        index < 0x00ef_fffe,
        "peer index is outside the benchmark address range"
    );
    IpAddr::V4(Ipv4Addr::from(0x7f01_0001u32 + index as u32))
}

fn persisted_ip(index: usize) -> IpAddr {
    assert!(
        index < 0x00ff_fffe,
        "persisted peer index is outside the model address range"
    );
    IpAddr::V4(Ipv4Addr::from(0x0a00_0001u32 + index as u32))
}

fn authoritative_config(addr: IpAddr, node_id: u64, max_peers: usize, fanout: usize) -> Config {
    let local_net = format!("{addr}/32").parse().expect("valid host network");
    Config::new(PORT)
        .with_listen_addr(addr)
        .with_net(local_net)
        .expect("one network fits")
        .with_node_id(NodeId::new(node_id))
        .with_max_peers(max_peers)
        .with_remote_interval(1)
        .with_remote_fanout(fanout)
        .with_reconcile_interval(LONG_INTERVAL)
        .with_repair_interval(LONG_INTERVAL)
        .with_snapshot_interval(None)
        .with_insecure_no_key()
}

fn central_config(max_peers: usize) -> Config {
    // With no declared network, admitted peers share the unclassified remote bucket. Fanout is
    // raised to the measured membership so one explicit round contacts every known peer. No
    // discovery network also means no random probe perturbs the count.
    Config::new(PORT)
        .with_listen_addr(CENTRAL_IP)
        .with_node_id(NodeId::new(1))
        .with_max_peers(max_peers)
        .with_remote_interval(1)
        .with_remote_fanout(max_peers)
        .with_reconcile_interval(LONG_INTERVAL)
        .with_repair_interval(LONG_INTERVAL)
        .with_snapshot_interval(None)
        .with_insecure_no_key()
}

fn read_config(addr: IpAddr, max_peers: usize) -> Config {
    let local_net = format!("{addr}/32").parse().expect("valid host network");
    Config::new(PORT)
        .with_listen_addr(addr)
        .with_net(local_net)
        .expect("one network fits")
        .with_max_peers(max_peers)
        .with_reconcile_interval(LONG_INTERVAL)
        .with_snapshot_interval(None)
        .with_insecure_no_key()
}

fn authoritative_routing_heap(members: usize) -> i64 {
    let network = InMemoryNetwork::new();
    let addr = IpAddr::V4(Ipv4Addr::new(127, 251, 0, 1));
    let store = ReplicatedMap::<u64, u64>::new_with_transport(
        authoritative_config(addr, 10, members.saturating_add(8), members.max(1)),
        Arc::new(network.bind(SocketAddr::new(addr, PORT))),
    )
    .expect("valid authoritative routing control");
    let before = live_bytes();
    for index in 0..members {
        store.seed_peer(peer_ip(index));
    }
    let delta = live_bytes() - before;
    assert_eq!(store.peers().len(), members);
    assert!(
        store.members().is_empty(),
        "seeding must not grant causal membership"
    );
    black_box(&store);
    delta
}

fn read_replica_routing_heap(members: usize) -> i64 {
    let network = InMemoryNetwork::new();
    let addr = IpAddr::V4(Ipv4Addr::new(127, 252, 0, 1));
    let store = ReadReplicaMap::<u64, u64>::new_with_transport(
        read_config(addr, members.saturating_add(8)),
        Arc::new(network.bind(SocketAddr::new(addr, PORT))),
    )
    .expect("valid read-replica routing control");
    let before = live_bytes();
    for index in 0..members {
        store.seed_peer(peer_ip(index));
    }
    let delta = live_bytes() - before;
    assert_eq!(store.peers().len(), members);
    black_box(&store);
    delta
}

fn tombstone_entry(key: u64) -> Entry<Timestamp, u64> {
    Entry::tombstone(Timestamp::new(
        Hlc::new(
            PhysicalTime::from_millis(1_800_000_000_000 + key),
            LogicalCounter::ZERO,
        ),
        NodeId::new(1),
    ))
}

fn causal_state(members: usize, tombstones: usize) -> PersistedState<u64, u64> {
    let member_set: HashSet<_> = (0..members).map(persisted_ip).collect();
    let entries = (0..tombstones as u64)
        .map(|key| (key, tombstone_entry(key)))
        .collect();
    let tombstone_acks = (0..tombstones as u64)
        .map(|key| {
            let version = key.wrapping_mul(2_654_435_761).wrapping_add(1);
            let acks: HashMap<_, _> = member_set
                .iter()
                .copied()
                .map(|peer| (peer, version))
                .collect();
            (key, acks)
        })
        .collect();
    PersistedState::new(entries, member_set, tombstone_acks)
}

fn durable_point(members: usize, tombstones: usize) -> DurablePoint {
    let before = live_bytes();
    let state = causal_state(members, tombstones);
    let heap_bytes = live_bytes() - before;

    let dir = tempfile::tempdir().expect("create membership benchmark directory");
    let path = dir.path().join("snapshot.bin");
    let backend = FileSnapshot::new(&path);

    let started = Instant::now();
    Persistence::<u64, u64>::save(&backend, &state).expect("save causal metadata snapshot");
    let save_ms = started.elapsed().as_secs_f64() * 1_000.0;
    let file_bytes = fs::metadata(&path).expect("snapshot metadata").len();

    let started = Instant::now();
    let loaded = Persistence::<u64, u64>::load(&backend)
        .expect("load causal metadata snapshot")
        .expect("snapshot exists");
    let load_ms = started.elapsed().as_secs_f64() * 1_000.0;

    assert_eq!(loaded.entries.len(), tombstones);
    assert_eq!(loaded.members.len(), members);
    assert_eq!(loaded.tombstone_acks.len(), tombstones);
    if tombstones > 0 {
        assert!(loaded
            .tombstone_acks
            .values()
            .all(|acks| acks.len() == members));
    }
    black_box((&state, &loaded));

    DurablePoint {
        members,
        tombstones,
        heap_bytes,
        file_bytes,
        save_ms,
        load_ms,
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

fn tombstone_exists(store: &ReplicatedMap<u64, u64>) -> bool {
    store
        .snapshot()
        .get(&GC_KEY)
        .is_some_and(|entry| entry.is_tombstone())
}

async fn live_point(members: usize, exercise_gc: bool) -> LivePoint {
    let network = InMemoryNetwork::new();
    let traffic = Traffic::default();
    let central_transport = CountingTransport {
        inner: network.bind(SocketAddr::new(CENTRAL_IP, PORT)),
        traffic: traffic.clone(),
    };
    let central = ReplicatedMap::<u64, u64>::new_with_transport(
        central_config(members.saturating_add(8)),
        Arc::new(central_transport),
    )
    .expect("valid central benchmark node")
    .with_tombstone_timeout(Duration::ZERO);

    let shutdown = CancellationToken::new();
    let run_store = central.clone();
    let run_shutdown = shutdown.clone();
    let run_task = tokio::spawn(async move {
        let _ = run_store.run(run_shutdown).await;
    });
    wait_until(
        WAIT_TIMEOUT,
        || central.sync_state().rounds >= 1,
        "central initial reconciliation round",
    )
    .await;

    traffic.reset();
    let admission_started = Instant::now();
    for index in 0..members {
        let addr = peer_ip(index);
        let peer = ReplicatedMap::<u64, u64>::new_with_transport(
            authoritative_config(addr, index as u64 + 2, 4, 1),
            Arc::new(network.bind(SocketAddr::new(addr, PORT))),
        )
        .expect("valid one-shot authoritative peer");
        peer.seed_peer(CENTRAL_IP);
        // An empty dated comparison proves authoritative membership without changing business data.
        peer.start_reconciliation().await;
        drop(peer);
    }
    wait_until(
        WAIT_TIMEOUT,
        || central.members().len() == members,
        "all dated peers to enter causal membership",
    )
    .await;
    let admission_ms = admission_started.elapsed().as_secs_f64() * 1_000.0;
    let admission = traffic.snapshot();
    assert_eq!(central.peers().len(), members);

    traffic.reset();
    let round_started = Instant::now();
    central.start_reconciliation().await;
    let round_ms = round_started.elapsed().as_secs_f64() * 1_000.0;
    let round = traffic.snapshot();

    if exercise_gc {
        central.load_bulk(&[(GC_KEY, 7)]);
        central.remove(&GC_KEY);
        assert!(tombstone_exists(&central));
        tokio::time::sleep(GC_BLOCK_WINDOW).await;
        assert!(
            tombstone_exists(&central),
            "expired tombstone must stay while authoritative members have not acknowledged it"
        );

        let forget_started = Instant::now();
        for index in 0..members {
            central.forget_peer(peer_ip(index));
        }
        let forget_ms = forget_started.elapsed().as_secs_f64() * 1_000.0;
        assert!(central.members().is_empty());

        let gc_started = Instant::now();
        wait_until(
            GC_WAIT_TIMEOUT,
            || !tombstone_exists(&central),
            "tombstone GC after decommissioning unreachable members",
        )
        .await;
        println!(
            "[membership-gc] members={members},blocked_before_forget=true,forget_all_ms={forget_ms:.3},gc_after_forget_ms={:.3}",
            gc_started.elapsed().as_secs_f64() * 1_000.0
        );
    }

    shutdown.cancel();
    run_task.await.expect("central run task");

    LivePoint {
        members,
        admission_ms,
        admission,
        round_ms,
        round,
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
            "[membership-model] MODEL target={target},metric={label},estimate={estimate:.3} {unit},based_on={n1},{n2},method=linear-last-two"
        );
    }
}

fn main() {
    let counts = env_list("RECONCILE_MEMBERSHIP_COUNTS", "2,10,100,1000");
    let tombstones = env_list("RECONCILE_MEMBERSHIP_TOMBSTONES", "0,10,100");
    let target = model_target();
    assert!(
        counts.iter().all(|&n| n > 0),
        "member counts must be positive"
    );

    println!("[membership-routing] members,authoritative_heap_bytes,read_replica_heap_bytes");
    let mut routing = Vec::new();
    for &members in &counts {
        let point = RoutingPoint {
            members,
            authoritative_heap: authoritative_routing_heap(members),
            read_replica_heap: read_replica_routing_heap(members),
        };
        println!(
            "[membership-routing] {},{},{}",
            point.members, point.authoritative_heap, point.read_replica_heap
        );
        routing.push(point);
    }

    println!(
        "[membership-durable] members,tombstones,full_ack_entries,heap_bytes,file_bytes,save_ms,load_ms"
    );
    let mut durable = Vec::new();
    for &tombstone_count in &tombstones {
        for &members in &counts {
            let point = durable_point(members, tombstone_count);
            println!(
                "[membership-durable] {},{},{},{},{},{:.3},{:.3}",
                point.members,
                point.tombstones,
                point.members.saturating_mul(point.tombstones),
                point.heap_bytes,
                point.file_bytes,
                point.save_ms,
                point.load_ms,
            );
            durable.push(point);
        }
    }

    let runtime = Runtime::new().expect("Tokio runtime");
    println!(
        "[membership-live] members,admission_ms,admission_rx_bytes,admission_rx_dgrams,admission_tx_bytes,admission_tx_dgrams,round_ms,round_tx_bytes,round_tx_dgrams"
    );
    let largest = *counts.last().expect("non-empty member sweep");
    let mut live = Vec::new();
    for &members in &counts {
        let point = runtime.block_on(live_point(members, members == largest));
        println!(
            "[membership-live] {},{:.3},{},{},{},{},{:.3},{},{}",
            point.members,
            point.admission_ms,
            point.admission.rx_bytes,
            point.admission.rx_datagrams,
            point.admission.tx_bytes,
            point.admission.tx_datagrams,
            point.round_ms,
            point.round.tx_bytes,
            point.round.tx_datagrams,
        );
        live.push(point);
    }

    report_model(
        "authoritative_routing_heap",
        &routing
            .iter()
            .map(|p| (p.members, p.authoritative_heap as f64))
            .collect::<Vec<_>>(),
        target,
        "bytes",
    );
    report_model(
        "read_replica_routing_heap",
        &routing
            .iter()
            .map(|p| (p.members, p.read_replica_heap as f64))
            .collect::<Vec<_>>(),
        target,
        "bytes",
    );
    report_model(
        "full_round_tx_wire",
        &live
            .iter()
            .map(|p| (p.members, p.round.tx_bytes as f64))
            .collect::<Vec<_>>(),
        target,
        "bytes",
    );
    for &tombstone_count in &tombstones {
        let subset: Vec<_> = durable
            .iter()
            .filter(|p| p.tombstones == tombstone_count)
            .map(|p| (p.members, p.file_bytes as f64))
            .collect();
        report_model(
            &format!("snapshot_file_tombstones_{tombstone_count}"),
            &subset,
            target,
            "bytes",
        );
    }
}
