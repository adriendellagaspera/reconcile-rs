// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// Product/runtime probe for sustained authoritative membership churn.
//
// Keeps authoritative fleet size constant while replacing members through the real dated protocol.
// New members hold the same tombstones and rebuild their ACK coverage through the shipped bounded
// resend cursor. This is a runtime lifecycle benchmark, not an algorithm comparison.
//
// Run:
//   cargo bench --bench membership_churn
//
// Overrides:
//   RECONCILE_CHURN_MEMBERS=1000
//   RECONCILE_CHURN_TOMBSTONES=0,100,1000
//   RECONCILE_CHURN_REPLACEMENTS=100
//   RECONCILE_CHURN_ACK_ROUNDS=3

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use reconcile::{
    replicated_map::Config, ClusterKey, Entry, FileSnapshot, Hlc, InMemoryNetwork,
    InMemoryPersistence, InMemoryTransport, LogicalCounter, NodeId, PersistedState, Persistence,
    PhysicalTime, ReplicatedMap, Timestamp, Transport,
};
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

const PORT: u16 = 9_875;
const CENTRAL_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 254, 1, 1));
const LONG_INTERVAL: Duration = Duration::from_secs(60 * 60);
const TOMBSTONE_TIMEOUT: Duration = Duration::from_secs(60 * 60 * 24 * 365 * 100);
const WAIT_TIMEOUT: Duration = Duration::from_secs(20);
const SETTLE: Duration = Duration::from_millis(2);

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
struct ChurnSummary {
    tombstones: usize,
    replacements: usize,
    initial_snapshot_bytes: u64,
    final_snapshot_bytes: u64,
    snapshot_ms: f64,
    forget_p50_ms: f64,
    forget_p95_ms: f64,
    forget_max_ms: f64,
    cycle_p50_ms: f64,
    cycle_p95_ms: f64,
    cycle_max_ms: f64,
    rx_bytes_per_replacement: f64,
    tx_bytes_per_replacement: f64,
}

fn cluster_key() -> ClusterKey {
    ClusterKey::new([0x6b; 32])
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |raw| {
        raw.parse::<usize>()
            .unwrap_or_else(|_| panic!("{name} must be an integer"))
    })
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

fn member_ip(index: usize) -> IpAddr {
    assert!(index < 0x00bf_fffe);
    IpAddr::V4(Ipv4Addr::from(0x7f01_0001u32 + index as u32))
}

fn replacement_ip(index: usize) -> IpAddr {
    assert!(index < 0x000f_fffe);
    IpAddr::V4(Ipv4Addr::from(0x7fc8_0001u32 + index as u32))
}

fn central_config(max_peers: usize) -> Config {
    Config::new(PORT)
        .with_listen_addr(CENTRAL_IP)
        .with_net("127.0.0.0/8".parse().expect("valid local net"))
        .expect("one network fits")
        .with_node_id(NodeId::new(1))
        .with_max_peers(max_peers)
        .with_remote_interval(1)
        .with_remote_fanout(max_peers)
        .with_reconcile_interval(LONG_INTERVAL)
        .with_repair_interval(LONG_INTERVAL)
        .with_snapshot_interval(None)
        .with_cluster_key(cluster_key())
}

fn newcomer_config(addr: IpAddr, node_id: u64) -> Config {
    Config::new(PORT)
        .with_listen_addr(addr)
        .with_net("127.0.0.0/8".parse().expect("valid local net"))
        .expect("one network fits")
        .with_node_id(NodeId::new(node_id))
        .with_max_peers(8)
        .with_remote_interval(1)
        .with_remote_fanout(1)
        .with_reconcile_interval(LONG_INTERVAL)
        .with_repair_interval(LONG_INTERVAL)
        .with_snapshot_interval(None)
        .with_cluster_key(cluster_key())
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

fn entries(tombstones: usize) -> Vec<(u64, Entry<Timestamp, u64>)> {
    (0..tombstones as u64)
        .map(|key| (key, tombstone_entry(key)))
        .collect()
}

fn initial_state(members: usize, tombstones: usize) -> PersistedState<u64, u64> {
    let member_set: HashSet<_> = (0..members).map(member_ip).collect();
    let tombstone_acks = (0..tombstones as u64)
        .map(|key| {
            let version = rsos::digest(&tombstone_entry(key)).0[0];
            let acks = member_set
                .iter()
                .copied()
                .map(|peer| (peer, version))
                .collect();
            (key, acks)
        })
        .collect();
    PersistedState::new(entries(tombstones), member_set, tombstone_acks)
}

fn newcomer(
    network: &InMemoryNetwork,
    addr: IpAddr,
    node_id: u64,
    tombstones: usize,
) -> ReplicatedMap<u64, u64> {
    let backend = Arc::new(InMemoryPersistence::<u64, u64>::new());
    let state = PersistedState::new(entries(tombstones), HashSet::new(), HashMap::new());
    Persistence::<u64, u64>::save(&*backend, &state).expect("seed newcomer state");
    let replica = ReplicatedMap::<u64, u64>::new_with_transport(
        newcomer_config(addr, node_id),
        Arc::new(network.bind(SocketAddr::new(addr, PORT))),
    )
    .expect("valid newcomer")
    .with_tombstone_timeout(TOMBSTONE_TIMEOUT)
    .with_persistence(backend)
    .expect("load newcomer state");
    replica.seed_peer(CENTRAL_IP);
    replica
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

fn percentile(values: &[f64], fraction: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let index = ((sorted.len() - 1) as f64 * fraction).round() as usize;
    sorted[index]
}

async fn churn_scenario(
    members: usize,
    tombstones: usize,
    replacements: usize,
    ack_rounds: usize,
) -> ChurnSummary {
    assert!(members > 0);
    assert!(replacements > 0 && replacements <= members);
    assert!(ack_rounds > 0);

    let initial = initial_state(members, tombstones);
    let dir = tempfile::tempdir().expect("create churn snapshot directory");
    let path = dir.path().join("snapshot.bin");
    let backend = Arc::new(FileSnapshot::new(&path));
    Persistence::<u64, u64>::save(&*backend, &initial).expect("persist initial churn state");
    let initial_snapshot_bytes = fs::metadata(&path).expect("initial snapshot metadata").len();

    let network = InMemoryNetwork::new();
    let traffic = Traffic::default();
    let central = ReplicatedMap::<u64, u64>::new_with_transport(
        central_config(members.saturating_add(8)),
        Arc::new(CountingTransport {
            inner: network.bind(SocketAddr::new(CENTRAL_IP, PORT)),
            traffic: traffic.clone(),
        }),
    )
    .expect("valid churn central")
    .with_tombstone_timeout(TOMBSTONE_TIMEOUT)
    .with_persistence(backend.clone())
    .expect("load churn central state");

    assert_eq!(central.members().len(), members);

    let shutdown = CancellationToken::new();
    let run_store = central.clone();
    let run_shutdown = shutdown.clone();
    let run_task = tokio::spawn(async move {
        let _ = run_store.run(run_shutdown).await;
    });
    wait_until(
        WAIT_TIMEOUT,
        || central.sync_state().rounds >= 1,
        "central startup round",
    )
    .await;

    let traffic_before = traffic.snapshot();
    let mut forget_times = Vec::with_capacity(replacements);
    let mut cycle_times = Vec::with_capacity(replacements);

    for index in 0..replacements {
        let old = member_ip(index);
        let new = replacement_ip(index);
        let newcomer = newcomer(&network, new, 10_000 + index as u64, tombstones);

        let cycle_started = Instant::now();
        let forget_started = Instant::now();
        central.forget_peer(old);
        forget_times.push(forget_started.elapsed().as_secs_f64() * 1_000.0);
        assert!(!central.members().contains(&old));

        let rx_before = traffic.rx_datagrams.load(Ordering::Relaxed);
        for _ in 0..ack_rounds {
            newcomer.start_reconciliation().await;
        }
        wait_until(
            WAIT_TIMEOUT,
            || {
                traffic.rx_datagrams.load(Ordering::Relaxed)
                    >= rx_before.saturating_add(ack_rounds as u64)
                    && central.members().contains(&new)
                    && central.members().len() == members
            },
            "replacement membership and ACK rounds",
        )
        .await;
        tokio::time::sleep(SETTLE).await;
        cycle_times.push(cycle_started.elapsed().as_secs_f64() * 1_000.0);
    }

    let snapshot_started = Instant::now();
    central.snapshot_now().expect("persist final churn state");
    let snapshot_ms = snapshot_started.elapsed().as_secs_f64() * 1_000.0;
    let final_snapshot_bytes = fs::metadata(&path).expect("final snapshot metadata").len();

    let final_state = Persistence::<u64, u64>::load(&*backend)
        .expect("reload final churn state")
        .expect("final churn state exists");
    assert_eq!(final_state.members.len(), members);
    for index in 0..replacements {
        assert!(!final_state.members.contains(&member_ip(index)));
        assert!(final_state.members.contains(&replacement_ip(index)));
    }
    assert_eq!(final_state.tombstone_acks.len(), tombstones);
    assert!(
        final_state
            .tombstone_acks
            .iter()
            .all(|(key, acks)| {
                let version = rsos::digest(&tombstone_entry(*key)).0[0];
                acks.len() == members && acks.values().all(|ack| *ack == version)
            }),
        "every replacement must restore complete, version-correct ACK coverage"
    );

    let traffic_after = traffic.snapshot();
    let rx_bytes = traffic_after
        .rx_bytes
        .saturating_sub(traffic_before.rx_bytes);
    let tx_bytes = traffic_after
        .tx_bytes
        .saturating_sub(traffic_before.tx_bytes);

    shutdown.cancel();
    run_task.await.expect("central run task");

    ChurnSummary {
        tombstones,
        replacements,
        initial_snapshot_bytes,
        final_snapshot_bytes,
        snapshot_ms,
        forget_p50_ms: percentile(&forget_times, 0.50),
        forget_p95_ms: percentile(&forget_times, 0.95),
        forget_max_ms: forget_times.iter().copied().fold(0.0, f64::max),
        cycle_p50_ms: percentile(&cycle_times, 0.50),
        cycle_p95_ms: percentile(&cycle_times, 0.95),
        cycle_max_ms: cycle_times.iter().copied().fold(0.0, f64::max),
        rx_bytes_per_replacement: rx_bytes as f64 / replacements as f64,
        tx_bytes_per_replacement: tx_bytes as f64 / replacements as f64,
    }
}

fn main() {
    let members = env_usize("RECONCILE_CHURN_MEMBERS", 1_000);
    let tombstones = env_list("RECONCILE_CHURN_TOMBSTONES", "0,100,1000");
    let replacements = env_usize("RECONCILE_CHURN_REPLACEMENTS", 100);
    let ack_rounds = env_usize("RECONCILE_CHURN_ACK_ROUNDS", 3);
    let runtime = Runtime::new().expect("Tokio runtime");

    for debt in tombstones {
        let summary =
            runtime.block_on(churn_scenario(members, debt, replacements, ack_rounds));
        println!(
            "[membership-churn] members={members},tombstones={},replacements={},ack_rounds={ack_rounds},initial_snapshot_bytes={},final_snapshot_bytes={},snapshot_ms={:.3},forget_p50_ms={:.3},forget_p95_ms={:.3},forget_max_ms={:.3},cycle_p50_ms={:.3},cycle_p95_ms={:.3},cycle_max_ms={:.3},rx_bytes_per_replacement={:.3},tx_bytes_per_replacement={:.3}",
            summary.tombstones,
            summary.replacements,
            summary.initial_snapshot_bytes,
            summary.final_snapshot_bytes,
            summary.snapshot_ms,
            summary.forget_p50_ms,
            summary.forget_p95_ms,
            summary.forget_max_ms,
            summary.cycle_p50_ms,
            summary.cycle_p95_ms,
            summary.cycle_max_ms,
            summary.rx_bytes_per_replacement,
            summary.tx_bytes_per_replacement,
        );
    }
}
