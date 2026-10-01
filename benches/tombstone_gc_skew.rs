// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// Controlled one-sided tombstone-GC skew probe for runtime causal-stability behavior.
//
// Both authoritative peers have the same application-visible state: the first d keys are deleted.
// In the aligned control both peers already GC'd those tombstones. In the measured case the left
// peer already GC'd them while the right peer still retains them. The difference between the two
// cases is therefore runtime tombstone history only, not application divergence.
//
// d is the deployment term delete_rate_per_second * gc_window_seconds; callers can sweep it
// directly without baking one delete-rate/window pair into the harness.
//
// Run:
//   cargo bench --bench tombstone_gc_skew
//
// Overrides:
//   RECONCILE_GC_SKEW_N=20000
//   RECONCILE_GC_SKEW_DELETIONS=0,100,1000,10000
//   RECONCILE_GC_SKEW_BASE_DIVERGENCE=100

use std::collections::{HashMap, HashSet};
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::Utc;
use reconcile::{
    replicated_map::Config, Entry, Hlc, InMemoryNetwork, InMemoryPersistence, InMemoryTransport,
    LogicalCounter, NodeId, PersistedState, Persistence, PhysicalTime, ReplicatedMap, Timestamp,
    Transport,
};
use tokio_util::sync::CancellationToken;

const PORT: u16 = 9_876;
const LEFT_IP: IpAddr = IpAddr::V4(std::net::Ipv4Addr::new(127, 9, 0, 1));
const RIGHT_IP: IpAddr = IpAddr::V4(std::net::Ipv4Addr::new(127, 9, 0, 2));
const LONG_INTERVAL: Duration = Duration::from_secs(60 * 60);
const REPAIR_INTERVAL: Duration = Duration::from_millis(500);
const TOMBSTONE_TIMEOUT: Duration = Duration::from_secs(60 * 60);
const WAIT_TIMEOUT: Duration = Duration::from_secs(20);
const SETTLE: Duration = Duration::from_millis(25);

#[derive(Clone, Default)]
struct Traffic {
    tx_bytes: Arc<AtomicU64>,
    tx_datagrams: Arc<AtomicU64>,
    rx_bytes: Arc<AtomicU64>,
    rx_datagrams: Arc<AtomicU64>,
    advertised_ranges: Arc<AtomicU64>,
    enumerated_elements: Arc<AtomicU64>,
}

impl Traffic {
    fn snapshot(&self) -> (u64, u64, u64, u64) {
        (
            self.tx_bytes.load(Ordering::Relaxed),
            self.tx_datagrams.load(Ordering::Relaxed),
            self.rx_bytes.load(Ordering::Relaxed),
            self.rx_datagrams.load(Ordering::Relaxed),
        )
    }

    fn protocol_snapshot(&self) -> (u64, u64) {
        (
            self.advertised_ranges.load(Ordering::Relaxed),
            self.enumerated_elements.load(Ordering::Relaxed),
        )
    }
}

struct CountingTransport {
    inner: InMemoryTransport,
    measured_peer: IpAddr,
    traffic: Traffic,
}

#[async_trait]
impl Transport for CountingTransport {
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let (size, peer) = self.inner.recv_from(buf).await?;
        if peer.ip() == self.measured_peer {
            self.traffic
                .rx_bytes
                .fetch_add(size as u64, Ordering::Relaxed);
            self.traffic.rx_datagrams.fetch_add(1, Ordering::Relaxed);
        }
        Ok((size, peer))
    }

    async fn send_to(&self, buf: &[u8], dst: &SocketAddr) -> io::Result<usize> {
        let sent = self.inner.send_to(buf, dst).await?;
        if dst.ip() == self.measured_peer {
            self.traffic
                .tx_bytes
                .fetch_add(sent as u64, Ordering::Relaxed);
            self.traffic.tx_datagrams.fetch_add(1, Ordering::Relaxed);

            let counts = reconcile::testing::count_u64_dated_protocol_messages(buf);
            self.traffic
                .advertised_ranges
                .fetch_add(counts.advertised_ranges as u64, Ordering::Relaxed);
            self.traffic
                .enumerated_elements
                .fetch_add(counts.enumerated_elements as u64, Ordering::Relaxed);
        }
        Ok(sent)
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}

struct Peer {
    store: ReplicatedMap<u64, u64>,
    persistence: Arc<InMemoryPersistence<u64, u64>>,
    traffic: Traffic,
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

fn present_entry(key: u64, base_ms: u64) -> Entry<Timestamp, u64> {
    Entry::present(
        Timestamp::new(
            Hlc::new(
                PhysicalTime::from_millis(base_ms.saturating_add(key)),
                LogicalCounter::ZERO,
            ),
            NodeId::new(1),
        ),
        key.wrapping_mul(2_654_435_761),
    )
}

fn divergent_present_entry(key: u64, base_ms: u64, n: usize) -> Entry<Timestamp, u64> {
    Entry::present(
        Timestamp::new(
            Hlc::new(
                PhysicalTime::from_millis(
                    base_ms
                        .saturating_add((n as u64).saturating_mul(2))
                        .saturating_add(key)
                        .saturating_add(1),
                ),
                LogicalCounter::ZERO,
            ),
            NodeId::new(2),
        ),
        key.wrapping_mul(2_654_435_761).wrapping_add(1),
    )
}

fn tombstone_entry(key: u64, base_ms: u64, n: usize) -> Entry<Timestamp, u64> {
    Entry::tombstone(Timestamp::new(
        Hlc::new(
            PhysicalTime::from_millis(
                base_ms
                    .saturating_add(n as u64)
                    .saturating_add(key)
                    .saturating_add(1),
            ),
            LogicalCounter::ZERO,
        ),
        NodeId::new(2),
    ))
}

fn state(
    n: usize,
    deleted: usize,
    retain_tombstones: bool,
    member: IpAddr,
    base_ms: u64,
    live_divergence: usize,
    divergent_side: bool,
) -> PersistedState<u64, u64> {
    assert!(deleted <= n);
    assert!(live_divergence <= n - deleted);
    let divergence_end = deleted + live_divergence;
    let mut entries = Vec::with_capacity(n);
    if retain_tombstones {
        entries.extend((0..deleted as u64).map(|key| (key, tombstone_entry(key, base_ms, n))));
    }
    entries.extend((deleted..n).map(|index| {
        let key = index as u64;
        let entry = if divergent_side && index < divergence_end {
            divergent_present_entry(key, base_ms, n)
        } else {
            present_entry(key, base_ms)
        };
        (key, entry)
    }));
    PersistedState::new(entries, HashSet::from([member]), HashMap::new())
}

fn config(addr: IpAddr, node_id: u64) -> Config {
    Config::new(PORT)
        .with_listen_addr(addr)
        .with_node_id(NodeId::new(node_id))
        .with_reconcile_interval(LONG_INTERVAL)
        .with_repair_interval(REPAIR_INTERVAL)
        .with_snapshot_interval(None)
        .with_insecure_no_key()
}

fn peer(
    network: &InMemoryNetwork,
    addr: IpAddr,
    other: IpAddr,
    node_id: u64,
    initial: PersistedState<u64, u64>,
) -> Peer {
    let traffic = Traffic::default();
    let transport = CountingTransport {
        inner: network.bind(SocketAddr::new(addr, PORT)),
        measured_peer: other,
        traffic: traffic.clone(),
    };
    let persistence = Arc::new(InMemoryPersistence::new());
    persistence.save(&initial).expect("seed benchmark state");

    let store =
        ReplicatedMap::<u64, u64>::new_with_transport(config(addr, node_id), Arc::new(transport))
            .expect("valid benchmark peer")
            .with_tombstone_timeout(TOMBSTONE_TIMEOUT)
            .with_persistence(persistence.clone())
            .expect("load benchmark state");
    store.seed_peer(other);

    Peer {
        store,
        persistence,
        traffic,
    }
}

fn tombstone_count(store: &ReplicatedMap<u64, u64>) -> usize {
    store
        .snapshot()
        .values()
        .filter(|entry| entry.is_tombstone())
        .count()
}

fn persisted_ack_keys(peer: &Peer) -> usize {
    peer.store.snapshot_now().expect("capture causal state");
    peer.persistence
        .load()
        .expect("load causal state")
        .expect("causal state present")
        .tombstone_acks
        .len()
}

fn pair_tx(left: &Peer, right: &Peer) -> (u64, u64) {
    let l = left.traffic.snapshot();
    let r = right.traffic.snapshot();
    (l.0 + r.0, l.1 + r.1)
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

async fn scenario(n: usize, deleted: usize, one_sided: bool, live_divergence: usize) {
    let network = InMemoryNetwork::new();
    let now_ms = Utc::now().timestamp_millis().max(0) as u64;
    let base_ms = now_ms.saturating_sub((n as u64).saturating_mul(3).saturating_add(10_000));

    let left_state = state(n, deleted, false, RIGHT_IP, base_ms, live_divergence, false);
    let right_state = state(
        n,
        deleted,
        one_sided,
        LEFT_IP,
        base_ms,
        live_divergence,
        true,
    );
    let left = peer(&network, LEFT_IP, RIGHT_IP, 1, left_state);
    let right = peer(&network, RIGHT_IP, LEFT_IP, 2, right_state);

    let left_before = tombstone_count(&left.store);
    let right_before = tombstone_count(&right.store);
    if live_divergence == 0 {
        assert_eq!(
            left.store.to_vec(),
            right.store.to_vec(),
            "live state must match"
        );
    } else {
        assert_ne!(
            left.store.fingerprint(..),
            right.store.fingerprint(..),
            "control live divergence must be visible before reconciliation"
        );
    }

    let shutdown = CancellationToken::new();
    let left_run = left.store.clone();
    let left_shutdown = shutdown.clone();
    let left_task = tokio::spawn(async move {
        let _ = left_run.run(left_shutdown).await;
    });
    let right_run = right.store.clone();
    let right_shutdown = shutdown.clone();
    let right_task = tokio::spawn(async move {
        let _ = right_run.run(right_shutdown).await;
    });

    let started = Instant::now();
    wait_until(
        || {
            left.store.sync_state().rounds >= 1
                && right.store.sync_state().rounds >= 1
                && left.store.fingerprint(..) == right.store.fingerprint(..)
        },
        "initial reconciliation convergence",
    )
    .await;
    let convergence = started.elapsed();
    tokio::time::sleep(SETTLE).await;

    let left_wire = left.traffic.snapshot();
    let right_wire = right.traffic.snapshot();
    let left_protocol = left.traffic.protocol_snapshot();
    let right_protocol = right.traffic.protocol_snapshot();
    let advertised_ranges = left_protocol.0 + right_protocol.0;
    let enumerated_elements = left_protocol.1 + right_protocol.1;
    let tx_bytes = left_wire.0 + right_wire.0;
    let tx_datagrams = left_wire.1 + right_wire.1;
    let rx_bytes = left_wire.2 + right_wire.2;
    let rx_datagrams = left_wire.3 + right_wire.3;

    let left_after = tombstone_count(&left.store);
    let right_after = tombstone_count(&right.store);
    if one_sided {
        assert_eq!(
            left_after, deleted,
            "GC'd side should re-learn retained tombstones"
        );
        assert_eq!(right_after, deleted);
    } else {
        assert_eq!(left_after, 0);
        assert_eq!(right_after, 0);
    }

    left.store
        .snapshot_now()
        .expect("capture left causal state");
    right
        .store
        .snapshot_now()
        .expect("capture right causal state");
    let left_persisted = left
        .persistence
        .load()
        .expect("load left state")
        .expect("left state present");
    let right_persisted = right
        .persistence
        .load()
        .expect("load right state")
        .expect("right state present");
    let left_ack_pairs: usize = left_persisted
        .tombstone_acks
        .values()
        .map(HashMap::len)
        .sum();
    let right_ack_pairs: usize = right_persisted
        .tombstone_acks
        .values()
        .map(HashMap::len)
        .sum();

    println!(
        "[gc-skew] kind={},n={n},deleted={deleted},live_divergence={live_divergence},left_tombstones_before={left_before},right_tombstones_before={right_before},elapsed_ms={:.3},tx_bytes={tx_bytes},tx_dgrams={tx_datagrams},rx_bytes={rx_bytes},rx_dgrams={rx_datagrams},advertised_ranges={advertised_ranges},enumerated_elements={enumerated_elements},left_tombstones_after={left_after},right_tombstones_after={right_after},left_members={},right_members={},left_ack_keys={},right_ack_keys={},left_ack_pairs={left_ack_pairs},right_ack_pairs={right_ack_pairs}",
        match (one_sided, live_divergence > 0) {
            (true, true) => "one-sided+live",
            (true, false) => "one-sided",
            (false, true) => "live-control",
            (false, false) => "aligned-control",
        },
        convergence.as_secs_f64() * 1_000.0,
        left_persisted.members.len(),
        right_persisted.members.len(),
        left_persisted.tombstone_acks.len(),
        right_persisted.tombstone_acks.len(),
    );

    if one_sided && deleted > 0 && live_divergence == 0 {
        let (tx_before, dgrams_before) = pair_tx(&left, &right);
        let ack_started = Instant::now();
        let mut ack_rounds = 0usize;
        let mut covered = persisted_ack_keys(&left);
        while covered < deleted {
            let previous = covered;
            right.store.start_reconciliation().await;
            ack_rounds += 1;
            wait_until(
                || {
                    covered = persisted_ack_keys(&left);
                    covered > previous
                },
                "new tombstone acknowledgement coverage",
            )
            .await;
        }
        let ack_elapsed = ack_started.elapsed();
        let (tx_after, dgrams_after) = pair_tx(&left, &right);
        println!(
            "[gc-skew-acks] n={n},deleted={deleted},rounds={ack_rounds},elapsed_ms={:.3},tx_bytes={},tx_dgrams={},covered={covered}",
            ack_elapsed.as_secs_f64() * 1_000.0,
            tx_after.saturating_sub(tx_before),
            dgrams_after.saturating_sub(dgrams_before),
        );
    }

    shutdown.cancel();
    left_task.await.expect("left run task");
    right_task.await.expect("right run task");
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let n = env_usize("RECONCILE_GC_SKEW_N", 20_000);
    let deletions = env_list("RECONCILE_GC_SKEW_DELETIONS", "0,100,1000,10000");
    let base_divergence = env_usize("RECONCILE_GC_SKEW_BASE_DIVERGENCE", 100);
    assert!(n > 0);
    for deleted in deletions {
        assert!(deleted <= n);
        scenario(n, deleted, false, 0).await;
        if deleted > 0 {
            scenario(n, deleted, true, 0).await;
            if base_divergence > 0 {
                assert!(base_divergence <= n - deleted);
                scenario(n, deleted, false, base_divergence).await;
                scenario(n, deleted, true, base_divergence).await;
            }
        }
    }
}
