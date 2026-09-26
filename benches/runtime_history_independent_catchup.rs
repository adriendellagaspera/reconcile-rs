// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// ReplicatedMap catch-up when partition history leaves tombstones.
//
// Two authoritative peers first converge to one dated baseline and establish causal membership.
// Their transport is then blocked while each peer deletes a fixed set of baseline keys and creates
// a disjoint set of transient keys that it immediately deletes. Those transient keys are absent
// both before and after the partition from the application's point of view, but remain in the raw
// dated store as tombstones until causal-stability GC.
//
// The report varies the number of transient tombstones while holding the live states and fixed
// final deletions constant. It counts the resulting RBSR trace before healing, observes real
// in-memory catch-up traffic, proves expired tombstones survive while the causal peer is
// unreachable, then verifies GC removes the historical footprint after convergence.
//
// Defaults:
//   n = 10_000 baseline live keys
//   d = 100 fixed final deletions
//   t = 0, 100, 1_000, 10_000 transient tombstones
//
// Overrides:
//   RECONCILE_RUNTIME_HISTORY_N=100000
//   RECONCILE_RUNTIME_HISTORY_D=1000
//   RECONCILE_RUNTIME_TOMBSTONES=0,100,1000,10000
//
// Run with `cargo bench --bench runtime_history_independent_catchup`.

use std::collections::BTreeSet;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use devkit::protocol_cost::{reconcile, Cost, Counting, Decisions};
use rand::rngs::StdRng;
use rand::SeedableRng;
use rbsr::{FanOut, FixedFanOut, RefinementPolicy};
use reconcile::{
    replicated_map::Config, Entry, InMemoryNetwork, InMemoryTransport, NodeId, ReplicatedMap,
    Timestamp, Transport,
};
use rsos::FingerprintTreeMap;
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

const DEFAULT_N: usize = 10_000;
const DEFAULT_D: usize = 100;
const DEFAULT_TRANSIENT_TOMBSTONES: &[usize] = &[0, 100, 1_000, 10_000];
const PORT: u16 = 9_870;
const SESSION_SEED: u64 = 42;
const RECONCILE_INTERVAL: Duration = Duration::from_millis(20);
const REPAIR_INTERVAL: Duration = Duration::from_millis(5);
const PARTITION_WRITE_SETTLE: Duration = Duration::from_millis(100);
const BLOCKED_GC_WINDOW: Duration = Duration::from_millis(1_100);
const WAIT_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone, Default)]
struct Traffic {
    bytes: Arc<AtomicU64>,
    datagrams: Arc<AtomicU64>,
}

impl Traffic {
    fn reset(&self) {
        self.bytes.store(0, Ordering::Relaxed);
        self.datagrams.store(0, Ordering::Relaxed);
    }

    fn snapshot(&self) -> (u64, u64) {
        (
            self.bytes.load(Ordering::Relaxed),
            self.datagrams.load(Ordering::Relaxed),
        )
    }
}

struct GateTransport {
    inner: InMemoryTransport,
    blocked: Arc<AtomicBool>,
    traffic: Traffic,
}

#[async_trait]
impl Transport for GateTransport {
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.inner.recv_from(buf).await
    }

    async fn send_to(&self, buf: &[u8], dst: &SocketAddr) -> io::Result<usize> {
        if self.blocked.load(Ordering::Relaxed) {
            // Model a partition as silent packet loss: writers believe the datagram left, while
            // anti-entropy must recover from the current states once the link heals.
            return Ok(buf.len());
        }

        let sent = self.inner.send_to(buf, dst).await?;
        self.traffic.datagrams.fetch_add(1, Ordering::Relaxed);
        self.traffic.bytes.fetch_add(sent as u64, Ordering::Relaxed);
        Ok(sent)
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}

struct Peer {
    store: ReplicatedMap<u64, u64>,
    addr: IpAddr,
    blocked: Arc<AtomicBool>,
    traffic: Traffic,
}

struct Pair {
    left: Peer,
    right: Peer,
}

type LiveState = Vec<(u64, u64)>;
type LiveStatePair = (LiveState, LiveState);

#[derive(Debug, Eq, PartialEq)]
struct CostSignature {
    decisions: Decisions,
    refinement_bytes: usize,
    datagrams: usize,
    fragments: usize,
    largest_message: usize,
    largest_message_bytes: usize,
}

impl From<&Cost> for CostSignature {
    fn from(cost: &Cost) -> Self {
        Self {
            decisions: cost.decisions(),
            refinement_bytes: cost.refinement_bytes,
            datagrams: cost.datagrams,
            fragments: cost.fragments,
            largest_message: cost.largest_message,
            largest_message_bytes: cost.largest_message_bytes,
        }
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |raw| {
        raw.parse::<usize>()
            .unwrap_or_else(|_| panic!("{name} must be a non-negative integer"))
    })
}

fn transient_tombstone_sweep() -> Vec<usize> {
    std::env::var("RECONCILE_RUNTIME_TOMBSTONES").map_or_else(
        |_| DEFAULT_TRANSIENT_TOMBSTONES.to_vec(),
        |raw| {
            raw.split(',')
                .map(|part| {
                    part.trim().parse::<usize>().unwrap_or_else(|_| {
                        panic!(
                            "RECONCILE_RUNTIME_TOMBSTONES must be a comma-separated list of integers"
                        )
                    })
                })
                .collect()
        },
    )
}

fn corpus(n: usize) -> Vec<(u64, u64)> {
    (0..n as u64)
        .map(|key| (key, key.wrapping_mul(2_654_435_761)))
        .collect()
}

fn fixed_deletion_keys(n: usize, d: usize) -> Vec<u64> {
    assert!(d >= 2, "RECONCILE_RUNTIME_HISTORY_D must be at least 2");
    assert!(
        d < n,
        "RECONCILE_RUNTIME_HISTORY_D must be smaller than the baseline"
    );
    let stride = n / (d + 1);
    assert!(stride > 0, "fixed-deletion key stride must make progress");
    (1..=d).map(|i| (stride * i) as u64).collect()
}

fn tombstone_count(store: &ReplicatedMap<u64, u64>) -> usize {
    store
        .snapshot()
        .iter()
        .filter(|(_, entry)| entry.is_tombstone())
        .count()
}

fn raw_diff_keys(
    left: &FingerprintTreeMap<u64, Entry<Timestamp, u64>>,
    right: &FingerprintTreeMap<u64, Entry<Timestamp, u64>>,
) -> Vec<u64> {
    let keys: BTreeSet<_> = left
        .iter()
        .map(|(key, _)| *key)
        .chain(right.iter().map(|(key, _)| *key))
        .collect();

    keys.into_iter()
        .filter(|key| left.get(key) != right.get(key))
        .collect()
}

fn counted_reconcile(
    left: &FingerprintTreeMap<u64, Entry<Timestamp, u64>>,
    right: &FingerprintTreeMap<u64, Entry<Timestamp, u64>>,
    policy: &dyn RefinementPolicy,
) -> Cost {
    let (counted_left, counted_right) = (Counting::new(left), Counting::new(right));
    let mut rng = StdRng::seed_from_u64(SESSION_SEED);
    let mut cost = reconcile(&counted_left, &counted_right, policy, None, &mut rng);
    cost.queries = counted_left.queries() + counted_right.queries();
    cost
}

fn transient_keys(start: u64, count: usize) -> Vec<u64> {
    (start..start + count as u64).collect()
}

fn leave_transient_tombstones(store: &ReplicatedMap<u64, u64>, keys: &[u64], salt: u64) {
    if keys.is_empty() {
        return;
    }

    // load_bulk avoids pricing an eager insert broadcast that would be lost by construction during
    // the partition. remove_bulk still exercises the ordinary public delete path and leaves the
    // same dated tombstone that a disconnected propagating insert/delete sequence would leave.
    let values: Vec<_> = keys
        .iter()
        .map(|&key| (key, key.wrapping_mul(2_654_435_761) ^ salt))
        .collect();
    store.load_bulk(&values);
    store.remove_bulk(keys);
}

fn peer(network: &InMemoryNetwork, addr: IpAddr, node_id: u64) -> Peer {
    let blocked = Arc::new(AtomicBool::new(false));
    let traffic = Traffic::default();
    let transport = GateTransport {
        inner: network.bind(SocketAddr::new(addr, PORT)),
        blocked: Arc::clone(&blocked),
        traffic: traffic.clone(),
    };
    let config = Config::default()
        .with_port(PORT)
        .with_listen_addr(addr)
        .with_net("127.0.0.1/8".parse().unwrap())
        .unwrap()
        .with_node_id(NodeId::new(node_id))
        .with_reconcile_interval(RECONCILE_INTERVAL)
        .with_repair_interval(REPAIR_INTERVAL)
        .with_insecure_no_key();
    let store = ReplicatedMap::new_with_transport(config, Arc::new(transport))
        .expect("valid in-memory peer")
        .with_tombstone_timeout(Duration::ZERO);

    Peer {
        store,
        addr,
        blocked,
        traffic,
    }
}

fn pair() -> Pair {
    let network = InMemoryNetwork::new();
    let left = peer(&network, "127.8.0.1".parse().unwrap(), 1);
    let right = peer(&network, "127.8.0.2".parse().unwrap(), 2);
    left.store.seed_peer(right.addr);
    right.store.seed_peer(left.addr);
    Pair { left, right }
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

async fn run_pair(
    pair: &Pair,
) -> (
    CancellationToken,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
) {
    let shutdown = CancellationToken::new();

    let left_store = pair.left.store.clone();
    let left_shutdown = shutdown.clone();
    let left = tokio::spawn(async move {
        let _ = left_store.run(left_shutdown).await;
    });

    let right_store = pair.right.store.clone();
    let right_shutdown = shutdown.clone();
    let right = tokio::spawn(async move {
        let _ = right_store.run(right_shutdown).await;
    });

    (shutdown, left, right)
}

async fn stop_pair(
    shutdown: CancellationToken,
    left: tokio::task::JoinHandle<()>,
    right: tokio::task::JoinHandle<()>,
) {
    shutdown.cancel();
    left.await.expect("left run task");
    right.await.expect("right run task");
}

async fn scenario(
    n: usize,
    fixed_keys: &[u64],
    transient_tombstones: usize,
    policy: &dyn RefinementPolicy,
    reference_live_states: &mut Option<LiveStatePair>,
    reference_post_gc: &mut Option<CostSignature>,
) {
    let pair = pair();
    pair.left.store.load_bulk(&corpus(n));

    let (shutdown, left_task, right_task) = run_pair(&pair).await;
    wait_until(
        || {
            pair.left.store.fingerprint(..) == pair.right.store.fingerprint(..)
                && pair.left.store.members().contains(&pair.right.addr)
                && pair.right.store.members().contains(&pair.left.addr)
        },
        "baseline convergence and causal membership",
    )
    .await;
    stop_pair(shutdown, left_task, right_task).await;

    pair.left.blocked.store(true, Ordering::Relaxed);
    pair.right.blocked.store(true, Ordering::Relaxed);

    let fixed_split = fixed_keys.len() / 2;
    let (left_fixed, right_fixed) = fixed_keys.split_at(fixed_split);
    pair.left.store.remove_bulk(left_fixed);
    pair.right.store.remove_bulk(right_fixed);

    let transient_left_count = transient_tombstones / 2 + transient_tombstones % 2;
    let transient_right_count = transient_tombstones / 2;
    let left_transient = transient_keys(n as u64, transient_left_count);
    let right_transient = transient_keys(
        n as u64 + transient_left_count as u64,
        transient_right_count,
    );
    leave_transient_tombstones(&pair.left.store, &left_transient, 0xa11c_e001);
    leave_transient_tombstones(&pair.right.store, &right_transient, 0xb22d_e002);

    tokio::time::sleep(PARTITION_WRITE_SETTLE).await;

    let expected_left_tombstones = left_fixed.len() + left_transient.len();
    let expected_right_tombstones = right_fixed.len() + right_transient.len();
    assert_eq!(pair.left.store.len(), n - left_fixed.len());
    assert_eq!(pair.right.store.len(), n - right_fixed.len());
    assert_eq!(tombstone_count(&pair.left.store), expected_left_tombstones);
    assert_eq!(
        tombstone_count(&pair.right.store),
        expected_right_tombstones
    );

    let left_live = pair.left.store.to_vec();
    let right_live = pair.right.store.to_vec();
    if let Some((reference_left, reference_right)) = reference_live_states {
        assert_eq!(&left_live, reference_left);
        assert_eq!(&right_live, reference_right);
    } else {
        *reference_live_states = Some((left_live, right_live));
    }

    let mut expected_diff_keys = fixed_keys.to_vec();
    expected_diff_keys.extend(left_transient.iter().copied());
    expected_diff_keys.extend(right_transient.iter().copied());
    expected_diff_keys.sort_unstable();

    let left_raw = pair.left.store.snapshot();
    let right_raw = pair.right.store.snapshot();
    assert_eq!(
        raw_diff_keys(&left_raw, &right_raw),
        expected_diff_keys,
        "raw divergence does not match fixed deletions plus historical tombstones"
    );
    let pre_heal_cost = counted_reconcile(&left_raw, &right_raw, policy);

    let (shutdown, left_task, right_task) = run_pair(&pair).await;
    tokio::time::sleep(BLOCKED_GC_WINDOW).await;
    assert_eq!(
        tombstone_count(&pair.left.store),
        expected_left_tombstones,
        "left GC collected expired tombstones without its causal peer's ack"
    );
    assert_eq!(
        tombstone_count(&pair.right.store),
        expected_right_tombstones,
        "right GC collected expired tombstones without its causal peer's ack"
    );

    pair.left.traffic.reset();
    pair.right.traffic.reset();
    let started = Instant::now();
    pair.left.blocked.store(false, Ordering::Relaxed);
    pair.right.blocked.store(false, Ordering::Relaxed);

    let converged_tombstones = fixed_keys.len() + transient_tombstones;
    wait_until(
        || {
            pair.left.store.fingerprint(..) == pair.right.store.fingerprint(..)
                && tombstone_count(&pair.left.store) == converged_tombstones
                && tombstone_count(&pair.right.store) == converged_tombstones
        },
        "dated catch-up before tombstone GC",
    )
    .await;
    let caught_up = started.elapsed();
    let catchup_traffic = {
        let (left_bytes, left_datagrams) = pair.left.traffic.snapshot();
        let (right_bytes, right_datagrams) = pair.right.traffic.snapshot();
        (left_bytes + right_bytes, left_datagrams + right_datagrams)
    };
    assert_eq!(pair.left.store.len(), n - fixed_keys.len());
    assert_eq!(pair.right.store.len(), n - fixed_keys.len());

    wait_until(
        || {
            tombstone_count(&pair.left.store) == 0
                && tombstone_count(&pair.right.store) == 0
                && pair.left.store.snapshot().len() == n - fixed_keys.len()
                && pair.right.store.snapshot().len() == n - fixed_keys.len()
                && pair.left.store.fingerprint(..) == pair.right.store.fingerprint(..)
        },
        "causal-stability tombstone GC",
    )
    .await;
    let gc_complete = started.elapsed();
    let full_traffic = {
        let (left_bytes, left_datagrams) = pair.left.traffic.snapshot();
        let (right_bytes, right_datagrams) = pair.right.traffic.snapshot();
        (left_bytes + right_bytes, left_datagrams + right_datagrams)
    };

    let left_post_gc = pair.left.store.snapshot();
    let right_post_gc = pair.right.store.snapshot();
    let post_gc_cost = counted_reconcile(&left_post_gc, &right_post_gc, policy);
    let post_gc_signature = CostSignature::from(&post_gc_cost);
    if let Some(reference) = reference_post_gc {
        assert_eq!(
            &post_gc_signature, reference,
            "transient tombstone history changed the post-GC steady-state RBSR trace"
        );
    } else {
        *reference_post_gc = Some(post_gc_signature);
    }

    println!(
        "[runtime-tombstone-catchup] t={transient_tombstones:>8} | raw={:>6}/{:>6}, tomb={:>5}/{:>5}, diff={:>6} | pre-heal refine={:>8} B, messages={:>3}, ranges={:>6}, idlist={:>6} elem | catch-up={:>8.3} ms, wire={:>9} B/{:>5} dg | GC={:>8.3} ms, wire+acks={:>9} B/{:>5} dg | post-GC raw={}",
        left_raw.len(),
        right_raw.len(),
        expected_left_tombstones,
        expected_right_tombstones,
        expected_diff_keys.len(),
        pre_heal_cost.refinement_bytes,
        pre_heal_cost.messages,
        pre_heal_cost.ranges,
        pre_heal_cost.enumerated_elements,
        caught_up.as_secs_f64() * 1_000.0,
        catchup_traffic.0,
        catchup_traffic.1,
        gc_complete.as_secs_f64() * 1_000.0,
        full_traffic.0,
        full_traffic.1,
        n - fixed_keys.len(),
    );

    stop_pair(shutdown, left_task, right_task).await;
}

async fn report() {
    let n = env_usize("RECONCILE_RUNTIME_HISTORY_N", DEFAULT_N);
    let d = env_usize("RECONCILE_RUNTIME_HISTORY_D", DEFAULT_D);
    let tombstone_sweep = transient_tombstone_sweep();
    assert!(
        !tombstone_sweep.is_empty(),
        "at least one transient tombstone count is required"
    );

    let fixed_keys = fixed_deletion_keys(n, d);
    let policy = FixedFanOut::new(FanOut::NEGENTROPY);
    let mut reference_live_states = None;
    let mut reference_post_gc = None;

    println!(
        "[runtime-tombstone-catchup] n={n} d={d}; t is the number of logically absent transient keys retained as tombstones"
    );
    for transient_tombstones in tombstone_sweep {
        scenario(
            n,
            &fixed_keys,
            transient_tombstones,
            &policy,
            &mut reference_live_states,
            &mut reference_post_gc,
        )
        .await;
    }
}

fn main() {
    Runtime::new().expect("Tokio runtime").block_on(report());
}
