// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// ReplicatedMap catch-up after a partition with superseded update/delete/recreate history.
//
// Two authoritative peers first converge to one dated baseline and establish causal membership.
// Their transport is then blocked while both keep writing disjoint key sets. The number of
// superseded writes varies, but every corpus ends with the same value-only states and the same
// final divergent keys as tombstones. The report checks the deterministic RBSR trace over the raw
// dated snapshots, then heals the in-memory transport and observes real catch-up plus
// causal-stability GC.
//
// Defaults:
//   n = 10_000 keys
//   d = 100 final divergent keys
//   h = 0, 1_000, 100_000 superseded writes across both peers
//
// Overrides:
//   RECONCILE_RUNTIME_HISTORY_N=100000
//   RECONCILE_RUNTIME_HISTORY_D=1000
//   RECONCILE_RUNTIME_HISTORY_OPS=0,1000,100000,1000000
//
// Run with `cargo bench --bench runtime_history_independent_catchup`.

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
    State, Timestamp, Transport,
};
use rsos::FingerprintTreeMap;
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

const DEFAULT_N: usize = 10_000;
const DEFAULT_D: usize = 100;
const DEFAULT_HISTORY_OPS: &[usize] = &[0, 1_000, 100_000];
const PORT: u16 = 9_870;
const SESSION_SEED: u64 = 42;
const RECONCILE_INTERVAL: Duration = Duration::from_millis(20);
const REPAIR_INTERVAL: Duration = Duration::from_millis(5);
const BLOCKED_GC_WINDOW: Duration = Duration::from_millis(1_100);
const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

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

fn history_sizes() -> Vec<usize> {
    std::env::var("RECONCILE_RUNTIME_HISTORY_OPS").map_or_else(
        |_| DEFAULT_HISTORY_OPS.to_vec(),
        |raw| {
            raw.split(',')
                .map(|part| {
                    part.trim().parse::<usize>().unwrap_or_else(|_| {
                        panic!(
                            "RECONCILE_RUNTIME_HISTORY_OPS must be a comma-separated list of integers"
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

fn divergent_keys(n: usize, d: usize) -> Vec<u64> {
    assert!(d >= 2, "RECONCILE_RUNTIME_HISTORY_D must be at least 2");
    assert!(
        d < n,
        "RECONCILE_RUNTIME_HISTORY_D must be smaller than the store"
    );
    let stride = n / (d + 1);
    assert!(stride > 0, "divergent-key stride must make progress");
    (1..=d).map(|i| (stride * i) as u64).collect()
}

fn value_state(store: &ReplicatedMap<u64, u64>) -> Vec<(u64, State<u64>)> {
    store
        .value_snapshot()
        .iter()
        .map(|(&key, state)| (key, state.clone()))
        .collect()
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
    assert_eq!(
        left.len(),
        right.len(),
        "pre-heal raw key sets differ in size"
    );
    left.iter()
        .zip(right.iter())
        .filter_map(|((left_key, left_value), (right_key, right_value))| {
            assert_eq!(left_key, right_key, "pre-heal raw key sets differ");
            (left_value != right_value).then_some(*left_key)
        })
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

fn apply_superseded_history(
    store: &ReplicatedMap<u64, u64>,
    keys: &[u64],
    history_ops: usize,
    salt: u64,
) {
    assert!(!keys.is_empty());
    let mut remaining = history_ops;
    let mut generation = 0u64;

    while remaining > 0 {
        let take = remaining.min(keys.len());
        let active = &keys[..take];
        if generation % 2 == 0 {
            let updates: Vec<_> = active
                .iter()
                .map(|&key| {
                    (
                        key,
                        key.wrapping_mul(2_654_435_761)
                            ^ salt
                            ^ generation.wrapping_mul(0x9e37_79b9),
                    )
                })
                .collect();
            store.insert_bulk(&updates);
        } else {
            store.remove_bulk(active);
        }
        remaining -= take;
        generation = generation.wrapping_add(1);
    }

    // The fixed normalization makes the current value-only state independent of history length.
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
    keys: &[u64],
    history_ops: usize,
    policy: &dyn RefinementPolicy,
    reference_value_states: &mut Option<(Vec<(u64, State<u64>)>, Vec<(u64, State<u64>)>)>,
    reference_pre_heal: &mut Option<CostSignature>,
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

    let split = keys.len() / 2;
    let (left_keys, right_keys) = keys.split_at(split);
    let left_history = history_ops / 2 + history_ops % 2;
    let right_history = history_ops / 2;
    apply_superseded_history(&pair.left.store, left_keys, left_history, 0xa11c_e001);
    apply_superseded_history(&pair.right.store, right_keys, right_history, 0xb22d_e002);

    // Let detached eager-broadcast tasks observe the blocked transport before it is healed.
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert_eq!(pair.left.store.len(), n - left_keys.len());
    assert_eq!(pair.right.store.len(), n - right_keys.len());
    assert_eq!(tombstone_count(&pair.left.store), left_keys.len());
    assert_eq!(tombstone_count(&pair.right.store), right_keys.len());

    let left_values = value_state(&pair.left.store);
    let right_values = value_state(&pair.right.store);
    if let Some((reference_left, reference_right)) = reference_value_states {
        assert_eq!(&left_values, reference_left);
        assert_eq!(&right_values, reference_right);
    } else {
        *reference_value_states = Some((left_values, right_values));
    }

    let left_raw = pair.left.store.snapshot();
    let right_raw = pair.right.store.snapshot();
    assert_eq!(
        raw_diff_keys(&left_raw, &right_raw),
        keys,
        "history changed the current divergent-key set"
    );
    let pre_heal_cost = counted_reconcile(&left_raw, &right_raw, policy);
    let pre_heal_signature = CostSignature::from(&pre_heal_cost);
    if let Some(reference) = reference_pre_heal {
        assert_eq!(
            &pre_heal_signature, reference,
            "history changed the deterministic RBSR trace"
        );
    } else {
        *reference_pre_heal = Some(pre_heal_signature);
    }

    let (shutdown, left_task, right_task) = run_pair(&pair).await;
    tokio::time::sleep(BLOCKED_GC_WINDOW).await;
    assert_eq!(
        tombstone_count(&pair.left.store),
        left_keys.len(),
        "left GC collected tombstones without the causal peer's ack"
    );
    assert_eq!(
        tombstone_count(&pair.right.store),
        right_keys.len(),
        "right GC collected tombstones without the causal peer's ack"
    );

    pair.left.traffic.reset();
    pair.right.traffic.reset();
    let started = Instant::now();
    pair.left.blocked.store(false, Ordering::Relaxed);
    pair.right.blocked.store(false, Ordering::Relaxed);

    wait_until(
        || {
            pair.left.store.fingerprint(..) == pair.right.store.fingerprint(..)
                && tombstone_count(&pair.left.store) == keys.len()
                && tombstone_count(&pair.right.store) == keys.len()
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
    assert_eq!(pair.left.store.len(), n - keys.len());
    assert_eq!(pair.right.store.len(), n - keys.len());

    wait_until(
        || {
            tombstone_count(&pair.left.store) == 0
                && tombstone_count(&pair.right.store) == 0
                && pair.left.store.snapshot().len() == n - keys.len()
                && pair.right.store.snapshot().len() == n - keys.len()
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
            "history changed the post-GC steady-state RBSR trace"
        );
    } else {
        *reference_post_gc = Some(post_gc_signature);
    }

    println!(
        "[runtime-history-catchup] h={history_ops:>10} | pre-heal refine={:>7} B, messages={:>3}, ranges={:>5}, idlist={:>4} elem | catch-up={:>8.3} ms, wire={:>8} B/{:>4} dg | GC complete={:>8.3} ms, wire+acks={:>8} B/{:>4} dg | raw {} -> {}",
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
        n,
        n - keys.len(),
    );

    stop_pair(shutdown, left_task, right_task).await;
}

async fn report() {
    let n = env_usize("RECONCILE_RUNTIME_HISTORY_N", DEFAULT_N);
    let d = env_usize("RECONCILE_RUNTIME_HISTORY_D", DEFAULT_D);
    let histories = history_sizes();
    assert!(
        !histories.is_empty(),
        "at least one history size is required"
    );

    let keys = divergent_keys(n, d);
    let policy = FixedFanOut::new(FanOut::NEGENTROPY);
    let mut reference_value_states = None;
    let mut reference_pre_heal = None;
    let mut reference_post_gc = None;

    println!(
        "[runtime-history-catchup] n={n} d={d}; h counts superseded writes across both authoritative peers"
    );
    for history_ops in histories {
        scenario(
            n,
            &keys,
            history_ops,
            &policy,
            &mut reference_value_states,
            &mut reference_pre_heal,
            &mut reference_post_gc,
        )
        .await;
    }
}

fn main() {
    Runtime::new().expect("Tokio runtime").block_on(report());
}
