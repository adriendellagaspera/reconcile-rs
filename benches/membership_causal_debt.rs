// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// Product/runtime probe for causal-stability debt.
//
// Measures shipped ReplicatedMap mechanics only:
// - bounded tombstone-ack resend traffic;
// - the retry tax of one silent authoritative member;
// - decommission work while tombstone ACK maps are populated.
//
// Run:
//   cargo bench --bench membership_causal_debt
//
// Overrides:
//   RECONCILE_CAUSAL_MEMBERS=10,100,1000
//   RECONCILE_CAUSAL_TOMBSTONES=0,100,1000
//   RECONCILE_CAUSAL_ROUNDS=4
//   RECONCILE_FAILURE_MEMBERS=1000
//   RECONCILE_FAILURE_TOMBSTONES=100

use std::collections::{HashMap, HashSet};
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use reconcile::{
    replicated_map::Config, Entry, Hlc, InMemoryNetwork, InMemoryPersistence, InMemoryTransport,
    LogicalCounter, NodeId, PersistedState, Persistence, PhysicalTime, ReplicatedMap, Timestamp,
    Transport,
};
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

const PORT: u16 = 9_873;
const CENTRAL_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 250, 1, 1));
const LONG_INTERVAL: Duration = Duration::from_secs(60 * 60);
const REPAIR_INTERVAL: Duration = Duration::from_millis(200);
const REPAIR_OBSERVATION: Duration = Duration::from_millis(1_100);
const SETTLE: Duration = Duration::from_millis(25);
const WAIT_TIMEOUT: Duration = Duration::from_secs(20);
const GC_WAIT_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone, Default)]
struct Traffic {
    rx_bytes: Arc<AtomicU64>,
    rx_datagrams: Arc<AtomicU64>,
    tx_bytes: Arc<AtomicU64>,
    tx_datagrams: Arc<AtomicU64>,
    tx_by_peer: Arc<Mutex<HashMap<IpAddr, (u64, u64)>>>,
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
        self.tx_by_peer
            .lock()
            .expect("traffic mutex poisoned")
            .clear();
    }

    fn peer_tx(&self, peer: IpAddr) -> (u64, u64) {
        self.tx_by_peer
            .lock()
            .expect("traffic mutex poisoned")
            .get(&peer)
            .copied()
            .unwrap_or_default()
    }

    fn classify_tx(&self, known_members: &HashSet<IpAddr>) -> (u64, u64, u64, u64) {
        self.tx_by_peer
            .lock()
            .expect("traffic mutex poisoned")
            .iter()
            .fold(
                (0, 0, 0, 0),
                |(known_bytes, known_dgrams, speculative_bytes, speculative_dgrams),
                 (peer, (bytes, dgrams))| {
                    if known_members.contains(peer) {
                        (
                            known_bytes + bytes,
                            known_dgrams + dgrams,
                            speculative_bytes,
                            speculative_dgrams,
                        )
                    } else {
                        (
                            known_bytes,
                            known_dgrams,
                            speculative_bytes + bytes,
                            speculative_dgrams + dgrams,
                        )
                    }
                },
            )
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
        let mut by_peer = self
            .traffic
            .tx_by_peer
            .lock()
            .expect("traffic mutex poisoned");
        let entry = by_peer.entry(dst.ip()).or_default();
        entry.0 += sent as u64;
        entry.1 += 1;
        Ok(sent)
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}

#[derive(Default)]
struct CapturePersistence {
    state: Mutex<Option<PersistedState<u64, u64>>>,
}

impl CapturePersistence {
    fn with_state(state: PersistedState<u64, u64>) -> Self {
        Self {
            state: Mutex::new(Some(state)),
        }
    }

    fn latest(&self) -> PersistedState<u64, u64> {
        self.state
            .lock()
            .expect("capture persistence mutex poisoned")
            .clone()
            .expect("captured state is present")
    }
}

impl Persistence<u64, u64> for CapturePersistence {
    fn load(&self) -> io::Result<Option<PersistedState<u64, u64>>> {
        Ok(self
            .state
            .lock()
            .expect("capture persistence mutex poisoned")
            .clone())
    }

    fn save(&self, state: &PersistedState<u64, u64>) -> io::Result<()> {
        *self
            .state
            .lock()
            .expect("capture persistence mutex poisoned") = Some(state.clone());
        Ok(())
    }
}

struct Responder {
    armed: Arc<AtomicBool>,
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

fn peer_ip(index: usize) -> IpAddr {
    assert!(index < 0x00ef_fffe);
    IpAddr::V4(Ipv4Addr::from(0x7f01_0001u32 + index as u32))
}

fn config(max_peers: usize, repair_interval: Duration) -> Config {
    Config::new(PORT)
        .with_listen_addr(CENTRAL_IP)
        .with_node_id(NodeId::new(1))
        .with_max_peers(max_peers)
        .with_remote_interval(1)
        .with_remote_fanout(max_peers)
        .with_reconcile_interval(LONG_INTERVAL)
        .with_repair_interval(repair_interval)
        .with_snapshot_interval(None)
        .with_insecure_no_key()
}

fn tombstone_entry(key: u64) -> Entry<Timestamp, u64> {
    Entry::tombstone(Timestamp::new(
        Hlc::new(
            PhysicalTime::from_millis(key.saturating_add(1)),
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

fn state_with_members(
    members: usize,
    tombstones: usize,
    ack_all: bool,
    omit_ack_for: Option<IpAddr>,
) -> PersistedState<u64, u64> {
    let member_set: HashSet<_> = (0..members).map(peer_ip).collect();
    let tombstone_acks = if ack_all {
        (0..tombstones as u64)
            .map(|key| {
                let version = key.wrapping_mul(2_654_435_761).wrapping_add(1);
                let acks = member_set
                    .iter()
                    .copied()
                    .filter(|peer| Some(*peer) != omit_ack_for)
                    .map(|peer| (peer, version))
                    .collect();
                (key, acks)
            })
            .collect()
    } else {
        HashMap::new()
    };
    PersistedState::new(entries(tombstones), member_set, tombstone_acks)
}

fn store_from_state(
    state: PersistedState<u64, u64>,
    network: &InMemoryNetwork,
    traffic: Traffic,
    repair_interval: Duration,
) -> (ReplicatedMap<u64, u64>, Arc<CapturePersistence>) {
    let backend = Arc::new(CapturePersistence::with_state(state));
    let transport = CountingTransport {
        inner: network.bind(SocketAddr::new(CENTRAL_IP, PORT)),
        traffic,
    };
    let store = ReplicatedMap::<u64, u64>::new_with_transport(
        config(200_000, repair_interval),
        Arc::new(transport),
    )
    .expect("valid benchmark store")
    .with_tombstone_timeout(Duration::ZERO)
    .with_persistence(backend.clone())
    .expect("load benchmark causal state");
    (store, backend)
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

fn tombstone_count(store: &ReplicatedMap<u64, u64>) -> usize {
    store
        .snapshot()
        .values()
        .filter(|entry| entry.is_tombstone())
        .count()
}

async fn resend_point(members: usize, tombstones: usize, rounds: usize) {
    let network = InMemoryNetwork::new();
    let traffic = Traffic::default();
    let initial = PersistedState::new(entries(tombstones), HashSet::new(), HashMap::new());
    let (store, _) = store_from_state(initial, &network, traffic.clone(), LONG_INTERVAL);
    for index in 0..members {
        store.seed_peer(peer_ip(index));
    }

    let known_members: HashSet<_> = (0..members).map(peer_ip).collect();
    for round in 1..=rounds {
        traffic.reset();
        let started = Instant::now();
        store.start_reconciliation().await;
        let elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0;
        let wire = traffic.snapshot();
        let (known_bytes, known_dgrams, speculative_bytes, speculative_dgrams) =
            traffic.classify_tx(&known_members);
        let bytes_per_member = known_bytes as f64 / members as f64;
        println!(
            "[causal-resend] members={members},tombstones={tombstones},round={round},tx_bytes={},tx_dgrams={},known_member_tx_bytes={known_bytes},known_member_tx_dgrams={known_dgrams},speculative_tx_bytes={speculative_bytes},speculative_tx_dgrams={speculative_dgrams},bytes_per_member={bytes_per_member:.3},round_ms={elapsed_ms:.3}",
            wire.tx_bytes, wire.tx_datagrams
        );
    }
}

fn spawn_responder(
    network: &InMemoryNetwork,
    addr: IpAddr,
    shutdown: CancellationToken,
) -> Responder {
    let transport = Arc::new(network.bind(SocketAddr::new(addr, PORT)));
    let armed = Arc::new(AtomicBool::new(false));
    let armed_task = armed.clone();
    tokio::spawn(async move {
        let mut buf = vec![0u8; 65_536];
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                recv = transport.recv_from(&mut buf) => {
                    let Ok((size, _)) = recv else { break };
                    if armed_task.swap(false, Ordering::AcqRel) {
                        let _ = transport
                            .send_to(&buf[..size], &SocketAddr::new(CENTRAL_IP, PORT))
                            .await;
                    }
                }
            }
        }
    });
    Responder { armed }
}

async fn one_unreachable(members: usize, tombstones: usize) {
    assert!(members >= 2);
    assert!(tombstones > 0);

    let network = InMemoryNetwork::new();
    let traffic = Traffic::default();
    let initial = state_with_members(members, tombstones, false, None);
    let (store, capture) = store_from_state(initial, &network, traffic.clone(), REPAIR_INTERVAL);

    let run_shutdown = CancellationToken::new();
    let run_store = store.clone();
    let run_token = run_shutdown.clone();
    let run_task = tokio::spawn(async move {
        let _ = run_store.run(run_token).await;
    });
    wait_until(
        WAIT_TIMEOUT,
        || store.sync_state().rounds >= 1,
        "initial no-peer round",
    )
    .await;

    let responder_shutdown = CancellationToken::new();
    let mut responders = Vec::with_capacity(members);
    for index in 0..members {
        responders.push(spawn_responder(
            &network,
            peer_ip(index),
            responder_shutdown.clone(),
        ));
        store.seed_peer(peer_ip(index));
    }
    tokio::time::sleep(SETTLE).await;

    let unreachable = members - 1;
    for (index, responder) in responders.iter().enumerate() {
        responder
            .armed
            .store(index != unreachable, Ordering::Release);
    }

    traffic.reset();
    let round_started = Instant::now();
    store.start_reconciliation().await;
    wait_until(
        WAIT_TIMEOUT,
        || traffic.snapshot().rx_datagrams >= (members - 1) as u64,
        "responsive replies",
    )
    .await;
    tokio::time::sleep(SETTLE).await;
    let round_ms = round_started.elapsed().as_secs_f64() * 1_000.0;
    let round = traffic.snapshot();

    store.snapshot_now().expect("capture ACK state");
    let captured = capture.latest();
    assert_eq!(captured.members.len(), members);
    assert_eq!(captured.tombstone_acks.len(), tombstones);
    assert!(captured
        .tombstone_acks
        .values()
        .all(|acks| acks.len() == members - 1));
    assert!(captured
        .tombstone_acks
        .values()
        .all(|acks| !acks.contains_key(&peer_ip(unreachable))));

    traffic.reset();
    tokio::time::sleep(REPAIR_OBSERVATION).await;
    let repair = traffic.snapshot();
    let (unreachable_repair_bytes, unreachable_repair_dgrams) =
        traffic.peer_tx(peer_ip(unreachable));
    assert_eq!(
        unreachable_repair_dgrams, 4,
        "the silent member must receive exactly the bounded four repair retries"
    );
    let known_members: HashSet<_> = (0..members).map(peer_ip).collect();
    let (
        _known_member_repair_bytes,
        known_member_repair_dgrams,
        speculative_repair_bytes,
        speculative_repair_dgrams,
    ) = traffic.classify_tx(&known_members);
    let responsive_member_repair_dgrams =
        known_member_repair_dgrams.saturating_sub(unreachable_repair_dgrams);
    assert_eq!(
        responsive_member_repair_dgrams, 0,
        "responsive known members must clear their pending repairs"
    );

    assert_eq!(tombstone_count(&store), tombstones);

    let started = Instant::now();
    store.forget_peer(peer_ip(0));
    let forget_acked_ms = started.elapsed().as_secs_f64() * 1_000.0;

    let started = Instant::now();
    store.forget_peer(peer_ip(unreachable));
    let forget_unacked_ms = started.elapsed().as_secs_f64() * 1_000.0;

    store.snapshot_now().expect("capture post-forget state");
    let captured = capture.latest();
    assert_eq!(captured.members.len(), members - 2);
    assert!(captured
        .tombstone_acks
        .values()
        .all(|acks| acks.len() == members - 2));

    let gc_started = Instant::now();
    wait_until(
        GC_WAIT_TIMEOUT,
        || tombstone_count(&store) == 0,
        "GC after removing the silent member",
    )
    .await;
    let gc_after_forget_ms = gc_started.elapsed().as_secs_f64() * 1_000.0;

    println!(
        "[causal-unreachable] members={members},tombstones={tombstones},responsive={},round_tx_bytes={},round_tx_dgrams={},round_rx_bytes={},round_rx_dgrams={},round_ms={round_ms:.3},repair_tx_bytes={},repair_tx_dgrams={},unreachable_repair_tx_bytes={unreachable_repair_bytes},unreachable_repair_tx_dgrams={unreachable_repair_dgrams},responsive_member_repair_tx_dgrams={responsive_member_repair_dgrams},speculative_repair_tx_bytes={speculative_repair_bytes},speculative_repair_tx_dgrams={speculative_repair_dgrams},forget_acked_ms={forget_acked_ms:.3},forget_unacked_ms={forget_unacked_ms:.3},gc_after_forget_ms={gc_after_forget_ms:.3}",
        members - 1,
        round.tx_bytes,
        round.tx_datagrams,
        round.rx_bytes,
        round.rx_datagrams,
        repair.tx_bytes,
        repair.tx_datagrams,
    );

    responder_shutdown.cancel();
    run_shutdown.cancel();
    run_task.await.expect("central run task");
}

fn forget_from_state(members: usize, tombstones: usize, missing_ack: bool) -> f64 {
    let target = if missing_ack {
        peer_ip(members - 1)
    } else {
        peer_ip(0)
    };
    let state = state_with_members(members, tombstones, true, missing_ack.then_some(target));
    let backend = Arc::new(InMemoryPersistence::<u64, u64>::new());
    Persistence::<u64, u64>::save(&*backend, &state).expect("seed persistence");

    let network = InMemoryNetwork::new();
    let store = ReplicatedMap::<u64, u64>::new_with_transport(
        config(members.saturating_add(8), LONG_INTERVAL),
        Arc::new(network.bind(SocketAddr::new(CENTRAL_IP, PORT))),
    )
    .expect("valid decommission store")
    .with_persistence(backend.clone())
    .expect("load decommission state");

    let started = Instant::now();
    store.forget_peer(target);
    let elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0;

    store.snapshot_now().expect("persist decommissioned state");
    let saved = Persistence::<u64, u64>::load(&*backend)
        .expect("reload state")
        .expect("state exists");
    assert_eq!(saved.members.len(), members - 1);
    assert!(saved
        .tombstone_acks
        .values()
        .all(|acks| acks.len() == members - 1));
    elapsed_ms
}

fn main() {
    let members = env_list("RECONCILE_CAUSAL_MEMBERS", "10,100,1000");
    let tombstones = env_list("RECONCILE_CAUSAL_TOMBSTONES", "0,100,1000");
    let rounds = env_usize("RECONCILE_CAUSAL_ROUNDS", 4);
    let failure_members = env_usize("RECONCILE_FAILURE_MEMBERS", 1000);
    let failure_tombstones = env_usize("RECONCILE_FAILURE_TOMBSTONES", 100);
    let runtime = Runtime::new().expect("Tokio runtime");

    for &debt in &tombstones {
        for &fleet in &members {
            runtime.block_on(resend_point(fleet, debt, rounds));
        }
    }

    runtime.block_on(one_unreachable(failure_members, failure_tombstones));

    let largest = *members.last().expect("non-empty member sweep");
    for &debt in &tombstones {
        let acked_ms = forget_from_state(largest, debt, false);
        let unacked_ms = forget_from_state(largest, debt, true);
        println!(
            "[causal-decommission] members={largest},tombstones={debt},forget_acked_ms={acked_ms:.3},forget_unacked_ms={unacked_ms:.3}"
        );
    }
}
