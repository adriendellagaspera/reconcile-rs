// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// Runtime framing benchmark for #263. It intentionally uses only public APIs so the exact same
// harness can be run before and after framing changes.
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use gossip::netem::{Link, Netem, NetemTransport, Probability, Rtt, Seed};
use reconcile::{
    replicated_map::Config, InMemoryNetwork, InMemoryTransport, ReplicatedMap, Transport,
};
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

const PORT: u16 = 9_863;
const A_IP: IpAddr = IpAddr::V4(std::net::Ipv4Addr::new(127, 83, 0, 1));
const B_IP: IpAddr = IpAddr::V4(std::net::Ipv4Addr::new(127, 83, 0, 2));

#[derive(Clone, Default)]
struct Traffic {
    bytes: Arc<AtomicU64>,
    datagrams: Arc<AtomicU64>,
    max_datagram: Arc<AtomicUsize>,
    over_1200: Arc<AtomicU64>,
    over_1472: Arc<AtomicU64>,
    over_8972: Arc<AtomicU64>,
}

impl Traffic {
    fn record(&self, size: usize) {
        self.bytes.fetch_add(size as u64, Ordering::Relaxed);
        self.datagrams.fetch_add(1, Ordering::Relaxed);
        self.max_datagram.fetch_max(size, Ordering::Relaxed);
        if size > 1_200 {
            self.over_1200.fetch_add(1, Ordering::Relaxed);
        }
        if size > 1_472 {
            self.over_1472.fetch_add(1, Ordering::Relaxed);
        }
        if size > 8_972 {
            self.over_8972.fetch_add(1, Ordering::Relaxed);
        }
    }
}

struct CountingTransport<T> {
    inner: T,
    traffic: Traffic,
}

#[async_trait]
impl<T: Transport> Transport for CountingTransport<T> {
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.inner.recv_from(buf).await
    }

    async fn send_to(&self, buf: &[u8], dst: &SocketAddr) -> io::Result<usize> {
        self.traffic.record(buf.len());
        self.inner.send_to(buf, dst).await
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}

#[derive(Clone, Copy)]
struct Profile {
    name: &'static str,
    rtt_ms: f64,
    loss_percent: f64,
    reorder_percent: f64,
}

const PROFILES: &[Profile] = &[
    Profile {
        name: "lan-clean",
        rtt_ms: 1.0,
        loss_percent: 0.0,
        reorder_percent: 0.0,
    },
    Profile {
        name: "wan-loss-1",
        rtt_ms: 50.0,
        loss_percent: 1.0,
        reorder_percent: 0.0,
    },
    Profile {
        name: "wan-reorder-5",
        rtt_ms: 50.0,
        loss_percent: 0.0,
        reorder_percent: 5.0,
    },
];

fn value_sizes() -> Vec<usize> {
    std::env::var("RECONCILE_FRAGMENT_VALUE_SIZES")
        .unwrap_or_else(|_| "512,4096,131072".to_owned())
        .split(',')
        .map(|raw| raw.trim().parse().expect("value sizes must be usize"))
        .collect()
}

fn timeout_duration() -> Duration {
    let millis = std::env::var("RECONCILE_FRAGMENT_TIMEOUT_MS")
        .unwrap_or_else(|_| "5000".to_owned())
        .parse()
        .expect("RECONCILE_FRAGMENT_TIMEOUT_MS must be u64");
    Duration::from_millis(millis)
}

fn config(ip: IpAddr) -> Config {
    Config::new(PORT)
        .with_listen_addr(ip)
        .with_reconcile_interval(Duration::from_millis(100))
        .with_repair_interval(Duration::from_millis(150))
        .with_insecure_no_key()
}

fn link(profile: Profile) -> Link {
    Link::at(Rtt::from_millis(profile.rtt_ms))
        .with_loss(Probability::percent(profile.loss_percent))
        .with_reorder(Probability::percent(profile.reorder_percent))
}

async fn run_case(profile: Profile, value_len: usize) {
    let network = InMemoryNetwork::new();
    let a_inner: Arc<InMemoryTransport> =
        Arc::new(network.bind(SocketAddr::new(A_IP, PORT)));
    let b_inner: Arc<InMemoryTransport> =
        Arc::new(network.bind(SocketAddr::new(B_IP, PORT)));

    let a_netem = NetemTransport::new(
        a_inner,
        Netem::uniform(link(profile), Seed::new(0x2630_0001)),
    );
    let b_netem = NetemTransport::new(
        b_inner,
        Netem::uniform(link(profile), Seed::new(0x2630_0002)),
    );
    let a_impairments = a_netem.impairments();
    let b_impairments = b_netem.impairments();
    let a_traffic = Traffic::default();
    let b_traffic = Traffic::default();

    let a = ReplicatedMap::<u64, Vec<u8>>::new_with_transport(
        config(A_IP),
        Arc::new(CountingTransport {
            inner: a_netem,
            traffic: a_traffic.clone(),
        }),
    )
    .expect("valid source store");
    let b = ReplicatedMap::<u64, Vec<u8>>::new_with_transport(
        config(B_IP),
        Arc::new(CountingTransport {
            inner: b_netem,
            traffic: b_traffic.clone(),
        }),
    )
    .expect("valid destination store");

    a.load_bulk(&[(7, vec![0x5a; value_len])]);
    let target = a.fingerprint(..);
    b.seed_peer(A_IP);

    let shutdown = CancellationToken::new();
    let ta = tokio::spawn(a.clone().run(shutdown.clone()));
    let tb = tokio::spawn(b.clone().run(shutdown.clone()));
    let started = Instant::now();
    let converged = tokio::time::timeout(timeout_duration(), async {
        while b.fingerprint(..) != target {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .is_ok();
    let elapsed = started.elapsed();

    shutdown.cancel();
    let _ = tokio::join!(ta, tb);

    let wire_bytes =
        a_traffic.bytes.load(Ordering::Relaxed) + b_traffic.bytes.load(Ordering::Relaxed);
    let datagrams = a_traffic.datagrams.load(Ordering::Relaxed)
        + b_traffic.datagrams.load(Ordering::Relaxed);
    let max_datagram = a_traffic
        .max_datagram
        .load(Ordering::Relaxed)
        .max(b_traffic.max_datagram.load(Ordering::Relaxed));
    let over_1200 =
        a_traffic.over_1200.load(Ordering::Relaxed) + b_traffic.over_1200.load(Ordering::Relaxed);
    let over_1472 =
        a_traffic.over_1472.load(Ordering::Relaxed) + b_traffic.over_1472.load(Ordering::Relaxed);
    let over_8972 =
        a_traffic.over_8972.load(Ordering::Relaxed) + b_traffic.over_8972.load(Ordering::Relaxed);
    let dropped = a_impairments.dropped() + b_impairments.dropped();
    let offered = a_impairments.offered() + b_impairments.offered();

    println!(
        "[fragmentation-runtime] profile={},rtt_ms={:.1},loss_percent={:.1},reorder_percent={:.1},value_bytes={},converged={},elapsed_ms={:.3},wire_bytes={},datagrams={},max_datagram_bytes={},over_1200={},over_1472={},over_8972={},netem_offered={},netem_dropped={}",
        profile.name,
        profile.rtt_ms,
        profile.loss_percent,
        profile.reorder_percent,
        value_len,
        converged,
        elapsed.as_secs_f64() * 1_000.0,
        wire_bytes,
        datagrams,
        max_datagram,
        over_1200,
        over_1472,
        over_8972,
        offered,
        dropped,
    );
}

fn main() {
    let runtime = Runtime::new().expect("Tokio runtime");
    runtime.block_on(async {
        for &profile in PROFILES {
            for value_len in value_sizes() {
                run_case(profile, value_len).await;
            }
        }
    });
}
