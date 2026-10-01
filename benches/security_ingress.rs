// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// Runtime ingress/security cost probe for #204.
//
// Measures the shipped gossip authentication and replay-protection boundary without inventing a
// connection/session abstraction the UDP runtime does not have:
// - valid MAC verification, invalid-MAC rejection and authenticated wrong-version rejection;
// - replay-filter cost as retained authenticated-sender state grows;
// - the combined auth -> version -> replay gate for accepted traffic.
//
// Allocation/state amplification is measured separately by read_replica_fleet.
//
// Run:
//   cargo bench --bench security_ingress
//
// Overrides:
//   RECONCILE_SECURITY_ITERS=5000
//   RECONCILE_SECURITY_PAYLOADS=64,1024,8192
//   RECONCILE_SECURITY_REPLAY_POPULATIONS=1,128,1024

use std::hint::black_box;
use std::net::{IpAddr, Ipv4Addr};
use std::time::{Duration, Instant};

use chrono::Utc;
use gossip::auth::{Authenticator, ClusterKey, WIRE_VERSION};
use gossip::replay::{ReplayFilter, Seq, Stamp, FRESHNESS_WINDOW_DEFAULT};

const KEY_BYTES: [u8; 32] = [0x5a; 32];

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

fn sender_ip(index: usize) -> IpAddr {
    assert!(index < 0x00ff_fffe);
    IpAddr::V4(Ipv4Addr::from(0x0a00_0001u32 + index as u32))
}

fn now_stamp() -> Stamp {
    Stamp::new(Utc::now().timestamp_millis().max(0) as u64)
}

fn keyed_authenticator() -> Authenticator {
    Authenticator::new(Some(ClusterKey::new(KEY_BYTES)), false).expect("MAC mode is available")
}

fn wrong_version_datagram(seq: u64, stamp: Stamp, payload: &[u8]) -> Vec<u8> {
    let mut protected = Vec::with_capacity(16 + 1 + payload.len());
    protected.extend_from_slice(&seq.to_le_bytes());
    protected.extend_from_slice(&stamp.to_le_bytes());
    protected.push(WIRE_VERSION.wrapping_add(1));
    protected.extend_from_slice(payload);

    let tag = blake3::keyed_hash(&KEY_BYTES, &protected);
    let mut framed = Vec::with_capacity(32 + protected.len());
    framed.extend_from_slice(tag.as_bytes());
    framed.extend_from_slice(&protected);
    framed
}

fn time_loop(iters: usize, mut f: impl FnMut()) -> f64 {
    let start = Instant::now();
    for _ in 0..iters {
        f();
    }
    start.elapsed().as_nanos() as f64 / iters as f64
}

fn auth_points(iters: usize, payload_size: usize) {
    let auth = keyed_authenticator();
    let stamp = now_stamp();
    let payload = vec![0x42; payload_size];
    let valid = auth.seal(Seq::new(1), stamp, &payload);

    let valid_ns = time_loop(iters, || {
        let opened = auth.open(black_box(&valid)).expect("valid MAC");
        black_box(opened);
    });

    let mut invalid = valid.clone();
    invalid[0] ^= 0x80;
    let invalid_ns = time_loop(iters, || {
        assert!(auth.open(black_box(&invalid)).is_none());
    });

    let wrong_version = wrong_version_datagram(1, stamp, &payload);
    let wrong_version_ns = time_loop(iters, || {
        let opened = auth
            .open(black_box(&wrong_version))
            .expect("wrong version remains correctly authenticated");
        assert!(opened.check_version().is_err());
    });

    println!(
        "[security-auth] payload_bytes={payload_size},iters={iters},valid_mac_ns={valid_ns:.1},bad_mac_ns={invalid_ns:.1},wrong_version_ns={wrong_version_ns:.1},framed_bytes={}",
        valid.len()
    );
}

fn populated_filter(population: usize, stamp: Stamp) -> ReplayFilter {
    let filter = ReplayFilter::new(FRESHNESS_WINDOW_DEFAULT, true)
        .with_max_senders(population.saturating_add(1));
    for index in 0..population {
        assert!(filter.check_and_record(sender_ip(index), Seq::new(1), stamp));
    }
    assert_eq!(filter.len(), population);
    filter
}

fn replay_points(iters: usize, population: usize) {
    assert!(population > 0);
    let stamp = now_stamp();
    let sender = sender_ip(0);

    let accepted = populated_filter(population, stamp);
    let mut seq = 2u64;
    let accepted_ns = time_loop(iters, || {
        assert!(accepted.check_and_record(sender, Seq::new(seq), stamp));
        seq += 1;
    });

    let duplicate = populated_filter(population, stamp);
    let duplicate_ns = time_loop(iters, || {
        assert!(!duplicate.check_and_record(sender, Seq::new(1), stamp));
    });

    println!(
        "[security-replay] retained_senders={population},iters={iters},accepted_ns={accepted_ns:.1},duplicate_ns={duplicate_ns:.1}"
    );
}

fn full_gate_points(iters: usize, population: usize, payload_size: usize) {
    assert!(population > 0);
    let auth = keyed_authenticator();
    let stamp = now_stamp();
    let sender = sender_ip(0);
    let payload = vec![0x37; payload_size];

    let frames: Vec<_> = (0..iters)
        .map(|index| auth.seal(Seq::new(index as u64 + 2), stamp, &payload))
        .collect();
    let filter = populated_filter(population, stamp);

    let start = Instant::now();
    for frame in &frames {
        let opened = auth
            .open(black_box(frame))
            .expect("pre-generated frame authenticates")
            .check_version()
            .expect("current wire version");
        assert!(opened.verify_replay(&filter, sender).is_some());
    }
    let ns_per_op = start.elapsed().as_nanos() as f64 / iters as f64;

    let duplicate = auth.seal(Seq::new(1), stamp, &payload);
    let filter = populated_filter(population, stamp);
    let duplicate_ns = time_loop(iters, || {
        let opened = auth
            .open(black_box(&duplicate))
            .expect("duplicate still authenticates")
            .check_version()
            .expect("current wire version");
        assert!(opened.verify_replay(&filter, sender).is_none());
    });

    println!(
        "[security-full-gate] retained_senders={population},payload_bytes={payload_size},iters={iters},accepted_ns={ns_per_op:.1},replayed_ns={duplicate_ns:.1},framed_bytes={}",
        duplicate.len()
    );
}

fn main() {
    let iters = env_usize("RECONCILE_SECURITY_ITERS", 5_000);
    assert!(iters > 0);
    let payloads = env_list("RECONCILE_SECURITY_PAYLOADS", "64,1024,8192");
    let populations = env_list("RECONCILE_SECURITY_REPLAY_POPULATIONS", "1,128,1024");

    for payload_size in payloads {
        auth_points(iters, payload_size);
    }
    for population in populations {
        replay_points(iters, population);
        full_gate_points(iters, population, 1024);
    }

    // Keep the freshness window visible in the artifact: sender-state retention and purge cost are
    // meaningful only relative to this runtime policy.
    println!(
        "[security-policy] replay_freshness_seconds={:.0}",
        FRESHNESS_WINDOW_DEFAULT.as_secs_f64()
    );

    black_box(Duration::ZERO);
}
