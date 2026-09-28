// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// Validate the finite-flight analytical transport model against gossip::NetemTransport.
//
// This is deliberately a causal-trace replay, not the full ReplicatedMap runtime: the latter adds
// reconcile cadence, membership/discovery and runtime scheduling that are outside the transport
// projection validated here. The replay uses exactly the CausalFlight decomposition consumed by the model.
//
// A 16-byte validation header identifies flight/attempt/chunk. The same 16 bytes are declared as
// frame overhead in the analytical model, so no-loss frame and wire-byte counts must match exactly.
//
// Run with `cargo bench --bench netem_trace_validation`.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use devkit::corpus::mutation::{corpus, Scenario};
use devkit::protocol_cost::reconcile_traced;
use devkit::transport_model::{
    causal_flights, estimate_finite_trace, FlightDirection, LinkProfile, Reliability,
    TransportProfile,
};
use gossip::netem::{Link, Netem, NetemTransport, Probability, Rtt, Seed};
use gossip::{InMemoryNetwork, InMemoryTransport, Transport};
use rand::rngs::StdRng;
use rand::SeedableRng;
use rbsr::{FanOut, FixedFanOut};
use rsos::FingerprintTreeMap;
use tokio::time::Instant;

const N: usize = 100_000;
const D: usize = 1_000;
const CORPUS_SEED: u64 = 42;
const SESSION_SEED: u64 = 42;
const SYMBOL_BYTES: usize = 16;
const MTU: usize = 1_200;
const HEADER_BYTES: usize = 16;
const PAYLOAD_CAPACITY: usize = MTU - HEADER_BYTES;
const VALIDATION_MBPS: f64 = 1_000_000_000.0;

#[derive(Clone, Copy)]
struct ValidationCase {
    name: &'static str,
    rtt_ms: f64,
    loss: f64,
    reorder: f64,
    trials: usize,
    tolerance: f64,
}

const CASES: &[ValidationCase] = &[
    ValidationCase {
        name: "low-rtt-clean",
        rtt_ms: 10.0,
        loss: 0.0,
        reorder: 0.0,
        trials: 5,
        tolerance: 0.20,
    },
    ValidationCase {
        name: "high-rtt-clean",
        rtt_ms: 100.0,
        loss: 0.0,
        reorder: 0.0,
        trials: 3,
        tolerance: 0.12,
    },
    ValidationCase {
        name: "moderate-rtt-loss",
        rtt_ms: 50.0,
        loss: 0.05,
        reorder: 0.0,
        trials: 30,
        tolerance: 0.35,
    },
    ValidationCase {
        name: "moderate-rtt-reorder",
        rtt_ms: 50.0,
        loss: 0.0,
        reorder: 0.05,
        trials: 30,
        tolerance: 0.35,
    },
];

struct Replay {
    elapsed_ms: f64,
    sent_frames: u64,
    sent_wire_bytes: u64,
    dropped_frames: u64,
}

fn map(rows: &[(u64, u64)]) -> FingerprintTreeMap<u64, u64> {
    let mut map = FingerprintTreeMap::new();
    for &(key, value) in rows {
        map.insert(key, value);
    }
    map
}

fn trace() -> devkit::experiment::RepairTrace {
    let corpus = corpus(N, D, Scenario::InsertOutsideRange, CORPUS_SEED);
    let left = map(&corpus.left);
    let right = map(&corpus.right);
    let mut seen = BTreeSet::new();
    let mut price = |key| {
        seen.insert(key);
        vec![SYMBOL_BYTES]
    };
    let mut rng = StdRng::seed_from_u64(SESSION_SEED);
    let (_cost, trace) = reconcile_traced(
        &left,
        &right,
        &FixedFanOut::new(FanOut::NEGENTROPY),
        Some(&mut price),
        &mut rng,
    );
    let recovered: Vec<_> = seen
        .into_iter()
        .filter(|key| left.get(key) != right.get(key))
        .collect();
    assert_eq!(recovered, corpus.expected_diff);
    trace
}

fn model_link(case: ValidationCase) -> LinkProfile {
    LinkProfile {
        name: case.name,
        rtt_ms: case.rtt_ms,
        forward_mbps: VALIDATION_MBPS,
        reverse_mbps: VALIDATION_MBPS,
        mtu_bytes: MTU,
        packet_loss: case.loss,
        packet_reorder: case.reorder,
    }
}

fn model_transport() -> TransportProfile {
    TransportProfile {
        name: "netem-validation-datagram",
        frame_overhead_bytes: HEADER_BYTES,
        handshake_rtts: 0.0,
        reliability: Reliability::DatagramRetry,
    }
}

fn netem_link(case: ValidationCase) -> Link {
    Link::at(Rtt::from_millis(case.rtt_ms))
        .with_loss(Probability::percent(case.loss * 100.0))
        .with_reorder(Probability::percent(case.reorder * 100.0))
}

fn header(flight: u32, attempt: u32, chunk: u32, chunks: u32) -> [u8; HEADER_BYTES] {
    let mut bytes = [0u8; HEADER_BYTES];
    bytes[0..4].copy_from_slice(&flight.to_le_bytes());
    bytes[4..8].copy_from_slice(&attempt.to_le_bytes());
    bytes[8..12].copy_from_slice(&chunk.to_le_bytes());
    bytes[12..16].copy_from_slice(&chunks.to_le_bytes());
    bytes
}

fn parse_header(bytes: &[u8]) -> Option<(u32, u32, u32, u32)> {
    (bytes.len() >= HEADER_BYTES).then(|| {
        (
            u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
        )
    })
}

async fn receive_attempt(
    receiver: &NetemTransport<InMemoryTransport>,
    flight: u32,
    attempt: u32,
    chunks: usize,
    patience: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + patience;
    let mut seen = vec![false; chunks];
    let mut received = 0usize;
    let mut buf = vec![0u8; MTU];

    while received < chunks {
        let Ok(result) = tokio::time::timeout_at(deadline, receiver.recv_from(&mut buf)).await
        else {
            return false;
        };
        let (n, _) = result.expect("in-memory netem receive");
        let Some((got_flight, got_attempt, chunk, declared_chunks)) = parse_header(&buf[..n])
        else {
            continue;
        };
        if got_flight != flight
            || got_attempt != attempt
            || declared_chunks as usize != chunks
            || chunk as usize >= chunks
        {
            continue;
        }
        if !seen[chunk as usize] {
            seen[chunk as usize] = true;
            received += 1;
        }
    }
    true
}

async fn send_attempt(
    sender: &NetemTransport<InMemoryTransport>,
    destination: SocketAddr,
    flight: u32,
    attempt: u32,
    app_bytes: usize,
) -> (u64, u64) {
    let chunks = app_bytes.div_ceil(PAYLOAD_CAPACITY);
    let mut remaining = app_bytes;
    let mut frames = 0u64;
    let mut wire = 0u64;

    for chunk in 0..chunks {
        let payload = remaining.min(PAYLOAD_CAPACITY);
        remaining -= payload;
        let mut bytes = Vec::with_capacity(HEADER_BYTES + payload);
        bytes.extend_from_slice(&header(flight, attempt, chunk as u32, chunks as u32));
        bytes.resize(HEADER_BYTES + payload, 0);
        sender
            .send_to(&bytes, &destination)
            .await
            .expect("send validation datagram");
        frames += 1;
        wire += bytes.len() as u64;
    }
    (frames, wire)
}

async fn replay_once(
    trace: &devkit::experiment::RepairTrace,
    case: ValidationCase,
    seed: u64,
) -> Replay {
    let network = InMemoryNetwork::new();
    let left_addr: SocketAddr = "127.9.0.1:9401".parse().unwrap();
    let right_addr: SocketAddr = "127.9.0.2:9401".parse().unwrap();
    let link = netem_link(case);

    let left = NetemTransport::new(
        Arc::new(network.bind(left_addr)),
        Netem::uniform(link, Seed::new(seed)),
    );
    let right = NetemTransport::new(
        Arc::new(network.bind(right_addr)),
        Netem::uniform(link, Seed::new(seed)),
    );
    let left_impairments = left.impairments();
    let right_impairments = right.impairments();

    let one_way = Rtt::from_millis(case.rtt_ms).one_way();
    let patience = if case.reorder > 0.0 {
        one_way * 2 + Duration::from_millis(2)
    } else {
        one_way + Duration::from_millis(2)
    };

    let started = Instant::now();
    let mut sent_frames = 0u64;
    let mut sent_wire_bytes = 0u64;

    for (flight_index, flight) in causal_flights(trace).into_iter().enumerate() {
        let (sender, receiver, destination) = match flight.direction {
            FlightDirection::Forward => (&left, &right, right_addr),
            FlightDirection::Reverse => (&right, &left, left_addr),
        };
        let chunks = flight.app_bytes.div_ceil(PAYLOAD_CAPACITY);
        let mut attempt = 0u32;
        loop {
            let (frames, wire) = send_attempt(
                sender,
                destination,
                flight_index as u32,
                attempt,
                flight.app_bytes,
            )
            .await;
            sent_frames += frames;
            sent_wire_bytes += wire;

            if receive_attempt(receiver, flight_index as u32, attempt, chunks, patience).await {
                break;
            }
            attempt += 1;
            assert!(attempt < 100, "validation flight failed to make progress");
        }
    }

    let offered = left_impairments.offered() + right_impairments.offered();
    assert_eq!(offered, sent_frames);

    Replay {
        elapsed_ms: started.elapsed().as_secs_f64() * 1_000.0,
        sent_frames,
        sent_wire_bytes,
        dropped_frames: left_impairments.dropped() + right_impairments.dropped(),
    }
}

fn relative_error(actual: f64, expected: f64) -> f64 {
    if expected == 0.0 {
        actual.abs()
    } else {
        (actual - expected).abs() / expected
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let trace = trace();

    for &case in CASES {
        let projected = estimate_finite_trace(&trace, model_link(case), model_transport());
        let mut elapsed = 0.0;
        let mut wire = 0.0;
        let mut frames = 0.0;
        let mut dropped = 0.0;

        for trial in 0..case.trials {
            let replay =
                replay_once(&trace, case, 0x5eed_0280_u64.wrapping_add(trial as u64)).await;
            elapsed += replay.elapsed_ms;
            wire += replay.sent_wire_bytes as f64;
            frames += replay.sent_frames as f64;
            dropped += replay.dropped_frames as f64;

            if case.loss == 0.0 && case.reorder == 0.0 {
                assert_eq!(
                    replay.sent_frames as usize, projected.network.packets,
                    "clean netem replay must use exactly the modeled frame count"
                );
                assert_eq!(
                    replay.sent_wire_bytes as usize, projected.network.framed_bytes,
                    "clean netem replay must use exactly the modeled framed bytes"
                );
            }
        }

        let trials = case.trials as f64;
        let observed_ms = elapsed / trials;
        let observed_wire = wire / trials;
        let observed_frames = frames / trials;
        let observed_dropped = dropped / trials;
        let timing_error = relative_error(observed_ms, projected.network.total_ms);
        let wire_error = relative_error(observed_wire, projected.network.expected_wire_bytes);

        println!(
            "[netem-validation] case={} trials={} rtt={}ms loss={:.1}% reorder={:.1}% flights={} base_frames={} model={:.2}ms observed={:.2}ms timing_error={:.1}% model_wire={:.1} observed_wire={:.1} wire_error={:.1}% observed_frames={:.2} dropped={:.2}",
            case.name,
            case.trials,
            case.rtt_ms,
            case.loss * 100.0,
            case.reorder * 100.0,
            projected.flights.len(),
            projected.network.packets,
            projected.network.total_ms,
            observed_ms,
            timing_error * 100.0,
            projected.network.expected_wire_bytes,
            observed_wire,
            wire_error * 100.0,
            observed_frames,
            observed_dropped,
        );

        // Timing includes Tokio scheduling and the validation harness's loss-detection margin.
        // Keep the assertion deliberately broader than the result we report; it should catch a
        // factor-of-two/causal-depth error, not police runner jitter.
        assert!(
            timing_error <= case.tolerance
                || (observed_ms - projected.network.total_ms).abs() <= 5.0,
            "{} timing residual {:.1}% exceeds tolerance {:.1}%",
            case.name,
            timing_error * 100.0,
            case.tolerance * 100.0,
        );
        if case.loss > 0.0 {
            assert!(
                wire_error <= 0.35,
                "{} wire residual {:.1}% exceeds stochastic tolerance",
                case.name,
                wire_error * 100.0,
            );
        }
    }
}
