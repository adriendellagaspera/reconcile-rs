// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Deterministic fragment-recovery model shared by the benchmark and its integration tests.
//!
//! This is deliberately benchmark-only. The selective-control frames below are accounting shapes,
//! not a production wire format.

use std::time::Duration;

use gossip::auth::{Authenticator, ClusterKey};
use gossip::framing::{
    complete_payload_capacity, fragment_payload_capacity, COMPLETE_HEADER_LEN, FRAGMENT_HEADER_LEN,
};

const CONTROL_TAG_LEN: usize = 1;
const TRANSFER_ID_LEN: usize = 32;
const FRAGMENT_COUNT_LEN: usize = 4;
const DATA_DOMAIN: u64 = 0x6461_7461_2710_0001;
const CONTROL_DOMAIN: u64 = 0x6374_726c_2710_0001;
const MAX_RECOVERY_ROUNDS: usize = 10_000;

/// Recovery strategy compared by the benchmark.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryPolicy {
    /// Re-send every data unit after an incomplete flight, while the receiver reuses progress.
    WholeRetry,
    /// Send a bitmap NACK and re-send only data units still missing.
    MissingOnly,
}

impl RecoveryPolicy {
    /// Stable output label.
    pub fn label(self) -> &'static str {
        match self {
            Self::WholeRetry => "whole_retry",
            Self::MissingOnly => "missing_only",
        }
    }
}

/// One deterministic isolated-transfer experiment.
#[derive(Clone, Copy, Debug)]
pub struct Case {
    /// Desired number of production data frames: one complete frame, or N fragmented frames.
    pub fragment_count: usize,
    /// Maximum authenticated UDP payload.
    pub datagram_payload_budget: usize,
    /// Round-trip propagation time.
    pub rtt: Duration,
    /// Independent loss probability, applied to data and benchmark-only control datagrams.
    pub loss_percent: f64,
    /// Symmetric serialization rate.
    pub bandwidth_bps: u64,
    /// Reproducible loss seed.
    pub seed: u64,
}

/// Metrics from one policy/case/seed.
#[derive(Clone, Debug, PartialEq)]
pub struct Metrics {
    /// Logical application bytes delivered.
    pub useful_bytes: usize,
    /// Total data plus control bytes put on the wire, including framing/auth overhead.
    pub wire_bytes: u64,
    /// Data wire bytes sent after the first flight.
    pub retransmitted_wire_bytes: u64,
    /// Benchmark-only NACK/ACK wire bytes.
    pub control_bytes: u64,
    /// Number of data plus control datagrams offered.
    pub datagrams: u64,
    /// Incomplete data flights before the completing flight.
    pub recovery_rounds: usize,
    /// Useful bytes absent after the initial data flight.
    pub initial_missing_useful_bytes: usize,
    /// Time until the receiver owns the complete logical payload.
    pub receiver_completion: Duration,
    /// Equal to receiver completion in this isolated-transfer model.
    pub domain_convergence: Duration,
    /// Time until no policy-specific sender recovery/control state remains.
    pub sender_quiescence: Duration,
    /// Peak extra sender state retained specifically for recovery.
    pub peak_sender_recovery_state_bytes: usize,
    /// Peak unique useful bytes retained in incomplete receiver reassembly state.
    pub peak_receiver_reassembly_state_bytes: usize,
}

#[derive(Clone, Copy, Debug)]
struct DataUnit {
    useful_len: usize,
    wire_len: usize,
}

/// Authentication overhead used by the research model: the normal keyed, non-encrypted mode.
pub fn authenticated_overhead() -> usize {
    Authenticator::new(Some(ClusterKey::new([0x27; 32])), false)
        .expect("non-encrypted authentication is always available")
        .overhead()
}

/// Logical size that yields exactly `fragment_count` production data frames at this budget.
///
/// A one-frame case uses the real complete-frame path. N > 1 deliberately crosses the complete
/// payload threshold, then fills enough fragment payload to produce exactly N fragments.
pub fn logical_bytes_for_fragments(fragment_count: usize, datagram_payload_budget: usize) -> usize {
    assert!(fragment_count > 0, "fragment_count must be non-zero");
    let auth = authenticated_overhead();
    let complete = complete_payload_capacity(datagram_payload_budget, auth)
        .expect("datagram budget must fit complete-frame overhead");
    if fragment_count == 1 {
        return complete.max(1);
    }
    let fragment = fragment_payload_capacity(datagram_payload_budget, auth)
        .expect("datagram budget must fit fragment overhead");
    assert!(fragment > 0, "datagram budget leaves no fragment payload");
    complete.saturating_add(1).max(
        fragment
            .saturating_mul(fragment_count - 1)
            .saturating_add(1),
    )
}

/// Simulate one isolated transfer under one recovery policy.
pub fn simulate(case: Case, policy: RecoveryPolicy) -> Metrics {
    assert!(case.bandwidth_bps > 0, "bandwidth must be non-zero");
    assert!(
        (0.0..100.0).contains(&case.loss_percent),
        "loss must be in [0, 100)"
    );

    let auth = authenticated_overhead();
    let logical_bytes =
        logical_bytes_for_fragments(case.fragment_count, case.datagram_payload_budget);
    let units = data_units(logical_bytes, case.datagram_payload_budget, auth);
    assert_eq!(
        units.len(),
        case.fragment_count,
        "logical-size helper must produce the requested frame count"
    );

    let one_way_s = case.rtt.as_secs_f64() / 2.0;
    let rtt_s = case.rtt.as_secs_f64();
    let mut received = vec![false; units.len()];
    let mut attempts = vec![0_u32; units.len()];
    let mut remaining = units.len();
    let mut retained_useful = 0_usize;
    let mut peak_receiver = 0_usize;
    let mut wire_bytes = 0_u64;
    let mut retransmitted_wire_bytes = 0_u64;
    let mut control_bytes = 0_u64;
    let mut datagrams = 0_u64;
    let mut initial_missing_useful_bytes = 0_usize;
    let mut receiver_completion_s = None;
    let mut round_start_s = 0.0_f64;
    let mut final_send_end_s = 0.0_f64;
    let mut control_seq = 0_u64;

    let peak_sender_recovery_state_bytes = match policy {
        RecoveryPolicy::WholeRetry => 0,
        RecoveryPolicy::MissingOnly => units.iter().map(|unit| unit.wire_len).sum(),
    };

    let mut completing_round = 0_usize;
    for round in 0..MAX_RECOVERY_ROUNDS {
        let selected: Vec<usize> = match policy {
            RecoveryPolicy::WholeRetry => (0..units.len()).collect(),
            RecoveryPolicy::MissingOnly => received
                .iter()
                .enumerate()
                .filter_map(|(index, present)| (!present).then_some(index))
                .collect(),
        };

        let mut send_cursor_s = round_start_s;
        for index in selected {
            let unit = units[index];
            attempts[index] += 1;
            wire_bytes += unit.wire_len as u64;
            datagrams += 1;
            if round > 0 {
                retransmitted_wire_bytes += unit.wire_len as u64;
            }
            send_cursor_s += serialization_seconds(unit.wire_len, case.bandwidth_bps);
            if data_is_lost(case, index, attempts[index]) || received[index] {
                continue;
            }

            received[index] = true;
            remaining -= 1;
            retained_useful += unit.useful_len;
            let arrival_s = send_cursor_s + one_way_s;
            if remaining == 0 {
                receiver_completion_s = Some(arrival_s);
            } else {
                peak_receiver = peak_receiver.max(retained_useful);
            }
        }
        final_send_end_s = send_cursor_s;

        if round == 0 {
            initial_missing_useful_bytes = units
                .iter()
                .zip(&received)
                .filter_map(|(unit, present)| (!present).then_some(unit.useful_len))
                .sum();
        }

        if remaining == 0 {
            completing_round = round;
            break;
        }

        let receiver_round_boundary_s = send_cursor_s + one_way_s;
        round_start_s = match policy {
            RecoveryPolicy::WholeRetry => send_cursor_s + rtt_s,
            RecoveryPolicy::MissingOnly => {
                let nack_len = nack_wire_len(units.len(), auth);
                deliver_control(
                    case,
                    nack_len,
                    receiver_round_boundary_s,
                    one_way_s,
                    rtt_s,
                    &mut control_seq,
                    &mut wire_bytes,
                    &mut control_bytes,
                    &mut datagrams,
                )
            }
        };
    }

    let receiver_completion_s =
        receiver_completion_s.expect("transfer did not complete within recovery-round bound");
    let sender_quiescence_s = match policy {
        RecoveryPolicy::WholeRetry => receiver_completion_s.max(final_send_end_s),
        RecoveryPolicy::MissingOnly => deliver_control(
            case,
            ack_wire_len(auth),
            receiver_completion_s,
            one_way_s,
            rtt_s,
            &mut control_seq,
            &mut wire_bytes,
            &mut control_bytes,
            &mut datagrams,
        ),
    };

    Metrics {
        useful_bytes: logical_bytes,
        wire_bytes,
        retransmitted_wire_bytes,
        control_bytes,
        datagrams,
        recovery_rounds: completing_round,
        initial_missing_useful_bytes,
        receiver_completion: Duration::from_secs_f64(receiver_completion_s),
        domain_convergence: Duration::from_secs_f64(receiver_completion_s),
        sender_quiescence: Duration::from_secs_f64(sender_quiescence_s),
        peak_sender_recovery_state_bytes,
        peak_receiver_reassembly_state_bytes: peak_receiver,
    }
}

fn data_units(logical_bytes: usize, datagram_payload_budget: usize, auth: usize) -> Vec<DataUnit> {
    let complete = complete_payload_capacity(datagram_payload_budget, auth)
        .expect("datagram budget must fit complete-frame overhead");
    if logical_bytes <= complete {
        return vec![DataUnit {
            useful_len: logical_bytes,
            wire_len: auth + COMPLETE_HEADER_LEN + logical_bytes,
        }];
    }

    let capacity = fragment_payload_capacity(datagram_payload_budget, auth)
        .expect("datagram budget must fit fragment overhead");
    assert!(capacity > 0, "datagram budget leaves no fragment payload");
    let mut remaining = logical_bytes;
    let mut units = Vec::new();
    while remaining > 0 {
        let useful_len = remaining.min(capacity);
        units.push(DataUnit {
            useful_len,
            wire_len: auth + FRAGMENT_HEADER_LEN + useful_len,
        });
        remaining -= useful_len;
    }
    units
}

fn nack_wire_len(fragment_count: usize, auth: usize) -> usize {
    let bitmap_bytes = fragment_count.div_ceil(8);
    auth + CONTROL_TAG_LEN + TRANSFER_ID_LEN + FRAGMENT_COUNT_LEN + bitmap_bytes
}

fn ack_wire_len(auth: usize) -> usize {
    auth + CONTROL_TAG_LEN + TRANSFER_ID_LEN
}

#[allow(clippy::too_many_arguments)]
fn deliver_control(
    case: Case,
    wire_len: usize,
    first_send_s: f64,
    one_way_s: f64,
    rtt_s: f64,
    control_seq: &mut u64,
    wire_bytes: &mut u64,
    control_bytes: &mut u64,
    datagrams: &mut u64,
) -> f64 {
    let mut send_s = first_send_s;
    loop {
        *control_seq += 1;
        *wire_bytes += wire_len as u64;
        *control_bytes += wire_len as u64;
        *datagrams += 1;
        let send_end_s = send_s + serialization_seconds(wire_len, case.bandwidth_bps);
        if !control_is_lost(case, *control_seq) {
            return send_end_s + one_way_s;
        }
        send_s += rtt_s;
    }
}

fn serialization_seconds(bytes: usize, bandwidth_bps: u64) -> f64 {
    bytes as f64 * 8.0 / bandwidth_bps as f64
}

fn data_is_lost(case: Case, unit: usize, attempt: u32) -> bool {
    sample_loss(
        case.seed,
        DATA_DOMAIN,
        unit as u64,
        attempt as u64,
        case.loss_percent,
    )
}

fn control_is_lost(case: Case, sequence: u64) -> bool {
    sample_loss(case.seed, CONTROL_DOMAIN, sequence, 0, case.loss_percent)
}

fn sample_loss(seed: u64, domain: u64, unit: u64, attempt: u64, loss_percent: f64) -> bool {
    if loss_percent <= 0.0 {
        return false;
    }
    let state = splitmix64(
        seed ^ domain
            ^ unit.wrapping_mul(0x9e37_79b9_7f4a_7c15)
            ^ attempt.wrapping_mul(0xbf58_476d_1ce4_e5b9),
    );
    let sample = state as f64 / u64::MAX as f64;
    sample < loss_percent / 100.0
}

fn splitmix64(mut state: u64) -> u64 {
    state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}
