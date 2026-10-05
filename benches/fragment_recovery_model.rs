// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Deterministic fragment-recovery model shared by the benchmark and its integration tests.
//!
//! This is deliberately benchmark-only. The selective-control and parity frames below are
//! accounting shapes, not a production wire format.

use std::time::Duration;

use gossip::auth::{Authenticator, ClusterKey};
use gossip::framing::{
    complete_payload_capacity, fragment_payload_capacity, COMPLETE_HEADER_LEN, FRAGMENT_HEADER_LEN,
};

const CONTROL_TAG_LEN: usize = 1;
const TRANSFER_ID_LEN: usize = 32;
const FRAGMENT_COUNT_LEN: usize = 4;
const PARITY_GROUP_INDEX_LEN: usize = 4;
const PARITY_GROUP_COUNT_LEN: usize = 1;
const PARITY_HEADER_LEN: usize =
    CONTROL_TAG_LEN + TRANSFER_ID_LEN + PARITY_GROUP_INDEX_LEN + PARITY_GROUP_COUNT_LEN;
const FEC_DATA_PER_GROUP: usize = 8;
const DATA_DOMAIN: u64 = 0x6461_7461_2710_0001;
const CONTROL_DOMAIN: u64 = 0x6374_726c_2710_0001;
const PARITY_DOMAIN: u64 = 0x6665_635f_2710_0001;
const MAX_RECOVERY_ROUNDS: usize = 10_000;

/// Recovery strategy compared by the benchmark.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryPolicy {
    /// Re-send every data unit after an incomplete flight, while the receiver reuses progress.
    WholeRetry,
    /// Send a bitmap NACK and re-send only data units still missing.
    MissingOnly,
    /// Send one XOR parity unit per group of at most eight data units, then fall back to NACKs.
    Xor8Plus1,
}

impl RecoveryPolicy {
    /// Stable output label.
    pub fn label(self) -> &'static str {
        match self {
            Self::WholeRetry => "whole_retry",
            Self::MissingOnly => "missing_only",
            Self::Xor8Plus1 => "xor_8_plus_1",
        }
    }
}

/// One contiguous first-flight data-fragment loss burst.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BurstLoss {
    /// First data-fragment index dropped.
    pub start_fragment: usize,
    /// Number of consecutive data fragments dropped.
    pub fragment_count: usize,
}

impl BurstLoss {
    fn contains(self, fragment: usize) -> bool {
        (self.start_fragment..self.start_fragment.saturating_add(self.fragment_count))
            .contains(&fragment)
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
    /// Independent loss probability, applied to data, parity, and benchmark-only control datagrams.
    pub loss_percent: f64,
    /// Optional contiguous first-flight data-fragment burst, layered on independent loss.
    pub burst_loss: Option<BurstLoss>,
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
    /// Total data, parity, and control bytes put on the wire, including framing/auth overhead.
    pub wire_bytes: u64,
    /// Data wire bytes sent after the first flight.
    pub retransmitted_wire_bytes: u64,
    /// Redundant parity wire bytes.
    pub parity_wire_bytes: u64,
    /// Benchmark-only NACK/ACK wire bytes.
    pub control_bytes: u64,
    /// Number of data, parity, and control datagrams offered.
    pub datagrams: u64,
    /// Incomplete data flights before the completing flight.
    pub recovery_rounds: usize,
    /// Useful bytes lost from the initial data transmission, before any parity recovery.
    pub initial_data_loss_useful_bytes: usize,
    /// Useful bytes still absent after the initial parity opportunity, if any.
    pub missing_after_initial_recovery_bytes: usize,
    /// Data fragments reconstructed from parity without retransmission.
    pub fec_recovered_fragments: usize,
    /// Useful bytes reconstructed from parity without retransmission.
    pub fec_recovered_useful_bytes: usize,
    /// Time until the receiver owns the complete logical payload.
    pub receiver_completion: Duration,
    /// Equal to receiver completion in this isolated-transfer model.
    pub domain_convergence: Duration,
    /// Time until no policy-specific sender recovery/control state remains.
    pub sender_quiescence: Duration,
    /// Peak extra sender state retained specifically for recovery.
    pub peak_sender_recovery_state_bytes: usize,
    /// Peak useful/parity bytes retained in incomplete receiver recovery state.
    pub peak_receiver_reassembly_state_bytes: usize,
}

#[derive(Clone, Copy, Debug)]
struct DataUnit {
    useful_len: usize,
    wire_len: usize,
}

#[derive(Clone, Copy, Debug)]
struct ParityUnit {
    first_data: usize,
    data_count: usize,
    payload_len: usize,
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
    let parity = parity_units(&units, auth);

    let one_way_s = case.rtt.as_secs_f64() / 2.0;
    let rtt_s = case.rtt.as_secs_f64();
    let mut received = vec![false; units.len()];
    let mut attempts = vec![0_u32; units.len()];
    let mut remaining = units.len();
    let mut retained_useful = 0_usize;
    let mut peak_receiver = 0_usize;
    let mut wire_bytes = 0_u64;
    let mut retransmitted_wire_bytes = 0_u64;
    let mut parity_wire_bytes = 0_u64;
    let mut control_bytes = 0_u64;
    let mut datagrams = 0_u64;
    let mut initial_data_loss_useful_bytes = 0_usize;
    let mut missing_after_initial_recovery_bytes = 0_usize;
    let mut fec_recovered_fragments = 0_usize;
    let mut fec_recovered_useful_bytes = 0_usize;
    let mut receiver_completion_s = None;
    let mut round_start_s = 0.0_f64;
    let mut final_send_end_s = 0.0_f64;
    let mut control_seq = 0_u64;

    let retained_data_wire: usize = units.iter().map(|unit| unit.wire_len).sum();
    let retained_parity_wire: usize = parity.iter().map(|unit| unit.wire_len).sum();
    let peak_sender_recovery_state_bytes = match policy {
        RecoveryPolicy::WholeRetry => 0,
        RecoveryPolicy::MissingOnly => retained_data_wire,
        RecoveryPolicy::Xor8Plus1 => retained_data_wire + retained_parity_wire,
    };

    let mut completing_round = 0_usize;
    for round in 0..MAX_RECOVERY_ROUNDS {
        let selected: Vec<usize> = match policy {
            RecoveryPolicy::WholeRetry => (0..units.len()).collect(),
            RecoveryPolicy::MissingOnly | RecoveryPolicy::Xor8Plus1 => received
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

        if round == 0 {
            initial_data_loss_useful_bytes = missing_useful_bytes(&units, &received);

            if policy == RecoveryPolicy::Xor8Plus1 {
                let mut parity_arrivals = vec![None; parity.len()];
                let mut retained_parity_payload = 0_usize;
                let mut parity_scratch_payload = 0_usize;
                for (group, parity_unit) in parity.iter().copied().enumerate() {
                    wire_bytes += parity_unit.wire_len as u64;
                    parity_wire_bytes += parity_unit.wire_len as u64;
                    datagrams += 1;
                    send_cursor_s +=
                        serialization_seconds(parity_unit.wire_len, case.bandwidth_bps);
                    if !parity_is_lost(case, group) {
                        parity_arrivals[group] = Some(send_cursor_s + one_way_s);
                        let missing_in_group = (parity_unit.first_data
                            ..parity_unit.first_data + parity_unit.data_count)
                            .filter(|&index| !received[index])
                            .count();
                        if missing_in_group > 1 {
                            retained_parity_payload += parity_unit.payload_len;
                        } else if missing_in_group == 1 {
                            parity_scratch_payload =
                                parity_scratch_payload.max(parity_unit.payload_len);
                        }
                    }
                }
                if remaining > 0 {
                    peak_receiver = peak_receiver.max(
                        retained_useful
                            .saturating_add(retained_parity_payload)
                            .saturating_add(parity_scratch_payload),
                    );
                    apply_parity_recovery(
                        &units,
                        &parity,
                        &parity_arrivals,
                        &mut received,
                        &mut remaining,
                        &mut retained_useful,
                        &mut fec_recovered_fragments,
                        &mut fec_recovered_useful_bytes,
                        &mut receiver_completion_s,
                    );
                    if remaining > 0 {
                        peak_receiver = peak_receiver.max(retained_useful);
                    }
                }
            }

            missing_after_initial_recovery_bytes = missing_useful_bytes(&units, &received);
        }

        final_send_end_s = send_cursor_s;
        if remaining == 0 {
            completing_round = round;
            break;
        }

        let receiver_round_boundary_s = send_cursor_s + one_way_s;
        round_start_s = match policy {
            RecoveryPolicy::WholeRetry => send_cursor_s + rtt_s,
            RecoveryPolicy::MissingOnly | RecoveryPolicy::Xor8Plus1 => {
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
        RecoveryPolicy::MissingOnly | RecoveryPolicy::Xor8Plus1 => {
            let ack_arrival = deliver_control(
                case,
                ack_wire_len(auth),
                receiver_completion_s,
                one_way_s,
                rtt_s,
                &mut control_seq,
                &mut wire_bytes,
                &mut control_bytes,
                &mut datagrams,
            );
            ack_arrival.max(final_send_end_s)
        }
    };

    Metrics {
        useful_bytes: logical_bytes,
        wire_bytes,
        retransmitted_wire_bytes,
        parity_wire_bytes,
        control_bytes,
        datagrams,
        recovery_rounds: completing_round,
        initial_data_loss_useful_bytes,
        missing_after_initial_recovery_bytes,
        fec_recovered_fragments,
        fec_recovered_useful_bytes,
        receiver_completion: Duration::from_secs_f64(receiver_completion_s),
        domain_convergence: Duration::from_secs_f64(receiver_completion_s),
        sender_quiescence: Duration::from_secs_f64(sender_quiescence_s),
        peak_sender_recovery_state_bytes,
        peak_receiver_reassembly_state_bytes: peak_receiver,
    }
}

/// Result of a clean-link contact interruption followed by resumption.
#[derive(Clone, Debug, PartialEq)]
pub struct InterruptionMetrics {
    /// Logical application bytes in the transfer.
    pub useful_bytes: usize,
    /// Useful bytes retained at the receiver when contact stops.
    pub progress_retained_useful_bytes: usize,
    /// Data wire bytes sent before the interruption.
    pub wire_bytes_before_interruption: u64,
    /// Data plus control wire bytes required after contact resumes.
    pub additional_wire_bytes: u64,
    /// Control subset of `additional_wire_bytes`.
    pub additional_control_bytes: u64,
    /// Time from first send until receiver completion, including the interruption gap.
    pub receiver_completion: Duration,
    /// Time from first send until no selective sender state remains.
    pub sender_quiescence: Duration,
}

/// Simulate a clean-link contact interruption after a fraction of data frames arrived.
///
/// The whole-retry arm restarts the full logical transfer. Missing-only and bounded-parity arms
/// use the same bitmap-NACK recovery after resumption; parity is not useful once the receiver can
/// explicitly identify the retained prefix. This isolates retained-progress value from packet loss.
pub fn simulate_interruption(
    case: Case,
    policy: RecoveryPolicy,
    arrived_fraction: f64,
    gap: Duration,
) -> InterruptionMetrics {
    assert_eq!(
        case.loss_percent, 0.0,
        "interruption model isolates contact loss"
    );
    assert!(
        case.burst_loss.is_none(),
        "interruption model isolates contact loss"
    );
    assert!(
        (0.0..1.0).contains(&arrived_fraction),
        "arrived_fraction must be in [0, 1)"
    );

    let auth = authenticated_overhead();
    let logical_bytes =
        logical_bytes_for_fragments(case.fragment_count, case.datagram_payload_budget);
    let units = data_units(logical_bytes, case.datagram_payload_budget, auth);
    assert!(
        units.len() > 1,
        "interruption requires a multi-frame logical transfer"
    );

    let arrived_units =
        ((units.len() as f64 * arrived_fraction).round() as usize).clamp(1, units.len() - 1);
    let progress_retained_useful_bytes: usize = units[..arrived_units]
        .iter()
        .map(|unit| unit.useful_len)
        .sum();
    let wire_bytes_before_interruption: u64 = units[..arrived_units]
        .iter()
        .map(|unit| unit.wire_len as u64)
        .sum();

    let one_way_s = case.rtt.as_secs_f64() / 2.0;
    let pre_serialization_s =
        serialization_seconds(wire_bytes_before_interruption as usize, case.bandwidth_bps);
    let resume_receiver_s = pre_serialization_s + one_way_s + gap.as_secs_f64();

    let (data_indices, nack_len, ack_len) = match policy {
        RecoveryPolicy::WholeRetry => ((0..units.len()).collect::<Vec<_>>(), 0, 0),
        RecoveryPolicy::MissingOnly | RecoveryPolicy::Xor8Plus1 => (
            (arrived_units..units.len()).collect::<Vec<_>>(),
            nack_wire_len(units.len(), auth),
            ack_wire_len(auth),
        ),
    };

    let mut additional_wire_bytes = 0_u64;
    let mut additional_control_bytes = 0_u64;
    let mut sender_start_s = resume_receiver_s;
    if nack_len > 0 {
        additional_wire_bytes += nack_len as u64;
        additional_control_bytes += nack_len as u64;
        sender_start_s += serialization_seconds(nack_len, case.bandwidth_bps) + one_way_s;
    }

    let mut send_cursor_s = sender_start_s;
    let mut completion_s = sender_start_s;
    for index in data_indices {
        let unit = units[index];
        additional_wire_bytes += unit.wire_len as u64;
        send_cursor_s += serialization_seconds(unit.wire_len, case.bandwidth_bps);
        if index >= arrived_units {
            completion_s = send_cursor_s + one_way_s;
        }
    }

    let sender_quiescence_s = if ack_len == 0 {
        completion_s.max(send_cursor_s)
    } else {
        additional_wire_bytes += ack_len as u64;
        additional_control_bytes += ack_len as u64;
        completion_s + serialization_seconds(ack_len, case.bandwidth_bps) + one_way_s
    };

    InterruptionMetrics {
        useful_bytes: logical_bytes,
        progress_retained_useful_bytes,
        wire_bytes_before_interruption,
        additional_wire_bytes,
        additional_control_bytes,
        receiver_completion: Duration::from_secs_f64(completion_s),
        sender_quiescence: Duration::from_secs_f64(sender_quiescence_s),
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

fn parity_units(data: &[DataUnit], auth: usize) -> Vec<ParityUnit> {
    if data.len() < 2 {
        return Vec::new();
    }
    data.chunks(FEC_DATA_PER_GROUP)
        .enumerate()
        .map(|(group, units)| {
            let payload_len = units
                .iter()
                .map(|unit| unit.useful_len)
                .max()
                .expect("a parity group is non-empty");
            ParityUnit {
                first_data: group * FEC_DATA_PER_GROUP,
                data_count: units.len(),
                payload_len,
                wire_len: auth + PARITY_HEADER_LEN + payload_len,
            }
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn apply_parity_recovery(
    data: &[DataUnit],
    parity: &[ParityUnit],
    parity_arrivals: &[Option<f64>],
    received: &mut [bool],
    remaining: &mut usize,
    retained_useful: &mut usize,
    recovered_fragments: &mut usize,
    recovered_useful: &mut usize,
    completion_s: &mut Option<f64>,
) {
    for (group, parity_unit) in parity.iter().enumerate() {
        let range = parity_unit.first_data..parity_unit.first_data + parity_unit.data_count;
        let mut missing = range.clone().filter(|&index| !received[index]);
        let Some(index) = missing.next() else {
            continue;
        };
        if missing.next().is_some() {
            continue;
        }
        let Some(arrival_s) = parity_arrivals[group] else {
            continue;
        };

        received[index] = true;
        *remaining -= 1;
        *retained_useful += data[index].useful_len;
        *recovered_fragments += 1;
        *recovered_useful += data[index].useful_len;
        if *remaining == 0 {
            *completion_s = Some(arrival_s);
        }
    }
}

fn missing_useful_bytes(data: &[DataUnit], received: &[bool]) -> usize {
    data.iter()
        .zip(received)
        .filter_map(|(unit, present)| (!present).then_some(unit.useful_len))
        .sum()
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
    if attempt == 1 && case.burst_loss.is_some_and(|burst| burst.contains(unit)) {
        return true;
    }
    sample_loss(
        case.seed,
        DATA_DOMAIN,
        unit as u64,
        attempt as u64,
        case.loss_percent,
    )
}

fn parity_is_lost(case: Case, group: usize) -> bool {
    sample_loss(case.seed, PARITY_DOMAIN, group as u64, 1, case.loss_percent)
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
