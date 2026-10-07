// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Deterministic semantic model for ordered-stream and multiplexed transport research.
//!
//! This is deliberately not a TCP or QUIC implementation. The model keeps the shipped UDP
//! framing sizes and the generic 48-byte stream-frame overhead used by the prior analytical model. The setup byte
//! counts are explicit semantic-model assumptions rather than claims about a concrete transport. The model
//! uses one shared forward serialization bottleneck, deterministic
//! paired packet loss/reordering, reliable retransmission, per-stream ordered delivery, and
//! application-level UDP recovery. Congestion control, kernel scheduling and transport CPU are out
//! of scope and must not be inferred from these results.

use std::cmp::Ordering;
use std::time::Duration;

use gossip::auth::{Authenticator, ClusterKey};
use gossip::framing::{
    complete_payload_capacity, fragment_payload_capacity, COMPLETE_HEADER_LEN, FRAGMENT_HEADER_LEN,
};

// Generic stream framing from the prior analytical transport profile.
const STREAM_FRAME_OVERHEAD_BYTES: usize = 48;
// Semantic-model assumptions: two MTU-sized packets for a cold setup, one for resumption.
// These are printed with every benchmark run and are not QUIC/TCP wire claims.
const COLD_SETUP_BYTES: usize = 2_400;
const RESUMED_SETUP_BYTES: usize = 1_200;
const SELECTIVE_CONTROL_TAG_LEN: usize = 1;
const TRANSFER_ID_LEN: usize = 32;
const FRAGMENT_COUNT_LEN: usize = 4;
const LOSS_DOMAIN: u64 = 0x6c6f_7373_2720_0001;
const REORDER_DOMAIN: u64 = 0x7265_6f72_2720_0001;
const NACK_DOMAIN: u64 = 0x6e61_636b_2720_0001;
const ACK_DOMAIN: u64 = 0x6163_6b5f_2720_0001;
const MAX_RECOVERY_ATTEMPTS: u32 = 10_000;

/// Transport semantics compared by this model.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Candidate {
    /// Whole-message control: retransmit a whole logical message after an incomplete flight.
    UdpWholeMessage,
    /// Missing-only control: report and retransmit only missing fragments.
    UdpMissingOnly,
    /// All logical traffic shares one reliable ordered byte stream.
    SingleReliableStream,
    /// Every logical message owns an independent reliable ordered stream.
    MultipleReliableStreams,
    /// Bulk uses independent reliable streams; small control messages remain datagrams.
    HybridDatagramControl,
}

impl Candidate {
    /// Stable benchmark label.
    pub fn label(self) -> &'static str {
        match self {
            Self::UdpWholeMessage => "udp_whole_message",
            Self::UdpMissingOnly => "udp_missing_only",
            Self::SingleReliableStream => "single_reliable_stream",
            Self::MultipleReliableStreams => "multiple_reliable_streams",
            Self::HybridDatagramControl => "hybrid_datagram_control",
        }
    }

    fn uses_stream_session(self) -> bool {
        matches!(
            self,
            Self::SingleReliableStream
                | Self::MultipleReliableStreams
                | Self::HybridDatagramControl
        )
    }

    fn is_udp(self) -> bool {
        matches!(self, Self::UdpWholeMessage | Self::UdpMissingOnly)
    }

    fn uses_datagram_for(self, class: MessageClass) -> bool {
        self.is_udp() || (self == Self::HybridDatagramControl && class == MessageClass::Control)
    }
}

/// Connection/session state for connected stream candidates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Session {
    /// Generic cold setup: one blocking RTT plus two 1200-byte setup packets.
    Cold,
    /// Generic resumed/0-RTT setup: application data is not blocked, but one setup packet is sent.
    Resumed,
    /// Connection already exists and setup is fully amortized.
    Established,
}

impl Session {
    /// Stable benchmark label.
    pub fn label(self) -> &'static str {
        match self {
            Self::Cold => "cold",
            Self::Resumed => "resumed",
            Self::Established => "established",
        }
    }
}

/// Application traffic class used by the mixed workload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MessageClass {
    /// Large value/enumeration transfer.
    Bulk,
    /// Fingerprint/refinement/reconciliation control message.
    Control,
}

/// One logical application message.
#[derive(Clone, Copy, Debug)]
pub struct Message {
    /// Traffic class.
    pub class: MessageClass,
    /// Useful application bytes.
    pub useful_bytes: usize,
    /// Time at which the application makes this message available to the transport.
    pub release: Duration,
}

/// Transport-centric workload consisting of one or more logical messages.
#[derive(Clone, Debug)]
pub struct Workload {
    /// Logical messages in stable identity order.
    pub messages: Vec<Message>,
}

impl Workload {
    /// One isolated large logical transfer.
    pub fn single_bulk(bytes: usize) -> Self {
        Self {
            messages: vec![Message {
                class: MessageClass::Bulk,
                useful_bytes: bytes,
                release: Duration::ZERO,
            }],
        }
    }

    /// Several equal large transfers released concurrently.
    pub fn concurrent_bulk(transfers: usize, bytes_each: usize) -> Self {
        assert!(
            transfers > 0,
            "concurrent workload needs at least one transfer"
        );
        Self {
            messages: (0..transfers)
                .map(|_| Message {
                    class: MessageClass::Bulk,
                    useful_bytes: bytes_each,
                    release: Duration::ZERO,
                })
                .collect(),
        }
    }

    /// Two bulk transfers with latency-sensitive control traffic released while bulk is in flight.
    pub fn mixed_control_bulk(
        bulk_bytes_each: usize,
        control_messages: usize,
        control_bytes: usize,
        control_interval: Duration,
    ) -> Self {
        let mut messages = vec![
            Message {
                class: MessageClass::Bulk,
                useful_bytes: bulk_bytes_each,
                release: Duration::ZERO,
            },
            Message {
                class: MessageClass::Bulk,
                useful_bytes: bulk_bytes_each,
                release: Duration::ZERO,
            },
        ];
        messages.extend((0..control_messages).map(|index| Message {
            class: MessageClass::Control,
            useful_bytes: control_bytes,
            release: control_interval.mul_f64((index + 1) as f64),
        }));
        Self { messages }
    }
}

/// Network/model parameters for one paired simulation.
#[derive(Clone, Copy, Debug)]
pub struct Case {
    /// Maximum physical packet/datagram payload budget.
    pub datagram_payload_budget: usize,
    /// Round-trip propagation time.
    pub rtt: Duration,
    /// Independent loss percentage.
    pub loss_percent: f64,
    /// Independent targeted reordering percentage. A reordered packet incurs one extra one-way
    /// delay.
    pub reorder_percent: f64,
    /// Shared forward serialization rate.
    pub bandwidth_bps: u64,
    /// Reproducible paired-event seed.
    pub seed: u64,
    /// Stream session state. UDP ignores this field and always has zero setup.
    pub session: Session,
}

/// Result from one workload/candidate/case simulation.
#[derive(Clone, Debug, PartialEq)]
pub struct Metrics {
    /// Useful application bytes across bulk and protocol-control messages.
    pub useful_application_bytes: usize,
    /// Total modeled wire bytes, including retransmission, selective control, and setup.
    pub wire_bytes: u64,
    /// Useful bytes belonging to fingerprint/refinement/reconciliation-control messages.
    pub useful_control_application_bytes: usize,
    /// Wire bytes used by those application control messages, including their retransmissions.
    pub application_control_wire_bytes: u64,
    /// Application-packet bytes sent after their first attempt.
    pub retransmitted_wire_bytes: u64,
    /// Benchmark-only missing-fragment NACK/completion-ACK bytes.
    pub control_bytes: u64,
    /// Connected-session setup bytes.
    pub setup_bytes: u64,
    /// Total physical packets/datagrams represented by the model.
    pub packets: u64,
    /// Time until every logical message is application-deliverable at the receiver.
    pub receiver_completion: Duration,
    /// Equal to receiver completion in this transport-only workload model.
    pub domain_convergence: Duration,
    /// Time until modeled sender recovery/acknowledgement state is quiescent.
    pub sender_quiescence: Duration,
    /// Blocking setup latency before first application transmission.
    pub setup_latency: Duration,
    /// Total serialization budget implied by all modeled wire bytes.
    pub serialization_time: Duration,
    /// Receiver-completion time excluding blocking setup.
    pub network_elapsed_excluding_setup: Duration,
    /// Mean completion time of bulk logical messages.
    pub mean_bulk_completion: Duration,
    /// Mean release-to-delivery latency of small control messages.
    pub mean_control_latency: Duration,
    /// P95 release-to-delivery latency of small control messages.
    pub p95_control_latency: Duration,
    /// Sum of per-packet physical-arrival to ordered-application-delivery delay.
    pub total_hol_delay: Duration,
    /// Largest per-packet physical-arrival to ordered-application-delivery delay.
    pub max_hol_delay: Duration,
    /// Peak sender bytes retained for selective recovery or unacknowledged stream data.
    pub peak_sender_state_bytes: usize,
    /// Peak delivered-but-incomplete logical-message bytes retained for application reassembly.
    pub peak_receiver_reassembly_state_bytes: usize,
    /// Peak physically arrived bytes blocked behind an earlier missing/reordered stream segment.
    pub peak_hol_buffer_bytes: usize,
}

/// Interruption/reconnection result for one large transfer.
#[derive(Clone, Debug, PartialEq)]
pub struct InterruptionMetrics {
    /// Logical application bytes in the transfer.
    pub useful_application_bytes: usize,
    /// Useful prefix retained by the receiver when contact stops.
    pub retained_progress_bytes: usize,
    /// Bytes sent before interruption.
    pub wire_bytes_before_interruption: u64,
    /// Bytes required after contact resumes.
    pub additional_wire_bytes: u64,
    /// Setup subset of additional bytes.
    pub additional_setup_bytes: u64,
    /// Selective-control subset of additional bytes.
    pub additional_control_bytes: u64,
    /// Completion time including the contact gap.
    pub receiver_completion: Duration,
    /// Sender quiescence including the contact gap.
    pub sender_quiescence: Duration,
}

#[derive(Clone, Debug)]
struct ChunkState {
    useful_len: usize,
    udp_wire_len: usize,
    stream_wire_len: usize,
    attempts: u32,
    first_send_end_s: Option<f64>,
    success_arrival_s: Option<f64>,
    stream_id: Option<usize>,
    stream_seq: Option<usize>,
}

#[derive(Clone, Copy, Debug)]
struct RetransmitTask {
    message: usize,
    chunk: usize,
    release_s: f64,
}

#[derive(Clone, Copy, Debug)]
struct SetupCost {
    bytes: usize,
    blocking_s: f64,
}

/// Simulate one deterministic matched transport point.
pub fn simulate(workload: &Workload, case: Case, candidate: Candidate) -> Metrics {
    validate_case(case);
    assert!(!workload.messages.is_empty(), "workload must not be empty");

    let auth = authenticated_overhead();
    let chunk_payload = fragment_payload_capacity(case.datagram_payload_budget, auth)
        .expect("datagram budget must fit shipped fragment framing");
    assert!(
        chunk_payload > 0,
        "datagram budget leaves no useful payload"
    );

    let mut chunks: Vec<Vec<ChunkState>> = workload
        .messages
        .iter()
        .map(|message| chunk_message(*message, case.datagram_payload_budget, auth, chunk_payload))
        .collect();
    let useful_application_bytes = workload
        .messages
        .iter()
        .map(|message| message.useful_bytes)
        .sum();
    let useful_control_application_bytes = workload
        .messages
        .iter()
        .filter(|message| message.class == MessageClass::Control)
        .map(|message| message.useful_bytes)
        .sum();

    let setup = setup_cost(candidate, case);
    let mut wire_bytes = setup.bytes as u64;
    let mut retransmitted_wire_bytes = 0_u64;
    let mut control_bytes = 0_u64;
    let setup_bytes = setup.bytes as u64;
    let mut packets = setup.bytes.div_ceil(case.datagram_payload_budget) as u64;
    let mut link_free_s = setup.blocking_s + serialization_seconds(setup.bytes, case.bandwidth_bps);
    let mut next_initial = vec![0_usize; workload.messages.len()];
    let mut retransmits = Vec::<RetransmitTask>::new();
    let mut udp_batch_remaining = vec![0_usize; workload.messages.len()];
    let mut udp_ack_times = vec![None; workload.messages.len()];
    let mut stream_next_seq = vec![0_usize; workload.messages.len() + 1];
    let mut bulk_cursor = 0_usize;
    let one_way_s = case.rtt.as_secs_f64() / 2.0;

    loop {
        if all_initial_sent(&next_initial, &chunks) && retransmits.is_empty() {
            break;
        }

        if let Some((message, chunk)) =
            select_fresh_control(workload, &chunks, &next_initial, link_free_s)
        {
            send_packet(
                workload,
                case,
                candidate,
                message,
                chunk,
                false,
                &mut chunks,
                &mut retransmits,
                &mut udp_batch_remaining,
                &mut stream_next_seq,
                &mut link_free_s,
                &mut wire_bytes,
                &mut retransmitted_wire_bytes,
                &mut control_bytes,
                &mut packets,
            );
            next_initial[message] += 1;
            maybe_schedule_udp_recovery(
                workload,
                case,
                candidate,
                message,
                link_free_s,
                &chunks,
                &mut retransmits,
                &mut udp_batch_remaining,
                &mut wire_bytes,
                &mut control_bytes,
                &mut packets,
            );
            continue;
        }

        if let Some(task_index) = select_retransmit(&retransmits, workload, link_free_s, true) {
            let task = retransmits.swap_remove(task_index);
            send_packet(
                workload,
                case,
                candidate,
                task.message,
                task.chunk,
                true,
                &mut chunks,
                &mut retransmits,
                &mut udp_batch_remaining,
                &mut stream_next_seq,
                &mut link_free_s,
                &mut wire_bytes,
                &mut retransmitted_wire_bytes,
                &mut control_bytes,
                &mut packets,
            );
            if candidate.uses_datagram_for(workload.messages[task.message].class) {
                udp_batch_remaining[task.message] -= 1;
                if udp_batch_remaining[task.message] == 0 {
                    maybe_schedule_udp_recovery(
                        workload,
                        case,
                        candidate,
                        task.message,
                        link_free_s,
                        &chunks,
                        &mut retransmits,
                        &mut udp_batch_remaining,
                        &mut wire_bytes,
                        &mut control_bytes,
                        &mut packets,
                    );
                }
            }
            continue;
        }

        if let Some(task_index) = select_retransmit(&retransmits, workload, link_free_s, false) {
            let task = retransmits.swap_remove(task_index);
            send_packet(
                workload,
                case,
                candidate,
                task.message,
                task.chunk,
                true,
                &mut chunks,
                &mut retransmits,
                &mut udp_batch_remaining,
                &mut stream_next_seq,
                &mut link_free_s,
                &mut wire_bytes,
                &mut retransmitted_wire_bytes,
                &mut control_bytes,
                &mut packets,
            );
            if candidate.uses_datagram_for(workload.messages[task.message].class) {
                udp_batch_remaining[task.message] -= 1;
                if udp_batch_remaining[task.message] == 0 {
                    maybe_schedule_udp_recovery(
                        workload,
                        case,
                        candidate,
                        task.message,
                        link_free_s,
                        &chunks,
                        &mut retransmits,
                        &mut udp_batch_remaining,
                        &mut wire_bytes,
                        &mut control_bytes,
                        &mut packets,
                    );
                }
            }
            continue;
        }

        if let Some(message) = select_fresh_bulk(
            workload,
            &chunks,
            &next_initial,
            link_free_s,
            &mut bulk_cursor,
        ) {
            let chunk = next_initial[message];
            send_packet(
                workload,
                case,
                candidate,
                message,
                chunk,
                false,
                &mut chunks,
                &mut retransmits,
                &mut udp_batch_remaining,
                &mut stream_next_seq,
                &mut link_free_s,
                &mut wire_bytes,
                &mut retransmitted_wire_bytes,
                &mut control_bytes,
                &mut packets,
            );
            next_initial[message] += 1;
            maybe_schedule_udp_recovery(
                workload,
                case,
                candidate,
                message,
                link_free_s,
                &chunks,
                &mut retransmits,
                &mut udp_batch_remaining,
                &mut wire_bytes,
                &mut control_bytes,
                &mut packets,
            );
            continue;
        }

        let next_release = next_ready_time(workload, &chunks, &next_initial, &retransmits)
            .expect("unfinished simulation must have a future packet");
        link_free_s = link_free_s.max(next_release);
    }

    let deliveries = delivery_times(candidate, workload, &chunks);
    let message_completion: Vec<f64> = deliveries
        .iter()
        .map(|message| message.iter().copied().fold(0.0_f64, f64::max))
        .collect();
    let receiver_completion_s = message_completion.iter().copied().fold(0.0_f64, f64::max);

    let mut sender_quiescence_s = receiver_completion_s;
    if candidate == Candidate::UdpMissingOnly {
        for message in 0..workload.messages.len() {
            if chunks[message].len() <= 1 {
                continue;
            }
            let ack_arrival = deliver_selective_control(
                case,
                message,
                ACK_DOMAIN,
                completion_ack_wire_len(auth),
                message_completion[message],
                &mut wire_bytes,
                &mut control_bytes,
                &mut packets,
            );
            udp_ack_times[message] = Some(ack_arrival);
            sender_quiescence_s = sender_quiescence_s.max(ack_arrival);
        }
    } else if candidate.uses_stream_session() {
        for message in &chunks {
            for chunk in message {
                if chunk.stream_id.is_some() {
                    let ack = chunk
                        .success_arrival_s
                        .expect("reliable stream chunk must eventually arrive")
                        + one_way_s;
                    sender_quiescence_s = sender_quiescence_s.max(ack);
                }
            }
        }
    }

    let application_control_wire_bytes = workload
        .messages
        .iter()
        .enumerate()
        .filter(|(_, message)| message.class == MessageClass::Control)
        .flat_map(|(message, _)| &chunks[message])
        .map(|chunk| {
            let wire_len = if chunk.stream_id.is_some() {
                chunk.stream_wire_len
            } else {
                chunk.udp_wire_len
            };
            wire_len as u64 * chunk.attempts as u64
        })
        .sum();
    let (total_hol_s, max_hol_s, peak_hol_buffer_bytes) = hol_metrics(&chunks, &deliveries);
    let peak_receiver_reassembly_state_bytes =
        peak_reassembly_state(&chunks, &deliveries, &message_completion);
    let peak_sender_state_bytes = peak_sender_state(
        candidate,
        workload,
        &chunks,
        &message_completion,
        &udp_ack_times,
        one_way_s,
    );
    let (mean_bulk_completion_s, mean_control_latency_s, p95_control_latency_s) =
        message_latency_metrics(workload, &message_completion);
    let serialization_s = serialization_seconds(wire_bytes as usize, case.bandwidth_bps);
    let setup_latency_s = setup.blocking_s + serialization_seconds(setup.bytes, case.bandwidth_bps);

    Metrics {
        useful_application_bytes,
        wire_bytes,
        useful_control_application_bytes,
        application_control_wire_bytes,
        retransmitted_wire_bytes,
        control_bytes,
        setup_bytes,
        packets,
        receiver_completion: seconds(receiver_completion_s),
        domain_convergence: seconds(receiver_completion_s),
        sender_quiescence: seconds(sender_quiescence_s),
        setup_latency: seconds(setup_latency_s),
        serialization_time: seconds(serialization_s),
        network_elapsed_excluding_setup: seconds(
            (receiver_completion_s - setup_latency_s).max(0.0),
        ),
        mean_bulk_completion: seconds(mean_bulk_completion_s),
        mean_control_latency: seconds(mean_control_latency_s),
        p95_control_latency: seconds(p95_control_latency_s),
        total_hol_delay: seconds(total_hol_s),
        max_hol_delay: seconds(max_hol_s),
        peak_sender_state_bytes,
        peak_receiver_reassembly_state_bytes,
        peak_hol_buffer_bytes,
    }
}

/// Simulate a clean-link interruption after a fraction of useful bulk chunks arrived.
///
/// `reconnect=false` means a connected reliable session survives the contact gap and retains its
/// retransmission state. `reconnect=true` means a new resumed stream session is created; generic
/// reliable streams do not implicitly resume an old stream byte offset, so the logical payload is
/// replayed. UDP retains content-addressed receiver fragments in both cases.
pub fn simulate_interruption(
    logical_bytes: usize,
    case: Case,
    candidate: Candidate,
    arrived_fraction: f64,
    gap: Duration,
    reconnect: bool,
) -> InterruptionMetrics {
    validate_case(case);
    assert_eq!(case.loss_percent, 0.0, "interruption isolates contact loss");
    assert_eq!(
        case.reorder_percent, 0.0,
        "interruption isolates contact loss"
    );
    assert!(
        (0.0..1.0).contains(&arrived_fraction),
        "arrived_fraction must be in [0, 1)"
    );

    let auth = authenticated_overhead();
    let payload = fragment_payload_capacity(case.datagram_payload_budget, auth)
        .expect("datagram budget must fit shipped fragment framing");
    let chunks = logical_bytes.div_ceil(payload);
    assert!(chunks > 1, "interruption requires a fragmented transfer");
    let arrived = ((chunks as f64 * arrived_fraction).round() as usize).clamp(1, chunks - 1);
    let retained_progress_bytes = useful_prefix_bytes(logical_bytes, payload, arrived);
    let udp_wire = chunk_wire_lengths(
        logical_bytes,
        payload,
        auth,
        case.datagram_payload_budget,
        false,
    );
    let stream_wire = chunk_wire_lengths(
        logical_bytes,
        payload,
        auth,
        case.datagram_payload_budget,
        true,
    );
    let source_wire = if candidate.is_udp() {
        &udp_wire
    } else {
        &stream_wire
    };
    let wire_bytes_before_interruption: u64 = source_wire[..arrived]
        .iter()
        .map(|bytes| *bytes as u64)
        .sum();
    let one_way_s = case.rtt.as_secs_f64() / 2.0;
    let pre_s = serialization_seconds(wire_bytes_before_interruption as usize, case.bandwidth_bps)
        + one_way_s;
    let resume_s = pre_s + gap.as_secs_f64();

    let mut additional_setup_bytes = 0_u64;
    let mut additional_control_bytes = 0_u64;
    let additional_data_bytes: u64 = match candidate {
        Candidate::UdpWholeMessage => udp_wire.iter().map(|bytes| *bytes as u64).sum(),
        Candidate::UdpMissingOnly => {
            additional_control_bytes =
                (missing_nack_wire_len(chunks, auth) + completion_ack_wire_len(auth)) as u64;
            udp_wire[arrived..].iter().map(|bytes| *bytes as u64).sum()
        }
        Candidate::SingleReliableStream
        | Candidate::MultipleReliableStreams
        | Candidate::HybridDatagramControl => {
            if reconnect {
                additional_setup_bytes = RESUMED_SETUP_BYTES as u64;
                stream_wire.iter().map(|bytes| *bytes as u64).sum()
            } else {
                stream_wire[arrived..]
                    .iter()
                    .map(|bytes| *bytes as u64)
                    .sum()
            }
        }
    };
    let additional_wire_bytes =
        additional_data_bytes + additional_setup_bytes + additional_control_bytes;
    let resumed_serialization_s =
        serialization_seconds(additional_wire_bytes as usize, case.bandwidth_bps);
    let completion_s = resume_s + resumed_serialization_s + one_way_s;
    let sender_quiescence_s = if candidate == Candidate::UdpWholeMessage {
        completion_s
    } else {
        completion_s + one_way_s
    };

    InterruptionMetrics {
        useful_application_bytes: logical_bytes,
        retained_progress_bytes,
        wire_bytes_before_interruption,
        additional_wire_bytes,
        additional_setup_bytes,
        additional_control_bytes,
        receiver_completion: seconds(completion_s),
        sender_quiescence: seconds(sender_quiescence_s),
    }
}

fn validate_case(case: Case) {
    assert!(case.bandwidth_bps > 0, "bandwidth must be non-zero");
    assert!(
        (0.0..100.0).contains(&case.loss_percent),
        "loss must be in [0, 100)"
    );
    assert!(
        (0.0..100.0).contains(&case.reorder_percent),
        "reorder must be in [0, 100)"
    );
}

fn chunk_message(message: Message, budget: usize, auth: usize, payload: usize) -> Vec<ChunkState> {
    assert!(
        message.useful_bytes > 0,
        "logical messages must be non-empty"
    );
    let complete = complete_payload_capacity(budget, auth)
        .expect("datagram budget must fit complete-frame overhead");
    let fragmented = message.useful_bytes > complete;
    let mut remaining = message.useful_bytes;
    let mut chunks = Vec::new();
    while remaining > 0 {
        let useful_len = remaining.min(payload);
        let udp_header = if fragmented {
            FRAGMENT_HEADER_LEN
        } else {
            COMPLETE_HEADER_LEN
        };
        chunks.push(ChunkState {
            useful_len,
            udp_wire_len: auth + udp_header + useful_len,
            stream_wire_len: STREAM_FRAME_OVERHEAD_BYTES + useful_len,
            attempts: 0,
            first_send_end_s: None,
            success_arrival_s: None,
            stream_id: None,
            stream_seq: None,
        });
        remaining -= useful_len;
    }
    assert!(
        chunks.len() == 1 || fragmented,
        "multi-chunk message must use shipped fragment framing"
    );
    chunks
}

fn setup_cost(candidate: Candidate, case: Case) -> SetupCost {
    if !candidate.uses_stream_session() {
        return SetupCost {
            bytes: 0,
            blocking_s: 0.0,
        };
    }
    match case.session {
        Session::Cold => SetupCost {
            bytes: COLD_SETUP_BYTES,
            blocking_s: case.rtt.as_secs_f64(),
        },
        Session::Resumed => SetupCost {
            bytes: RESUMED_SETUP_BYTES,
            blocking_s: 0.0,
        },
        Session::Established => SetupCost {
            bytes: 0,
            blocking_s: 0.0,
        },
    }
}

fn stream_id(candidate: Candidate, message: usize, class: MessageClass) -> Option<usize> {
    match candidate {
        Candidate::SingleReliableStream => Some(0),
        Candidate::MultipleReliableStreams => Some(message + 1),
        Candidate::HybridDatagramControl => (class == MessageClass::Bulk).then_some(message + 1),
        Candidate::UdpWholeMessage | Candidate::UdpMissingOnly => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn send_packet(
    workload: &Workload,
    case: Case,
    candidate: Candidate,
    message: usize,
    chunk: usize,
    retransmission: bool,
    chunks: &mut [Vec<ChunkState>],
    retransmits: &mut Vec<RetransmitTask>,
    _udp_batch_remaining: &mut [usize],
    stream_next_seq: &mut [usize],
    link_free_s: &mut f64,
    wire_bytes: &mut u64,
    retransmitted_wire_bytes: &mut u64,
    _control_bytes: &mut u64,
    packets: &mut u64,
) {
    let class = workload.messages[message].class;
    let reliable_stream = stream_id(candidate, message, class);
    let wire_len = if reliable_stream.is_some() {
        chunks[message][chunk].stream_wire_len
    } else {
        chunks[message][chunk].udp_wire_len
    };

    if chunks[message][chunk].stream_id.is_none() {
        chunks[message][chunk].stream_id = reliable_stream;
    }
    if chunks[message][chunk].stream_seq.is_none() {
        if let Some(id) = reliable_stream {
            chunks[message][chunk].stream_seq = Some(stream_next_seq[id]);
            stream_next_seq[id] += 1;
        }
    }

    chunks[message][chunk].attempts += 1;
    assert!(
        chunks[message][chunk].attempts < MAX_RECOVERY_ATTEMPTS,
        "packet exceeded recovery-attempt bound"
    );
    let attempt = chunks[message][chunk].attempts;
    *wire_bytes += wire_len as u64;
    *packets += 1;
    if retransmission || attempt > 1 {
        *retransmitted_wire_bytes += wire_len as u64;
    }

    let send_end_s = *link_free_s + serialization_seconds(wire_len, case.bandwidth_bps);
    *link_free_s = send_end_s;
    chunks[message][chunk]
        .first_send_end_s
        .get_or_insert(send_end_s);

    let unit = logical_unit(message, chunk);
    let lost = probability_event(
        case.seed,
        LOSS_DOMAIN,
        unit,
        attempt as u64,
        case.loss_percent,
    );
    if lost {
        if reliable_stream.is_some() {
            retransmits.push(RetransmitTask {
                message,
                chunk,
                release_s: send_end_s + case.rtt.as_secs_f64(),
            });
        }
        return;
    }

    let reordered = probability_event(
        case.seed,
        REORDER_DOMAIN,
        unit,
        attempt as u64,
        case.reorder_percent,
    );
    let one_way_s = case.rtt.as_secs_f64() / 2.0;
    let arrival_s = send_end_s + one_way_s + if reordered { one_way_s } else { 0.0 };
    let slot = &mut chunks[message][chunk].success_arrival_s;
    *slot = Some(slot.map_or(arrival_s, |previous| previous.min(arrival_s)));
}

#[allow(clippy::too_many_arguments)]
fn maybe_schedule_udp_recovery(
    workload: &Workload,
    case: Case,
    candidate: Candidate,
    message: usize,
    batch_tail_send_end_s: f64,
    chunks: &[Vec<ChunkState>],
    retransmits: &mut Vec<RetransmitTask>,
    udp_batch_remaining: &mut [usize],
    wire_bytes: &mut u64,
    control_bytes: &mut u64,
    packets: &mut u64,
) {
    if !candidate.uses_datagram_for(workload.messages[message].class)
        || udp_batch_remaining[message] != 0
    {
        return;
    }
    if chunks[message].iter().any(|chunk| chunk.attempts == 0) {
        return;
    }
    let missing: Vec<usize> = chunks[message]
        .iter()
        .enumerate()
        .filter_map(|(index, chunk)| chunk.success_arrival_s.is_none().then_some(index))
        .collect();
    if missing.is_empty() {
        return;
    }

    let fragmented = chunks[message].len() > 1;
    let selective = candidate == Candidate::UdpMissingOnly && fragmented;
    let release_s = if selective {
        let auth = authenticated_overhead();
        let receiver_boundary_s = batch_tail_send_end_s + case.rtt.as_secs_f64() / 2.0;
        deliver_selective_control(
            case,
            message,
            NACK_DOMAIN,
            missing_nack_wire_len(chunks[message].len(), auth),
            receiver_boundary_s,
            wire_bytes,
            control_bytes,
            packets,
        )
    } else {
        batch_tail_send_end_s + case.rtt.as_secs_f64()
    };

    let selected: Vec<usize> = if selective {
        missing
    } else {
        (0..chunks[message].len()).collect()
    };
    udp_batch_remaining[message] = selected.len();
    retransmits.extend(selected.into_iter().map(|chunk| RetransmitTask {
        message,
        chunk,
        release_s,
    }));
}

#[allow(clippy::too_many_arguments)]
fn deliver_selective_control(
    case: Case,
    message: usize,
    domain: u64,
    wire_len: usize,
    first_send_s: f64,
    wire_bytes: &mut u64,
    control_bytes: &mut u64,
    packets: &mut u64,
) -> f64 {
    let one_way_s = case.rtt.as_secs_f64() / 2.0;
    let mut send_s = first_send_s;
    let mut attempt = 1_u32;
    loop {
        assert!(
            attempt < MAX_RECOVERY_ATTEMPTS,
            "control exceeded retry bound"
        );
        *wire_bytes += wire_len as u64;
        *control_bytes += wire_len as u64;
        *packets += 1;
        let send_end_s = send_s + serialization_seconds(wire_len, case.bandwidth_bps);
        if !probability_event(
            case.seed,
            domain,
            message as u64,
            attempt as u64,
            case.loss_percent,
        ) {
            return send_end_s + one_way_s;
        }
        attempt += 1;
        send_s += case.rtt.as_secs_f64();
    }
}

fn select_fresh_control(
    workload: &Workload,
    chunks: &[Vec<ChunkState>],
    next_initial: &[usize],
    now_s: f64,
) -> Option<(usize, usize)> {
    workload
        .messages
        .iter()
        .enumerate()
        .filter(|(message, spec)| {
            spec.class == MessageClass::Control
                && spec.release.as_secs_f64() <= now_s
                && next_initial[*message] < chunks[*message].len()
        })
        .min_by(|(left_id, left), (right_id, right)| {
            left.release
                .cmp(&right.release)
                .then_with(|| left_id.cmp(right_id))
        })
        .map(|(message, _)| (message, next_initial[message]))
}

fn select_fresh_bulk(
    workload: &Workload,
    chunks: &[Vec<ChunkState>],
    next_initial: &[usize],
    now_s: f64,
    cursor: &mut usize,
) -> Option<usize> {
    let len = workload.messages.len();
    for offset in 0..len {
        let message = (*cursor + offset) % len;
        let spec = workload.messages[message];
        if spec.class == MessageClass::Bulk
            && spec.release.as_secs_f64() <= now_s
            && next_initial[message] < chunks[message].len()
        {
            *cursor = (message + 1) % len;
            return Some(message);
        }
    }
    None
}

fn select_retransmit(
    retransmits: &[RetransmitTask],
    workload: &Workload,
    now_s: f64,
    control: bool,
) -> Option<usize> {
    retransmits
        .iter()
        .enumerate()
        .filter(|(_, task)| {
            task.release_s <= now_s
                && (workload.messages[task.message].class == MessageClass::Control) == control
        })
        .min_by(|(_, left), (_, right)| compare_task(left, right))
        .map(|(index, _)| index)
}

fn compare_task(left: &RetransmitTask, right: &RetransmitTask) -> Ordering {
    left.release_s
        .total_cmp(&right.release_s)
        .then_with(|| left.message.cmp(&right.message))
        .then_with(|| left.chunk.cmp(&right.chunk))
}

fn next_ready_time(
    workload: &Workload,
    chunks: &[Vec<ChunkState>],
    next_initial: &[usize],
    retransmits: &[RetransmitTask],
) -> Option<f64> {
    let fresh = workload
        .messages
        .iter()
        .enumerate()
        .filter(|(message, _)| next_initial[*message] < chunks[*message].len())
        .map(|(_, message)| message.release.as_secs_f64())
        .min_by(f64::total_cmp);
    let retry = retransmits
        .iter()
        .map(|task| task.release_s)
        .min_by(f64::total_cmp);
    match (fresh, retry) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn all_initial_sent(next_initial: &[usize], chunks: &[Vec<ChunkState>]) -> bool {
    next_initial
        .iter()
        .enumerate()
        .all(|(message, next)| *next == chunks[message].len())
}

fn delivery_times(
    candidate: Candidate,
    workload: &Workload,
    chunks: &[Vec<ChunkState>],
) -> Vec<Vec<f64>> {
    let mut delivery: Vec<Vec<f64>> = chunks
        .iter()
        .map(|message| vec![0.0; message.len()])
        .collect();
    if candidate.is_udp() || candidate == Candidate::HybridDatagramControl {
        for (message, spec) in workload.messages.iter().enumerate() {
            if candidate == Candidate::HybridDatagramControl && spec.class == MessageClass::Bulk {
                continue;
            }
            for (chunk, state) in chunks[message].iter().enumerate() {
                delivery[message][chunk] = state
                    .success_arrival_s
                    .expect("datagram chunk must eventually arrive");
            }
        }
    }

    let stream_count = workload.messages.len() + 1;
    for stream in 0..stream_count {
        let mut segments = Vec::new();
        for (message, message_chunks) in chunks.iter().enumerate() {
            for (chunk, state) in message_chunks.iter().enumerate() {
                if state.stream_id == Some(stream) {
                    segments.push((
                        state.stream_seq.expect("stream chunk must have sequence"),
                        message,
                        chunk,
                        state
                            .success_arrival_s
                            .expect("reliable stream chunk must eventually arrive"),
                    ));
                }
            }
        }
        segments.sort_by_key(|segment| segment.0);
        let mut prefix_delivery = 0.0_f64;
        for (_, message, chunk, arrival) in segments {
            prefix_delivery = prefix_delivery.max(arrival);
            delivery[message][chunk] = prefix_delivery;
        }
    }
    delivery
}

fn hol_metrics(chunks: &[Vec<ChunkState>], delivery: &[Vec<f64>]) -> (f64, f64, usize) {
    let mut total_hol = 0.0_f64;
    let mut max_hol = 0.0_f64;
    let mut events = Vec::<(f64, i64)>::new();
    for (message, message_chunks) in chunks.iter().enumerate() {
        for (chunk, state) in message_chunks.iter().enumerate() {
            if state.stream_id.is_none() {
                continue;
            }
            let arrival = state
                .success_arrival_s
                .expect("stream chunk must eventually arrive");
            let delivered = delivery[message][chunk];
            let hol = (delivered - arrival).max(0.0);
            total_hol += hol;
            max_hol = max_hol.max(hol);
            if hol > 0.0 {
                events.push((arrival, state.useful_len as i64));
                events.push((delivered, -(state.useful_len as i64)));
            }
        }
    }
    (total_hol, max_hol, peak_from_events(events))
}

fn peak_reassembly_state(
    chunks: &[Vec<ChunkState>],
    delivery: &[Vec<f64>],
    message_completion: &[f64],
) -> usize {
    let mut events = Vec::<(f64, i64)>::new();
    for (message, message_chunks) in chunks.iter().enumerate() {
        if message_chunks.len() <= 1 {
            continue;
        }
        let completion = message_completion[message];
        let mut retained = 0_usize;
        for (chunk, state) in message_chunks.iter().enumerate() {
            let delivered = delivery[message][chunk];
            if delivered + f64::EPSILON < completion {
                retained += state.useful_len;
                events.push((delivered, state.useful_len as i64));
            }
        }
        if retained > 0 {
            events.push((completion, -(retained as i64)));
        }
    }
    peak_from_events(events)
}

fn peak_sender_state(
    candidate: Candidate,
    workload: &Workload,
    chunks: &[Vec<ChunkState>],
    message_completion: &[f64],
    udp_ack_times: &[Option<f64>],
    one_way_s: f64,
) -> usize {
    let mut events = Vec::<(f64, i64)>::new();
    match candidate {
        Candidate::UdpMissingOnly => {
            for (message, spec) in workload.messages.iter().enumerate() {
                if chunks[message].len() <= 1 || spec.class == MessageClass::Control {
                    continue;
                }
                let bytes: usize = chunks[message].iter().map(|chunk| chunk.udp_wire_len).sum();
                let start = chunks[message]
                    .iter()
                    .filter_map(|chunk| chunk.first_send_end_s)
                    .min_by(f64::total_cmp)
                    .expect("sent message has first-send time");
                let end = udp_ack_times[message].unwrap_or(message_completion[message]);
                events.push((start, bytes as i64));
                events.push((end, -(bytes as i64)));
            }
        }
        Candidate::SingleReliableStream
        | Candidate::MultipleReliableStreams
        | Candidate::HybridDatagramControl => {
            for message in chunks {
                for chunk in message {
                    if chunk.stream_id.is_none() {
                        continue;
                    }
                    let start = chunk.first_send_end_s.expect("stream chunk was sent");
                    let end = chunk
                        .success_arrival_s
                        .expect("stream chunk eventually arrives")
                        + one_way_s;
                    events.push((start, chunk.stream_wire_len as i64));
                    events.push((end, -(chunk.stream_wire_len as i64)));
                }
            }
        }
        Candidate::UdpWholeMessage => {}
    }
    peak_from_events(events)
}

fn peak_from_events(mut events: Vec<(f64, i64)>) -> usize {
    events.sort_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    let mut current = 0_i64;
    let mut peak = 0_i64;
    for (_, delta) in events {
        current += delta;
        peak = peak.max(current);
    }
    peak.max(0) as usize
}

fn message_latency_metrics(workload: &Workload, completion: &[f64]) -> (f64, f64, f64) {
    let mut bulk = Vec::new();
    let mut control = Vec::new();
    for (message, spec) in workload.messages.iter().enumerate() {
        match spec.class {
            MessageClass::Bulk => bulk.push(completion[message]),
            MessageClass::Control => {
                control.push((completion[message] - spec.release.as_secs_f64()).max(0.0));
            }
        }
    }
    let mean_bulk = mean(&bulk);
    let mean_control = mean(&control);
    let p95_control = percentile(&control, 0.95);
    (mean_bulk, mean_control, p95_control)
}

fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

fn percentile(values: &[f64], percentile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut samples = values.to_vec();
    samples.sort_by(f64::total_cmp);
    let index = ((samples.len() - 1) as f64 * percentile).round() as usize;
    samples[index]
}

fn authenticated_overhead() -> usize {
    Authenticator::new(Some(ClusterKey::new([0x27; 32])), false)
        .expect("non-encrypted authentication is always available")
        .overhead()
}

fn missing_nack_wire_len(fragment_count: usize, auth: usize) -> usize {
    auth + SELECTIVE_CONTROL_TAG_LEN
        + TRANSFER_ID_LEN
        + FRAGMENT_COUNT_LEN
        + fragment_count.div_ceil(8)
}

fn completion_ack_wire_len(auth: usize) -> usize {
    auth + SELECTIVE_CONTROL_TAG_LEN + TRANSFER_ID_LEN
}

fn logical_unit(message: usize, chunk: usize) -> u64 {
    ((message as u64) << 32) | chunk as u64
}

fn probability_event(seed: u64, domain: u64, unit: u64, attempt: u64, percent: f64) -> bool {
    if percent <= 0.0 {
        return false;
    }
    let state = splitmix64(
        seed ^ domain
            ^ unit.wrapping_mul(0x9e37_79b9_7f4a_7c15)
            ^ attempt.wrapping_mul(0xbf58_476d_1ce4_e5b9),
    );
    let sample = state as f64 / u64::MAX as f64;
    sample < percent / 100.0
}

fn splitmix64(mut state: u64) -> u64 {
    state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn serialization_seconds(bytes: usize, bandwidth_bps: u64) -> f64 {
    bytes as f64 * 8.0 / bandwidth_bps as f64
}

fn seconds(value: f64) -> Duration {
    Duration::from_secs_f64(value.max(0.0))
}

fn useful_prefix_bytes(total: usize, payload: usize, chunks: usize) -> usize {
    total.min(payload.saturating_mul(chunks))
}

fn chunk_wire_lengths(
    logical_bytes: usize,
    payload: usize,
    auth: usize,
    budget: usize,
    stream: bool,
) -> Vec<usize> {
    let complete = complete_payload_capacity(budget, auth)
        .expect("datagram budget must fit complete-frame overhead");
    let fragmented = logical_bytes > complete;
    let mut remaining = logical_bytes;
    let mut result = Vec::new();
    while remaining > 0 {
        let useful = remaining.min(payload);
        result.push(if stream {
            STREAM_FRAME_OVERHEAD_BYTES + useful
        } else {
            auth + if fragmented {
                FRAGMENT_HEADER_LEN
            } else {
                COMPLETE_HEADER_LEN
            } + useful
        });
        remaining -= useful;
    }
    result
}
