// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Small analytical transport model used by repository benchmarks.
//!
//! This is intentionally not a TCP/QUIC simulator. It exposes every assumption that materially
//! affects the projection: RTT, directional bandwidth, MTU, packet loss/reordering, transport
//! framing, handshake RTTs and reliability semantics. Results are useful for comparative
//! sensitivity analysis, not for predicting a particular kernel/network stack to the millisecond.

use crate::experiment::{PeerSide, RepairStage, RepairStrategy, RepairTrace};

#[derive(Clone, Copy, Debug)]
pub struct LinkProfile {
    pub name: &'static str,
    pub rtt_ms: f64,
    pub forward_mbps: f64,
    pub reverse_mbps: f64,
    pub mtu_bytes: usize,
    pub packet_loss: f64,
    pub packet_reorder: f64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Reliability {
    /// Self-framed datagrams retried by the application per logical interaction.
    DatagramRetry,
    /// Ordered reliable byte stream. Loss/reordering is repaired below the protocol.
    ReliableOrdered,
}

#[derive(Clone, Copy, Debug)]
pub struct TransportProfile {
    pub name: &'static str,
    pub frame_overhead_bytes: usize,
    pub handshake_rtts: f64,
    pub reliability: Reliability,
}

#[derive(Clone, Copy, Debug)]
pub struct Exchange {
    /// Application bytes sent in the nominal forward direction.
    pub forward_app_bytes: usize,
    /// Application bytes sent in the reverse direction.
    pub reverse_app_bytes: usize,
    /// Protocol critical path expressed in RTTs, excluding transport handshake.
    pub interaction_rtts: f64,
    /// One-way protocol messages/stages used for datagram retry modeling.
    pub logical_messages: usize,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Estimate {
    pub app_bytes: usize,
    pub framed_bytes: usize,
    pub expected_wire_bytes: f64,
    pub packets: usize,
    pub retransmitted_bytes: f64,
    pub handshake_ms: f64,
    pub propagation_ms: f64,
    pub serialization_ms: f64,
    pub loss_recovery_ms: f64,
    pub reorder_wait_ms: f64,
    pub total_ms: f64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RatelessEstimate {
    /// Time until the receiver has enough coded symbols to decode.
    pub discovery: Estimate,
    /// Time/bytes until the sender receives a stop signal and becomes quiescent.
    pub quiescence: Estimate,
    /// Application coded bytes sent after the receiver's decode threshold because feedback has not
    /// reached the sender yet. Full-rate steady-state upper envelope: one bandwidth-delay product.
    pub stop_overshoot_app_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FlightDirection {
    Forward,
    Reverse,
}

/// One causally ordered one-way flight. Bytes in a flight may span multiple MTU-sized frames, but
/// the next flight cannot begin until this one reaches the peer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CausalFlight {
    pub direction: FlightDirection,
    pub app_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct FiniteTraceEstimate {
    pub network: Estimate,
    pub forward_app_bytes: usize,
    pub reverse_app_bytes: usize,
    pub interaction_rtts: f64,
    pub flights: Vec<CausalFlight>,
}

fn one_payload_variant(bytes: &[u64]) -> usize {
    match bytes {
        [] => 0,
        [bytes] => *bytes as usize,
        _ => panic!("transport projection requires one concrete payload-size variant"),
    }
}

/// Reconstruct the finite causal flights represented by an RBSR or Merkle repair trace.
///
/// RBSR enumeration produced by one responder shares the return flight with the next round's
/// refined child ranges; only a final enumeration creates an extra half-RTT. Merkle requests and
/// responses are separate one-way flights.
pub fn causal_flights(trace: &RepairTrace) -> Vec<CausalFlight> {
    let mut flights = Vec::new();
    match trace.strategy {
        RepairStrategy::Rbsr => {
            let mut pending_enumeration = 0usize;
            let mut last_forward = true;
            for stage in &trace.stages {
                let RepairStage::RbsrRound {
                    responder,
                    refinement_bytes,
                    enumerated_bytes,
                    ..
                } = stage
                else {
                    panic!("RBSR trace contains a non-RBSR stage");
                };
                let forward = matches!(responder, PeerSide::Right);
                let bytes = *refinement_bytes as usize + pending_enumeration;
                if bytes > 0 {
                    flights.push(CausalFlight {
                        direction: if forward {
                            FlightDirection::Forward
                        } else {
                            FlightDirection::Reverse
                        },
                        app_bytes: bytes,
                    });
                }
                pending_enumeration = one_payload_variant(enumerated_bytes);
                last_forward = forward;
            }
            if pending_enumeration > 0 {
                flights.push(CausalFlight {
                    direction: if last_forward {
                        FlightDirection::Reverse
                    } else {
                        FlightDirection::Forward
                    },
                    app_bytes: pending_enumeration,
                });
            }
        }
        RepairStrategy::Merkle => {
            for stage in &trace.stages {
                match stage {
                    RepairStage::MerkleExchange {
                        request_bytes,
                        response_bytes,
                        ..
                    }
                    | RepairStage::MerkleFetch {
                        request_bytes,
                        response_bytes,
                        ..
                    } => {
                        if *request_bytes > 0 {
                            flights.push(CausalFlight {
                                direction: FlightDirection::Forward,
                                app_bytes: *request_bytes as usize,
                            });
                        }
                        if *response_bytes > 0 {
                            flights.push(CausalFlight {
                                direction: FlightDirection::Reverse,
                                app_bytes: *response_bytes as usize,
                            });
                        }
                    }
                    _ => panic!("Merkle trace contains a non-Merkle stage"),
                }
            }
        }
        RepairStrategy::Riblt => panic!("RIBLT is a stream, not a finite-flight trace"),
    }
    flights
}

fn swapped(link: LinkProfile) -> LinkProfile {
    LinkProfile {
        forward_mbps: link.reverse_mbps,
        reverse_mbps: link.forward_mbps,
        ..link
    }
}

fn accumulate(mut left: Estimate, right: Estimate) -> Estimate {
    left.app_bytes += right.app_bytes;
    left.framed_bytes += right.framed_bytes;
    left.expected_wire_bytes += right.expected_wire_bytes;
    left.packets += right.packets;
    left.retransmitted_bytes += right.retransmitted_bytes;
    left.propagation_ms += right.propagation_ms;
    left.serialization_ms += right.serialization_ms;
    left.loss_recovery_ms += right.loss_recovery_ms;
    left.reorder_wait_ms += right.reorder_wait_ms;
    left.total_ms += right.total_ms;
    left
}

/// Project a finite RBSR/Merkle causal trace one flight at a time.
///
/// Packetization and datagram retry are deliberately applied per causal flight, not after
/// aggregating the whole session. This is the decomposition used by NetemTransport validation.
pub fn estimate_finite_trace(
    trace: &RepairTrace,
    link: LinkProfile,
    transport: TransportProfile,
) -> FiniteTraceEstimate {
    let flights = causal_flights(trace);
    let bare = TransportProfile {
        handshake_rtts: 0.0,
        ..transport
    };
    let mut network = Estimate::default();
    let mut forward = 0usize;
    let mut reverse = 0usize;

    for flight in &flights {
        let flight_link = match flight.direction {
            FlightDirection::Forward => {
                forward += flight.app_bytes;
                link
            }
            FlightDirection::Reverse => {
                reverse += flight.app_bytes;
                swapped(link)
            }
        };
        network = accumulate(
            network,
            estimate(
                Exchange {
                    forward_app_bytes: flight.app_bytes,
                    reverse_app_bytes: 0,
                    interaction_rtts: 0.5,
                    logical_messages: 1,
                },
                flight_link,
                bare,
            ),
        );
    }

    network.handshake_ms = transport.handshake_rtts * link.rtt_ms;
    network.total_ms += network.handshake_ms;

    FiniteTraceEstimate {
        network,
        forward_app_bytes: forward,
        reverse_app_bytes: reverse,
        interaction_rtts: flights.len() as f64 * 0.5,
        flights,
    }
}

fn payload_capacity(link: LinkProfile, transport: TransportProfile) -> usize {
    assert!(link.mtu_bytes > transport.frame_overhead_bytes);
    link.mtu_bytes - transport.frame_overhead_bytes
}

fn packetize(bytes: usize, link: LinkProfile, transport: TransportProfile) -> (usize, usize) {
    if bytes == 0 {
        return (0, 0);
    }
    let packets = bytes.div_ceil(payload_capacity(link, transport));
    (packets, bytes + packets * transport.frame_overhead_bytes)
}

fn serialization_ms(bytes: usize, mbps: f64) -> f64 {
    assert!(mbps > 0.0);
    bytes as f64 * 8.0 / (mbps * 1_000_000.0) * 1_000.0
}

fn expected_wire(framed: usize, loss: f64) -> (f64, f64) {
    assert!((0.0..1.0).contains(&loss));
    let expected = framed as f64 / (1.0 - loss);
    (expected, expected - framed as f64)
}

/// Project one finite exchange onto a link/transport profile.
///
/// Loss modeling is deliberately simple:
/// - reliable streams inflate wire bytes by 1/(1-p) and add a fractional RTT recovery penalty
///   proportional to the critical-path interaction count;
/// - datagram mode assumes each logical interaction is packetized independently and retried whole
///   until every packet in that interaction arrives. This is intentionally pessimistic for a large
///   multi-packet message and motivates the selective-reliability work tracked separately.
///
/// Reordering adds waiting only for ordered streams. Self-framed datagrams are assumed sequenceable
/// and idempotent at the application layer, so arrival order itself does not change bytes/correctness.
pub fn estimate(exchange: Exchange, link: LinkProfile, transport: TransportProfile) -> Estimate {
    let (forward_packets, forward_framed) = packetize(exchange.forward_app_bytes, link, transport);
    let (reverse_packets, reverse_framed) = packetize(exchange.reverse_app_bytes, link, transport);
    let packets = forward_packets + reverse_packets;
    let framed = forward_framed + reverse_framed;

    let handshake_ms = transport.handshake_rtts * link.rtt_ms;
    let base_propagation = exchange.interaction_rtts * link.rtt_ms;
    let serialization = serialization_ms(forward_framed, link.forward_mbps)
        + serialization_ms(reverse_framed, link.reverse_mbps);

    let (
        propagation_ms,
        loss_recovery_ms,
        reorder_wait_ms,
        expected_wire_bytes,
        retransmitted_bytes,
    ) = match transport.reliability {
        Reliability::DatagramRetry => {
            let messages = exchange.logical_messages.max(1);
            let average_packets = packets.div_ceil(messages).max(1);
            let success = (1.0 - link.packet_loss).powi(average_packets as i32);
            let attempts = 1.0 / success;
            let loss_recovery = base_propagation * (attempts - 1.0);
            let any_reordered = 1.0 - (1.0 - link.packet_reorder).powi(average_packets as i32);
            // A reordered datagram is displaced by one additional one-way link delay in netem.
            // Only the successful attempt determines completion, so this term is not multiplied by
            // the number of loss retries.
            let reorder_wait = base_propagation * any_reordered;
            let expected = framed as f64 * attempts;
            (
                base_propagation,
                loss_recovery,
                reorder_wait,
                expected,
                expected - framed as f64,
            )
        }
        Reliability::ReliableOrdered => {
            let loss_recovery = exchange.interaction_rtts * link.rtt_ms * link.packet_loss
                / (1.0 - link.packet_loss);
            let reorder_wait = exchange.interaction_rtts * link.rtt_ms * link.packet_reorder;
            let (expected, retransmitted) = expected_wire(framed, link.packet_loss);
            (
                base_propagation,
                loss_recovery,
                reorder_wait,
                expected,
                retransmitted,
            )
        }
    };

    Estimate {
        app_bytes: exchange.forward_app_bytes + exchange.reverse_app_bytes,
        framed_bytes: framed,
        expected_wire_bytes,
        packets,
        retransmitted_bytes,
        handshake_ms,
        propagation_ms,
        serialization_ms: serialization,
        loss_recovery_ms,
        reorder_wait_ms,
        total_ms: handshake_ms
            + propagation_ms
            + serialization
            + loss_recovery_ms
            + reorder_wait_ms,
    }
}

mod rateless;
pub use rateless::estimate_rateless_stop;

#[cfg(test)]
mod tests;
