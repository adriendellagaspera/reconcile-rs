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
    left.handshake_ms += right.handshake_ms;
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
    let (expected, retransmitted) = expected_wire(framed, link.packet_loss);

    let handshake_ms = transport.handshake_rtts * link.rtt_ms;
    let base_propagation = exchange.interaction_rtts * link.rtt_ms;
    let serialization = serialization_ms(forward_framed, link.forward_mbps)
        + serialization_ms(reverse_framed, link.reverse_mbps);

    let (propagation_ms, loss_recovery_ms, reorder_wait_ms) = match transport.reliability {
        Reliability::DatagramRetry => {
            let messages = exchange.logical_messages.max(1);
            let average_packets = packets.div_ceil(messages).max(1);
            let success = (1.0 - link.packet_loss).powi(average_packets as i32);
            let attempts = 1.0 / success;
            let propagation = base_propagation * attempts;
            let any_reordered =
                1.0 - (1.0 - link.packet_reorder).powi(average_packets as i32);
            // A reordered datagram is displaced by one additional one-way link delay in netem.
            // Only the successful attempt determines completion, so this term is not multiplied by
            // the number of loss retries.
            let reorder_wait = base_propagation * any_reordered;
            (
                propagation,
                propagation - base_propagation,
                reorder_wait,
            )
        }
        Reliability::ReliableOrdered => {
            let loss_recovery = exchange.interaction_rtts * link.rtt_ms * link.packet_loss
                / (1.0 - link.packet_loss);
            let reorder_wait = exchange.interaction_rtts * link.rtt_ms * link.packet_reorder;
            (base_propagation, loss_recovery, reorder_wait)
        }
    };

    Estimate {
        app_bytes: exchange.forward_app_bytes + exchange.reverse_app_bytes,
        framed_bytes: framed,
        expected_wire_bytes: expected,
        packets,
        retransmitted_bytes: retransmitted,
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

/// Project a one-way rateless stream with a reverse stop signal.
///
/// decoded_app_bytes is how many coded-symbol bytes must arrive before the receiver can decode.
/// In the full-rate steady-state envelope the sender transmits for one full RTT after those bytes
/// cross the receiver: one half-RTT of bytes were already in flight, then another half-RTT is sent
/// before the stop signal arrives. That is one forward bandwidth-delay product of overshoot.
///
/// This intentionally does not model congestion-window startup. It is the reusable/full-rate case;
/// cold-start transport handshakes are still priced by transport.handshake_rtts.
pub fn estimate_rateless_stop(
    decoded_app_bytes: usize,
    stop_app_bytes: usize,
    link: LinkProfile,
    transport: TransportProfile,
) -> RatelessEstimate {
    let discovery = estimate(
        Exchange {
            forward_app_bytes: decoded_app_bytes,
            reverse_app_bytes: 0,
            interaction_rtts: 0.5,
            logical_messages: 1,
        },
        link,
        transport,
    );

    let bdp = link.forward_mbps * 1_000_000.0 / 8.0 * (link.rtt_ms / 1_000.0);
    let overshoot = bdp.ceil() as usize;

    let quiescence = estimate(
        Exchange {
            forward_app_bytes: decoded_app_bytes + overshoot,
            reverse_app_bytes: stop_app_bytes,
            interaction_rtts: 1.0,
            logical_messages: 2,
        },
        link,
        transport,
    );

    RatelessEstimate {
        discovery,
        quiescence,
        stop_overshoot_app_bytes: overshoot,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LINK: LinkProfile = LinkProfile {
        name: "test",
        rtt_ms: 10.0,
        forward_mbps: 10.0,
        reverse_mbps: 10.0,
        mtu_bytes: 1_500,
        packet_loss: 0.0,
        packet_reorder: 0.0,
    };

    const STREAM: TransportProfile = TransportProfile {
        name: "stream",
        frame_overhead_bytes: 40,
        handshake_rtts: 1.0,
        reliability: Reliability::ReliableOrdered,
    };

    #[test]
    fn packet_framing_and_handshake_are_explicit() {
        let estimate = estimate(
            Exchange {
                forward_app_bytes: 1_460,
                reverse_app_bytes: 0,
                interaction_rtts: 1.0,
                logical_messages: 1,
            },
            LINK,
            STREAM,
        );
        assert_eq!(estimate.packets, 1);
        assert_eq!(estimate.framed_bytes, 1_500);
        assert_eq!(estimate.handshake_ms, 10.0);
        assert_eq!(estimate.propagation_ms, 10.0);
    }

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-9,
            "expected {expected}, got {actual}"
        );
    }

    #[test]
    fn serialization_formula_and_bidirectional_sum_are_exact() {
        assert_close(serialization_ms(125_000, 1.0), 1_000.0);

        let estimate = estimate(
            Exchange {
                forward_app_bytes: 1_460,
                reverse_app_bytes: 2_920,
                interaction_rtts: 0.0,
                logical_messages: 2,
            },
            LinkProfile {
                rtt_ms: 10.0,
                forward_mbps: 10.0,
                reverse_mbps: 10.0,
                ..LINK
            },
            TransportProfile {
                handshake_rtts: 0.0,
                ..STREAM
            },
        );
        assert_eq!(estimate.packets, 3);
        assert_eq!(estimate.framed_bytes, 4_500);
        assert_close(estimate.serialization_ms, 3.6);
        assert_close(estimate.total_ms, 3.6);
    }

    #[test]
    fn reliable_loss_reorder_and_total_terms_are_exact() {
        let link = LinkProfile {
            rtt_ms: 10.0,
            packet_loss: 0.1,
            packet_reorder: 0.2,
            forward_mbps: 10.0,
            reverse_mbps: 10.0,
            ..LINK
        };
        let estimate = estimate(
            Exchange {
                forward_app_bytes: 1_460,
                reverse_app_bytes: 1_460,
                interaction_rtts: 2.0,
                logical_messages: 2,
            },
            link,
            STREAM,
        );

        assert_eq!(estimate.packets, 2);
        assert_eq!(estimate.framed_bytes, 3_000);
        assert_close(estimate.propagation_ms, 20.0);
        assert_close(estimate.loss_recovery_ms, 20.0 * 0.1 / 0.9);
        assert_close(estimate.reorder_wait_ms, 4.0);
        assert_close(estimate.serialization_ms, 2.4);
        assert_close(
            estimate.total_ms,
            10.0 + 20.0 + 2.4 + 20.0 * 0.1 / 0.9 + 4.0,
        );
    }

    #[test]
    fn datagram_retry_exposes_exact_retry_penalty() {
        let link = LinkProfile {
            rtt_ms: 10.0,
            packet_loss: 0.1,
            ..LINK
        };
        let datagram = TransportProfile {
            name: "datagram",
            frame_overhead_bytes: 28,
            handshake_rtts: 0.0,
            reliability: Reliability::DatagramRetry,
        };
        let estimate = estimate(
            Exchange {
                forward_app_bytes: 1_000,
                reverse_app_bytes: 0,
                interaction_rtts: 1.0,
                logical_messages: 1,
            },
            link,
            datagram,
        );
        let propagation = 10.0 / 0.9;
        assert_close(estimate.propagation_ms, propagation);
        assert_close(estimate.loss_recovery_ms, propagation - 10.0);
        assert_close(estimate.reorder_wait_ms, 0.0);
        assert_close(
            estimate.total_ms,
            propagation + estimate.serialization_ms + estimate.loss_recovery_ms,
        );
    }

    #[test]
    fn rateless_stop_exposes_one_bdp_overshoot() {
        let estimate = estimate_rateless_stop(1_000, 32, LINK, STREAM);
        assert_eq!(estimate.stop_overshoot_app_bytes, 12_500);
        assert!(estimate.quiescence.app_bytes > estimate.discovery.app_bytes);
    }

    #[test]
    fn rbsr_causal_flights_piggyback_prior_enumeration() {
        let trace = RepairTrace::new(
            RepairStrategy::Rbsr,
            vec![
                RepairStage::RbsrRound {
                    responder: PeerSide::Right,
                    refinement_ranges: 1,
                    refinement_bytes: 10,
                    enumeration_ranges: 1,
                    enumerated_elements: 1,
                    enumerated_bytes: vec![20],
                    frameable_outputs: 2,
                },
                RepairStage::RbsrRound {
                    responder: PeerSide::Left,
                    refinement_ranges: 1,
                    refinement_bytes: 30,
                    enumeration_ranges: 0,
                    enumerated_elements: 0,
                    enumerated_bytes: vec![],
                    frameable_outputs: 1,
                },
            ],
        );
        assert_eq!(
            causal_flights(&trace),
            vec![
                CausalFlight {
                    direction: FlightDirection::Forward,
                    app_bytes: 10,
                },
                CausalFlight {
                    direction: FlightDirection::Reverse,
                    app_bytes: 50,
                },
            ]
        );
    }

    #[test]
    fn finite_trace_packetizes_each_causal_flight() {
        let trace = RepairTrace::new(
            RepairStrategy::Merkle,
            vec![RepairStage::MerkleExchange {
                depth: 1,
                request_prefixes: 1,
                request_bytes: 500,
                response_hashes: 1,
                response_bytes: 500,
            }],
        );
        let estimate = estimate_finite_trace(
            &trace,
            LINK,
            TransportProfile {
                handshake_rtts: 0.0,
                ..STREAM
            },
        );
        assert_eq!(estimate.network.app_bytes, 1_000);
        // Aggregate packetization would fit 1,000 B in one 1,460 B payload; causal packetization
        // correctly keeps request and response in separate one-way frames.
        assert_eq!(estimate.network.packets, 2);
        assert_eq!(estimate.interaction_rtts, 1.0);
    }

    #[test]
    fn datagram_reordering_prices_completion_of_the_slowest_frame() {
        let link = LinkProfile {
            rtt_ms: 10.0,
            packet_loss: 0.0,
            packet_reorder: 0.2,
            ..LINK
        };
        let datagram = TransportProfile {
            name: "datagram",
            frame_overhead_bytes: 28,
            handshake_rtts: 0.0,
            reliability: Reliability::DatagramRetry,
        };
        let estimate = estimate(
            Exchange {
                forward_app_bytes: 2_000,
                reverse_app_bytes: 0,
                interaction_rtts: 0.5,
                logical_messages: 1,
            },
            link,
            datagram,
        );
        let any_reordered = 1.0 - 0.8_f64.powi(2);
        assert_close(estimate.reorder_wait_ms, 5.0 * any_reordered);
    }

    #[test]
    fn datagram_loss_increases_expected_latency() {
        let lossy = LinkProfile {
            packet_loss: 0.1,
            ..LINK
        };
        let datagram = TransportProfile {
            name: "datagram",
            frame_overhead_bytes: 28,
            handshake_rtts: 0.0,
            reliability: Reliability::DatagramRetry,
        };
        let estimate = estimate(
            Exchange {
                forward_app_bytes: 3_000,
                reverse_app_bytes: 0,
                interaction_rtts: 1.0,
                logical_messages: 1,
            },
            lossy,
            datagram,
        );
        assert!(estimate.propagation_ms > lossy.rtt_ms);
        assert!(estimate.expected_wire_bytes > estimate.framed_bytes as f64);
    }
}
