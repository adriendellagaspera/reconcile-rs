use super::*;

mod edge_cases;

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
    let completion = 10.0 / 0.9;
    assert_close(estimate.propagation_ms, 10.0);
    assert_close(estimate.loss_recovery_ms, completion - 10.0);
    assert_close(estimate.reorder_wait_ms, 0.0);
    assert_close(estimate.total_ms, completion + estimate.serialization_ms);
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
    assert_eq!(estimate.propagation_ms, lossy.rtt_ms);
    assert!(estimate.loss_recovery_ms > 0.0);
    assert!(estimate.total_ms > lossy.rtt_ms + estimate.serialization_ms);
    assert!(estimate.expected_wire_bytes > estimate.framed_bytes as f64);
}
