use super::*;

#[test]
fn rbsr_skips_empty_refinement_but_preserves_final_enumeration() {
    let trace = RepairTrace::new(
        RepairStrategy::Rbsr,
        vec![
            RepairStage::RbsrRound {
                responder: PeerSide::Left,
                refinement_ranges: 0,
                refinement_bytes: 0,
                enumeration_ranges: 0,
                enumerated_elements: 0,
                enumerated_bytes: vec![],
                frameable_outputs: 0,
            },
            RepairStage::RbsrRound {
                responder: PeerSide::Left,
                refinement_ranges: 1,
                refinement_bytes: 7,
                enumeration_ranges: 1,
                enumerated_elements: 1,
                enumerated_bytes: vec![11],
                frameable_outputs: 2,
            },
        ],
    );
    assert_eq!(
        causal_flights(&trace),
        vec![
            CausalFlight {
                direction: FlightDirection::Reverse,
                app_bytes: 7,
            },
            CausalFlight {
                direction: FlightDirection::Forward,
                app_bytes: 11,
            },
        ]
    );
}

#[test]
fn finite_trace_sums_directional_costs_and_charges_handshake_once() {
    let trace = RepairTrace::new(
        RepairStrategy::Merkle,
        vec![
            RepairStage::MerkleExchange {
                depth: 1,
                request_prefixes: 1,
                request_bytes: 500,
                response_hashes: 1,
                response_bytes: 200,
            },
            RepairStage::MerkleFetch {
                request_keys: 1,
                request_bytes: 700,
                returned_rows: 0,
                response_bytes: 0,
            },
        ],
    );
    let link = LinkProfile {
        rtt_ms: 20.0,
        forward_mbps: 2.0,
        reverse_mbps: 1.0,
        mtu_bytes: 600,
        packet_loss: 0.2,
        packet_reorder: 0.1,
        ..LINK
    };
    let transport = TransportProfile {
        frame_overhead_bytes: 100,
        handshake_rtts: 2.0,
        ..STREAM
    };
    let projection = estimate_finite_trace(&trace, link, transport);
    let network = projection.network;
    assert_eq!(projection.forward_app_bytes, 1_200);
    assert_eq!(projection.reverse_app_bytes, 200);
    assert_eq!(projection.interaction_rtts, 1.5);
    assert_eq!(network.app_bytes, 1_400);
    assert_eq!(network.packets, 4);
    assert_eq!(network.framed_bytes, 1_800);
    assert_close(network.expected_wire_bytes, 2_250.0);
    assert_close(network.retransmitted_bytes, 450.0);
    assert_close(network.handshake_ms, 40.0);
    assert_close(network.propagation_ms, 30.0);
    assert_close(network.serialization_ms, 8.4);
    assert_close(network.loss_recovery_ms, 7.5);
    assert_close(network.reorder_wait_ms, 3.0);
    assert_close(network.total_ms, 88.9);
}

#[test]
fn datagram_retry_reports_wire_bytes_and_retransmissions() {
    let datagram = TransportProfile {
        name: "datagram",
        frame_overhead_bytes: 28,
        handshake_rtts: 0.0,
        reliability: Reliability::DatagramRetry,
    };
    let projection = estimate(
        Exchange {
            forward_app_bytes: 100,
            reverse_app_bytes: 0,
            interaction_rtts: 0.5,
            logical_messages: 1,
        },
        LinkProfile {
            packet_loss: 0.2,
            ..LINK
        },
        datagram,
    );
    assert_eq!(projection.app_bytes, 100);
    assert_eq!(projection.framed_bytes, 128);
    assert_close(projection.expected_wire_bytes, 160.0);
    assert_close(projection.retransmitted_bytes, 32.0);
}

#[test]
fn rateless_quiescence_counts_only_one_stop_signal_and_one_bdp() {
    let projection = estimate_rateless_stop(1_000, 32, LINK, STREAM);
    assert_eq!(projection.discovery.app_bytes, 1_000);
    assert_eq!(projection.stop_overshoot_app_bytes, 12_500);
    assert_eq!(projection.quiescence.app_bytes, 13_532);
    assert_eq!(projection.quiescence.packets, 11);
}

#[test]
fn merkle_response_without_request_uses_one_reverse_flight() {
    let trace = RepairTrace::new(
        RepairStrategy::Merkle,
        vec![RepairStage::MerkleExchange {
            depth: 0,
            request_prefixes: 0,
            request_bytes: 0,
            response_hashes: 1,
            response_bytes: 17,
        }],
    );
    assert_eq!(
        causal_flights(&trace),
        vec![CausalFlight {
            direction: FlightDirection::Reverse,
            app_bytes: 17,
        }]
    );
}
