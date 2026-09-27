// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// Project fixed arbitrary-key repair traces onto explicit generic network/transport assumptions.
//
// This benchmark does not rerun RBSR/RIBLT/Merkle. It freezes representative measured traces so
// RTT/MTU/loss/handshake sensitivity can be changed without changing the algorithmic workload.
//
// Interactive-protocol bytes are split 50/50 by direction because the source trace records total wire bytes,
// not per-direction round payloads. That approximation only affects directional serialization on
// asymmetric links; total bytes, interaction depth and packetization remain explicit.
//
// RIBLT reports receiver discovery and sender quiescence separately. Quiescence uses the full-rate
// steady-state stop-ACK envelope from devkit::transport_model: one forward bandwidth-delay product
// of coded-symbol overshoot. It is not a congestion-control simulation.

use devkit::transport_model::{
    estimate, estimate_rateless_stop, Exchange, LinkProfile, Reliability, TransportProfile,
};

#[derive(Clone, Copy)]
struct Trace {
    name: &'static str,
    rbsr_bytes: usize,
    rbsr_messages: usize,
    rbsr_cpu_ms: f64,
    riblt_bytes: usize,
    riblt_cpu_ms: f64,
    merkle_bytes: usize,
    merkle_rounds: usize,
    merkle_cpu_ms: f64,
}

const TRACES: &[Trace] = &[
    Trace {
        name: "outside-d1k",
        rbsr_bytes: 19_928,
        rbsr_messages: 7,
        rbsr_cpu_ms: 0.190,
        riblt_bytes: 33_848,
        riblt_cpu_ms: 554.787,
        merkle_bytes: 101_661,
        merkle_rounds: 18,
        merkle_cpu_ms: 0.224,
    },
    Trace {
        name: "outside-d10k",
        rbsr_bytes: 165_890,
        rbsr_messages: 9,
        rbsr_cpu_ms: 0.951,
        riblt_bytes: 325_400,
        riblt_cpu_ms: 4_779.677,
        merkle_bytes: 942_340,
        merkle_rounds: 18,
        merkle_cpu_ms: 2.100,
    },
    Trace {
        name: "mixed-d1k",
        rbsr_bytes: 848_985,
        rbsr_messages: 7,
        rbsr_cpu_ms: 10.520,
        riblt_bytes: 44_816,
        riblt_cpu_ms: 711.363,
        merkle_bytes: 860_758,
        merkle_rounds: 18,
        merkle_cpu_ms: 2.895,
    },
    Trace {
        name: "mixed-d10k",
        rbsr_bytes: 3_957_013,
        rbsr_messages: 7,
        rbsr_cpu_ms: 37.470,
        riblt_bytes: 436_760,
        riblt_cpu_ms: 6_226.091,
        merkle_bytes: 4_421_578,
        merkle_rounds: 18,
        merkle_cpu_ms: 13.739,
    },
];

const LINKS: &[LinkProfile] = &[
    LinkProfile {
        name: "same-host",
        rtt_ms: 0.05,
        forward_mbps: 10_000.0,
        reverse_mbps: 10_000.0,
        mtu_bytes: 9_000,
        packet_loss: 0.0,
        packet_reorder: 0.0,
    },
    LinkProfile {
        name: "lan",
        rtt_ms: 0.5,
        forward_mbps: 1_000.0,
        reverse_mbps: 1_000.0,
        mtu_bytes: 1_500,
        packet_loss: 0.0001,
        packet_reorder: 0.0001,
    },
    LinkProfile {
        name: "regional-wan",
        rtt_ms: 20.0,
        forward_mbps: 200.0,
        reverse_mbps: 100.0,
        mtu_bytes: 1_400,
        packet_loss: 0.001,
        packet_reorder: 0.001,
    },
    LinkProfile {
        name: "intercontinental",
        rtt_ms: 100.0,
        forward_mbps: 100.0,
        reverse_mbps: 50.0,
        mtu_bytes: 1_400,
        packet_loss: 0.005,
        packet_reorder: 0.002,
    },
    LinkProfile {
        name: "high-latency",
        rtt_ms: 600.0,
        forward_mbps: 20.0,
        reverse_mbps: 5.0,
        mtu_bytes: 1_200,
        packet_loss: 0.01,
        packet_reorder: 0.005,
    },
    LinkProfile {
        name: "constrained-asymmetric",
        rtt_ms: 80.0,
        forward_mbps: 10.0,
        reverse_mbps: 2.0,
        mtu_bytes: 1_200,
        packet_loss: 0.02,
        packet_reorder: 0.01,
    },
];

const TRANSPORTS: &[TransportProfile] = &[
    TransportProfile {
        name: "udp-like-retry",
        frame_overhead_bytes: 28,
        handshake_rtts: 0.0,
        reliability: Reliability::DatagramRetry,
    },
    TransportProfile {
        name: "tcp-like-cold",
        frame_overhead_bytes: 40,
        handshake_rtts: 1.0,
        reliability: Reliability::ReliableOrdered,
    },
    TransportProfile {
        name: "tcp-like-warm",
        frame_overhead_bytes: 40,
        handshake_rtts: 0.0,
        reliability: Reliability::ReliableOrdered,
    },
    TransportProfile {
        name: "quic-like-cold",
        frame_overhead_bytes: 48,
        handshake_rtts: 1.0,
        reliability: Reliability::ReliableOrdered,
    },
    TransportProfile {
        name: "quic-like-resumed",
        frame_overhead_bytes: 48,
        handshake_rtts: 0.0,
        reliability: Reliability::ReliableOrdered,
    },
];

fn split_interactive(bytes: usize) -> (usize, usize) {
    (bytes.div_ceil(2), bytes / 2)
}

#[derive(Clone, Copy)]
struct InteractiveShape {
    algorithm: &'static str,
    bytes: usize,
    interaction_rtts: f64,
    logical_messages: usize,
    cpu_ms: f64,
}

fn print_interactive(
    trace: Trace,
    shape: InteractiveShape,
    link: LinkProfile,
    transport: TransportProfile,
) {
    let (forward, reverse) = split_interactive(shape.bytes);
    let network = estimate(
        Exchange {
            forward_app_bytes: forward,
            reverse_app_bytes: reverse,
            interaction_rtts: shape.interaction_rtts,
            logical_messages: shape.logical_messages,
        },
        link,
        transport,
    );
    println!(
        "[transport] trace={} algorithm={} link={} transport={} app_bytes={} framed_bytes={} expected_wire={:.0} packets={} handshake={:.2}ms propagation={:.2}ms serialization={:.2}ms loss={:.2}ms reorder={:.2}ms network={:.2}ms cpu={:.3}ms additive_total={:.2}ms",
        trace.name,
        shape.algorithm,
        link.name,
        transport.name,
        network.app_bytes,
        network.framed_bytes,
        network.expected_wire_bytes,
        network.packets,
        network.handshake_ms,
        network.propagation_ms,
        network.serialization_ms,
        network.loss_recovery_ms,
        network.reorder_wait_ms,
        network.total_ms,
        shape.cpu_ms,
        network.total_ms + shape.cpu_ms,
    );
}

fn print_riblt(trace: Trace, link: LinkProfile, transport: TransportProfile) {
    let network = estimate_rateless_stop(trace.riblt_bytes, 32, link, transport);
    println!(
        "[transport-riblt] trace={} link={} transport={} decoded_bytes={} discovery_wire={:.0} discovery_network={:.2}ms cpu={:.3}ms additive_discovery={:.2}ms stop_overshoot={} quiescence_wire={:.0} quiescence_network={:.2}ms",
        trace.name,
        link.name,
        transport.name,
        trace.riblt_bytes,
        network.discovery.expected_wire_bytes,
        network.discovery.total_ms,
        trace.riblt_cpu_ms,
        network.discovery.total_ms + trace.riblt_cpu_ms,
        network.stop_overshoot_app_bytes,
        network.quiescence.expected_wire_bytes,
        network.quiescence.total_ms,
    );
}

fn main() {
    println!(
        "[transport-model] generic projection only; frame overheads/handshake RTTs are model constants, not wire-accurate TCP/QUIC claims"
    );
    println!(
        "[transport-model] RIBLT stop overshoot is one full-rate forward BDP; CPU+network additive totals ignore overlap"
    );

    for &trace in TRACES {
        for &link in LINKS {
            for &transport in TRANSPORTS {
                print_interactive(
                    trace,
                    InteractiveShape {
                        algorithm: "rbsr",
                        bytes: trace.rbsr_bytes,
                        interaction_rtts: trace.rbsr_messages as f64 / 2.0,
                        logical_messages: trace.rbsr_messages,
                        cpu_ms: trace.rbsr_cpu_ms,
                    },
                    link,
                    transport,
                );
                print_interactive(
                    trace,
                    InteractiveShape {
                        algorithm: "merkle",
                        bytes: trace.merkle_bytes,
                        interaction_rtts: trace.merkle_rounds as f64,
                        logical_messages: trace.merkle_rounds * 2,
                        cpu_ms: trace.merkle_cpu_ms,
                    },
                    link,
                    transport,
                );
                print_riblt(trace, link, transport);
            }
        }
    }
}
