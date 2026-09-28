// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// Project structured repair traces onto explicit generic network/transport assumptions.
//
// Input is produced by the comparison benchmarks through RECONCILE_BENCH_OUTPUT. This target reads
// both the ExperimentReport (for measured local SessionWork elapsed time) and its per-arm
// RepairTrace sidecar (for causal direction/stage structure). It does not parse benchmark stdout.
//
// Run:
//   RECONCILE_TRANSPORT_TRACE_DIR=/path/to/results cargo bench --bench transport_sensitivity
//
// The transport constants below are explicit analytical assumptions, not wire-accurate TCP/QUIC
// claims. RIBLT reports receiver discovery separately from sender quiescence and models a
// full-rate one-BDP stop-ACK overshoot envelope.

use std::env;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use devkit::experiment::{
    read_experiment_report, read_repair_trace, CostOwner, ExperimentReport, LifecyclePhase,
    Measurement, MetricKind, MetricValue, RepairStage, RepairStrategy, RepairTrace, RunObservation,
};
use devkit::transport_model::{
    estimate, estimate_finite_trace, estimate_rateless_stop, Estimate, Exchange, LinkProfile,
    Reliability, TransportProfile,
};

#[derive(Clone)]
struct TraceInput {
    workload: String,
    arm: String,
    cpu_ms: f64,
    trace: RepairTrace,
}

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

fn observed_seconds(measurement: &Measurement) -> Option<f64> {
    match measurement {
        Measurement::Observed {
            value: MetricValue::Seconds(value),
            ..
        } => Some(*value),
        _ => None,
    }
}

fn session_cpu_ms(observation: &RunObservation) -> f64 {
    observation
        .costs
        .iter()
        .filter(|cost| {
            cost.phase == LifecyclePhase::SessionWork && cost.owner == CostOwner::Protocol
        })
        .flat_map(|cost| &cost.metrics)
        .find_map(|metric| {
            (metric.kind == MetricKind::LocalElapsedSeconds)
                .then(|| observed_seconds(&metric.measurement))
                .flatten()
        })
        .expect("benchmark report must contain measured protocol SessionWork elapsed time")
        * 1_000.0
}

fn trace_path(
    directory: &Path,
    report: &ExperimentReport,
    observation: &RunObservation,
) -> PathBuf {
    directory.join(format!(
        "{}-{}-{}-{}.trace.json",
        report.experiment.logical_task_id,
        report.experiment.workload_id,
        observation.seed,
        observation.architecture_id
    ))
}

fn load_inputs(directory: &Path) -> Vec<TraceInput> {
    let mut report_paths: Vec<_> = fs::read_dir(directory)
        .expect("read transport trace directory")
        .map(|entry| entry.expect("read trace-directory entry").path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
                && !path
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .ends_with(".trace.json")
        })
        .collect();
    report_paths.sort();

    let mut inputs = Vec::new();
    for report_path in report_paths {
        let report =
            read_experiment_report(File::open(&report_path).expect("open experiment report"))
                .expect("read experiment report");
        for observation in &report.observations {
            let path = trace_path(directory, &report, observation);
            if !path.exists() {
                continue;
            }
            let trace = read_repair_trace(File::open(&path).expect("open repair trace"))
                .expect("read repair trace");
            inputs.push(TraceInput {
                workload: report.experiment.workload_id.clone(),
                arm: observation.architecture_id.clone(),
                cpu_ms: session_cpu_ms(observation),
                trace,
            });
        }
    }
    inputs.sort_by(|left, right| (&left.workload, &left.arm).cmp(&(&right.workload, &right.arm)));
    assert!(
        !inputs.is_empty(),
        "transport input directory contained no joined report/trace pairs"
    );
    inputs
}

fn no_handshake(transport: TransportProfile) -> TransportProfile {
    TransportProfile {
        handshake_rtts: 0.0,
        ..transport
    }
}

fn swap_link(link: LinkProfile) -> LinkProfile {
    LinkProfile {
        forward_mbps: link.reverse_mbps,
        reverse_mbps: link.forward_mbps,
        ..link
    }
}

fn handshake_ms(link: LinkProfile, transport: TransportProfile) -> f64 {
    transport.handshake_rtts * link.rtt_ms
}

fn print_finite(input: &TraceInput, link: LinkProfile, transport: TransportProfile) {
    let projected = estimate_finite_trace(&input.trace, link, transport);
    let network = projected.network;
    println!(
        "[transport] workload={} arm={} strategy={:?} link={} transport={} forward_app={} reverse_app={} app_bytes={} framed_bytes={} expected_wire={:.0} packets={} interaction_rtts={:.1} logical_messages={} handshake={:.2}ms propagation={:.2}ms serialization={:.2}ms loss={:.2}ms reorder={:.2}ms network={:.2}ms cpu={:.3}ms additive_total={:.2}ms",
        input.workload,
        input.arm,
        input.trace.strategy,
        link.name,
        transport.name,
        projected.forward_app_bytes,
        projected.reverse_app_bytes,
        network.app_bytes,
        network.framed_bytes,
        network.expected_wire_bytes,
        network.packets,
        projected.interaction_rtts,
        projected.flights.len(),
        network.handshake_ms,
        network.propagation_ms,
        network.serialization_ms,
        network.loss_recovery_ms,
        network.reorder_wait_ms,
        network.total_ms,
        input.cpu_ms,
        network.total_ms + input.cpu_ms,
    );
}
fn riblt_parts(trace: &RepairTrace) -> (usize, usize, usize) {
    let mut equality = 0usize;
    let mut stream = 0usize;
    let mut stop = 0usize;
    for stage in &trace.stages {
        match stage {
            RepairStage::RibltEquality { bytes } => equality += *bytes as usize,
            RepairStage::RibltStream {
                coded_symbols,
                coded_symbol_bytes,
            } => stream += (*coded_symbols * *coded_symbol_bytes) as usize,
            RepairStage::RibltStopAck { bytes } => stop += *bytes as usize,
            _ => panic!("RIBLT trace contains a non-RIBLT stage"),
        }
    }
    (equality, stream, stop)
}

fn add_estimates(mut left: Estimate, right: Estimate) -> Estimate {
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

fn print_riblt(input: &TraceInput, link: LinkProfile, transport: TransportProfile) {
    let (equality_bytes, stream_bytes, stop_bytes) = riblt_parts(&input.trace);
    let bare_transport = no_handshake(transport);
    let preflight = estimate(
        Exchange {
            forward_app_bytes: equality_bytes,
            reverse_app_bytes: 0,
            interaction_rtts: 0.5,
            logical_messages: 1,
        },
        link,
        bare_transport,
    );
    let handshake = handshake_ms(link, transport);

    if stream_bytes == 0 {
        println!(
            "[transport-riblt] workload={} arm={} link={} transport={} equality_bytes={} decoded_bytes=0 discovery_wire={:.0} discovery_network={:.2}ms cpu={:.3}ms additive_discovery={:.2}ms stop_overshoot=0 quiescence_wire={:.0} quiescence_network={:.2}ms",
            input.workload,
            input.arm,
            link.name,
            transport.name,
            equality_bytes,
            preflight.expected_wire_bytes,
            handshake + preflight.total_ms,
            input.cpu_ms,
            handshake + preflight.total_ms + input.cpu_ms,
            preflight.expected_wire_bytes,
            handshake + preflight.total_ms,
        );
        return;
    }

    // RIBLT convention from RepairTrace: Left sends equality to Right, Right streams coded symbols
    // to Left, then Left sends the stop signal to Right. Swap directional bandwidth so the generic
    // rateless helper's "forward" direction is the actual Right -> Left stream.
    let stream = estimate_rateless_stop(stream_bytes, stop_bytes, swap_link(link), bare_transport);
    let discovery = add_estimates(preflight, stream.discovery);
    let quiescence = add_estimates(preflight, stream.quiescence);

    println!(
        "[transport-riblt] workload={} arm={} link={} transport={} equality_bytes={} decoded_bytes={} discovery_wire={:.0} discovery_network={:.2}ms cpu={:.3}ms additive_discovery={:.2}ms stop_overshoot={} quiescence_wire={:.0} quiescence_network={:.2}ms",
        input.workload,
        input.arm,
        link.name,
        transport.name,
        equality_bytes,
        stream_bytes,
        discovery.expected_wire_bytes,
        handshake + discovery.total_ms,
        input.cpu_ms,
        handshake + discovery.total_ms + input.cpu_ms,
        stream.stop_overshoot_app_bytes,
        quiescence.expected_wire_bytes,
        handshake + quiescence.total_ms,
    );
}

fn main() {
    let directory = PathBuf::from(
        env::var_os("RECONCILE_TRANSPORT_TRACE_DIR")
            .expect("set RECONCILE_TRANSPORT_TRACE_DIR to structured benchmark output"),
    );
    let inputs = load_inputs(&directory);

    println!(
        "[transport-model] inputs={} structured repair traces; no hardcoded algorithm byte/message totals",
        inputs.len()
    );
    println!(
        "[transport-model] generic projection only; frame overheads/handshake RTTs are model constants, not wire-accurate TCP/QUIC claims"
    );
    println!(
        "[transport-model] RIBLT equality Left->Right, stream Right->Left, stop Left->Right; stop overshoot is one full-rate stream-direction BDP"
    );
    println!(
        "[transport-model] measured local SessionWork CPU is reported separately; additive totals assume no CPU/network overlap"
    );

    for input in &inputs {
        for &link in LINKS {
            for &transport in TRANSPORTS {
                match input.trace.strategy {
                    RepairStrategy::Riblt => print_riblt(input, link, transport),
                    RepairStrategy::Rbsr | RepairStrategy::Merkle => {
                        print_finite(input, link, transport)
                    }
                }
            }
        }
    }
}
