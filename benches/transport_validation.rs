// Copyright 2023 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your option.
// You may not use this file except in compliance with those terms.

//! Matched validation of the transport mechanisms that survived the preceding research passes.
//! This benchmark reuses the existing deterministic semantic models and does not introduce a new
//! transport, loss model, or production wire format.

#[allow(clippy::comparison_chain)]
#[path = "fragment_recovery_model.rs"]
mod fragment_recovery_model;
#[allow(dead_code)]
#[path = "ordered_stream_model.rs"]
mod ordered_stream_model;

use std::time::{Duration, Instant};

use fragment_recovery_model::{
    simulate as simulate_fragment, simulate_interruption as simulate_fragment_interruption,
    Case as FragmentCase, Metrics as FragmentMetrics, RecoveryPolicy,
};
use ordered_stream_model::{
    simulate as simulate_stream, simulate_interruption as simulate_stream_interruption, Candidate,
    Case as StreamCase, Metrics as StreamMetrics, Session, Workload,
};

const DEFAULT_TRIALS: u64 = 64;
const DATAGRAM_BUDGET: usize = 1_200;
const BANDWIDTH_BPS: u64 = 100_000_000;
const ONE_MIB: usize = 1_048_576;
const SEED_STRIDE: u64 = 0x9e37_79b9_7f4a_7c15;
const SELECTIVE_SEED_BASE: u64 = 0x2710_0000_0000_0000;
const STREAM_SEED_BASE: u64 = 0x2720_0000_0000_0000;

#[derive(Clone, Copy)]
struct SelectiveScenario {
    name: &'static str,
    rtt_ms: f64,
    loss_percent: f64,
}

const SELECTIVE_SCENARIOS: &[SelectiveScenario] = &[
    SelectiveScenario {
        name: "selective_clean_common",
        rtt_ms: 50.0,
        loss_percent: 0.0,
    },
    SelectiveScenario {
        name: "selective_wan_loss_5",
        rtt_ms: 150.0,
        loss_percent: 5.0,
    },
    SelectiveScenario {
        name: "selective_high_bdp_loss_5",
        rtt_ms: 600.0,
        loss_percent: 5.0,
    },
];

#[derive(Clone, Copy)]
enum StreamWorkload {
    Mixed,
    Concurrent,
}

impl StreamWorkload {
    fn label(self) -> &'static str {
        match self {
            Self::Mixed => "mixed_control_bulk",
            Self::Concurrent => "concurrent_bulk",
        }
    }

    fn build(self) -> Workload {
        match self {
            Self::Mixed => {
                Workload::mixed_control_bulk(512 * 1024, 16, 256, Duration::from_millis(4))
            }
            Self::Concurrent => Workload::concurrent_bulk(4, 256 * 1024),
        }
    }
}

#[derive(Clone, Copy)]
struct StreamScenario {
    name: &'static str,
    workload: StreamWorkload,
    rtt_ms: f64,
    loss_percent: f64,
}

const STREAM_SCENARIOS: &[StreamScenario] = &[
    StreamScenario {
        name: "multiplex_clean_common",
        workload: StreamWorkload::Mixed,
        rtt_ms: 50.0,
        loss_percent: 0.0,
    },
    StreamScenario {
        name: "multiplex_mixed_wan_loss_5",
        workload: StreamWorkload::Mixed,
        rtt_ms: 150.0,
        loss_percent: 5.0,
    },
    StreamScenario {
        name: "multiplex_mixed_high_bdp_loss_5",
        workload: StreamWorkload::Mixed,
        rtt_ms: 600.0,
        loss_percent: 5.0,
    },
    StreamScenario {
        name: "multiplex_concurrent_high_bdp_loss_5",
        workload: StreamWorkload::Concurrent,
        rtt_ms: 600.0,
        loss_percent: 5.0,
    },
];

const STREAM_CANDIDATES: &[Candidate] = &[
    Candidate::UdpWholeMessage,
    Candidate::UdpMissingOnly,
    Candidate::SingleReliableStream,
    Candidate::MultipleReliableStreams,
];

#[derive(Default)]
struct FragmentSummary {
    samples: u64,
    useful_bytes: usize,
    wire_bytes: f64,
    retransmitted_wire_bytes: f64,
    control_bytes: f64,
    parity_bytes: f64,
    receiver_completion_ms: Vec<f64>,
    domain_convergence_ms: f64,
    sender_quiescence_ms: f64,
    peak_sender_state_bytes: f64,
    peak_receiver_state_bytes: f64,
    model_cpu_us: f64,
}

impl FragmentSummary {
    fn push(&mut self, metrics: FragmentMetrics, elapsed: Duration) {
        self.samples += 1;
        self.useful_bytes = metrics.useful_bytes;
        self.wire_bytes += metrics.wire_bytes as f64;
        self.retransmitted_wire_bytes += metrics.retransmitted_wire_bytes as f64;
        self.control_bytes += metrics.control_bytes as f64;
        self.parity_bytes += metrics.parity_wire_bytes as f64;
        self.receiver_completion_ms
            .push(metrics.receiver_completion.as_secs_f64() * 1_000.0);
        self.domain_convergence_ms += metrics.domain_convergence.as_secs_f64() * 1_000.0;
        self.sender_quiescence_ms += metrics.sender_quiescence.as_secs_f64() * 1_000.0;
        self.peak_sender_state_bytes += metrics.peak_sender_recovery_state_bytes as f64;
        self.peak_receiver_state_bytes += metrics.peak_receiver_reassembly_state_bytes as f64;
        self.model_cpu_us += elapsed.as_secs_f64() * 1_000_000.0;
    }

    fn mean_wire_bytes(&self) -> f64 {
        self.wire_bytes / self.samples as f64
    }

    fn mean_receiver_completion_ms(&self) -> f64 {
        mean(&self.receiver_completion_ms)
    }

    fn print(&self, scenario: SelectiveScenario, policy: RecoveryPolicy) {
        println!(
            "[transport-validation] family=selective,point={},candidate={},trials={},useful_bytes={},budget_bytes={},rtt_ms={:.1},loss_percent={:.1},bandwidth_bps={},mean_wire_bytes={:.1},wire_amplification={:.4},mean_retransmitted_wire_bytes={:.1},mean_control_bytes={:.1},mean_parity_bytes={:.1},mean_receiver_completion_ms={:.3},p95_receiver_completion_ms={:.3},mean_domain_convergence_ms={:.3},mean_sender_quiescence_ms={:.3},mean_peak_sender_state_bytes={:.1},mean_peak_receiver_state_bytes={:.1},model_cpu_us_per_run={:.3}",
            scenario.name,
            policy.label(),
            self.samples,
            self.useful_bytes,
            DATAGRAM_BUDGET,
            scenario.rtt_ms,
            scenario.loss_percent,
            BANDWIDTH_BPS,
            self.mean_wire_bytes(),
            self.mean_wire_bytes() / self.useful_bytes as f64,
            self.retransmitted_wire_bytes / self.samples as f64,
            self.control_bytes / self.samples as f64,
            self.parity_bytes / self.samples as f64,
            self.mean_receiver_completion_ms(),
            percentile(&self.receiver_completion_ms, 0.95),
            self.domain_convergence_ms / self.samples as f64,
            self.sender_quiescence_ms / self.samples as f64,
            self.peak_sender_state_bytes / self.samples as f64,
            self.peak_receiver_state_bytes / self.samples as f64,
            self.model_cpu_us / self.samples as f64,
        );
    }
}

#[derive(Default)]
struct StreamSummary {
    samples: u64,
    useful_bytes: usize,
    useful_control_bytes: usize,
    wire_bytes: f64,
    retransmitted_wire_bytes: f64,
    recovery_control_bytes: f64,
    setup_bytes: f64,
    receiver_completion_ms: Vec<f64>,
    domain_convergence_ms: f64,
    sender_quiescence_ms: f64,
    setup_latency_ms: f64,
    mean_bulk_completion_ms: f64,
    mean_control_latency_ms: f64,
    p95_control_latency_ms: f64,
    total_hol_delay_ms: f64,
    max_hol_delay_ms: Vec<f64>,
    peak_sender_state_bytes: f64,
    peak_receiver_state_bytes: f64,
    peak_hol_buffer_bytes: f64,
    model_cpu_us: f64,
}

impl StreamSummary {
    fn push(&mut self, metrics: StreamMetrics, elapsed: Duration) {
        self.samples += 1;
        self.useful_bytes = metrics.useful_application_bytes;
        self.useful_control_bytes = metrics.useful_control_application_bytes;
        self.wire_bytes += metrics.wire_bytes as f64;
        self.retransmitted_wire_bytes += metrics.retransmitted_wire_bytes as f64;
        self.recovery_control_bytes += metrics.control_bytes as f64;
        self.setup_bytes += metrics.setup_bytes as f64;
        self.receiver_completion_ms
            .push(metrics.receiver_completion.as_secs_f64() * 1_000.0);
        self.domain_convergence_ms += metrics.domain_convergence.as_secs_f64() * 1_000.0;
        self.sender_quiescence_ms += metrics.sender_quiescence.as_secs_f64() * 1_000.0;
        self.setup_latency_ms += metrics.setup_latency.as_secs_f64() * 1_000.0;
        self.mean_bulk_completion_ms += metrics.mean_bulk_completion.as_secs_f64() * 1_000.0;
        self.mean_control_latency_ms += metrics.mean_control_latency.as_secs_f64() * 1_000.0;
        self.p95_control_latency_ms += metrics.p95_control_latency.as_secs_f64() * 1_000.0;
        self.total_hol_delay_ms += metrics.total_hol_delay.as_secs_f64() * 1_000.0;
        self.max_hol_delay_ms
            .push(metrics.max_hol_delay.as_secs_f64() * 1_000.0);
        self.peak_sender_state_bytes += metrics.peak_sender_state_bytes as f64;
        self.peak_receiver_state_bytes += metrics.peak_receiver_reassembly_state_bytes as f64;
        self.peak_hol_buffer_bytes += metrics.peak_hol_buffer_bytes as f64;
        self.model_cpu_us += elapsed.as_secs_f64() * 1_000_000.0;
    }

    fn mean_wire_bytes(&self) -> f64 {
        self.wire_bytes / self.samples as f64
    }

    fn mean_receiver_completion_ms(&self) -> f64 {
        mean(&self.receiver_completion_ms)
    }

    fn mean_control_latency_ms(&self) -> f64 {
        self.mean_control_latency_ms / self.samples as f64
    }

    fn mean_bulk_completion_ms(&self) -> f64 {
        self.mean_bulk_completion_ms / self.samples as f64
    }

    fn print(&self, scenario: StreamScenario, candidate: Candidate) {
        println!(
            "[transport-validation] family=multiplex,point={},workload={},candidate={},session=established,trials={},useful_bytes={},useful_control_bytes={},budget_bytes={},rtt_ms={:.1},loss_percent={:.1},bandwidth_bps={},mean_wire_bytes={:.1},wire_amplification={:.4},mean_retransmitted_wire_bytes={:.1},mean_recovery_control_bytes={:.1},mean_setup_bytes={:.1},mean_receiver_completion_ms={:.3},p95_receiver_completion_ms={:.3},mean_domain_convergence_ms={:.3},mean_sender_quiescence_ms={:.3},mean_setup_latency_ms={:.3},mean_bulk_completion_ms={:.3},mean_control_latency_ms={:.3},mean_p95_control_latency_ms={:.3},mean_total_hol_delay_ms={:.3},mean_max_hol_delay_ms={:.3},p95_max_hol_delay_ms={:.3},mean_peak_sender_state_bytes={:.1},mean_peak_receiver_state_bytes={:.1},mean_peak_hol_buffer_bytes={:.1},model_cpu_us_per_run={:.3}",
            scenario.name,
            scenario.workload.label(),
            candidate.label(),
            self.samples,
            self.useful_bytes,
            self.useful_control_bytes,
            DATAGRAM_BUDGET,
            scenario.rtt_ms,
            scenario.loss_percent,
            BANDWIDTH_BPS,
            self.mean_wire_bytes(),
            self.mean_wire_bytes() / self.useful_bytes as f64,
            self.retransmitted_wire_bytes / self.samples as f64,
            self.recovery_control_bytes / self.samples as f64,
            self.setup_bytes / self.samples as f64,
            self.mean_receiver_completion_ms(),
            percentile(&self.receiver_completion_ms, 0.95),
            self.domain_convergence_ms / self.samples as f64,
            self.sender_quiescence_ms / self.samples as f64,
            self.setup_latency_ms / self.samples as f64,
            self.mean_bulk_completion_ms(),
            self.mean_control_latency_ms(),
            self.p95_control_latency_ms / self.samples as f64,
            self.total_hol_delay_ms / self.samples as f64,
            mean(&self.max_hol_delay_ms),
            percentile(&self.max_hol_delay_ms, 0.95),
            self.peak_sender_state_bytes / self.samples as f64,
            self.peak_receiver_state_bytes / self.samples as f64,
            self.peak_hol_buffer_bytes / self.samples as f64,
            self.model_cpu_us / self.samples as f64,
        );
    }
}

fn mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

fn percentile(values: &[f64], percentile: f64) -> f64 {
    assert!(!values.is_empty(), "percentile needs samples");
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let index = ((sorted.len() - 1) as f64 * percentile).ceil() as usize;
    sorted[index]
}

fn percent_reduction(candidate: f64, control: f64) -> f64 {
    (1.0 - candidate / control) * 100.0
}

fn percent_change(candidate: f64, control: f64) -> f64 {
    (candidate / control - 1.0) * 100.0
}

fn trials() -> u64 {
    std::env::var("RECONCILE_TRANSPORT_VALIDATION_TRIALS")
        .ok()
        .map(|raw| {
            raw.parse()
                .expect("RECONCILE_TRANSPORT_VALIDATION_TRIALS must be u64")
        })
        .unwrap_or(DEFAULT_TRIALS)
}

fn selective_summary(
    scenario: SelectiveScenario,
    policy: RecoveryPolicy,
    trials: u64,
) -> FragmentSummary {
    let mut summary = FragmentSummary::default();
    for trial in 0..trials {
        let seed = SELECTIVE_SEED_BASE.wrapping_add(trial.wrapping_mul(SEED_STRIDE));
        let case = FragmentCase {
            fragment_count: 0,
            logical_bytes: Some(ONE_MIB),
            datagram_payload_budget: DATAGRAM_BUDGET,
            rtt: Duration::from_secs_f64(scenario.rtt_ms / 1_000.0),
            loss_percent: scenario.loss_percent,
            burst_loss: None,
            bandwidth_bps: BANDWIDTH_BPS,
            seed,
        };
        let started = Instant::now();
        let metrics = simulate_fragment(case, policy);
        summary.push(metrics, started.elapsed());
    }
    summary
}

fn run_selective_validation(trials: u64) {
    for &scenario in SELECTIVE_SCENARIOS {
        let whole = selective_summary(scenario, RecoveryPolicy::WholeRetry, trials);
        let missing = selective_summary(scenario, RecoveryPolicy::MissingOnly, trials);
        whole.print(scenario, RecoveryPolicy::WholeRetry);
        missing.print(scenario, RecoveryPolicy::MissingOnly);
        println!(
            "[transport-validation-evidence] candidate=missing_only,point={},control=whole_retry,wire_reduction_percent={:.2},receiver_completion_change_percent={:.2}",
            scenario.name,
            percent_reduction(missing.mean_wire_bytes(), whole.mean_wire_bytes()),
            percent_change(
                missing.mean_receiver_completion_ms(),
                whole.mean_receiver_completion_ms()
            ),
        );
    }

    let case = FragmentCase {
        fragment_count: 900,
        logical_bytes: None,
        datagram_payload_budget: DATAGRAM_BUDGET,
        rtt: Duration::from_millis(600),
        loss_percent: 0.0,
        burst_loss: None,
        bandwidth_bps: BANDWIDTH_BPS,
        seed: 0x2710_1a2b,
    };
    let whole = simulate_fragment_interruption(
        case,
        RecoveryPolicy::WholeRetry,
        0.8,
        Duration::from_secs(30),
        true,
    );
    let missing = simulate_fragment_interruption(
        case,
        RecoveryPolicy::MissingOnly,
        0.8,
        Duration::from_secs(30),
        true,
    );
    for (policy, metrics) in [
        (RecoveryPolicy::WholeRetry, &whole),
        (RecoveryPolicy::MissingOnly, &missing),
    ] {
        println!(
            "[transport-validation-interruption] family=selective,point=selective_resume_80,candidate={},rtt_ms=600.0,arrived_fraction=0.8,gap_ms=30000,useful_bytes={},retained_progress_bytes={},wire_bytes_before_interruption={},additional_wire_bytes={},additional_control_bytes={},receiver_completion_ms={:.3},sender_quiescence_ms={:.3}",
            policy.label(),
            metrics.useful_bytes,
            metrics.progress_retained_useful_bytes,
            metrics.wire_bytes_before_interruption,
            metrics.additional_wire_bytes,
            metrics.additional_control_bytes,
            metrics.receiver_completion.as_secs_f64() * 1_000.0,
            metrics.sender_quiescence.as_secs_f64() * 1_000.0,
        );
    }
    println!(
        "[transport-validation-evidence] candidate=missing_only,point=selective_resume_80,control=whole_retry,additional_wire_reduction_percent={:.2}",
        percent_reduction(
            missing.additional_wire_bytes as f64,
            whole.additional_wire_bytes as f64
        ),
    );
}

fn stream_summary(scenario: StreamScenario, candidate: Candidate, trials: u64) -> StreamSummary {
    let workload = scenario.workload.build();
    let mut summary = StreamSummary::default();
    for trial in 0..trials {
        let seed = STREAM_SEED_BASE.wrapping_add(trial.wrapping_mul(SEED_STRIDE));
        let case = StreamCase {
            datagram_payload_budget: DATAGRAM_BUDGET,
            rtt: Duration::from_secs_f64(scenario.rtt_ms / 1_000.0),
            loss_percent: scenario.loss_percent,
            reorder_percent: 0.0,
            bandwidth_bps: BANDWIDTH_BPS,
            seed,
            session: Session::Established,
        };
        let started = Instant::now();
        let metrics = simulate_stream(&workload, case, candidate);
        summary.push(metrics, started.elapsed());
    }
    summary
}

fn run_stream_validation(trials: u64) {
    for &scenario in STREAM_SCENARIOS {
        let whole = stream_summary(scenario, Candidate::UdpWholeMessage, trials);
        let missing = stream_summary(scenario, Candidate::UdpMissingOnly, trials);
        let single = stream_summary(scenario, Candidate::SingleReliableStream, trials);
        let multiplexed = stream_summary(scenario, Candidate::MultipleReliableStreams, trials);

        for (candidate, summary) in [
            (Candidate::UdpWholeMessage, &whole),
            (Candidate::UdpMissingOnly, &missing),
            (Candidate::SingleReliableStream, &single),
            (Candidate::MultipleReliableStreams, &multiplexed),
        ] {
            summary.print(scenario, candidate);
        }

        println!(
            "[transport-validation-evidence] candidate=multiple_reliable_streams,point={},control=single_reliable_stream,receiver_completion_change_percent={:.2},mean_bulk_completion_change_percent={:.2},control_latency_improvement_x={:.3},wire_change_percent={:.2}",
            scenario.name,
            percent_change(
                multiplexed.mean_receiver_completion_ms(),
                single.mean_receiver_completion_ms()
            ),
            percent_change(
                multiplexed.mean_bulk_completion_ms(),
                single.mean_bulk_completion_ms()
            ),
            if multiplexed.mean_control_latency_ms() > 0.0 {
                single.mean_control_latency_ms() / multiplexed.mean_control_latency_ms()
            } else {
                0.0
            },
            percent_change(multiplexed.mean_wire_bytes(), single.mean_wire_bytes()),
        );
        println!(
            "[transport-validation-evidence] candidate=multiple_reliable_streams,point={},control=udp_missing_only,receiver_completion_change_percent={:.2},mean_bulk_completion_change_percent={:.2},control_latency_improvement_x={:.3},wire_change_percent={:.2}",
            scenario.name,
            percent_change(
                multiplexed.mean_receiver_completion_ms(),
                missing.mean_receiver_completion_ms()
            ),
            percent_change(
                multiplexed.mean_bulk_completion_ms(),
                missing.mean_bulk_completion_ms()
            ),
            if multiplexed.mean_control_latency_ms() > 0.0 {
                missing.mean_control_latency_ms() / multiplexed.mean_control_latency_ms()
            } else {
                0.0
            },
            percent_change(multiplexed.mean_wire_bytes(), missing.mean_wire_bytes()),
        );
    }

    let case = StreamCase {
        datagram_payload_budget: DATAGRAM_BUDGET,
        rtt: Duration::from_millis(600),
        loss_percent: 0.0,
        reorder_percent: 0.0,
        bandwidth_bps: BANDWIDTH_BPS,
        seed: 0x2720_1a2b,
        session: Session::Established,
    };
    let mut interruption_rows = Vec::new();
    for &candidate in STREAM_CANDIDATES {
        let metrics = simulate_stream_interruption(
            ONE_MIB,
            case,
            candidate,
            0.8,
            Duration::from_secs(30),
            true,
        );
        println!(
            "[transport-validation-interruption] family=multiplex,point=multiplex_reconnect_80,candidate={},rtt_ms=600.0,arrived_fraction=0.8,gap_ms=30000,reconnect=true,useful_bytes={},retained_progress_bytes={},wire_bytes_before_interruption={},additional_wire_bytes={},additional_setup_bytes={},additional_control_bytes={},receiver_completion_ms={:.3},sender_quiescence_ms={:.3}",
            candidate.label(),
            metrics.useful_application_bytes,
            metrics.retained_progress_bytes,
            metrics.wire_bytes_before_interruption,
            metrics.additional_wire_bytes,
            metrics.additional_setup_bytes,
            metrics.additional_control_bytes,
            metrics.receiver_completion.as_secs_f64() * 1_000.0,
            metrics.sender_quiescence.as_secs_f64() * 1_000.0,
        );
        interruption_rows.push((candidate, metrics));
    }
    let missing = interruption_rows
        .iter()
        .find(|(candidate, _)| *candidate == Candidate::UdpMissingOnly)
        .map(|(_, metrics)| metrics)
        .expect("missing-only interruption row exists");
    let multiplexed = interruption_rows
        .iter()
        .find(|(candidate, _)| *candidate == Candidate::MultipleReliableStreams)
        .map(|(_, metrics)| metrics)
        .expect("multiplexed interruption row exists");
    println!(
        "[transport-validation-evidence] candidate=multiple_reliable_streams,point=multiplex_reconnect_80,control=udp_missing_only,additional_wire_change_percent={:.2}",
        percent_change(
            multiplexed.additional_wire_bytes as f64,
            missing.additional_wire_bytes as f64
        ),
    );
}

fn main() {
    let trials = trials();
    assert!(
        trials > 0,
        "RECONCILE_TRANSPORT_VALIDATION_TRIALS must be non-zero"
    );
    println!(
        "[transport-validation-model] deterministic analytical validation; trials={trials}; no kernel/TCP/QUIC implementation is measured"
    );
    println!(
        "[transport-validation-model] promotion thresholds remain external and pre-registered; model CPU timing is diagnostic only"
    );
    run_selective_validation(trials);
    run_stream_validation(trials);
}
