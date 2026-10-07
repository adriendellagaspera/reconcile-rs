// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// Research benchmark for ordered-stream / multiplexed repair semantics.
// This is an analytical semantic model, not a TCP/QUIC/kernel benchmark.

mod ordered_stream_model;

use std::time::{Duration, Instant};

use ordered_stream_model::{
    simulate, simulate_interruption, Candidate, Case, Metrics, Session, Workload,
};

const ONE_MIB: usize = 1_048_576;
const DEFAULT_TRIALS: u64 = 32;
const CANDIDATES: &[Candidate] = &[
    Candidate::UdpWholeMessage,
    Candidate::UdpMissingOnly,
    Candidate::SingleReliableStream,
    Candidate::MultipleReliableStreams,
    Candidate::HybridDatagramControl,
];

#[derive(Clone, Copy)]
enum WorkloadKind {
    Single,
    Concurrent,
    MixedControl,
}

impl WorkloadKind {
    fn label(self) -> &'static str {
        match self {
            Self::Single => "single_bulk",
            Self::Concurrent => "concurrent_bulk",
            Self::MixedControl => "mixed_control_bulk",
        }
    }

    fn build(self) -> Workload {
        match self {
            Self::Single => Workload::single_bulk(ONE_MIB),
            Self::Concurrent => Workload::concurrent_bulk(4, 256 * 1024),
            Self::MixedControl => {
                Workload::mixed_control_bulk(512 * 1024, 16, 256, Duration::from_millis(4))
            }
        }
    }
}

#[derive(Clone, Copy)]
struct Scenario {
    name: &'static str,
    workload: WorkloadKind,
    budget: usize,
    rtt_ms: f64,
    loss_percent: f64,
    reorder_percent: f64,
    bandwidth_bps: u64,
    session: Session,
}

const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "single_clean_lan",
        workload: WorkloadKind::Single,
        budget: 1_200,
        rtt_ms: 1.0,
        loss_percent: 0.0,
        reorder_percent: 0.0,
        bandwidth_bps: 100_000_000,
        session: Session::Established,
    },
    Scenario {
        name: "single_regional_loss_1",
        workload: WorkloadKind::Single,
        budget: 1_200,
        rtt_ms: 50.0,
        loss_percent: 1.0,
        reorder_percent: 0.0,
        bandwidth_bps: 100_000_000,
        session: Session::Established,
    },
    Scenario {
        name: "single_wan_loss_5",
        workload: WorkloadKind::Single,
        budget: 1_200,
        rtt_ms: 150.0,
        loss_percent: 5.0,
        reorder_percent: 0.0,
        bandwidth_bps: 100_000_000,
        session: Session::Established,
    },
    Scenario {
        name: "single_high_bdp_loss_5",
        workload: WorkloadKind::Single,
        budget: 1_200,
        rtt_ms: 600.0,
        loss_percent: 5.0,
        reorder_percent: 0.0,
        bandwidth_bps: 100_000_000,
        session: Session::Established,
    },
    Scenario {
        name: "concurrent_clean",
        workload: WorkloadKind::Concurrent,
        budget: 1_200,
        rtt_ms: 50.0,
        loss_percent: 0.0,
        reorder_percent: 0.0,
        bandwidth_bps: 100_000_000,
        session: Session::Established,
    },
    Scenario {
        name: "concurrent_wan_loss_5",
        workload: WorkloadKind::Concurrent,
        budget: 1_200,
        rtt_ms: 150.0,
        loss_percent: 5.0,
        reorder_percent: 0.0,
        bandwidth_bps: 100_000_000,
        session: Session::Established,
    },
    Scenario {
        name: "concurrent_high_bdp_loss_5",
        workload: WorkloadKind::Concurrent,
        budget: 1_200,
        rtt_ms: 600.0,
        loss_percent: 5.0,
        reorder_percent: 0.0,
        bandwidth_bps: 100_000_000,
        session: Session::Established,
    },
    Scenario {
        name: "mixed_clean",
        workload: WorkloadKind::MixedControl,
        budget: 1_200,
        rtt_ms: 50.0,
        loss_percent: 0.0,
        reorder_percent: 0.0,
        bandwidth_bps: 100_000_000,
        session: Session::Established,
    },
    Scenario {
        name: "mixed_wan_loss_5",
        workload: WorkloadKind::MixedControl,
        budget: 1_200,
        rtt_ms: 150.0,
        loss_percent: 5.0,
        reorder_percent: 0.0,
        bandwidth_bps: 100_000_000,
        session: Session::Established,
    },
    Scenario {
        name: "mixed_high_bdp_loss_5",
        workload: WorkloadKind::MixedControl,
        budget: 1_200,
        rtt_ms: 600.0,
        loss_percent: 5.0,
        reorder_percent: 0.0,
        bandwidth_bps: 100_000_000,
        session: Session::Established,
    },
    Scenario {
        name: "mixed_reorder_5",
        workload: WorkloadKind::MixedControl,
        budget: 1_200,
        rtt_ms: 150.0,
        loss_percent: 0.0,
        reorder_percent: 5.0,
        bandwidth_bps: 100_000_000,
        session: Session::Established,
    },
    Scenario {
        name: "mixed_constrained_1mbit",
        workload: WorkloadKind::MixedControl,
        budget: 1_200,
        rtt_ms: 150.0,
        loss_percent: 5.0,
        reorder_percent: 0.0,
        bandwidth_bps: 1_000_000,
        session: Session::Established,
    },
    Scenario {
        name: "mixed_budget_1472",
        workload: WorkloadKind::MixedControl,
        budget: 1_472,
        rtt_ms: 150.0,
        loss_percent: 5.0,
        reorder_percent: 0.0,
        bandwidth_bps: 100_000_000,
        session: Session::Established,
    },
    Scenario {
        name: "setup_150_established",
        workload: WorkloadKind::Single,
        budget: 1_200,
        rtt_ms: 150.0,
        loss_percent: 0.0,
        reorder_percent: 0.0,
        bandwidth_bps: 100_000_000,
        session: Session::Established,
    },
    Scenario {
        name: "setup_150_resumed",
        workload: WorkloadKind::Single,
        budget: 1_200,
        rtt_ms: 150.0,
        loss_percent: 0.0,
        reorder_percent: 0.0,
        bandwidth_bps: 100_000_000,
        session: Session::Resumed,
    },
    Scenario {
        name: "setup_150_cold",
        workload: WorkloadKind::Single,
        budget: 1_200,
        rtt_ms: 150.0,
        loss_percent: 0.0,
        reorder_percent: 0.0,
        bandwidth_bps: 100_000_000,
        session: Session::Cold,
    },
    Scenario {
        name: "setup_600_established",
        workload: WorkloadKind::Single,
        budget: 1_200,
        rtt_ms: 600.0,
        loss_percent: 0.0,
        reorder_percent: 0.0,
        bandwidth_bps: 100_000_000,
        session: Session::Established,
    },
    Scenario {
        name: "setup_600_resumed",
        workload: WorkloadKind::Single,
        budget: 1_200,
        rtt_ms: 600.0,
        loss_percent: 0.0,
        reorder_percent: 0.0,
        bandwidth_bps: 100_000_000,
        session: Session::Resumed,
    },
    Scenario {
        name: "setup_600_cold",
        workload: WorkloadKind::Single,
        budget: 1_200,
        rtt_ms: 600.0,
        loss_percent: 0.0,
        reorder_percent: 0.0,
        bandwidth_bps: 100_000_000,
        session: Session::Cold,
    },
];

#[derive(Default)]
struct Summary {
    samples: u64,
    useful_bytes: usize,
    useful_control_bytes: usize,
    wire_bytes: f64,
    application_control_wire_bytes: f64,
    retransmitted_wire_bytes: f64,
    control_bytes: f64,
    setup_bytes: f64,
    packets: f64,
    receiver_completion_ms: Vec<f64>,
    domain_convergence_ms: f64,
    sender_quiescence_ms: f64,
    setup_latency_ms: f64,
    serialization_ms: f64,
    network_elapsed_ms: f64,
    mean_bulk_completion_ms: f64,
    mean_control_latency_ms: f64,
    p95_control_latency_ms: f64,
    total_hol_delay_ms: f64,
    max_hol_delay_ms: Vec<f64>,
    peak_sender_state_bytes: f64,
    peak_receiver_reassembly_state_bytes: f64,
    peak_hol_buffer_bytes: f64,
    model_cpu_us: f64,
}

impl Summary {
    fn push(&mut self, metrics: Metrics, model_cpu: Duration) {
        self.samples += 1;
        self.useful_bytes = metrics.useful_application_bytes;
        self.useful_control_bytes = metrics.useful_control_application_bytes;
        self.wire_bytes += metrics.wire_bytes as f64;
        self.application_control_wire_bytes += metrics.application_control_wire_bytes as f64;
        self.retransmitted_wire_bytes += metrics.retransmitted_wire_bytes as f64;
        self.control_bytes += metrics.control_bytes as f64;
        self.setup_bytes += metrics.setup_bytes as f64;
        self.packets += metrics.packets as f64;
        self.receiver_completion_ms
            .push(metrics.receiver_completion.as_secs_f64() * 1_000.0);
        self.domain_convergence_ms += metrics.domain_convergence.as_secs_f64() * 1_000.0;
        self.sender_quiescence_ms += metrics.sender_quiescence.as_secs_f64() * 1_000.0;
        self.setup_latency_ms += metrics.setup_latency.as_secs_f64() * 1_000.0;
        self.serialization_ms += metrics.serialization_time.as_secs_f64() * 1_000.0;
        self.network_elapsed_ms += metrics.network_elapsed_excluding_setup.as_secs_f64() * 1_000.0;
        self.mean_bulk_completion_ms += metrics.mean_bulk_completion.as_secs_f64() * 1_000.0;
        self.mean_control_latency_ms += metrics.mean_control_latency.as_secs_f64() * 1_000.0;
        self.p95_control_latency_ms += metrics.p95_control_latency.as_secs_f64() * 1_000.0;
        self.total_hol_delay_ms += metrics.total_hol_delay.as_secs_f64() * 1_000.0;
        self.max_hol_delay_ms
            .push(metrics.max_hol_delay.as_secs_f64() * 1_000.0);
        self.peak_sender_state_bytes += metrics.peak_sender_state_bytes as f64;
        self.peak_receiver_reassembly_state_bytes +=
            metrics.peak_receiver_reassembly_state_bytes as f64;
        self.peak_hol_buffer_bytes += metrics.peak_hol_buffer_bytes as f64;
        self.model_cpu_us += model_cpu.as_secs_f64() * 1_000_000.0;
    }

    fn mean(&self, total: f64) -> f64 {
        total / self.samples as f64
    }

    fn percentile(values: &[f64], percentile: f64) -> f64 {
        let mut samples = values.to_vec();
        samples.sort_by(f64::total_cmp);
        let index = ((samples.len() - 1) as f64 * percentile).round() as usize;
        samples[index]
    }

    fn print(&self, scenario: Scenario, candidate: Candidate) {
        let mean_wire = self.mean(self.wire_bytes);
        let bdp_bytes = scenario.bandwidth_bps as f64 * scenario.rtt_ms / 8_000.0;
        println!(
            "[ordered-stream] scenario={},workload={},candidate={},session={},budget_bytes={},rtt_ms={:.1},loss_percent={:.1},reorder_percent={:.1},bandwidth_bps={},bdp_bytes={:.0},trials={},useful_application_bytes={},mean_wire_bytes={:.1},wire_amplification={:.4},useful_control_application_bytes={},mean_application_control_wire_bytes={:.1},mean_retransmitted_wire_bytes={:.1},mean_recovery_control_bytes={:.1},mean_setup_bytes={:.1},mean_packets={:.2},mean_receiver_completion_ms={:.3},p95_receiver_completion_ms={:.3},mean_domain_convergence_ms={:.3},mean_sender_quiescence_ms={:.3},mean_setup_latency_ms={:.3},mean_serialization_ms={:.3},mean_network_elapsed_excluding_setup_ms={:.3},mean_bulk_completion_ms={:.3},mean_control_latency_ms={:.3},mean_p95_control_latency_ms={:.3},mean_total_hol_delay_ms={:.3},mean_max_hol_delay_ms={:.3},p95_max_hol_delay_ms={:.3},mean_peak_sender_state_bytes={:.1},mean_peak_receiver_reassembly_state_bytes={:.1},mean_peak_hol_buffer_bytes={:.1},model_cpu_us_per_run={:.3}",
            scenario.name,
            scenario.workload.label(),
            candidate.label(),
            if candidate_uses_session(candidate) {
                scenario.session.label()
            } else {
                "none"
            },
            scenario.budget,
            scenario.rtt_ms,
            scenario.loss_percent,
            scenario.reorder_percent,
            scenario.bandwidth_bps,
            bdp_bytes,
            self.samples,
            self.useful_bytes,
            mean_wire,
            mean_wire / self.useful_bytes as f64,
            self.useful_control_bytes,
            self.mean(self.application_control_wire_bytes),
            self.mean(self.retransmitted_wire_bytes),
            self.mean(self.control_bytes),
            self.mean(self.setup_bytes),
            self.mean(self.packets),
            self.mean(self.receiver_completion_ms.iter().sum()),
            Self::percentile(&self.receiver_completion_ms, 0.95),
            self.mean(self.domain_convergence_ms),
            self.mean(self.sender_quiescence_ms),
            self.mean(self.setup_latency_ms),
            self.mean(self.serialization_ms),
            self.mean(self.network_elapsed_ms),
            self.mean(self.mean_bulk_completion_ms),
            self.mean(self.mean_control_latency_ms),
            self.mean(self.p95_control_latency_ms),
            self.mean(self.total_hol_delay_ms),
            self.mean(self.max_hol_delay_ms.iter().sum()),
            Self::percentile(&self.max_hol_delay_ms, 0.95),
            self.mean(self.peak_sender_state_bytes),
            self.mean(self.peak_receiver_reassembly_state_bytes),
            self.mean(self.peak_hol_buffer_bytes),
            self.mean(self.model_cpu_us),
        );
    }
}

fn candidate_uses_session(candidate: Candidate) -> bool {
    matches!(
        candidate,
        Candidate::SingleReliableStream
            | Candidate::MultipleReliableStreams
            | Candidate::HybridDatagramControl
    )
}

fn trials() -> u64 {
    std::env::var("RECONCILE_ORDERED_STREAM_TRIALS")
        .map(|raw| {
            raw.parse()
                .expect("RECONCILE_ORDERED_STREAM_TRIALS must be u64")
        })
        .unwrap_or(DEFAULT_TRIALS)
}

fn run_scenarios(trials: u64) {
    for &scenario in SCENARIOS {
        let workload = scenario.workload.build();
        for &candidate in CANDIDATES {
            if !candidate_uses_session(candidate) && scenario.session != Session::Established {
                continue;
            }
            let mut summary = Summary::default();
            for trial in 0..trials {
                let seed = 0x2720_0000_0000_0000_u64
                    .wrapping_add(trial.wrapping_mul(0x9e37_79b9_7f4a_7c15));
                let input = Case {
                    datagram_payload_budget: scenario.budget,
                    rtt: Duration::from_secs_f64(scenario.rtt_ms / 1_000.0),
                    loss_percent: scenario.loss_percent,
                    reorder_percent: scenario.reorder_percent,
                    bandwidth_bps: scenario.bandwidth_bps,
                    seed,
                    session: scenario.session,
                };
                let started = Instant::now();
                let metrics = simulate(&workload, input, candidate);
                summary.push(metrics, started.elapsed());
            }
            summary.print(scenario, candidate);
        }
    }
}

fn run_interruption_scenarios() {
    const PROGRESS: &[f64] = &[0.2, 0.5, 0.8];
    const CONTACTS: &[(f64, u64, bool)] = &[(50.0, 1_000, false), (600.0, 30_000, true)];

    for &(rtt_ms, gap_ms, reconnect) in CONTACTS {
        for &arrived_fraction in PROGRESS {
            for &candidate in CANDIDATES {
                let input = Case {
                    datagram_payload_budget: 1_200,
                    rtt: Duration::from_secs_f64(rtt_ms / 1_000.0),
                    loss_percent: 0.0,
                    reorder_percent: 0.0,
                    bandwidth_bps: 100_000_000,
                    seed: 0x2720_1a2b,
                    session: Session::Established,
                };
                let metrics = simulate_interruption(
                    ONE_MIB,
                    input,
                    candidate,
                    arrived_fraction,
                    Duration::from_millis(gap_ms),
                    reconnect,
                );
                println!(
                    "[ordered-stream-interruption] candidate={},rtt_ms={:.1},arrived_fraction={:.1},gap_ms={},reconnect={},useful_application_bytes={},retained_progress_bytes={},wire_bytes_before_interruption={},additional_wire_bytes={},additional_wire_over_useful={:.4},additional_setup_bytes={},additional_control_bytes={},receiver_completion_ms={:.3},sender_quiescence_ms={:.3}",
                    candidate.label(),
                    rtt_ms,
                    arrived_fraction,
                    gap_ms,
                    reconnect,
                    metrics.useful_application_bytes,
                    metrics.retained_progress_bytes,
                    metrics.wire_bytes_before_interruption,
                    metrics.additional_wire_bytes,
                    metrics.additional_wire_bytes as f64 / metrics.useful_application_bytes as f64,
                    metrics.additional_setup_bytes,
                    metrics.additional_control_bytes,
                    metrics.receiver_completion.as_secs_f64() * 1_000.0,
                    metrics.sender_quiescence.as_secs_f64() * 1_000.0,
                );
            }
        }
    }
}

fn main() {
    let trials = trials();
    assert!(
        trials > 0,
        "RECONCILE_ORDERED_STREAM_TRIALS must be non-zero"
    );
    println!(
        "[ordered-stream-model] analytical semantic model only; no TCP/QUIC/kernel implementation is measured"
    );
    println!(
        "[ordered-stream-model] matched loss/reorder draws are keyed by logical message/chunk/attempt; stream candidates share the same first-transmission scheduler"
    );
    println!(
        "[ordered-stream-model] 48B stream-frame overhead follows the prior generic transport model; setup assumptions are cold=1 blocking RTT + 2400B, resumed=0 blocking RTT + 1200B, established=0"
    );
    run_scenarios(trials);
    run_interruption_scenarios();
}
