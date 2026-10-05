// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// Research benchmark for selective fragment recovery. It models no production ACK/NACK wire:
// the shipped runtime control remains benches/fragmentation.rs.

mod fragment_recovery_model;

use std::hint::black_box;
use std::time::{Duration, Instant};

use gossip::framing::fragment_payload_capacity;

use fragment_recovery_model::{
    authenticated_overhead, simulate, simulate_interruption, BurstLoss, Case, Metrics,
    RecoveryPolicy,
};

const DEFAULT_BUDGETS: &[usize] = &[1_200, 1_472];
const DEFAULT_BANDWIDTH_BPS: u64 = 100_000_000;
const DEFAULT_TRIALS: u64 = 64;
const ONE_MIB: usize = 1_048_576;

#[derive(Clone, Copy)]
struct Scenario {
    fragments: usize,
    logical_bytes: Option<usize>,
    rtt_ms: f64,
    loss_percent: f64,
    burst_loss: Option<BurstLoss>,
}

// Sparse by design: all requested dimensions are represented without taking their Cartesian
// product. Add targeted cases only when a result needs sensitivity checking.
const SCENARIOS: &[Scenario] = &[
    Scenario {
        fragments: 1,
        logical_bytes: None,
        rtt_ms: 1.0,
        loss_percent: 0.0,
        burst_loss: None,
    },
    Scenario {
        fragments: 4,
        logical_bytes: None,
        rtt_ms: 50.0,
        loss_percent: 0.0,
        burst_loss: None,
    },
    Scenario {
        fragments: 4,
        logical_bytes: None,
        rtt_ms: 50.0,
        loss_percent: 1.0,
        burst_loss: None,
    },
    Scenario {
        fragments: 16,
        logical_bytes: None,
        rtt_ms: 1.0,
        loss_percent: 1.0,
        burst_loss: None,
    },
    Scenario {
        fragments: 16,
        logical_bytes: None,
        rtt_ms: 50.0,
        loss_percent: 5.0,
        burst_loss: None,
    },
    Scenario {
        fragments: 64,
        logical_bytes: None,
        rtt_ms: 50.0,
        loss_percent: 0.0,
        burst_loss: None,
    },
    Scenario {
        fragments: 64,
        logical_bytes: None,
        rtt_ms: 150.0,
        loss_percent: 1.0,
        burst_loss: None,
    },
    Scenario {
        fragments: 64,
        logical_bytes: None,
        rtt_ms: 600.0,
        loss_percent: 5.0,
        burst_loss: None,
    },
    Scenario {
        fragments: 64,
        logical_bytes: None,
        rtt_ms: 150.0,
        loss_percent: 0.0,
        burst_loss: Some(BurstLoss {
            start_fragment: 24,
            fragment_count: 4,
        }),
    },
    Scenario {
        fragments: 64,
        logical_bytes: None,
        rtt_ms: 150.0,
        loss_percent: 0.0,
        burst_loss: Some(BurstLoss {
            start_fragment: 24,
            fragment_count: 8,
        }),
    },
    Scenario {
        fragments: 64,
        logical_bytes: None,
        rtt_ms: 150.0,
        loss_percent: 0.0,
        burst_loss: Some(BurstLoss {
            start_fragment: 24,
            fragment_count: 16,
        }),
    },
    Scenario {
        fragments: 900,
        logical_bytes: None,
        rtt_ms: 50.0,
        loss_percent: 1.0,
        burst_loss: None,
    },
    Scenario {
        fragments: 900,
        logical_bytes: None,
        rtt_ms: 150.0,
        loss_percent: 5.0,
        burst_loss: None,
    },
    Scenario {
        fragments: 900,
        logical_bytes: None,
        rtt_ms: 600.0,
        loss_percent: 1.0,
        burst_loss: None,
    },
    Scenario {
        fragments: 900,
        logical_bytes: None,
        rtt_ms: 600.0,
        loss_percent: 5.0,
        burst_loss: None,
    },
    Scenario {
        fragments: 0,
        logical_bytes: Some(ONE_MIB),
        rtt_ms: 150.0,
        loss_percent: 5.0,
        burst_loss: None,
    },
    Scenario {
        fragments: 0,
        logical_bytes: Some(ONE_MIB),
        rtt_ms: 600.0,
        loss_percent: 5.0,
        burst_loss: None,
    },
];

#[derive(Clone, Copy)]
struct InterruptionScenario {
    fragments: usize,
    rtt_ms: f64,
    arrived_fraction: f64,
    gap_ms: u64,
}

const INTERRUPTION_SCENARIOS: &[InterruptionScenario] = &[
    InterruptionScenario {
        fragments: 64,
        logical_bytes: None,
        rtt_ms: 50.0,
        arrived_fraction: 0.2,
        gap_ms: 1_000,
    },
    InterruptionScenario {
        fragments: 64,
        logical_bytes: None,
        rtt_ms: 50.0,
        arrived_fraction: 0.5,
        gap_ms: 1_000,
    },
    InterruptionScenario {
        fragments: 64,
        logical_bytes: None,
        rtt_ms: 50.0,
        arrived_fraction: 0.8,
        gap_ms: 1_000,
    },
    InterruptionScenario {
        fragments: 900,
        logical_bytes: None,
        rtt_ms: 600.0,
        arrived_fraction: 0.2,
        gap_ms: 1_000,
    },
    InterruptionScenario {
        fragments: 900,
        logical_bytes: None,
        rtt_ms: 600.0,
        arrived_fraction: 0.5,
        gap_ms: 30_000,
    },
    InterruptionScenario {
        fragments: 900,
        logical_bytes: None,
        rtt_ms: 600.0,
        arrived_fraction: 0.8,
        gap_ms: 30_000,
    },
];

#[derive(Default)]
struct Summary {
    samples: u64,
    useful_bytes: usize,
    wire_bytes: f64,
    retransmitted_wire_bytes: f64,
    control_bytes: f64,
    datagrams: f64,
    recovery_rounds: f64,
    initial_data_loss_useful_bytes: f64,
    missing_after_initial_recovery_bytes: f64,
    parity_wire_bytes: f64,
    fec_recovered_fragments: f64,
    fec_recovered_useful_bytes: f64,
    receiver_completion_ms: Vec<f64>,
    domain_convergence_ms: f64,
    sender_quiescence_ms: f64,
    peak_sender_state_bytes: f64,
    peak_receiver_state_bytes: f64,
}

impl Summary {
    fn push(&mut self, metrics: Metrics) {
        self.samples += 1;
        self.useful_bytes = metrics.useful_bytes;
        self.wire_bytes += metrics.wire_bytes as f64;
        self.retransmitted_wire_bytes += metrics.retransmitted_wire_bytes as f64;
        self.control_bytes += metrics.control_bytes as f64;
        self.datagrams += metrics.datagrams as f64;
        self.recovery_rounds += metrics.recovery_rounds as f64;
        self.initial_data_loss_useful_bytes += metrics.initial_data_loss_useful_bytes as f64;
        self.missing_after_initial_recovery_bytes +=
            metrics.missing_after_initial_recovery_bytes as f64;
        self.parity_wire_bytes += metrics.parity_wire_bytes as f64;
        self.fec_recovered_fragments += metrics.fec_recovered_fragments as f64;
        self.fec_recovered_useful_bytes += metrics.fec_recovered_useful_bytes as f64;
        self.receiver_completion_ms
            .push(metrics.receiver_completion.as_secs_f64() * 1_000.0);
        self.domain_convergence_ms += metrics.domain_convergence.as_secs_f64() * 1_000.0;
        self.sender_quiescence_ms += metrics.sender_quiescence.as_secs_f64() * 1_000.0;
        self.peak_sender_state_bytes += metrics.peak_sender_recovery_state_bytes as f64;
        self.peak_receiver_state_bytes += metrics.peak_receiver_reassembly_state_bytes as f64;
    }

    fn mean(&self, total: f64) -> f64 {
        total / self.samples as f64
    }

    fn percentile_completion_ms(&self, percentile: f64) -> f64 {
        let mut samples = self.receiver_completion_ms.clone();
        samples.sort_by(f64::total_cmp);
        let index = ((samples.len() - 1) as f64 * percentile).round() as usize;
        samples[index]
    }

    fn print(&self, scenario: Scenario, policy: RecoveryPolicy, budget: usize, bandwidth_bps: u64) {
        let mean_wire = self.mean(self.wire_bytes);
        let mean_missing = self.mean(self.initial_data_loss_useful_bytes);
        let mean_recovery_cost =
            self.mean(self.retransmitted_wire_bytes + self.parity_wire_bytes + self.control_bytes);
        let recovery_cost_ratio = if mean_missing == 0.0 {
            0.0
        } else {
            mean_recovery_cost / mean_missing
        };
        let bdp_bytes = bandwidth_bps as f64 * (scenario.rtt_ms / 1_000.0) / 8.0;
        println!(
            "[fragment-recovery] policy={},fragments={},budget_bytes={},rtt_ms={:.1},loss_percent={:.1},burst_start={},burst_fragments={},bandwidth_bps={},bdp_bytes={:.0},trials={},useful_bytes={},mean_wire_bytes={:.1},wire_amplification={:.4},mean_retransmitted_wire_bytes={:.1},mean_parity_wire_bytes={:.1},mean_control_bytes={:.1},mean_datagrams={:.2},mean_recovery_rounds={:.3},mean_initial_data_loss_useful_bytes={:.1},mean_missing_after_initial_recovery_bytes={:.1},mean_fec_recovered_fragments={:.3},mean_fec_recovered_useful_bytes={:.1},recovery_cost_ratio={:.4},mean_receiver_completion_ms={:.3},p95_receiver_completion_ms={:.3},mean_domain_convergence_ms={:.3},mean_sender_quiescence_ms={:.3},mean_peak_sender_recovery_state_bytes={:.1},mean_peak_receiver_reassembly_state_bytes={:.1}",
            policy.label(),
            scenario.fragments,
            budget,
            scenario.rtt_ms,
            scenario.loss_percent,
            scenario.burst_loss.map_or(0, |burst| burst.start_fragment),
            scenario.burst_loss.map_or(0, |burst| burst.fragment_count),
            bandwidth_bps,
            bdp_bytes,
            self.samples,
            self.useful_bytes,
            mean_wire,
            mean_wire / self.useful_bytes as f64,
            self.mean(self.retransmitted_wire_bytes),
            self.mean(self.parity_wire_bytes),
            self.mean(self.control_bytes),
            self.mean(self.datagrams),
            self.mean(self.recovery_rounds),
            mean_missing,
            self.mean(self.missing_after_initial_recovery_bytes),
            self.mean(self.fec_recovered_fragments),
            self.mean(self.fec_recovered_useful_bytes),
            recovery_cost_ratio,
            self.mean(self.receiver_completion_ms.iter().sum()),
            self.percentile_completion_ms(0.95),
            self.mean(self.domain_convergence_ms),
            self.mean(self.sender_quiescence_ms),
            self.mean(self.peak_sender_state_bytes),
            self.mean(self.peak_receiver_state_bytes),
        );
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .map(|raw| raw.parse().unwrap_or_else(|_| panic!("{name} must be u64")))
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .map(|raw| {
            raw.parse()
                .unwrap_or_else(|_| panic!("{name} must be usize"))
        })
        .unwrap_or(default)
}

fn run_interruption_cases(budget: usize, bandwidth_bps: u64) {
    for &scenario in INTERRUPTION_SCENARIOS {
        for retain_progress in [true, false] {
        for policy in [
            RecoveryPolicy::WholeRetry,
            RecoveryPolicy::MissingOnly,
            RecoveryPolicy::Xor8Plus1,
        ] {
            let metrics = simulate_interruption(
                Case {
                    fragment_count: scenario.fragments,
                    datagram_payload_budget: budget,
                    rtt: Duration::from_secs_f64(scenario.rtt_ms / 1_000.0),
                    loss_percent: 0.0,
                    burst_loss: None,
                    bandwidth_bps,
                    seed: 0x2710_1a2b,
                },
                policy,
                scenario.arrived_fraction,
                Duration::from_millis(scenario.gap_ms),
                retain_progress,
            );
            println!(
                "[fragment-interruption] policy={},fragments={},budget_bytes={},rtt_ms={:.1},arrived_fraction={:.1},gap_ms={},retained={},useful_bytes={},progress_retained_useful_bytes={},wire_bytes_before_interruption={},additional_wire_bytes={},additional_control_bytes={},additional_wire_over_useful={:.4},receiver_completion_ms={:.3},sender_quiescence_ms={:.3}",
                policy.label(),
                scenario.fragments,
                budget,
                scenario.rtt_ms,
                scenario.arrived_fraction,
                scenario.gap_ms,
                retain_progress,
                metrics.useful_bytes,
                metrics.progress_retained_useful_bytes,
                metrics.wire_bytes_before_interruption,
                metrics.additional_wire_bytes,
                metrics.additional_control_bytes,
                metrics.additional_wire_bytes as f64 / metrics.useful_bytes as f64,
                metrics.receiver_completion.as_secs_f64() * 1_000.0,
                metrics.sender_quiescence.as_secs_f64() * 1_000.0,
            );
        }
        }
    }
}

fn run_policy_cpu_probes() {
    const FRAGMENTS: usize = 900;
    const FEC_WIDTH: usize = 8;
    let iterations = env_usize("RECONCILE_FRAGMENT_RECOVERY_CPU_ITERS", 128);
    assert!(
        iterations > 0,
        "RECONCILE_FRAGMENT_RECOVERY_CPU_ITERS must be non-zero"
    );
    let payload_bytes = fragment_payload_capacity(DEFAULT_BUDGETS[0], authenticated_overhead())
        .expect("default budget fits fragment overhead");
    let missing: Vec<bool> = (0..FRAGMENTS).map(|index| index % 20 == 0).collect();
    let mut bitmap = vec![0_u8; FRAGMENTS.div_ceil(8)];

    let started = Instant::now();
    for _ in 0..iterations {
        bitmap.fill(0);
        for (index, is_missing) in missing.iter().copied().enumerate() {
            if is_missing {
                bitmap[index / 8] |= 1 << (index % 8);
            }
        }
        black_box(&bitmap);
    }
    print_cpu_probe(
        "missing_bitmap_encode",
        "receiver",
        FRAGMENTS,
        payload_bytes,
        iterations,
        started.elapsed(),
    );

    let started = Instant::now();
    for _ in 0..iterations {
        let mut selected = 0_usize;
        for index in 0..FRAGMENTS {
            if bitmap[index / 8] & (1 << (index % 8)) != 0 {
                selected = selected.wrapping_add(index);
            }
        }
        black_box(selected);
    }
    print_cpu_probe(
        "missing_bitmap_decode",
        "sender",
        FRAGMENTS,
        payload_bytes,
        iterations,
        started.elapsed(),
    );

    let data: Vec<Vec<u8>> = (0..FEC_WIDTH)
        .map(|lane| vec![(lane as u8).wrapping_mul(31); payload_bytes])
        .collect();
    let groups = FRAGMENTS.div_ceil(FEC_WIDTH);
    let mut parity = vec![0_u8; payload_bytes];

    let started = Instant::now();
    for _ in 0..iterations {
        for _ in 0..groups {
            parity.fill(0);
            for fragment in &data {
                for (out, byte) in parity.iter_mut().zip(fragment) {
                    *out ^= *byte;
                }
            }
            black_box(&parity);
        }
    }
    print_cpu_probe(
        "xor8_encode",
        "sender",
        FRAGMENTS,
        payload_bytes,
        iterations,
        started.elapsed(),
    );

    let mut recovered = vec![0_u8; payload_bytes];
    let started = Instant::now();
    for _ in 0..iterations {
        for _ in 0..groups {
            recovered.copy_from_slice(&parity);
            for fragment in data.iter().skip(1) {
                for (out, byte) in recovered.iter_mut().zip(fragment) {
                    *out ^= *byte;
                }
            }
            black_box(&recovered);
        }
    }
    print_cpu_probe(
        "xor8_recover_one",
        "receiver",
        FRAGMENTS,
        payload_bytes,
        iterations,
        started.elapsed(),
    );
}

fn print_cpu_probe(
    primitive: &str,
    side: &str,
    fragments: usize,
    payload_bytes: usize,
    iterations: usize,
    elapsed: Duration,
) {
    println!(
        "[fragment-recovery-cpu] primitive={primitive},side={side},fragments={fragments},fragment_payload_bytes={payload_bytes},iterations={iterations},us_per_transfer={:.3}",
        elapsed.as_secs_f64() * 1_000_000.0 / iterations as f64,
    );
}

fn main() {
    let budgets = std::env::var("RECONCILE_FRAGMENT_RECOVERY_BUDGETS")
        .map(|raw| raw.split(',').map(|v| v.trim().parse().expect("budgets must be usize")).collect::<Vec<usize>>())
        .unwrap_or_else(|_| DEFAULT_BUDGETS.to_vec());
    let bandwidth_bps = env_u64(
        "RECONCILE_FRAGMENT_RECOVERY_BANDWIDTH_BPS",
        DEFAULT_BANDWIDTH_BPS,
    );
    let trials = env_u64("RECONCILE_FRAGMENT_RECOVERY_TRIALS", DEFAULT_TRIALS);
    assert!(
        trials > 0,
        "RECONCILE_FRAGMENT_RECOVERY_TRIALS must be non-zero"
    );

    for budget in budgets {
    for &scenario in SCENARIOS {
        for policy in [
            RecoveryPolicy::WholeRetry,
            RecoveryPolicy::MissingOnly,
            RecoveryPolicy::Xor8Plus1,
        ] {
            let mut summary = Summary::default();
            for trial in 0..trials {
                let seed = 0x2710_0000_0000_0000_u64
                    .wrapping_add(trial.wrapping_mul(0x9e37_79b9_7f4a_7c15));
                summary.push(simulate(
                    Case {
                        fragment_count: if let Some(bytes) = scenario.logical_bytes {
                            let capacity = fragment_payload_capacity(budget, authenticated_overhead())
                                .expect("budget fits fragment overhead");
                            bytes.div_ceil(capacity)
                        } else {
                            scenario.fragments
                        },
                        datagram_payload_budget: budget,
                        rtt: Duration::from_secs_f64(scenario.rtt_ms / 1_000.0),
                        loss_percent: scenario.loss_percent,
                        burst_loss: scenario.burst_loss,
                        bandwidth_bps,
                        seed,
                    },
                    policy,
                ));
            }
            summary.print(scenario, policy, budget, bandwidth_bps);
        }
    }

    run_interruption_cases(budget, bandwidth_bps);
    }
    run_policy_cpu_probes();
}
