// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/MIT>, at your option.
//
// Research benchmark for fragment-level recovery. It does not change the production wire
// protocol: it prices hypothetical recovery policies over the shipped fragment geometry.

use gossip::auth::{TAG_LEN, VERSION_LEN};
use gossip::framing::{fragment_payload_capacity, FRAGMENT_HEADER_LEN};
use gossip::replay::REPLAY_HEADER_LEN;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const DEFAULT_DATAGRAM_BUDGET: usize = 1_200;
const SELECTIVE_NACK_HEADER: usize = 32 + 2;
const SELECTIVE_NACK_INDEX: usize = 4;
const BASE_SEED: u64 = 0x2710_0001;

#[derive(Clone, Copy, Debug)]
enum Policy {
    WholeRetry,
    MissingOnly,
    Xor8,
}

impl Policy {
    fn label(self) -> &'static str {
        match self {
            Self::WholeRetry => "whole-retry",
            Self::MissingOnly => "missing-only",
            Self::Xor8 => "xor-8+1",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Loss {
    Iid(f64),
    Burst(usize),
}

impl Loss {
    fn label(self) -> String {
        match self {
            Self::Iid(p) => format!("iid-{:.1}%", p * 100.0),
            Self::Burst(n) => format!("burst-{n}"),
        }
    }
}

struct LossState {
    model: Loss,
    rng: StdRng,
}

impl LossState {
    fn new(model: Loss, seed: u64) -> Self {
        Self {
            model,
            rng: StdRng::seed_from_u64(seed),
        }
    }

    fn lost(&mut self, round: usize, fragment_index: usize, fragment_count: usize) -> bool {
        match self.model {
            Loss::Iid(p) => self.rng.gen_bool(p.clamp(0.0, 1.0)),
            Loss::Burst(n) => {
                let burst = n.min(fragment_count);
                let start = fragment_count.saturating_sub(burst) / 2;
                round == 1 && fragment_index >= start && fragment_index < start + burst
            }
        }
    }
}

#[derive(Default)]
struct ResultRow {
    rounds: usize,
    data_datagrams: usize,
    feedback_datagrams: usize,
    data_wire_bytes: usize,
    retransmitted_wire_bytes: usize,
    feedback_wire_bytes: usize,
    parity_datagrams: usize,
    parity_lost: usize,
    parity_wire_bytes: usize,
    duplicate_data_bytes: usize,
}

fn auth_overhead() -> usize {
    TAG_LEN + REPLAY_HEADER_LEN + VERSION_LEN
}

fn fragment_capacity(budget: usize) -> usize {
    fragment_payload_capacity(budget, auth_overhead())
        .expect("datagram budget must fit auth and fragment headers")
}

fn fragment_lengths(value_len: usize, budget: usize) -> Vec<usize> {
    let capacity = fragment_capacity(budget);
    assert!(capacity > 0, "datagram budget leaves no fragment payload");
    (0..value_len)
        .step_by(capacity)
        .map(|offset| capacity.min(value_len - offset))
        .collect()
}

fn data_wire_bytes(payload_len: usize) -> usize {
    auth_overhead() + FRAGMENT_HEADER_LEN + payload_len
}

fn nack_wire_bytes(missing: usize) -> usize {
    auth_overhead() + SELECTIVE_NACK_HEADER + SELECTIVE_NACK_INDEX * missing
}

fn run_case(policy: Policy, loss: Loss, value_len: usize, budget: usize, seed: u64) -> ResultRow {
    let fragments = fragment_lengths(value_len, budget);
    let mut received = vec![false; fragments.len()];
    let mut attempts = vec![0_usize; fragments.len()];
    let mut loss = LossState::new(loss, seed);
    let mut result = ResultRow::default();

    while received.iter().any(|received| !received) {
        result.rounds += 1;
        let send: Vec<usize> = match policy {
            Policy::WholeRetry => (0..fragments.len()).collect(),
            Policy::MissingOnly | Policy::Xor8 => received
                .iter()
                .enumerate()
                .filter_map(|(index, received)| (!received).then_some(index))
                .collect(),
        };

        for index in send {
            let payload_len = fragments[index];
            let wire_bytes = data_wire_bytes(payload_len);
            if attempts[index] > 0 {
                result.retransmitted_wire_bytes += wire_bytes;
            }
            attempts[index] += 1;
            result.data_datagrams += 1;
            result.data_wire_bytes += wire_bytes;
            if loss.lost(result.rounds, index, fragments.len()) {
                continue;
            }
            if received[index] {
                result.duplicate_data_bytes += payload_len;
            } else {
                received[index] = true;
            }
        }

        if matches!(policy, Policy::Xor8) && result.rounds == 1 {
            for group_start in (0..fragments.len()).step_by(8) {
                let group_end = (group_start + 8).min(fragments.len());
                let parity_payload = fragments[group_start..group_end]
                    .iter()
                    .copied()
                    .max()
                    .unwrap_or(0);
                result.parity_datagrams += 1;
                result.parity_wire_bytes += data_wire_bytes(parity_payload);
                // Parity is a real datagram and is subject to the same impairment model.
                // Its synthetic index is disjoint from data fragment indexes.
                let parity_index = fragments.len() + group_start / 8;
                if loss.lost(result.rounds, parity_index, fragments.len() + fragments.len().div_ceil(8)) {
                    result.parity_lost += 1;
                    continue;
                }
                let missing: Vec<usize> = (group_start..group_end)
                    .filter(|index| !received[*index])
                    .collect();
                if missing.len() == 1 {
                    received[missing[0]] = true;
                }
            }
        }

        if received.iter().all(|received| *received) {
            break;
        }

        if matches!(policy, Policy::MissingOnly | Policy::Xor8) {
            let missing = received.iter().filter(|received| !**received).count();
            result.feedback_datagrams += 1;
            result.feedback_wire_bytes += nack_wire_bytes(missing);
        }

        assert!(result.rounds < 10_000, "recovery did not converge");
    }

    result
}

fn env_usizes(name: &str, default: &str) -> Vec<usize> {
    std::env::var(name)
        .unwrap_or_else(|_| default.to_owned())
        .split(',')
        .map(|raw| {
            raw.trim()
                .parse()
                .unwrap_or_else(|_| panic!("{name} must contain usize values"))
        })
        .collect()
}

fn env_u64s(name: &str, default: &str) -> Vec<u64> {
    std::env::var(name)
        .unwrap_or_else(|_| default.to_owned())
        .split(',')
        .map(|raw| {
            raw.trim()
                .parse()
                .unwrap_or_else(|_| panic!("{name} must contain u64 values"))
        })
        .collect()
}

fn env_f64s(name: &str, default: &str) -> Vec<f64> {
    std::env::var(name)
        .unwrap_or_else(|_| default.to_owned())
        .split(',')
        .map(|raw| {
            raw.trim()
                .parse()
                .unwrap_or_else(|_| panic!("{name} must contain f64 values"))
        })
        .collect()
}

fn main() {
    let fragment_counts = env_usizes("RECONCILE_RECOVERY_FRAGMENT_COUNTS", "1,4,16,64");
    let losses = env_f64s("RECONCILE_RECOVERY_LOSS_PERCENT", "0,1,5");
    let bursts = env_usizes("RECONCILE_RECOVERY_BURST_FRAGMENTS", "4,8,16");
    let rtts = env_usizes("RECONCILE_RECOVERY_RTT_MS", "1,50,150,600");
    let seeds = env_u64s("RECONCILE_RECOVERY_SEEDS", "0,1,2,3,4");
    let budget = std::env::var("RECONCILE_RECOVERY_BUDGET")
        .unwrap_or_else(|_| DEFAULT_DATAGRAM_BUDGET.to_string())
        .parse()
        .expect("RECONCILE_RECOVERY_BUDGET must be usize");

    let capacity = fragment_capacity(budget);
    let mut sizes: Vec<usize> = fragment_counts
        .into_iter()
        .filter(|count| *count > 0)
        .map(|count| count * capacity)
        .collect();
    sizes.push(1_048_576);
    sizes.sort_unstable();
    sizes.dedup();

    let mut models: Vec<Loss> = losses
        .into_iter()
        .map(|percent| Loss::Iid(percent / 100.0))
        .collect();
    models.extend(
        bursts
            .into_iter()
            .filter(|n| *n > 0)
            .map(Loss::Burst),
    );

    for value_len in sizes {
        let fragments = fragment_lengths(value_len, budget);
        for rtt_ms in rtts.iter().copied() {
            for seed_offset in seeds.iter().copied() {
                let seed = BASE_SEED.wrapping_add(seed_offset);
                for model in models.iter().copied() {
                    for policy in [Policy::WholeRetry, Policy::MissingOnly, Policy::Xor8] {
                        let row = run_case(policy, model, value_len, budget, seed);
                        let total_wire_bytes =
                            row.data_wire_bytes + row.feedback_wire_bytes + row.parity_wire_bytes;
                        // Round timing is an idealized recovery lower bound. Runtime validation
                        // remains separate because the shipped whole-retry path is cadence-driven.
                        let receiver_completion_ms = row.rounds * rtt_ms;
                        let sender_quiescence_ms = if row.feedback_datagrams == 0 {
                            receiver_completion_ms
                        } else {
                            receiver_completion_ms + rtt_ms / 2
                        };
                        println!(
                            "[fragment-recovery] policy={},loss={},seed={:#x},rtt_ms={},budget_bytes={},value_bytes={},fragments={},rounds={},receiver_completion_ms={},sender_quiescence_ms={},data_datagrams={},feedback_datagrams={},data_wire_bytes={},retransmitted_wire_bytes={},feedback_wire_bytes={},parity_datagrams={},parity_lost={},parity_wire_bytes={},wire_bytes={},wire_amplification={:.6},recovery_efficiency={:.6},duplicate_data_bytes={}",
                            policy.label(),
                            model.label(),
                            seed,
                            rtt_ms,
                            budget,
                            value_len,
                            fragments.len(),
                            row.rounds,
                            receiver_completion_ms,
                            sender_quiescence_ms,
                            row.data_datagrams,
                            row.feedback_datagrams,
                            row.data_wire_bytes,
                            row.retransmitted_wire_bytes,
                            row.feedback_wire_bytes,
                            row.parity_datagrams,
                            row.parity_lost,
                            row.parity_wire_bytes,
                            total_wire_bytes,
                            total_wire_bytes as f64 / value_len as f64,
                            (row.retransmitted_wire_bytes
                                + row.feedback_wire_bytes
                                + row.parity_wire_bytes) as f64
                                / value_len as f64,
                            row.duplicate_data_bytes,
                        );
                    }
                }
            }
        }
    }
}
