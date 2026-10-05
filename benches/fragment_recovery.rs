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
const SELECTIVE_NACK_HEADER: usize = 32 + 2; // transfer id + missing-count
const SELECTIVE_NACK_INDEX: usize = 4; // fragment index
const SEED: u64 = 0x2710_0001;

#[derive(Clone, Copy, Debug)]
enum Policy {
    WholeRetry,
    MissingOnly,
}

impl Policy {
    fn label(self) -> &'static str {
        match self {
            Policy::WholeRetry => "whole-retry",
            Policy::MissingOnly => "missing-only",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Loss {
    Iid(f64),
    DeterministicEvery(usize),
}

impl Loss {
    fn label(self) -> String {
        match self {
            Loss::Iid(p) => format!("iid-{:.1}%", p * 100.0),
            Loss::DeterministicEvery(n) => format!("every-{n}"),
        }
    }
}

struct LossState {
    model: Loss,
    rng: StdRng,
    offered: usize,
}

impl LossState {
    fn new(model: Loss) -> Self {
        Self {
            model,
            rng: StdRng::seed_from_u64(SEED),
            offered: 0,
        }
    }

    fn lost(&mut self, round: usize, fragment_index: usize) -> bool {
        self.offered += 1;
        match self.model {
            Loss::Iid(p) => self.rng.gen_bool(p.clamp(0.0, 1.0)),
            // A controlled first-flight loss mask: unlike "every Nth send", this cannot
            // accidentally pin the same fragment forever when a whole-message retry has N
            // fragments. Recovery traffic itself is delivered in this deterministic lane.
            Loss::DeterministicEvery(n) => {
                round == 1 && n != 0 && (fragment_index + 1) % n == 0
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
    feedback_wire_bytes: usize,
    duplicate_data_bytes: usize,
}

fn auth_overhead() -> usize {
    TAG_LEN + REPLAY_HEADER_LEN + VERSION_LEN
}

fn fragment_lengths(value_len: usize, budget: usize) -> Vec<usize> {
    let capacity = fragment_payload_capacity(budget, auth_overhead())
        .expect("datagram budget must fit auth and fragment headers");
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

fn run_case(policy: Policy, loss: Loss, value_len: usize, budget: usize) -> ResultRow {
    let fragments = fragment_lengths(value_len, budget);
    let mut received = vec![false; fragments.len()];
    let mut loss = LossState::new(loss);
    let mut result = ResultRow::default();

    while received.iter().any(|received| !received) {
        result.rounds += 1;
        let send: Vec<usize> = match policy {
            Policy::WholeRetry => (0..fragments.len()).collect(),
            Policy::MissingOnly => received
                .iter()
                .enumerate()
                .filter_map(|(index, received)| (!received).then_some(index))
                .collect(),
        };

        for index in send {
            let payload_len = fragments[index];
            result.data_datagrams += 1;
            result.data_wire_bytes += data_wire_bytes(payload_len);
            if loss.lost(result.rounds, index) {
                continue;
            }
            if received[index] {
                result.duplicate_data_bytes += payload_len;
            } else {
                received[index] = true;
            }
        }

        if received.iter().all(|received| *received) {
            break;
        }

        if matches!(policy, Policy::MissingOnly) {
            let missing = received.iter().filter(|received| !**received).count();
            result.feedback_datagrams += 1;
            result.feedback_wire_bytes += nack_wire_bytes(missing);
        }

        // A pathological deterministic schedule can otherwise make no progress forever.
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
    let sizes = env_usizes("RECONCILE_RECOVERY_VALUE_SIZES", "4096,131072,1048576");
    let losses = env_f64s("RECONCILE_RECOVERY_LOSS_PERCENT", "0,0.1,1,5,10");
    let deterministic_every = env_usizes("RECONCILE_RECOVERY_DETERMINISTIC_EVERY", "8,32");
    let budget = std::env::var("RECONCILE_RECOVERY_BUDGET")
        .unwrap_or_else(|_| DEFAULT_DATAGRAM_BUDGET.to_string())
        .parse()
        .expect("RECONCILE_RECOVERY_BUDGET must be usize");

    let mut models: Vec<Loss> = losses
        .into_iter()
        .map(|percent| Loss::Iid(percent / 100.0))
        .collect();
    models.extend(
        deterministic_every
            .into_iter()
            .filter(|n| *n > 0)
            .map(Loss::DeterministicEvery),
    );

    for value_len in sizes {
        let fragments = fragment_lengths(value_len, budget);
        for model in models.iter().copied() {
            for policy in [Policy::WholeRetry, Policy::MissingOnly] {
                let row = run_case(policy, model, value_len, budget);
                let total_wire_bytes = row.data_wire_bytes + row.feedback_wire_bytes;
                println!(
                    "[fragment-recovery] policy={},loss={},seed={:#x},budget_bytes={},value_bytes={},fragments={},rounds={},data_datagrams={},feedback_datagrams={},data_wire_bytes={},feedback_wire_bytes={},wire_bytes={},wire_amplification={:.6},duplicate_data_bytes={}",
                    policy.label(),
                    model.label(),
                    SEED,
                    budget,
                    value_len,
                    fragments.len(),
                    row.rounds,
                    row.data_datagrams,
                    row.feedback_datagrams,
                    row.data_wire_bytes,
                    row.feedback_wire_bytes,
                    total_wire_bytes,
                    total_wire_bytes as f64 / value_len as f64,
                    row.duplicate_data_bytes,
                );
            }
        }
    }
}

