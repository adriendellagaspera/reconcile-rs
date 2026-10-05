// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

#[path = "../benches/fragment_recovery_model.rs"]
mod model;

use std::time::Duration;

use model::{simulate, Case, RecoveryPolicy};

fn case(fragment_count: usize, loss_percent: f64, seed: u64) -> Case {
    Case {
        fragment_count,
        datagram_payload_budget: 1_200,
        rtt: Duration::from_millis(50),
        loss_percent,
        bandwidth_bps: 100_000_000,
        seed,
    }
}

#[test]
fn clean_flights_use_exactly_the_requested_production_frame_count() {
    for fragment_count in [1, 4, 16, 64, 900] {
        let metrics = simulate(case(fragment_count, 0.0, 1), RecoveryPolicy::WholeRetry);
        assert_eq!(metrics.datagrams, fragment_count as u64);
        assert_eq!(metrics.recovery_rounds, 0);
        assert_eq!(metrics.retransmitted_wire_bytes, 0);
        assert_eq!(metrics.control_bytes, 0);
    }
}

#[test]
fn missing_only_prices_completion_ack_even_on_a_clean_flight() {
    let whole = simulate(case(16, 0.0, 7), RecoveryPolicy::WholeRetry);
    let selective = simulate(case(16, 0.0, 7), RecoveryPolicy::MissingOnly);

    assert_eq!(selective.retransmitted_wire_bytes, 0);
    assert_eq!(selective.datagrams, whole.datagrams + 1);
    assert!(selective.control_bytes > 0);
    assert!(selective.wire_bytes > whole.wire_bytes);
    assert!(selective.sender_quiescence > selective.receiver_completion);
}

#[test]
fn matched_loss_draws_give_selective_recovery_a_real_wire_win() {
    let pair = (0..20_000_u64).find_map(|seed| {
        let whole = simulate(case(64, 5.0, seed), RecoveryPolicy::WholeRetry);
        let selective = simulate(case(64, 5.0, seed), RecoveryPolicy::MissingOnly);
        (whole.initial_missing_useful_bytes > 0
            && whole.initial_missing_useful_bytes == selective.initial_missing_useful_bytes
            && selective.wire_bytes < whole.wire_bytes)
            .then_some((whole, selective))
    });
    let (whole, selective) = pair.expect("fixed seed range must contain a selective-recovery win");

    assert!(selective.retransmitted_wire_bytes < whole.retransmitted_wire_bytes);
    assert!(selective.control_bytes > whole.control_bytes);
    let whole_cost = whole.retransmitted_wire_bytes + whole.control_bytes;
    let selective_cost = selective.retransmitted_wire_bytes + selective.control_bytes;
    assert!(selective_cost < whole_cost);
}

#[test]
fn simulation_is_reproducible_for_the_same_seed() {
    let input = case(64, 5.0, 0x5eed_2710);
    assert_eq!(
        simulate(input, RecoveryPolicy::MissingOnly),
        simulate(input, RecoveryPolicy::MissingOnly)
    );
}

#[test]
fn selective_sender_state_is_explicit_and_domain_completion_is_not_hidden() {
    let whole = simulate(case(64, 1.0, 9), RecoveryPolicy::WholeRetry);
    let selective = simulate(case(64, 1.0, 9), RecoveryPolicy::MissingOnly);

    assert_eq!(whole.peak_sender_recovery_state_bytes, 0);
    assert!(selective.peak_sender_recovery_state_bytes > selective.useful_bytes);
    assert_eq!(whole.domain_convergence, whole.receiver_completion);
    assert_eq!(selective.domain_convergence, selective.receiver_completion);
}

#[test]
fn policy_labels_are_stable_for_benchmark_output() {
    assert_eq!(RecoveryPolicy::WholeRetry.label(), "whole_retry");
    assert_eq!(RecoveryPolicy::MissingOnly.label(), "missing_only");
}
