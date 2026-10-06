// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

#[path = "../benches/fragment_recovery_model.rs"]
mod model;

use std::time::Duration;

use model::{simulate, simulate_interruption, BurstLoss, Case, RecoveryPolicy};

fn case(fragment_count: usize, loss_percent: f64, seed: u64) -> Case {
    Case {
        fragment_count,
        logical_bytes: None,
        datagram_payload_budget: 1_200,
        rtt: Duration::from_millis(50),
        loss_percent,
        burst_loss: None,
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
        assert_eq!(metrics.parity_wire_bytes, 0);
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
        (whole.initial_data_loss_useful_bytes > 0
            && whole.initial_data_loss_useful_bytes == selective.initial_data_loss_useful_bytes
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
fn one_loss_per_xor_group_can_complete_without_a_recovery_round() {
    let sample = (0..100_000_u64).find_map(|seed| {
        let fec = simulate(case(16, 5.0, seed), RecoveryPolicy::Xor8Plus1);
        let selective = simulate(case(16, 5.0, seed), RecoveryPolicy::MissingOnly);
        (fec.fec_recovered_fragments > 0
            && fec.recovery_rounds == 0
            && selective.recovery_rounds > 0)
            .then_some((fec, selective))
    });
    let (fec, selective) = sample.expect("fixed seed range must contain a parity-only recovery");

    assert!(fec.parity_wire_bytes > 0);
    assert!(fec.fec_recovered_useful_bytes > 0);
    assert_eq!(fec.missing_after_initial_recovery_bytes, 0);
    assert!(fec.receiver_completion < selective.receiver_completion);
}

#[test]
fn bounded_parity_falls_back_to_missing_only_when_loss_exceeds_its_budget() {
    let fec = (0..100_000_u64)
        .map(|seed| simulate(case(16, 15.0, seed), RecoveryPolicy::Xor8Plus1))
        .find(|metrics| {
            metrics.fec_recovered_fragments > 0
                && metrics.missing_after_initial_recovery_bytes > 0
                && metrics.recovery_rounds > 0
        })
        .expect("fixed seed range must contain a parity-plus-NACK recovery");

    assert!(fec.retransmitted_wire_bytes > 0);
    assert!(fec.control_bytes > 0);
    assert!(fec.sender_quiescence > fec.receiver_completion);
}

#[test]
fn simulation_is_reproducible_for_the_same_seed() {
    let input = case(64, 5.0, 0x5eed_2710);
    assert_eq!(
        simulate(input, RecoveryPolicy::Xor8Plus1),
        simulate(input, RecoveryPolicy::Xor8Plus1)
    );
}

#[test]
fn recovery_state_and_completion_semantics_are_explicit() {
    let whole = simulate(case(64, 1.0, 9), RecoveryPolicy::WholeRetry);
    let selective = simulate(case(64, 1.0, 9), RecoveryPolicy::MissingOnly);
    let fec = simulate(case(64, 1.0, 9), RecoveryPolicy::Xor8Plus1);

    assert_eq!(whole.peak_sender_recovery_state_bytes, 0);
    assert!(selective.peak_sender_recovery_state_bytes > selective.useful_bytes);
    assert!(fec.peak_sender_recovery_state_bytes > selective.peak_sender_recovery_state_bytes);
    for metrics in [whole, selective, fec] {
        assert_eq!(metrics.domain_convergence, metrics.receiver_completion);
    }
}

#[test]
fn policy_labels_are_stable_for_benchmark_output() {
    assert_eq!(RecoveryPolicy::WholeRetry.label(), "whole_retry");
    assert_eq!(RecoveryPolicy::MissingOnly.label(), "missing_only");
    assert_eq!(RecoveryPolicy::Xor8Plus1.label(), "xor_8_plus_1");
}

#[test]
fn burst_loss_is_applied_to_first_flight_physical_datagrams() {
    let mut input = case(64, 0.0, 11);
    input.burst_loss = Some(BurstLoss {
        start_datagram: 16,
        datagram_count: 8,
    });
    let whole = simulate(input, RecoveryPolicy::WholeRetry);
    let selective = simulate(input, RecoveryPolicy::MissingOnly);

    assert!(whole.initial_data_loss_useful_bytes > 0);
    assert_eq!(
        whole.initial_data_loss_useful_bytes,
        selective.initial_data_loss_useful_bytes
    );
    assert_eq!(whole.recovery_rounds, 1);
    assert_eq!(selective.recovery_rounds, 1);
    assert!(selective.retransmitted_wire_bytes < whole.retransmitted_wire_bytes);
}

#[test]
fn interruption_retains_progress_and_selective_resume_avoids_restart_bytes() {
    for fraction in [0.2, 0.5, 0.8] {
        let input = case(64, 0.0, 17);
        let whole = simulate_interruption(
            input,
            RecoveryPolicy::WholeRetry,
            fraction,
            Duration::from_secs(1),
            true,
        );
        let selective = simulate_interruption(
            input,
            RecoveryPolicy::MissingOnly,
            fraction,
            Duration::from_secs(1),
            true,
        );

        assert!(selective.progress_retained_useful_bytes > 0);
        assert_eq!(
            selective.progress_retained_useful_bytes,
            whole.progress_retained_useful_bytes
        );
        assert!(selective.additional_wire_bytes < whole.additional_wire_bytes);
        assert!(selective.additional_control_bytes > 0);
    }
}

#[test]
fn more_pre_interruption_progress_monotonically_reduces_selective_resume_bytes() {
    let input = case(900, 0.0, 19);
    let resume_bytes: Vec<u64> = [0.2, 0.5, 0.8]
        .into_iter()
        .map(|fraction| {
            simulate_interruption(
                input,
                RecoveryPolicy::MissingOnly,
                fraction,
                Duration::from_secs(30),
                true,
            )
            .additional_wire_bytes
        })
        .collect();

    assert!(resume_bytes[0] > resume_bytes[1]);
    assert!(resume_bytes[1] > resume_bytes[2]);
}

#[test]
fn parity_policy_uses_missing_only_fallback_after_contact_resumes() {
    let input = case(64, 0.0, 23);
    let selective = simulate_interruption(
        input,
        RecoveryPolicy::MissingOnly,
        0.5,
        Duration::from_secs(1),
        true,
    );
    let fec = simulate_interruption(
        input,
        RecoveryPolicy::Xor8Plus1,
        0.5,
        Duration::from_secs(1),
        true,
    );
    assert_eq!(fec, selective);
}

#[test]
fn bounded_parity_does_not_tax_an_unfragmented_transfer() {
    let whole = simulate(case(1, 0.0, 29), RecoveryPolicy::WholeRetry);
    let fec = simulate(case(1, 0.0, 29), RecoveryPolicy::Xor8Plus1);

    assert_eq!(fec.parity_wire_bytes, 0);
    assert_eq!(fec.fec_recovered_fragments, 0);
    assert_eq!(fec.wire_bytes, whole.wire_bytes + fec.control_bytes);
}
