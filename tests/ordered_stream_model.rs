// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

#[path = "../benches/ordered_stream_model.rs"]
mod model;

use std::time::Duration;

use model::{simulate, simulate_interruption, Candidate, Case, Session, Workload};

const MIB: usize = 1_048_576;

fn case(loss_percent: f64, reorder_percent: f64, seed: u64) -> Case {
    Case {
        datagram_payload_budget: 1_200,
        rtt: Duration::from_millis(150),
        loss_percent,
        reorder_percent,
        bandwidth_bps: 100_000_000,
        seed,
        session: Session::Established,
    }
}

#[test]
fn benchmark_labels_are_stable() {
    assert_eq!(Candidate::UdpWholeMessage.label(), "udp_whole_message");
    assert_eq!(Candidate::UdpMissingOnly.label(), "udp_missing_only");
    assert_eq!(
        Candidate::SingleReliableStream.label(),
        "single_reliable_stream"
    );
    assert_eq!(
        Candidate::MultipleReliableStreams.label(),
        "multiple_reliable_streams"
    );
    assert_eq!(
        Candidate::HybridDatagramControl.label(),
        "hybrid_datagram_control"
    );
    assert_eq!(Session::Cold.label(), "cold");
    assert_eq!(Session::Resumed.label(), "resumed");
    assert_eq!(Session::Established.label(), "established");
}

#[test]
fn clean_ordered_stream_has_no_hol_delay() {
    let metrics = simulate(
        &Workload::single_bulk(128 * 1024),
        case(0.0, 0.0, 1),
        Candidate::SingleReliableStream,
    );

    assert_eq!(metrics.retransmitted_wire_bytes, 0);
    assert_eq!(metrics.total_hol_delay, Duration::ZERO);
    assert_eq!(metrics.max_hol_delay, Duration::ZERO);
    assert_eq!(metrics.peak_hol_buffer_bytes, 0);
}

#[test]
fn pure_bulk_single_and_multi_streams_pay_the_same_wire_for_matched_loss() {
    let workload = Workload::concurrent_bulk(4, 64 * 1024);
    let input = case(5.0, 0.0, 0x2720_0001);
    let single = simulate(&workload, input, Candidate::SingleReliableStream);
    let multi = simulate(&workload, input, Candidate::MultipleReliableStreams);

    assert_eq!(single.wire_bytes, multi.wire_bytes);
    assert_eq!(
        single.retransmitted_wire_bytes,
        multi.retransmitted_wire_bytes
    );
    assert_eq!(single.packets, multi.packets);
}

#[test]
fn independent_streams_remove_cross_transfer_hol_for_a_matched_loss_trace() {
    let workload = Workload::concurrent_bulk(4, 64 * 1024);
    let pair = (0..10_000_u64).find_map(|seed| {
        let input = case(10.0, 0.0, seed);
        let single = simulate(&workload, input, Candidate::SingleReliableStream);
        let multi = simulate(&workload, input, Candidate::MultipleReliableStreams);
        (single.max_hol_delay > multi.max_hol_delay
            && single.peak_hol_buffer_bytes > multi.peak_hol_buffer_bytes)
            .then_some((single, multi))
    });
    let (single, multi) = pair.expect("fixed seed range must contain cross-stream HOL");

    assert!(single.max_hol_delay > multi.max_hol_delay);
    assert!(single.peak_hol_buffer_bytes > multi.peak_hol_buffer_bytes);
}

#[test]
fn mixed_control_latency_exposes_single_stream_hol() {
    let workload = Workload::mixed_control_bulk(128 * 1024, 12, 256, Duration::from_millis(2));
    let pair = (0..10_000_u64).find_map(|seed| {
        let input = case(10.0, 0.0, seed);
        let single = simulate(&workload, input, Candidate::SingleReliableStream);
        let multi = simulate(&workload, input, Candidate::MultipleReliableStreams);
        (single.mean_control_latency > multi.mean_control_latency
            && single.max_hol_delay > multi.max_hol_delay)
            .then_some((single, multi))
    });
    let (single, multi) = pair.expect("fixed seed range must contain control HOL");

    assert!(single.mean_control_latency > multi.mean_control_latency);
    assert!(single.max_hol_delay > multi.max_hol_delay);
}

#[test]
fn selective_udp_recovery_can_reduce_wire_against_whole_message_retry() {
    let workload = Workload::single_bulk(256 * 1024);
    let pair = (0..10_000_u64).find_map(|seed| {
        let input = case(5.0, 0.0, seed);
        let whole = simulate(&workload, input, Candidate::UdpWholeMessage);
        let selective = simulate(&workload, input, Candidate::UdpMissingOnly);
        (whole.retransmitted_wire_bytes > 0 && selective.wire_bytes < whole.wire_bytes)
            .then_some((whole, selective))
    });
    let (whole, selective) = pair.expect("fixed seed range must contain selective recovery win");

    assert!(selective.retransmitted_wire_bytes < whole.retransmitted_wire_bytes);
    assert!(selective.control_bytes > 0);
    assert!(selective.wire_bytes < whole.wire_bytes);
}

#[test]
fn cold_resumed_and_established_setup_are_separate() {
    let workload = Workload::single_bulk(64 * 1024);
    let mut input = case(0.0, 0.0, 7);

    input.session = Session::Established;
    let established = simulate(&workload, input, Candidate::SingleReliableStream);
    input.session = Session::Resumed;
    let resumed = simulate(&workload, input, Candidate::SingleReliableStream);
    input.session = Session::Cold;
    let cold = simulate(&workload, input, Candidate::SingleReliableStream);

    assert_eq!(established.setup_bytes, 0);
    assert!(resumed.setup_bytes > established.setup_bytes);
    assert!(cold.setup_bytes > resumed.setup_bytes);
    assert_eq!(established.setup_latency, Duration::ZERO);
    assert!(resumed.setup_latency > established.setup_latency);
    assert!(cold.setup_latency > resumed.setup_latency);
    assert!(cold.receiver_completion > resumed.receiver_completion);
}

#[test]
fn udp_does_not_inherit_stream_handshake_cost() {
    let workload = Workload::single_bulk(64 * 1024);
    let mut input = case(0.0, 0.0, 11);
    input.session = Session::Cold;
    let cold = simulate(&workload, input, Candidate::UdpMissingOnly);
    input.session = Session::Established;
    let established = simulate(&workload, input, Candidate::UdpMissingOnly);

    assert_eq!(cold, established);
    assert_eq!(cold.setup_bytes, 0);
    assert_eq!(cold.setup_latency, Duration::ZERO);
}

#[test]
fn constrained_bandwidth_advances_the_actual_timeline() {
    let workload = Workload::single_bulk(256 * 1024);
    let mut fast_case = case(0.0, 0.0, 13);
    fast_case.bandwidth_bps = 100_000_000;
    let mut slow_case = fast_case;
    slow_case.bandwidth_bps = 1_000_000;

    let fast = simulate(&workload, fast_case, Candidate::MultipleReliableStreams);
    let slow = simulate(&workload, slow_case, Candidate::MultipleReliableStreams);

    assert!(slow.serialization_time > fast.serialization_time);
    assert!(slow.receiver_completion > fast.receiver_completion);
}

#[test]
fn targeted_reordering_is_hol_for_streams_but_not_datagrams() {
    let workload = Workload::single_bulk(128 * 1024);
    let sample = (0..10_000_u64).find_map(|seed| {
        let input = case(0.0, 20.0, seed);
        let stream = simulate(&workload, input, Candidate::SingleReliableStream);
        (stream.max_hol_delay > Duration::ZERO).then_some((input, stream))
    });
    let (input, stream) = sample.expect("fixed seed range must contain reordered stream segments");
    let udp = simulate(&workload, input, Candidate::UdpMissingOnly);

    assert!(stream.max_hol_delay > Duration::ZERO);
    assert_eq!(udp.max_hol_delay, Duration::ZERO);
}

#[test]
fn reconnect_discards_generic_stream_offset_but_missing_only_reuses_fragments() {
    let input = case(0.0, 0.0, 17);
    let missing = simulate_interruption(
        MIB,
        input,
        Candidate::UdpMissingOnly,
        0.5,
        Duration::from_secs(30),
        true,
    );
    let stream = simulate_interruption(
        MIB,
        input,
        Candidate::MultipleReliableStreams,
        0.5,
        Duration::from_secs(30),
        true,
    );

    assert_eq!(
        missing.retained_progress_bytes,
        stream.retained_progress_bytes
    );
    assert!(missing.additional_wire_bytes < stream.additional_wire_bytes);
    assert!(stream.additional_setup_bytes > 0);
}

#[test]
fn surviving_stream_session_reuses_acknowledged_prefix() {
    let input = case(0.0, 0.0, 19);
    let surviving = simulate_interruption(
        MIB,
        input,
        Candidate::MultipleReliableStreams,
        0.5,
        Duration::from_secs(1),
        false,
    );
    let reconnect = simulate_interruption(
        MIB,
        input,
        Candidate::MultipleReliableStreams,
        0.5,
        Duration::from_secs(1),
        true,
    );

    assert!(surviving.additional_wire_bytes < reconnect.additional_wire_bytes);
    assert_eq!(surviving.additional_setup_bytes, 0);
    assert!(reconnect.additional_setup_bytes > 0);
}

#[test]
fn more_retained_progress_monotonically_reduces_missing_only_resume_bytes() {
    let input = case(0.0, 0.0, 23);
    let bytes: Vec<u64> = [0.2, 0.5, 0.8]
        .into_iter()
        .map(|fraction| {
            simulate_interruption(
                MIB,
                input,
                Candidate::UdpMissingOnly,
                fraction,
                Duration::from_secs(30),
                true,
            )
            .additional_wire_bytes
        })
        .collect();

    assert!(bytes[0] > bytes[1]);
    assert!(bytes[1] > bytes[2]);
}

#[test]
fn simulation_is_reproducible() {
    let workload = Workload::mixed_control_bulk(64 * 1024, 8, 256, Duration::from_millis(2));
    let input = case(5.0, 5.0, 0x5eed_2720);

    assert_eq!(
        simulate(&workload, input, Candidate::HybridDatagramControl),
        simulate(&workload, input, Candidate::HybridDatagramControl)
    );
}
