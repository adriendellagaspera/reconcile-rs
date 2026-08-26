// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::replicated_map::Config;

/// Default budget is 1024, matching `DEFAULT_MAX_CONCURRENT_BROADCASTS` (#83).
#[test]
fn default_budget_is_1024() {
    assert_eq!(Config::default().max_concurrent_broadcasts, 1024);
}

/// `with_max_concurrent_broadcasts` overrides the value.
#[test]
fn builder_sets_budget() {
    let cfg = Config::default().with_max_concurrent_broadcasts(1);
    assert_eq!(cfg.max_concurrent_broadcasts, 1);
}

/// With a budget of 1, claiming a second slot before the first is released must fail. After the
/// first slot is dropped its count returns to zero and a fresh claim succeeds — mirrors
/// `dump_budget::budget_guard_limits_and_releases_slots` for the egress-side counter.
#[tokio::test]
async fn budget_guard_limits_and_releases_slots() {
    use crate::replica::Replica;

    let config = Config::default()
        .with_port(crate::replica::tests::next_ephemeral_test_port())
        .with_listen_addr("127.0.0.98".parse().unwrap())
        .with_max_concurrent_broadcasts(1)
        .with_insecure_no_key();
    let eng = Replica::<i32, i32>::new(config).await.expect("bind failed");

    // First claim succeeds. Checked through both accessors: `broadcasts_in_flight_count`
    // (test-only) and `broadcasts_in_flight` (the always-available one backing
    // `Backpressure::in_flight`) read the same counter, but are two separate methods — each
    // needs its own witness so a mutant gutting either one alone still fails a test.
    let slot_a = eng.try_claim_broadcast_slot();
    assert!(slot_a.is_some(), "first slot must be available");
    assert_eq!(eng.broadcasts_in_flight_count(), 1);
    assert_eq!(eng.broadcasts_in_flight(), 1);

    // Second claim is rejected — budget exhausted.
    let slot_b = eng.try_claim_broadcast_slot();
    assert!(slot_b.is_none(), "second slot must be rejected at budget 1");
    assert_eq!(eng.broadcasts_in_flight_count(), 1);
    assert_eq!(eng.broadcasts_in_flight(), 1);

    // Releasing the first slot (drop) frees it for the next caller.
    drop(slot_a);
    assert_eq!(eng.broadcasts_in_flight_count(), 0);
    assert_eq!(eng.broadcasts_in_flight(), 0);

    let slot_retry = eng.try_claim_broadcast_slot();
    assert!(slot_retry.is_some(), "slot must be available after release");
    drop(slot_retry);
}

/// A budget of `0` must reject every claim: `insert`'s infallible path relies on this to skip
/// the broadcast rather than spawn a task, and `try_insert`/`try_update` rely on it to reject
/// deterministically without racing a real in-flight send.
#[tokio::test]
async fn zero_budget_rejects_every_claim() {
    use crate::replica::Replica;

    let config = Config::default()
        .with_port(crate::replica::tests::next_ephemeral_test_port())
        .with_listen_addr("127.0.0.97".parse().unwrap())
        .with_max_concurrent_broadcasts(0)
        .with_insecure_no_key();
    let eng = Replica::<i32, i32>::new(config).await.expect("bind failed");

    assert!(eng.try_claim_broadcast_slot().is_none());
    assert_eq!(eng.broadcasts_in_flight_count(), 0);
}

/// `max_concurrent_broadcasts()` must report the configured, non-zero budget verbatim — it backs
/// `Backpressure::max_in_flight`, so a caller reading it after a rejection must see the real cap,
/// not an unrelated value (e.g. always `0`, indistinguishable from the zero-budget case above).
#[tokio::test]
async fn max_concurrent_broadcasts_reports_the_configured_non_zero_budget() {
    use crate::replica::Replica;

    let config = Config::default()
        .with_port(crate::replica::tests::next_ephemeral_test_port())
        .with_listen_addr("127.0.0.96".parse().unwrap())
        .with_max_concurrent_broadcasts(7)
        .with_insecure_no_key();
    let eng = Replica::<i32, i32>::new(config).await.expect("bind failed");

    assert_eq!(eng.max_concurrent_broadcasts(), 7);
}
