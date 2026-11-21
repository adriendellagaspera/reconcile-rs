// Copyright 2023 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::replicated_map::Config;

/// With a budget of 1, claiming a second slot before the first is released must fail. After the
/// first slot is dropped its count returns to zero and a fresh claim succeeds — mirrors
/// `dump_budget::budget_guard_limits_and_releases_slots` for the egress-side counter.
#[tokio::test]
async fn budget_guard_limits_and_releases_slots() {
    let config = Config::default()
        .with_port(5000)
        .with_listen_addr("127.0.0.98".parse().unwrap())
        .with_max_concurrent_broadcasts(1)
        .with_insecure_no_key();
    let eng = super::in_memory_test_replica::<i32, i32>(config);

    // The configured cap and the production counter are the only observable contract.
    assert_eq!(eng.max_concurrent_broadcasts(), 1);
    let slot_a = eng.try_claim_broadcast_slot();
    assert!(slot_a.is_some(), "first slot must be available");
    assert_eq!(eng.broadcasts_in_flight(), 1);

    // Second claim is rejected — budget exhausted.
    let slot_b = eng.try_claim_broadcast_slot();
    assert!(slot_b.is_none(), "second slot must be rejected at budget 1");
    assert_eq!(eng.broadcasts_in_flight(), 1);

    // Releasing the first slot (drop) frees it for the next caller.
    drop(slot_a);
    assert_eq!(eng.broadcasts_in_flight(), 0);

    let slot_retry = eng.try_claim_broadcast_slot();
    assert!(slot_retry.is_some(), "slot must be available after release");
    drop(slot_retry);
}
