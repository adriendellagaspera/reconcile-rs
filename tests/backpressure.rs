// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Tests for the write-broadcast egress budget (#83): `Config::max_concurrent_broadcasts`,
//! `ReplicatedMap::try_insert`/`try_update`, and the infallible `insert`'s
//! skip-only-the-broadcast behavior at the same budget. A budget of `0` makes the exhausted case
//! fully deterministic (no need to race a real in-flight send): `n < 0` never holds, so every
//! claim attempt fails immediately.

use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;

use reconcile::replicated_map::{Backpressure, Config};
use reconcile::ReplicatedMap;

fn config(port: u16, addr: &str) -> Config {
    Config::default()
        .with_port(port)
        .with_listen_addr(addr.parse().unwrap())
        .with_insecure_no_key()
}

async fn isolated(port: u16, addr: &str) -> ReplicatedMap<i32, i32> {
    ReplicatedMap::new(config(port, addr))
        .await
        .expect("bind failed")
}

/// `Backpressure` is `#[non_exhaustive]`, so this crate cannot build one via struct-literal
/// syntax to compare against — assert on its public fields and `Display` output instead (the
/// latter also proves the `Display` impl actually formats the real fields, not a stub).
fn assert_zero_budget_backpressure(err: Backpressure) {
    assert_eq!(err.in_flight, 0);
    assert_eq!(err.max_in_flight, 0);
    assert_eq!(
        err.to_string(),
        "write-broadcast egress budget exhausted: 0/0 broadcasts in flight"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn try_insert_succeeds_under_the_default_budget() {
    let store = isolated(8320, "127.0.0.240").await;
    assert_eq!(store.try_insert(1, 10), Ok(None));
    assert_eq!(store.try_insert(1, 20), Ok(Some(10)));
    assert_eq!(store.get(&1).as_deref(), Some(&20));

    // The slot each call claimed is held by its detached send task, not released synchronously
    // when try_insert itself returns — poll rather than assert immediately.
    let mut settled = false;
    for _ in 0..300 {
        if reconcile::testing::broadcasts_in_flight_count(&store) == 0 {
            settled = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        settled,
        "both claimed broadcast slots should be released once their sends complete"
    );
}

/// `try_update` on an absent/tombstoned key under a real (non-exhausted) budget must behave like
/// `update` does: claim and release the slot without ever broadcasting, and report `Ok(false)`
/// rather than mutating anything. Distinct from the zero-budget case below, which never gets far
/// enough to notice the key is absent at all.
#[tokio::test(flavor = "multi_thread")]
async fn try_update_on_an_absent_key_succeeds_as_a_no_op_under_the_default_budget() {
    let store = isolated(8327, "127.0.0.250").await;
    assert_eq!(store.try_update(&1, |v| *v += 1), Ok(false));
    assert!(store.get(&1).is_none(), "no key should have been created");
}

#[tokio::test(flavor = "multi_thread")]
async fn try_insert_rejects_and_leaves_the_map_untouched_at_a_zero_budget() {
    let store = ReplicatedMap::<i32, i32>::new(
        config(8321, "127.0.0.241").with_max_concurrent_broadcasts(0),
    )
    .await
    .expect("bind failed");

    assert_zero_budget_backpressure(
        store
            .try_insert(1, 10)
            .expect_err("a zero budget must reject the claim"),
    );
    assert!(
        store.get(&1).is_none(),
        "a rejected try_insert must not have applied the write"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn try_update_rejects_and_leaves_the_map_untouched_at_a_zero_budget() {
    let store = ReplicatedMap::<i32, i32>::new(
        config(8322, "127.0.0.242").with_max_concurrent_broadcasts(0),
    )
    .await
    .expect("bind failed");
    // `load_bulk` never broadcasts, so it is unaffected by the zero budget: the one way to seed
    // a live value here.
    store.load_bulk(&[(1, 10)]);

    assert_zero_budget_backpressure(
        store
            .try_update(&1, |v| *v += 1)
            .expect_err("a zero budget must reject the claim"),
    );
    assert_eq!(
        store.get(&1).as_deref(),
        Some(&10),
        "a rejected try_update must not have applied the mutation"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn try_update_on_an_absent_key_still_rejects_at_a_zero_budget() {
    // #83's all-or-nothing guarantee claims the slot *before* checking liveness, so even the
    // branch `update` treats as a free no-op is budget-gated here.
    let store = ReplicatedMap::<i32, i32>::new(
        config(8324, "127.0.0.245").with_max_concurrent_broadcasts(0),
    )
    .await
    .expect("bind failed");

    assert_zero_budget_backpressure(
        store
            .try_update(&1, |v| *v += 1)
            .expect_err("a zero budget must reject the claim"),
    );
}

/// A zero egress budget must never fail the infallible `insert` (#83's compatibility
/// requirement) — only silently skip the eager push, leaving anti-entropy as the sole remaining
/// delivery path. That path is starved here by a `reconcile_interval` far beyond the test's
/// deadline, so a peer receiving the value could only mean the broadcast slipped through the
/// exhausted budget.
#[tokio::test(flavor = "multi_thread")]
async fn insert_still_applies_locally_at_a_zero_budget_but_never_broadcasts() {
    let port = 8325u16;
    let cfg = |addr: &str| {
        config(port, addr)
            .with_max_concurrent_broadcasts(0)
            .with_reconcile_interval(Duration::from_secs(3600))
    };
    let sender = ReplicatedMap::<i32, i32>::new(cfg("127.0.0.246"))
        .await
        .expect("bind failed");
    let receiver = ReplicatedMap::<i32, i32>::new(cfg("127.0.0.247"))
        .await
        .expect("bind failed");

    let t_sender = tokio::spawn(sender.clone().run(CancellationToken::new()));
    let t_receiver = tokio::spawn(receiver.clone().run(CancellationToken::new()));
    // Let each engine's unconditional round-0 settle while neither knows any peer, so it is a
    // no-op — otherwise a stray first round could converge the value on its own and this test
    // would pass even with a broadcast that isn't actually suppressed (mirrors
    // `immediate_broadcast`'s engine-level tests, `src/replica/tests/immediate_broadcast.rs`).
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Only the sender is told about the receiver: the receiver must learn of it purely by
    // receiving a datagram, which a zero egress budget must prevent.
    sender.seed_peer("127.0.0.247".parse().unwrap());
    assert_eq!(
        sender.insert(1, 10),
        None,
        "the local write must still apply"
    );
    assert_eq!(sender.get(&1).as_deref(), Some(&10));

    tokio::time::sleep(Duration::from_millis(200)).await;
    t_sender.abort();
    t_receiver.abort();

    assert!(
        receiver.get(&1).is_none(),
        "a zero egress budget must have suppressed the broadcast entirely"
    );
}

/// `try_update`'s live/success branch must actually broadcast — not just apply the mutation
/// locally and return `Ok(true)`. Mirrors `immediate_broadcast`'s engine-level tests
/// (`src/replica/tests/immediate_broadcast.rs`): the receiver is never told about the sender, and
/// `reconcile_interval` is starved, so only `try_update`'s own push can deliver the new value.
#[tokio::test(flavor = "multi_thread")]
async fn try_update_broadcasts_the_live_update_to_a_peer() {
    let port = 8326u16;
    let cfg = |addr: &str| config(port, addr).with_reconcile_interval(Duration::from_secs(3600));
    let sender = ReplicatedMap::<i32, i32>::new(cfg("127.0.0.248"))
        .await
        .expect("bind failed");
    let receiver = ReplicatedMap::<i32, i32>::new(cfg("127.0.0.249"))
        .await
        .expect("bind failed");

    let t_sender = tokio::spawn(sender.clone().run(CancellationToken::new()));
    let t_receiver = tokio::spawn(receiver.clone().run(CancellationToken::new()));
    tokio::time::sleep(Duration::from_millis(100)).await;

    // `load_bulk` seeds the live key without broadcasting, so only `try_update`'s own push can
    // be the source of the receiver's copy.
    sender.load_bulk(&[(1, 10)]);
    sender.seed_peer("127.0.0.249".parse().unwrap());
    assert_eq!(sender.try_update(&1, |v| *v += 1), Ok(true));

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut received = None;
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
        received = receiver.get(&1).as_deref().copied();
        if received.is_some() {
            break;
        }
    }

    t_sender.abort();
    t_receiver.abort();
    assert_eq!(
        received,
        Some(11),
        "try_update's broadcast must reach the peer"
    );
}
