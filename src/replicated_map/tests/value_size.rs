// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::error::Error;
use std::sync::Arc;

use crate::replicated_map::{Backpressure, Config, ValueTooLarge, WriteRejected};
use crate::{InMemoryNetwork, ReplicatedMap};

/// Unwrap a rejection expected to be [`WriteRejected::TooLarge`], panicking with the actual
/// variant otherwise — these tests never touch `Config::max_concurrent_broadcasts`, so a
/// `Backpressure` rejection here would be a surprise worth failing loudly on, not silently
/// matching away.
fn expect_too_large(err: WriteRejected) -> crate::replicated_map::ValueTooLarge {
    let WriteRejected::TooLarge(err) = err else {
        panic!("expected WriteRejected::TooLarge, got {err:?}");
    };
    err
}

fn store_with_ceiling(port: u16, max_value_size: usize) -> ReplicatedMap<String, Vec<u8>> {
    let network = InMemoryNetwork::new();
    let transport = Arc::new(network.bind(format!("127.0.0.1:{port}").parse().unwrap()));
    ReplicatedMap::<String, Vec<u8>>::new_with_transport(
        Config::default()
            .with_insecure_no_key()
            .with_max_value_size(max_value_size),
        transport,
    )
}

/// #82: `Config::max_value_size` is unset by default, so `try_insert` on a default `Config`
/// behaves exactly like `insert` — never rejecting, however large the value.
///
/// `#[tokio::test]`: an accepted `try_insert` reaches `insert`'s broadcast, which needs an
/// ambient Tokio runtime like every other successful write (`insert`'s `# Panics`).
#[tokio::test]
async fn try_insert_without_max_value_size_never_rejects() {
    let network = InMemoryNetwork::new();
    let transport = Arc::new(network.bind("127.0.0.1:8420".parse().unwrap()));
    let store = ReplicatedMap::<String, Vec<u8>>::new_with_transport(
        Config::default().with_insecure_no_key(),
        transport,
    );
    assert!(store.try_insert("a".to_string(), vec![0; 10_000]).is_ok());
}

/// A value that encodes at or under the configured ceiling is accepted; one over it is rejected
/// with the actual encoded size and the configured ceiling, not placeholder values.
///
/// `#[tokio::test]`: the accepted `try_insert` below reaches `insert`'s broadcast, which needs an
/// ambient Tokio runtime (`insert`'s `# Panics`) — unlike a purely-rejected call, see
/// [`try_insert_rejection_needs_no_tokio_runtime_and_touches_no_local_state`].
#[tokio::test]
async fn try_insert_rejects_only_past_the_configured_ceiling() {
    let store = store_with_ceiling(8421, 8);

    // `Vec<u8>` encodes as a bincode varint length prefix (1 byte, for these small lengths) plus
    // the raw bytes: 3 bytes -> 4 encoded, comfortably under the 8-byte ceiling.
    assert_eq!(store.try_insert("a".to_string(), vec![0; 3]), Ok(None));

    let err = expect_too_large(
        store
            .try_insert("b".to_string(), vec![0; 100])
            .expect_err("100 raw bytes cannot fit an 8-byte ceiling"),
    );
    assert_eq!(err.max_value_size, 8);
    assert_eq!(err.encoded_size, 101); // 1-byte length prefix + 100 bytes
}

/// A `try_insert` rejection must not reach any local state or the broadcast path: calling it from
/// plain synchronous code (no ambient Tokio runtime) must not panic, unlike every write method
/// `insert`'s docs describe (`# Panics`) once a write actually broadcasts.
#[test]
fn try_insert_rejection_needs_no_tokio_runtime_and_touches_no_local_state() {
    let store = store_with_ceiling(8422, 4);
    let err = expect_too_large(
        store
            .try_insert("a".to_string(), vec![0; 100])
            .expect_err("100 raw bytes cannot fit a 4-byte ceiling"),
    );
    assert_eq!(err.max_value_size, 4);
    assert_eq!(store.get_cloned(&"a".to_string()), None);
}

/// `try_update`'s rejection leaves the previously stored value exactly as it was — the mutation
/// ran against a private clone that is discarded, never stored.
#[tokio::test]
async fn try_update_rejection_leaves_the_stored_value_untouched() {
    let store = store_with_ceiling(8423, 8);
    store.insert("a".to_string(), vec![0; 3]);

    let err = expect_too_large(
        store
            .try_update(&"a".to_string(), |v| v.extend(vec![0u8; 100]))
            .expect_err("growing to 100+ bytes cannot fit an 8-byte ceiling"),
    );
    assert_eq!(err.max_value_size, 8);
    assert_eq!(store.get_cloned(&"a".to_string()), Some(vec![0; 3]));
}

/// `try_update` on a key that fits accepts it and stores the mutated value, exactly like `update`.
#[tokio::test]
async fn try_update_accepts_a_mutation_within_the_ceiling() {
    let store = store_with_ceiling(8424, 8);
    store.insert("a".to_string(), vec![0; 3]);

    assert_eq!(store.try_update(&"a".to_string(), |v| v.push(0)), Ok(true));
    assert_eq!(store.get_cloned(&"a".to_string()), Some(vec![0; 4]));
}

/// `try_update` on an absent key reports `Ok(false)`, exactly like `update` — absence is not a
/// size rejection.
#[tokio::test]
async fn try_update_on_an_absent_key_reports_false_not_an_error() {
    let store = store_with_ceiling(8425, 8);
    assert_eq!(store.try_update(&"a".to_string(), |v| v.push(0)), Ok(false));
}

/// `ValueTooLarge`'s `Display` reports the actual encoded size and ceiling, not a generic
/// placeholder — asserted against an independently-written literal (not another call to the same
/// `fmt`), so a mutant collapsing the impl to an empty/default string cannot pass by agreeing with
/// itself.
#[test]
fn value_too_large_display_reports_the_actual_numbers() {
    let too_large = ValueTooLarge {
        encoded_size: 100,
        max_value_size: 8,
    };
    assert_eq!(
        too_large.to_string(),
        "value encodes to 100 bytes, exceeding Config::max_value_size (8 bytes)"
    );
}

/// `WriteRejected`'s `Display` delegates to whichever cause it wraps, rather than printing a
/// generic "rejected" placeholder that would hide which check actually failed.
#[test]
fn write_rejected_display_delegates_to_the_wrapped_cause() {
    let too_large = ValueTooLarge {
        encoded_size: 100,
        max_value_size: 8,
    };
    assert_eq!(
        WriteRejected::TooLarge(too_large).to_string(),
        "value encodes to 100 bytes, exceeding Config::max_value_size (8 bytes)"
    );

    let backpressure = Backpressure {
        in_flight: 4,
        max_in_flight: 4,
    };
    assert_eq!(
        WriteRejected::Backpressure(backpressure).to_string(),
        "write-broadcast egress budget exhausted: 4/4 broadcasts in flight"
    );
}

/// `WriteRejected::source()` exposes the wrapped cause through the standard `Error` trait, so a
/// caller matching on `dyn Error` (rather than `WriteRejected` directly) still reaches it.
#[test]
fn write_rejected_source_exposes_the_wrapped_cause() {
    let too_large = ValueTooLarge {
        encoded_size: 100,
        max_value_size: 8,
    };
    let err = WriteRejected::TooLarge(too_large);
    assert_eq!(
        err.source().unwrap().to_string(),
        "value encodes to 100 bytes, exceeding Config::max_value_size (8 bytes)"
    );

    let backpressure = Backpressure {
        in_flight: 4,
        max_in_flight: 4,
    };
    let err = WriteRejected::Backpressure(backpressure);
    assert_eq!(
        err.source().unwrap().to_string(),
        "write-broadcast egress budget exhausted: 4/4 broadcasts in flight"
    );
}

/// The size check rejects strictly *past* the ceiling, not at it: a value whose encoded size
/// exactly equals `max_value_size` must be accepted (`>`, not `>=`).
#[tokio::test]
async fn try_insert_accepts_a_value_whose_encoded_size_exactly_equals_the_ceiling() {
    // `Vec<u8>` encodes as a 1-byte length prefix + the raw bytes for these small lengths: 7 raw
    // bytes -> 8 encoded, exactly the ceiling below.
    let store = store_with_ceiling(8428, 8);
    assert_eq!(store.try_insert("a".to_string(), vec![0; 7]), Ok(None));

    // One byte more (8 raw -> 9 encoded) crosses the same ceiling and must reject.
    let err = expect_too_large(
        store
            .try_insert("b".to_string(), vec![0; 8])
            .expect_err("9 encoded bytes exceeds an 8-byte ceiling"),
    );
    assert_eq!(err.encoded_size, 9);
    assert_eq!(err.max_value_size, 8);
}

/// #82/#83 composition: `try_insert` checks the value's size *before* claiming a broadcast slot,
/// so a zero egress budget never masks a size rejection as `Backpressure` — the caller learns the
/// real reason even when both checks would otherwise fail.
#[test]
fn try_insert_checks_size_before_claiming_a_broadcast_slot() {
    let network = InMemoryNetwork::new();
    let transport = Arc::new(network.bind("127.0.0.1:8426".parse().unwrap()));
    let store = ReplicatedMap::<String, Vec<u8>>::new_with_transport(
        Config::default()
            .with_insecure_no_key()
            .with_max_value_size(4)
            .with_max_concurrent_broadcasts(0),
        transport,
    );

    let err = store
        .try_insert("a".to_string(), vec![0; 100])
        .expect_err("both checks would reject this, but size must win");
    assert!(
        matches!(err, WriteRejected::TooLarge(_)),
        "expected WriteRejected::TooLarge (checked first), got {err:?}"
    );
}

/// The same zero-budget store still reports `Backpressure` for a value that fits the size
/// ceiling — the size check does not spuriously reject a value it should accept. No ambient
/// Tokio runtime needed: `try_claim_broadcast_slot` fails before `Replica::try_insert` ever
/// reaches `just_insert`/`spawn_broadcast`.
#[test]
fn try_insert_reports_backpressure_when_size_is_fine_but_the_budget_is_not() {
    let network = InMemoryNetwork::new();
    let transport = Arc::new(network.bind("127.0.0.1:8427".parse().unwrap()));
    let store = ReplicatedMap::<String, Vec<u8>>::new_with_transport(
        Config::default()
            .with_insecure_no_key()
            .with_max_value_size(4096)
            .with_max_concurrent_broadcasts(0),
        transport,
    );

    let err = store
        .try_insert("a".to_string(), vec![0; 3])
        .expect_err("a zero broadcast budget must still reject the call");
    assert!(
        matches!(err, WriteRejected::Backpressure(_)),
        "expected WriteRejected::Backpressure, got {err:?}"
    );
}
