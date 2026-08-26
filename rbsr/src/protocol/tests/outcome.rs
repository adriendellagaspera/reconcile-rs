// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use rsos::Fingerprint;

use super::*;

/// The tally must add up and must attribute the malformed range, not swallow it.
#[test]
fn round_outcome_accounts_for_every_segment() {
    let store = tree(&[10, 20, 30, 40, 50]);
    let mut child_ranges = Vec::new();
    let mut enumeration_ranges = Vec::new();
    let outcome = protocol_round(
        &store,
        vec![
            RangeAggregate {
                range: KeyRange::new(StartBound::Unbounded, EndBound::Unbounded),
                aggregate: store.aggregate(..),
            },
            RangeAggregate {
                range: KeyRange::new(StartBound::Unbounded, EndBound::Unbounded),
                aggregate: Aggregate::ZERO,
            },
            RangeAggregate {
                range: KeyRange::new(StartBound::Unbounded, EndBound::Unbounded),
                aggregate: Aggregate::new(5, Fingerprint([7, 0, 0, 0])),
            },
            RangeAggregate {
                range: KeyRange::new(StartBound::Included(100), EndBound::Excluded(5)),
                aggregate: Aggregate::new(1, Fingerprint([1, 0, 0, 0])),
            },
        ],
        &mut child_ranges,
        &mut enumeration_ranges,
        &mut rng(),
    );
    assert_eq!(outcome.skipped(), 1);
    assert_eq!(outcome.enumerated(), 1);
    assert_eq!(outcome.split(), 1);
    assert_eq!(outcome.dropped_malformed(), 1);
    assert_eq!(outcome.children(), child_ranges.len());
    assert_eq!(enumeration_ranges.len(), outcome.enumerated());
}

/// `RoundOutcome` is otherwise only ever constructed fresh by `protocol_round` (never combined
/// with `+=` internally), so its `AddAssign` — accumulating a whole reconciliation across rounds
/// — needs its own direct witness, not just the fresh-construction totals above.
#[test]
fn add_assign_sums_every_field() {
    let mut a = RoundOutcome {
        skipped: 1,
        enumerated: 2,
        split: 3,
        children: 4,
        dropped_malformed: 5,
    };
    let b = RoundOutcome {
        skipped: 10,
        enumerated: 20,
        split: 30,
        children: 40,
        dropped_malformed: 50,
    };
    a += b;
    assert_eq!(a.skipped(), 11);
    assert_eq!(a.enumerated(), 22);
    assert_eq!(a.split(), 33);
    assert_eq!(a.children(), 44);
    assert_eq!(a.dropped_malformed(), 55);
}
