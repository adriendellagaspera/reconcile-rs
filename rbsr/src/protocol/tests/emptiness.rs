// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use rsos::Fingerprint;

use super::*;

// ----- Emptiness and equality decided on `size`, never the fingerprint alone -----

/// A non-empty peer range fingerprinting to `ZERO` against our empty tree: same
/// fingerprint, different size. Must be bounced back, not concluded in sync.
#[test]
fn nonempty_zero_fingerprint_vs_empty_is_not_in_sync() {
    let store = tree(&[]); // empty: local fingerprint == ZERO, local size == 0
    let segment = RangeAggregate {
        range: KeyRange::new(StartBound::Unbounded, EndBound::Unbounded),
        aggregate: Aggregate::new(2, Fingerprint::ZERO),
    };
    let (child_ranges, enumeration_ranges) = round(&store, segment);
    assert!(enumeration_ranges.is_empty());
    assert_eq!(child_ranges.len(), 1);
    assert_eq!(
        child_ranges[0],
        RangeAggregate {
            range: KeyRange::new(StartBound::Unbounded, EndBound::Unbounded),
            aggregate: Aggregate::ZERO,
        }
    );
}

/// A genuinely identical range must still be concluded in sync.
#[test]
fn matching_fingerprint_and_size_is_in_sync() {
    let store = tree(&[10, 20, 30]);
    let segment = RangeAggregate {
        range: KeyRange::new(StartBound::Unbounded, EndBound::Unbounded),
        aggregate: store.aggregate(..),
    };
    let (child_ranges, enumeration_ranges) = round(&store, segment);
    assert!(child_ranges.is_empty());
    assert!(enumeration_ranges.is_empty());
}

/// Matching fingerprints with mismatched sizes must refine, not conclude in sync.
#[test]
fn matching_fingerprint_but_wrong_size_is_refined() {
    let store = tree(&[10, 20, 30, 40, 50]);
    let segment = RangeAggregate {
        range: KeyRange::new(StartBound::Unbounded, EndBound::Unbounded),
        aggregate: Aggregate::new(store.len() + 7, store.aggregate(..).fingerprint()),
    };
    let (child_ranges, enumeration_ranges) = round(&store, segment);
    assert!(!child_ranges.is_empty());
    assert!(enumeration_ranges.is_empty());
}
