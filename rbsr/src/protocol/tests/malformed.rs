// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::ops::RangeBounds;

use rsos::Fingerprint;

use super::*;

// ----- Malformed wire segments -----
// Bad bound *shapes* are unrepresentable (`StartBound`/`EndBound`); only inversion is left.

/// An inverted range must be dropped, not answered: it would underflow and then `select` out
/// of bounds.
#[test]
fn inverted_range_is_dropped_not_panicking() {
    let store = tree(&[10, 20, 30]);
    let segment = RangeAggregate {
        range: KeyRange::new(StartBound::Included(100), EndBound::Excluded(5)),
        aggregate: Aggregate::new(1, Fingerprint([1, 0, 0, 0])),
    };
    let (child_ranges, enumeration_ranges) = round(&store, segment);
    assert!(child_ranges.is_empty());
    assert!(enumeration_ranges.is_empty());
}

// ----- Contract-violating backends -----

/// Breaks [`RsosView`]'s rank-within-store law: `rank` is the key's own magnitude, unbounded
/// by `size()`.
///
/// Hand-written rather than blanket-derived — the third-party shape the crate root advertises
/// (remote, lazy, cached), and the one the blanket impl does not cover. `select` indexes a
/// `Vec`, so it panics out of bounds; that is the trap the driver must not spring.
struct UnclampedRank {
    keys: Vec<i32>,
}

impl RsosView<i32> for UnclampedRank {
    fn size(&self) -> usize {
        self.keys.len()
    }

    fn aggregate<R: RangeBounds<i32>>(&self, _range: R) -> Aggregate {
        // Never equal to what the peer advertises below, so the driver reaches SPLIT.
        Aggregate::new(self.keys.len(), Fingerprint([7, 0, 0, 0]))
    }

    fn rank(&self, z: &i32) -> usize {
        *z as usize
    }

    fn select(&self, r: usize) -> &i32 {
        &self.keys[r]
    }
}

/// Worked example behind `no_backend_answer_can_drive_the_protocol_out_of_bounds` (the property
/// oracle). Returning at all is the assertion; `!is_empty()` keeps it failing if the bound is
/// ever "fixed" by dropping such segments instead, which is a different bug, not a milder one.
#[test]
fn backend_with_unclamped_rank_is_defended_against_not_trusted() {
    let store = UnclampedRank {
        keys: vec![10, 20, 30],
    };
    let segment = RangeAggregate {
        // rank(1000) = 1000 against size() = 3. Unclamped, the fan-out reaches `select(3)`.
        range: KeyRange::new(StartBound::Unbounded, EndBound::Excluded(1000)),
        aggregate: Aggregate::new(1, Fingerprint([1, 0, 0, 0])),
    };
    let (child_ranges, _) = round(&store, segment);
    assert!(!child_ranges.is_empty());
}

/// The guard must not reject legitimate segments: a well-formed range still produces the
/// normal output (here: an empty peer range, so our whole tree is reported as a difference).
#[test]
fn wellformed_segment_still_processed() {
    let store = tree(&[10, 20, 30]);
    let segment = RangeAggregate {
        range: KeyRange::new(StartBound::Unbounded, EndBound::Unbounded),
        aggregate: Aggregate::ZERO,
    };
    let (_child_ranges, enumeration_ranges) = round(&store, segment);
    assert_eq!(
        enumeration_ranges,
        vec![(Bound::Unbounded, Bound::Unbounded)]
    );
}
