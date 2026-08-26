// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::policy::SqrtFanOut;

use super::*;

// ----- The fan-out rule: this crate's communication cost, pinned -----

/// The default fan-out is a constant `b` whatever the range's size.
#[test]
fn default_split_fan_out_is_constant_at_sixteen() {
    for m in [100usize, 400, 2_500, 250_000] {
        let store = tree(&(0..m as i32).collect::<Vec<_>>());
        let (child_ranges, enumeration_ranges) = round(&store, splitting_segment(m));
        assert!(enumeration_ranges.is_empty());
        assert!(
            child_ranges.len() <= FanOut::NEGENTROPY.get(),
            "m={m}: SPLIT emitted {} children, expected at most b={} \
             (a size-dependent fan-out would grow with m)",
            child_ranges.len(),
            FanOut::NEGENTROPY.get()
        );
        assert!(child_ranges.len() > 1, "m={m}: the split must refine");
    }
}

/// `SqrtFanOut` is public API, so its cut positions are a contract.
#[test]
fn sqrt_fan_out_is_still_the_square_root_of_the_range_size() {
    for m in [100usize, 400, 2_500] {
        let store = tree(&(0..m as i32).collect::<Vec<_>>());
        let mut child_ranges = Vec::new();
        let mut enumeration_ranges = Vec::new();
        protocol_round_with_policy(
            &store,
            &SqrtFanOut,
            vec![splitting_segment(m)],
            &mut child_ranges,
            &mut enumeration_ranges,
            &mut rng(),
        );
        assert!(enumeration_ranges.is_empty());
        let root = (m as f64).sqrt() as usize;
        assert!(
            child_ranges.len() >= root / 2 && child_ranges.len() <= root * 2,
            "m={m}: SPLIT emitted {} children, expected ~√m = {root}",
            child_ranges.len()
        );
    }
}
