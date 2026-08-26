// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use rand::SeedableRng;

use super::*;

use rsos::{Fingerprint, FingerprintTreeMap};

/// A real `FingerprintTreeMap` over the given keys.
fn tree(keys: &[i32]) -> FingerprintTreeMap<i32, i32> {
    FingerprintTreeMap::from_iter(keys.iter().map(|&k| (k, 0)))
}

/// A fresh, deterministically seeded cut-offset RNG — every test below is seeded rather than
/// ambient, per `.claude/rules/tests.md`.
fn rng() -> StdRng {
    StdRng::seed_from_u64(0)
}

/// Run one crafted segment through `protocol_round`, via the trait rather than the tree.
fn round<B: RsosView<i32>>(
    store: &B,
    segment: RangeAggregate<i32>,
) -> (Vec<RangeAggregate<i32>>, Vec<EnumerationRange<i32>>) {
    let mut child_ranges = Vec::new();
    let mut enumeration_ranges = Vec::new();
    protocol_round(
        store,
        vec![segment],
        &mut child_ranges,
        &mut enumeration_ranges,
        &mut rng(),
    );
    (child_ranges, enumeration_ranges)
}

/// A `RangeAggregate` over `(−∞, +∞)` whose aggregate reports `m` elements with a fixed,
/// never-matching fingerprint — every SPLIT test below drives its fan-out from this shape.
fn splitting_segment(m: usize) -> RangeAggregate<i32> {
    RangeAggregate {
        range: KeyRange::new(StartBound::Unbounded, EndBound::Unbounded),
        aggregate: Aggregate::new(m, Fingerprint([7, 0, 0, 0])),
    }
}

mod convergence;
mod emptiness;
mod fan_out;
mod malformed;
mod outcome;
mod partition;
