// Copyright 2026 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use super::*;

#[test]
fn mean_of_uniform_values() {
    assert_eq!(mean(&[2.0, 4.0, 6.0]), 4.0);
}

#[test]
fn quantile_picks_the_nearest_rank() {
    let sorted = [1.0, 2.0, 3.0, 4.0, 5.0];
    assert_eq!(quantile(&sorted, 0.0), 1.0);
    assert_eq!(quantile(&sorted, 0.5), 3.0);
    assert_eq!(quantile(&sorted, 1.0), 5.0);
}

// A constant sample's bootstrap distribution is a point mass on that constant: every resample
// draws only that value, whatever the RNG does, so these assertions hold regardless of seed.

#[test]
fn summarize_of_a_constant_sample_has_a_degenerate_interval() {
    let summary = summarize(&[5.0, 5.0, 5.0, 5.0]);
    assert_eq!(summary.mean, 5.0);
    assert_eq!(summary.median, 5.0);
    assert_eq!(summary.lo, 5.0);
    assert_eq!(summary.hi, 5.0);
}

#[test]
#[should_panic(expected = "cannot summarize an empty sample")]
fn summarize_panics_on_an_empty_sample() {
    summarize(&[]);
}

#[test]
fn diff_ci_of_two_constant_samples_is_their_exact_difference() {
    let summary = diff_ci(&[10.0, 10.0], &[3.0, 3.0]);
    assert_eq!(summary.mean, 7.0);
    assert_eq!(summary.lo, 7.0);
    assert_eq!(summary.hi, 7.0);
}

#[test]
#[should_panic(expected = "cannot difference an empty sample")]
fn diff_ci_panics_on_an_empty_sample() {
    diff_ci(&[], &[1.0]);
}

#[test]
fn excludes_zero_reads_the_interval_boundaries() {
    assert!(excludes_zero(&Summary {
        mean: 1.5,
        median: 1.5,
        lo: 1.0,
        hi: 2.0
    }));
    assert!(excludes_zero(&Summary {
        mean: -1.5,
        median: -1.5,
        lo: -2.0,
        hi: -1.0
    }));
    assert!(!excludes_zero(&Summary {
        mean: 0.0,
        median: 0.0,
        lo: -1.0,
        hi: 1.0
    }));
}

#[test]
fn disjoint_from_is_symmetric() {
    let low = Summary {
        mean: 0.5,
        median: 0.5,
        lo: 0.0,
        hi: 1.0,
    };
    let high = Summary {
        mean: 2.5,
        median: 2.5,
        lo: 2.0,
        hi: 3.0,
    };
    let overlapping = Summary {
        mean: 0.75,
        median: 0.75,
        lo: 0.5,
        hi: 1.5,
    };
    assert!(low.disjoint_from(&high));
    assert!(high.disjoint_from(&low));
    assert!(!low.disjoint_from(&overlapping));
}
