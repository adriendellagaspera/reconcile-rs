// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use rand::SeedableRng;

use super::*;

/// `child_ranges` are consecutive, disjoint, and their union is `(−∞, +∞)` — `ARCHITECTURE.md` §5
/// invariant 10, factored out so both the plain and the shifted-cut tests below assert it the same
/// way.
fn assert_partitions_the_unbounded_parent(child_ranges: &[RangeAggregate<i32>]) {
    assert!(child_ranges.len() > 1);

    let first = &child_ranges[0].range;
    assert_eq!(first.0, StartBound::Unbounded, "partition must start at −∞");
    let last = &child_ranges[child_ranges.len() - 1].range;
    assert_eq!(last.1, EndBound::Unbounded, "partition must end at +∞");

    for pair in child_ranges.windows(2) {
        let (left, right) = (&pair[0].range, &pair[1].range);
        match (&left.1, &right.0) {
            (EndBound::Excluded(end), StartBound::Included(start)) => assert_eq!(end, start),
            other => panic!("children are not adjacent: {other:?}"),
        }
    }
}

/// `ARCHITECTURE.md` §5 invariant 10, under any policy.
#[test]
fn split_children_partition_the_parent_range() {
    let store = tree(&(0..400).collect::<Vec<_>>());
    let (child_ranges, _) = round(&store, splitting_segment(400));
    assert_partitions_the_unbounded_parent(&child_ranges);
}

/// `ARCHITECTURE.md` §5 invariant 10, re-asserted under the session-random cut offset: the
/// partition holds for every draw, not only the seed `round()` fixes. `397` (not `400`) so the
/// default stride (25 at `b=16`) does not divide the span evenly — otherwise there is no
/// undersized block for the shift to move and every draw would coincide by construction.
#[test]
fn split_children_partition_the_parent_range_under_every_shift() {
    let store = tree(&(0..397).collect::<Vec<_>>());
    for seed in 0..64u64 {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut child_ranges = Vec::new();
        let mut enumeration_ranges = Vec::new();
        protocol_round(
            &store,
            vec![splitting_segment(397)],
            &mut child_ranges,
            &mut enumeration_ranges,
            &mut rng,
        );
        assert_partitions_the_unbounded_parent(&child_ranges);
    }
}

/// `ARCHITECTURE.md` §5 invariant 13 "by construction": every child a shifted SPLIT emits is
/// strictly narrower than the parent, for every draw — the shift only moves which block is
/// undersized, it never produces the single-child identity split
/// [`Decision::Split`]'s docs reserve for `span() <= 1`, nor an empty one.
#[test]
fn shifted_split_never_emits_an_empty_or_non_narrowing_child() {
    let store = tree(&(0..397).collect::<Vec<_>>());
    for seed in 0..64u64 {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut child_ranges = Vec::new();
        let mut enumeration_ranges = Vec::new();
        protocol_round(
            &store,
            vec![splitting_segment(397)],
            &mut child_ranges,
            &mut enumeration_ranges,
            &mut rng,
        );
        assert!(child_ranges.len() > 1, "seed {seed}: must narrow");
        for child in &child_ranges {
            assert_ne!(
                store.aggregate(child.range.clone()).size(),
                0,
                "seed {seed}: a shifted cut produced an empty child"
            );
        }
    }
}

/// The whole point of a session-random offset: two sessions (seeds) over the identical store draw
/// different cut positions below the outer range. A regression that hardcodes the shift (e.g. back
/// to `0`) would make every seed agree, which this test would catch and
/// `split_children_partition_the_parent_range_under_every_shift` above would not, since a fixed,
/// unshifted cut still partitions correctly.
#[test]
fn different_seeds_draw_different_cut_positions() {
    let store = tree(&(0..397).collect::<Vec<_>>());
    let first_child_end = |seed: u64| -> i32 {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut child_ranges = Vec::new();
        let mut enumeration_ranges = Vec::new();
        protocol_round(
            &store,
            vec![splitting_segment(397)],
            &mut child_ranges,
            &mut enumeration_ranges,
            &mut rng,
        );
        match &child_ranges[0].range.1 {
            EndBound::Excluded(key) => *key,
            EndBound::Unbounded => panic!("397 elements over stride 25 must produce a real cut"),
        }
    };
    let cuts: std::collections::HashSet<i32> = (0..16u64).map(first_child_end).collect();
    assert!(
        cuts.len() > 1,
        "16 seeds drew {} distinct first-child boundaries out of 16 possible offsets, expected \
         more than one — the shift looks hardcoded",
        cuts.len()
    );
}
