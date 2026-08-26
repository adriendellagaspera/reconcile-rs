// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use rand::SeedableRng;
use rsos::Fingerprint;

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

/// `actual_span` is `end_index.get() - start_index.get()`, never assumed to start at rank `0` --
/// every other test in this file segments the *whole* store, where `start_index == 0` makes that
/// subtraction arithmetically indistinguishable from addition. This segment starts at rank `50`,
/// where the two diverge sharply (`397 - 50 = 347` vs. `397 + 50 = 447`, past the end of the
/// store), so a corrupted `actual_span` computation shows up in the wrong place: either a
/// mis-sized block (`this_stride` computed from the wrong span) or a short-block position drawn
/// from a range wider than the segment actually has room for.
#[test]
fn split_of_a_range_starting_past_rank_zero_still_partitions_correctly() {
    const START: usize = 50;
    const REAL_SPAN: usize = 397 - START;
    const STRIDE: usize = 22; // ceil(347 / 16)
    const REMAINDER: usize = REAL_SPAN % STRIDE;
    let store = tree(&(0..397).collect::<Vec<_>>());
    for seed in 0..16u64 {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut child_ranges = Vec::new();
        let mut enumeration_ranges = Vec::new();
        let segment = RangeAggregate {
            range: KeyRange::new(StartBound::Included(START as i32), EndBound::Unbounded),
            aggregate: Aggregate::new(REAL_SPAN, Fingerprint([7, 0, 0, 0])),
        };
        protocol_round(
            &store,
            vec![segment],
            &mut child_ranges,
            &mut enumeration_ranges,
            &mut rng,
        );
        assert!(
            !child_ranges.is_empty(),
            "seed {seed}: must produce children"
        );
        assert_eq!(
            child_ranges[0].range.0,
            StartBound::Included(START as i32),
            "seed {seed}: partition must start where the segment did"
        );
        let last = &child_ranges[child_ranges.len() - 1];
        assert_eq!(
            last.range.1,
            EndBound::Unbounded,
            "seed {seed}: partition must end where the segment did"
        );
        let sizes: Vec<usize> = child_ranges
            .iter()
            .map(|child| store.aggregate(child.range.clone()).size())
            .collect();
        assert!(
            sizes
                .iter()
                .all(|&size| size == STRIDE || size == REMAINDER),
            "seed {seed}: every child must be stride- or remainder-sized, sizes were {sizes:?}"
        );
        assert_eq!(
            sizes.iter().sum::<usize>(),
            REAL_SPAN,
            "seed {seed}: sizes must sum to the segment's real span, sizes were {sizes:?}"
        );
    }
}

/// `block_count`'s exact arithmetic, pinned directly rather than only exercised through the
/// fan-out loop, where a wrong-but-in-range block count is easy for a property test on the
/// resulting partition to miss (any block count still partitions the parent correctly).
#[test]
fn block_count_is_the_ceiling_of_actual_span_over_stride() {
    assert_eq!(
        block_count(397, 25),
        16,
        "397 = 25*15 + 22: 15 full-stride blocks plus one partial"
    );
    assert_eq!(
        block_count(400, 25),
        16,
        "400 = 25*16 exactly: divides evenly, no partial block needed"
    );
    assert_eq!(
        block_count(1, 25),
        1,
        "a span narrower than one stride is still one (undersized) block"
    );
    assert_eq!(block_count(0, 25), 0, "an empty span is zero blocks");
}

/// `ARCHITECTURE.md` §5 invariant 10's sizing half, direct rather than structural: every shifted
/// SPLIT child is exactly `stride`-sized except one `remainder`-sized block, for every draw — not
/// just "some partition of the parent, however sized" (which
/// `split_children_partition_the_parent_range_under_every_shift` already covers, and a `block`
/// counter that never advances could still satisfy by accident: it would just reproduce the old
/// always-last placement for every seed, which still partitions correctly).
#[test]
fn exactly_one_child_is_remainder_sized_the_rest_are_stride_sized() {
    const STRIDE: usize = 25;
    const REMAINDER: usize = 397 % STRIDE;
    let store = tree(&(0..397).collect::<Vec<_>>());
    let mut remainder_block_positions = std::collections::HashSet::new();
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
        let sizes: Vec<usize> = child_ranges
            .iter()
            .map(|child| store.aggregate(child.range.clone()).size())
            .collect();
        assert!(
            sizes
                .iter()
                .all(|&size| size == STRIDE || size == REMAINDER),
            "seed {seed}: every child must be stride- or remainder-sized, sizes were {sizes:?}"
        );
        let remainder_positions: Vec<usize> = sizes
            .iter()
            .enumerate()
            .filter(|(_, &size)| size == REMAINDER)
            .map(|(index, _)| index)
            .collect();
        assert_eq!(
            remainder_positions.len(),
            1,
            "seed {seed}: expected exactly one {REMAINDER}-sized block, sizes were {sizes:?}"
        );
        remainder_block_positions.insert(remainder_positions[0]);
    }
    // A `block` counter that never advances always places the remainder-sized block last
    // (reproducing the pre-shift placement for every seed but one, see the doc comment above);
    // real advancement puts it in the middle at least once across 64 draws.
    assert!(
        remainder_block_positions
            .iter()
            .any(|&position| position != 15),
        "the remainder-sized block was always at the last position (15) across every seed — \
         the shift looks like it never actually moves the block"
    );
}
