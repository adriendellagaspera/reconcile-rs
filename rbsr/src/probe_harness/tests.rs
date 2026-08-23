// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::collections::{HashSet, VecDeque};
use std::ops::Bound;

use rand::SeedableRng;
use rsos::{Aggregate, Fingerprint};

use super::*;
use crate::FixedFanOut;

#[test]
fn mask_returns_the_low_width_bits_set() {
    assert_eq!(mask(1), 1);
    assert_eq!(mask(4), 0xF);
    assert_eq!(mask(16), 0xFFFF);
    assert_eq!(mask(64), u64::MAX);
}

#[test]
fn span_start_bound_included_vs_excluded_at_an_exact_key() {
    let store = NarrowStore::new(16, vec![10, 20, 30]);
    // Included(20): 20 itself is in range, so the span starts at its own index.
    assert_eq!(store.span(&(Bound::Included(20), Bound::Unbounded)), (1, 3));
    // Excluded(20): 20 itself is not in range, so the span starts just past it.
    assert_eq!(store.span(&(Bound::Excluded(20), Bound::Unbounded)), (2, 3));
}

#[test]
fn span_end_bound_included_vs_excluded_at_an_exact_key() {
    let store = NarrowStore::new(16, vec![10, 20, 30]);
    // Included(20): 20 itself is in range, so the span ends just past it.
    assert_eq!(store.span(&(Bound::Unbounded, Bound::Included(20))), (0, 2));
    // Excluded(20): 20 itself is not in range, so the span ends at its own index.
    assert_eq!(store.span(&(Bound::Unbounded, Bound::Excluded(20))), (0, 1));
}

#[test]
fn keys_in_returns_exactly_the_enumerated_keys() {
    let a = NarrowStore::new(16, vec![1, 2, 3, 4, 5]);
    let b = NarrowStore::new(16, vec![1, 2, 3]); // 4 and 5 are the difference
    let result = drive(&a, &b, &FixedFanOut::default());
    assert_eq!(result.termination, Termination::Settled);

    let found: HashSet<u64> = result
        .enumerated
        .iter()
        .flat_map(|r| a.keys_in(r).into_iter().chain(b.keys_in(r)))
        .collect();
    assert!(found.contains(&4), "differing key 4 must be enumerated");
    assert!(found.contains(&5), "differing key 5 must be enumerated");
    // Nothing outside the true difference should surface.
    assert!(!found.contains(&1));
}

#[test]
fn aggregate_distinguishes_same_size_different_content() {
    let a = NarrowStore::new(16, vec![1, 2, 3]);
    let b = NarrowStore::new(16, vec![1, 2, 4]);
    let agg_a = a.aggregate(..);
    let agg_b = b.aggregate(..);
    assert_eq!(agg_a.size(), agg_b.size());
    assert_ne!(agg_a.fingerprint(), agg_b.fingerprint());
}

#[test]
fn balanced_swap_preserves_universe_size_and_swaps_the_requested_count() {
    let mut rng = StdRng::seed_from_u64(7);
    let (a, b) = balanced_swap(&mut rng, 50, 3);
    assert_eq!(a.len(), 50);
    assert_eq!(b.len(), a.len());

    let a_set: HashSet<u64> = a.iter().copied().collect();
    let b_set: HashSet<u64> = b.iter().copied().collect();
    assert_eq!(
        b_set.difference(&a_set).count(),
        3,
        "swap_size elements must genuinely be new"
    );
}

/// A scripted "random" source for [`unique_candidate`], so its rejection loop can be exercised
/// deterministically instead of waiting on an astronomically unlikely real collision.
struct ScriptedRng(VecDeque<u64>);

impl rand::RngCore for ScriptedRng {
    fn next_u32(&mut self) -> u32 {
        self.next_u64() as u32
    }

    fn next_u64(&mut self) -> u64 {
        self.0.pop_front().expect("script exhausted")
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        for chunk in dest.chunks_mut(8) {
            let bytes = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
    }

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rand::Error> {
        self.fill_bytes(dest);
        Ok(())
    }
}

#[test]
fn unique_candidate_rejects_values_already_in_either_set() {
    let a = [1u64, 2, 3];
    let b = [4u64, 5, 6];
    // First draw collides with `a`, second with `b`, third is genuinely fresh.
    let mut rng = ScriptedRng(VecDeque::from(vec![2u64, 5u64, 42u64]));
    assert_eq!(unique_candidate(&mut rng, &a, &b), 42);
}

#[test]
fn round_comparisons_sums_every_outcome() {
    assert_eq!(round_comparisons(1, 2, 3, 4), 10);
    assert_eq!(round_comparisons(0, 0, 0, 0), 0);
}

#[test]
fn cycle_length_is_the_gap_between_the_two_occurrences() {
    assert_eq!(cycle_length(5, 2), 3);
    assert_eq!(cycle_length(1, 0), 1);
}

fn segment(end: u64, size: usize, limb: u64) -> RangeAggregate<u64> {
    RangeAggregate::new(
        None,
        Some(end),
        Aggregate::new(size, Fingerprint([limb, 0, 0, 0])),
    )
}

#[test]
fn state_hash_distinguishes_states_that_differ() {
    let a = [segment(10, 3, 1)];
    let b = [segment(10, 3, 2)]; // same bounds/size, different fingerprint limb
    let c = [segment(10, 3, 1), segment(20, 1, 5)]; // an extra segment
    assert_ne!(state_hash(&a), state_hash(&b));
    assert_ne!(state_hash(&a), state_hash(&c));
    assert_eq!(
        state_hash(&a),
        state_hash(&[segment(10, 3, 1)]),
        "must be deterministic"
    );
}

#[test]
fn state_recurred_is_exact_equality() {
    let a = vec![segment(10, 3, 1)];
    let same = vec![segment(10, 3, 1)];
    let different = vec![segment(10, 3, 2)];
    assert!(state_recurred(&a, &same));
    assert!(!state_recurred(&a, &different));
}

#[test]
fn drive_reports_the_rounds_it_ran() {
    let a = NarrowStore::new(16, vec![1, 2, 3, 4, 5]);
    let b = NarrowStore::new(16, vec![1, 2, 3]);
    let result = drive(&a, &b, &FixedFanOut::default());
    assert!(
        result.rounds >= 1,
        "a genuine difference must take at least one round to settle"
    );
    assert!(
        result.comparisons >= 1,
        "at least one range must have been classified"
    );
}

#[test]
fn masked_keeps_only_the_low_width_bits() {
    assert_eq!(masked(0xFF, 4), 0xF);
    assert_eq!(masked(0x1_0000, 16), 0);
    assert_eq!(masked(0xABCD, 64), 0xABCD);
}

#[test]
fn other_responder_alternates() {
    assert!(!other_responder(true));
    assert!(other_responder(false));
}
