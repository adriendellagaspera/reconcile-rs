// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::fingerprint::LiftKey;

use super::super::{FingerprintTreeMap, MAX_CAPACITY};

/// The most items a subtree of `height` levels can hold, reimplemented independently of
/// `bulk.rs`'s own (private, so unreachable from here) `Capacities` -- an oracle, not a call into
/// the code under test, so it actually pins the fanout arithmetic rather than checking the
/// implementation agrees with itself.
fn max_capacity(height: usize) -> usize {
    let mut cap = 0usize;
    for _ in 0..height {
        cap = MAX_CAPACITY + (MAX_CAPACITY + 1) * cap;
    }
    cap
}

/// The smallest height whose [`max_capacity`] covers `n` -- `1` for `n == 0` too: an empty tree
/// is still a single (empty) leaf, matching [`tree_height`] and `check_invariants`'s own height
/// count, both of which start a leaf at `1` regardless of key count.
fn min_height_for(n: usize) -> usize {
    let mut height = 1;
    while max_capacity(height) < n {
        height += 1;
    }
    height
}

/// `1` for a single leaf, counting levels down the leftmost spine -- independent of, and the
/// same walk `check_invariants` itself cross-checks against a recursive count.
fn tree_height<K, V>(tree: &FingerprintTreeMap<K, V>) -> usize {
    let mut node = tree.root.as_ref();
    let mut height = 1;
    while let Some(children) = node.children.as_ref() {
        height += 1;
        node = &children[0];
    }
    height
}

/// Every size from 0 up through several tree heights (height 2 starts at 12 items, height 3 at
/// 144, height 4 at 1728 -- `B = 6`, `MAX_CAPACITY = 11`): a bulk-built tree must independently
/// pass [`FingerprintTreeMap::check_invariants`], agree, element for element and in aggregate,
/// with a tree built by `n` individual [`insert`](FingerprintTreeMap::insert) calls -- exactly
/// #51's acceptance criterion ("identical resulting fingerprints") -- and reach the minimum
/// possible height for `n` (dense packing is the whole point of a bottom-up build, not just a
/// nice-to-have; without this check a fanout-arithmetic bug can produce a *valid* tree that is
/// needlessly tall and nothing here would notice). Checked at every size rather than a handful of
/// samples so a capacity-planning off-by-one at a height boundary cannot hide.
#[test]
fn matches_serial_insert_at_every_size_through_several_tree_heights() {
    for n in 0..2000u32 {
        let items: Vec<(u32, u32)> = (0..n).map(|k| (k, k.wrapping_mul(31) ^ 0x5bd1)).collect();

        let bulk = FingerprintTreeMap::from_sorted_iter(items.iter().copied());
        bulk.check_invariants();

        let mut serial = FingerprintTreeMap::new();
        for &(k, v) in &items {
            serial.insert(k, v);
        }
        serial.check_invariants();

        assert_eq!(bulk.len(), items.len());
        assert_eq!(bulk.aggregate(..), serial.aggregate(..), "n = {n}");
        assert_eq!(
            bulk.iter().map(|(&k, &v)| (k, v)).collect::<Vec<_>>(),
            items,
            "n = {n}"
        );
        assert_eq!(tree_height(&bulk), min_height_for(n as usize), "n = {n}");
    }
}

/// Spot-checks well past the small-size sweep above, at the bulk-build benchmark scales
/// (`benches/README.md`'s "Re-measuring #47/#51/#52").
#[test]
fn matches_serial_insert_at_benchmark_scale() {
    for &n in &[50_000u32, 200_000] {
        let items: Vec<(u32, u32)> = (0..n).map(|k| (k, k.wrapping_mul(2654435761))).collect();

        let bulk = FingerprintTreeMap::from_sorted_iter(items.iter().copied());
        bulk.check_invariants();

        let mut serial = FingerprintTreeMap::new();
        for &(k, v) in &items {
            serial.insert(k, v);
        }

        assert_eq!(bulk.len(), n as usize);
        assert_eq!(bulk.aggregate(..), serial.aggregate(..), "n = {n}");
        assert_eq!(tree_height(&bulk), min_height_for(n as usize), "n = {n}");
    }
}

#[test]
fn from_sorted_iter_keyed_matches_with_lift_key_insert() {
    let lift_key = LiftKey::new([7; 32]);
    let items: Vec<(u32, u32)> = (0..500).map(|k| (k, k * 3)).collect();

    let bulk =
        FingerprintTreeMap::from_sorted_iter_keyed(items.iter().copied(), LiftKey::new([7; 32]));
    bulk.check_invariants();

    let mut serial = FingerprintTreeMap::with_lift_key(lift_key);
    for &(k, v) in &items {
        serial.insert(k, v);
    }

    assert_eq!(bulk.aggregate(..), serial.aggregate(..));

    // A different key lifts the same elements to a different aggregate -- confirms the keyed
    // build actually used `lift_key`, not the unkeyed lift.
    let unkeyed = FingerprintTreeMap::from_sorted_iter(items.iter().copied());
    assert_ne!(bulk.aggregate(..), unkeyed.aggregate(..));
}

#[test]
#[should_panic(expected = "strictly increasing")]
fn from_sorted_iter_rejects_out_of_order_input() {
    let _ = FingerprintTreeMap::from_sorted_iter([(2, "a"), (1, "b")]);
}

#[test]
#[should_panic(expected = "strictly increasing")]
fn from_sorted_iter_rejects_duplicate_keys() {
    let _ = FingerprintTreeMap::from_sorted_iter([(1, "a"), (1, "b")]);
}

#[test]
fn from_sorted_iter_on_empty_input_is_the_empty_tree() {
    let tree: FingerprintTreeMap<i32, i32> = FingerprintTreeMap::from_sorted_iter([]);
    tree.check_invariants();
    assert!(tree.is_empty());
    assert_eq!(tree.len(), 0);
}
