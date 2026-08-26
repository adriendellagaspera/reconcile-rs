// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! [`FingerprintTreeMap::from_sorted_iter`]/[`from_sorted_iter_keyed`]: a one-pass, bottom-up
//! bulk build from already-sorted, duplicate-free input -- the amortized alternative to `n`
//! individual [`insert`](super::FingerprintTreeMap::insert) calls (#51). Every node's
//! `fingerprints` and `subtree` cache is composed directly from the elements and children it is
//! built with, once, rather than incrementally maintained across `n` splits.
//!
//! The tree this produces can differ in *shape* from one built via serial `insert` (this module
//! favors dense, close-to-[`MAX_CAPACITY`] nodes at every level; `insert`'s incremental splitting
//! does not), but [`Aggregate`](crate::aggregate::Aggregate) is a commutative monoid over the
//! element set (`aggregate.rs`'s own `add_is_commutative_and_associative` pins this) -- so the two
//! builds' root aggregates always agree regardless of shape, which is exactly what a peer compares
//! over the wire.

use std::sync::Arc;

use arrayvec::ArrayVec;
use serde::Serialize;

use crate::fingerprint::{lift_with, LiftKey};

use super::node::{Children, Node};
use super::{FingerprintTreeMap, MAX_CAPACITY, MIN_CAPACITY};

/// Precomputed `min`/`max` element counts a subtree can hold at each height, up to the height
/// this build needs. `max[h]`/`min[h]`: the most/least items a subtree of `h` levels can hold --
/// `min` only meaningful for a **non-root** subtree, the root has no lower bound.
///
/// `max[0] == min[0] == 0` (an absent, height-0 child) is the base case both recurrences share,
/// which is why [`build_level`] does not special-case a leaf's "no children" contribution.
struct Capacities {
    max: Vec<usize>,
    min: Vec<usize>,
}

impl Capacities {
    /// The capacity table up to the smallest height whose `max` covers `n`, and that height.
    fn for_size(n: usize) -> (Self, usize) {
        let mut max = vec![0usize];
        let mut min = vec![0usize];
        loop {
            let h = max.len();
            max.push(MAX_CAPACITY + (MAX_CAPACITY + 1) * max[h - 1]);
            min.push(MIN_CAPACITY + (MIN_CAPACITY + 1) * min[h - 1]);
            if max[h] >= n {
                return (Capacities { max, min }, h);
            }
        }
    }
}

/// Builds a subtree over `items` (exactly `height` levels) whose own key count has a floor of
/// `min_own_keys` when `height > 1` (`1` for the tree root -- no minimum-occupancy invariant
/// applies there; [`MIN_CAPACITY`] for every recursive, non-root call).
///
/// Chooses its own key count `k` as large as possible (closest to [`MAX_CAPACITY`]) subject to
/// the `k + 1` children -- each recursively built at `height - 1` -- landing within
/// [`Capacities::min`]/[`Capacities::max`] at that height; the remaining items split as evenly as
/// possible across them. The caller only ever passes an `n` reachable through this same
/// splitting, so some valid `k` always exists; the `assert` below is a capacity-planning
/// self-check, not a real precondition on `items`.
fn build_level<K: Serialize + Ord + Clone, V: Serialize + Clone>(
    items: &[(K, V)],
    height: usize,
    capacities: &Capacities,
    lift_key: Option<&LiftKey>,
    min_own_keys: usize,
) -> Node<K, V> {
    let n = items.len();
    if height == 1 {
        let mut node = Node::new();
        for (k, v) in items {
            node.keys.push(k.clone());
            node.values.push(v.clone());
            node.fingerprints.push(lift_with(lift_key, k, v));
        }
        node.refresh_aggregate();
        return node;
    }

    let child_height = height - 1;
    let child_min = capacities.min[child_height];
    let child_max = capacities.max[child_height];

    let mut key_count = MAX_CAPACITY.min(n.saturating_sub(1));
    loop {
        let child_count = key_count + 1;
        let remaining = n - key_count;
        if remaining >= child_count * child_min && remaining <= child_count * child_max {
            break;
        }
        assert!(
            key_count > min_own_keys,
            "bulk-build: no valid split of {n} items at height {height} (own keys \
             {min_own_keys}..={MAX_CAPACITY}, child range {child_min}..={child_max})"
        );
        key_count -= 1;
    }

    let child_count = key_count + 1;
    let remaining = n - key_count;
    let base = remaining / child_count;
    let extra = remaining % child_count;

    let mut node = Node::new();
    let mut children: Children<K, V> = ArrayVec::new();
    let mut cursor = 0usize;
    for child_index in 0..child_count {
        let group_size = base + usize::from(child_index < extra);
        let child = build_level(
            &items[cursor..cursor + group_size],
            child_height,
            capacities,
            lift_key,
            MIN_CAPACITY,
        );
        cursor += group_size;
        children.push(Arc::new(child));
        if child_index + 1 < child_count {
            let (k, v) = &items[cursor];
            node.keys.push(k.clone());
            node.values.push(v.clone());
            node.fingerprints.push(lift_with(lift_key, k, v));
            cursor += 1;
        }
    }
    debug_assert_eq!(cursor, n, "bulk-build must consume every item exactly once");
    node.children = Some(Box::new(children));
    node.refresh_aggregate();
    node
}

fn build<K: Serialize + Ord + Clone, V: Serialize + Clone>(
    items: &[(K, V)],
    lift_key: Option<&LiftKey>,
) -> Node<K, V> {
    if items.is_empty() {
        return Node::new();
    }
    let (capacities, height) = Capacities::for_size(items.len());
    build_level(items, height, &capacities, lift_key, 1)
}

impl<K: Serialize + Ord + Clone, V: Serialize + Clone> FingerprintTreeMap<K, V> {
    /// Builds a tree from `items` in one bottom-up pass -- the amortized alternative to `n`
    /// individual [`insert`](Self::insert) calls for a known, already-sorted dataset (initial
    /// load, snapshot recovery). Unlike [`FromIterator`], this does **not** sort or de-duplicate
    /// `items` itself.
    ///
    /// # Panics
    ///
    /// If `items`' keys are not strictly increasing (which also rules out duplicates) --
    /// [`FromIterator`]'s `collect()` remains the right choice for unsorted or duplicate-keyed
    /// input.
    ///
    /// ```
    /// use rsos::FingerprintTreeMap;
    ///
    /// let bulk: FingerprintTreeMap<i32, i32> =
    ///     FingerprintTreeMap::from_sorted_iter((0..1000).map(|k| (k, k * 2)));
    /// let serial: FingerprintTreeMap<i32, i32> = (0..1000).map(|k| (k, k * 2)).collect();
    ///
    /// // Same elements, same aggregate -- regardless of the two builds' internal tree shape.
    /// assert_eq!(bulk.aggregate(..), serial.aggregate(..));
    /// ```
    #[must_use]
    pub fn from_sorted_iter<T: IntoIterator<Item = (K, V)>>(items: T) -> Self {
        Self::build_sorted(items, None)
    }

    /// [`from_sorted_iter`](Self::from_sorted_iter), keyed under `lift_key` -- the bulk-build
    /// counterpart to [`with_lift_key`](Self::with_lift_key).
    ///
    /// # Panics
    ///
    /// Same precondition as [`from_sorted_iter`](Self::from_sorted_iter): `items`' keys must be
    /// strictly increasing.
    #[must_use]
    pub fn from_sorted_iter_keyed<T: IntoIterator<Item = (K, V)>>(
        items: T,
        lift_key: LiftKey,
    ) -> Self {
        Self::build_sorted(items, Some(lift_key))
    }

    fn build_sorted<T: IntoIterator<Item = (K, V)>>(items: T, lift_key: Option<LiftKey>) -> Self {
        let items: Vec<(K, V)> = items.into_iter().collect();
        assert!(
            items.windows(2).all(|pair| pair[0].0 < pair[1].0),
            "from_sorted_iter requires strictly increasing, duplicate-free keys"
        );
        let root = Arc::new(build(&items, lift_key.as_ref()));
        FingerprintTreeMap { root, lift_key }
    }
}
