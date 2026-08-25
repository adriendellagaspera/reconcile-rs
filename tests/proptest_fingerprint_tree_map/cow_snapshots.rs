// Copyright 2026 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Structural sharing (#41): `FingerprintTreeMap::clone` is now shallow -- it bumps the root
//! `Arc`'s refcount rather than deep-copying every node -- so a subsequent `insert`/`remove` on
//! either the original or the clone must fork exactly the nodes it touches
//! ([`std::sync::Arc::make_mut`]) and leave every node still reachable from the *other* copy
//! untouched. A COW fork missed anywhere on a mutated path would silently corrupt an older
//! retained snapshot; this is the property a plain "clone always deep-copies" implementation
//! would also pass, so it is the property that actually exercises `Arc::make_mut` at splits,
//! merges and steals.

use std::collections::BTreeMap;

use proptest::prelude::*;

use rsos::{Aggregate, FingerprintTreeMap};

#[derive(Clone, Debug)]
enum Op {
    Insert(u8, u16),
    Remove(u8),
    /// Retains a clone of the tree as it stands *before* this op runs, to be checked against its
    /// own oracle snapshot once every op has run.
    Snapshot,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => (any::<u8>(), any::<u16>()).prop_map(|(k, v)| Op::Insert(k, v)),
        6 => any::<u8>().prop_map(Op::Remove),
        2 => Just(Op::Snapshot),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn a_retained_clone_is_unaffected_by_later_mutation_of_the_original(
        ops in prop::collection::vec(op_strategy(), 0..400),
    ) {
        let mut tree: FingerprintTreeMap<u8, u16> = FingerprintTreeMap::new();
        let mut oracle: BTreeMap<u8, u16> = BTreeMap::new();
        let mut snapshots: Vec<(BTreeMap<u8, u16>, FingerprintTreeMap<u8, u16>, Aggregate)> =
            Vec::new();

        for op in ops {
            match op {
                Op::Insert(k, v) => {
                    prop_assert_eq!(tree.insert(k, v), oracle.insert(k, v));
                }
                Op::Remove(k) => {
                    prop_assert_eq!(tree.remove(&k), oracle.remove(&k));
                }
                Op::Snapshot => {
                    let snap = tree.clone();
                    let snap_aggregate = snap.aggregate(..);
                    snapshots.push((oracle.clone(), snap, snap_aggregate));
                }
            }
            tree.check_invariants();
        }

        // Every snapshot taken along the way must still describe exactly what its own oracle
        // held at that point -- untouched by every insert/remove/split/merge/steal that ran on
        // `tree` afterward. Its `aggregate(..)` -- computed once right after cloning -- must also
        // still match when recomputed now: a COW fork that missed a shared node would leak a
        // later mutation into the snapshot's fingerprint without necessarily changing its
        // enumerated contents.
        for (oracle_snapshot, tree_snapshot, snap_aggregate) in &snapshots {
            tree_snapshot.check_invariants();
            let got: Vec<(u8, u16)> = tree_snapshot.range(..).map(|(k, v)| (*k, *v)).collect();
            let want: Vec<(u8, u16)> = oracle_snapshot.iter().map(|(k, v)| (*k, *v)).collect();
            prop_assert_eq!(got, want);
            prop_assert_eq!(tree_snapshot.aggregate(..), *snap_aggregate);
        }

        // The live tree still agrees with its own oracle too.
        let got: Vec<(u8, u16)> = tree.range(..).map(|(k, v)| (*k, *v)).collect();
        let want: Vec<(u8, u16)> = oracle.iter().map(|(k, v)| (*k, *v)).collect();
        prop_assert_eq!(got, want);
    }

    /// The mirror direction: mutating an *earlier* snapshot must not disturb a `tree` that kept
    /// running past it -- structural sharing has no privileged owner between the two clones.
    #[test]
    fn mutating_an_old_clone_does_not_disturb_the_tree_that_kept_going(
        initial in prop::collection::vec((any::<u8>(), any::<u16>()), 0..100),
        later_ops in prop::collection::vec(op_strategy(), 0..200),
        extra in prop::collection::vec((any::<u8>(), any::<u16>()), 0..50),
    ) {
        let mut tree: FingerprintTreeMap<u8, u16> = FingerprintTreeMap::new();
        let mut oracle: BTreeMap<u8, u16> = BTreeMap::new();
        for (k, v) in &initial {
            tree.insert(*k, *v);
            oracle.insert(*k, *v);
        }

        let mut old_clone = tree.clone();
        let old_oracle = oracle.clone();
        let old_clone_aggregate = old_clone.aggregate(..);

        for op in later_ops {
            match op {
                Op::Insert(k, v) => {
                    prop_assert_eq!(tree.insert(k, v), oracle.insert(k, v));
                }
                Op::Remove(k) => {
                    prop_assert_eq!(tree.remove(&k), oracle.remove(&k));
                }
                Op::Snapshot => {}
            }
        }
        tree.check_invariants();

        // `old_clone`'s aggregate, recorded right after cloning, must still hold: none of the
        // splits/merges/steals `tree` just ran should have leaked into a node `old_clone` shares.
        prop_assert_eq!(old_clone.aggregate(..), old_clone_aggregate);
        let tree_aggregate_before_old_clone_mutation = tree.aggregate(..);

        // Now mutate the old clone -- it must still be exactly what it was when cloned, and its
        // own edits from here must not reach `tree`.
        let mut old_clone_oracle = old_oracle.clone();
        for (k, v) in &extra {
            prop_assert_eq!(old_clone.insert(*k, *v), old_clone_oracle.insert(*k, *v));
        }
        old_clone.check_invariants();

        let got: Vec<(u8, u16)> = old_clone.range(..).map(|(k, v)| (*k, *v)).collect();
        let want: Vec<(u8, u16)> = old_clone_oracle.iter().map(|(k, v)| (*k, *v)).collect();
        prop_assert_eq!(got, want);

        let got: Vec<(u8, u16)> = tree.range(..).map(|(k, v)| (*k, *v)).collect();
        let want: Vec<(u8, u16)> = oracle.iter().map(|(k, v)| (*k, *v)).collect();
        prop_assert_eq!(got, want);

        // Mutating `old_clone` (going the other direction) must likewise leave `tree`'s
        // aggregate untouched.
        prop_assert_eq!(tree.aggregate(..), tree_aggregate_before_old_clone_mutation);
    }
}
