// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! `FingerprintTreeMap::with_lift_key`: every lift site the tree owns (`insert`'s two branches,
//! `with_mut`'s re-lift, `check_invariants`) must consult the configured key, not just the ones
//! exercised by `basic.rs`'s unkeyed tests.

use crate::fingerprint::LiftKey;

use super::super::FingerprintTreeMap;

#[test]
fn keyed_and_unkeyed_trees_disagree_on_identical_content() {
    let mut unkeyed: FingerprintTreeMap<u64, &str> = FingerprintTreeMap::new();
    let mut keyed: FingerprintTreeMap<u64, &str> =
        FingerprintTreeMap::with_lift_key(LiftKey::new([1; 32]));
    for (k, v) in [(1u64, "a"), (2, "b"), (3, "c")] {
        unkeyed.insert(k, v);
        keyed.insert(k, v);
    }
    // Same keys, same values, same tree shape -- only the lift differs, and that alone must move
    // the aggregate: this is what makes a plant ground against the unkeyed hash miss the keyed one.
    assert_eq!(unkeyed.len(), keyed.len());
    assert_ne!(
        unkeyed.aggregate(..).fingerprint(),
        keyed.aggregate(..).fingerprint()
    );
}

#[test]
fn two_trees_under_the_same_key_agree_exactly_like_unkeyed_trees_do() {
    let key = || LiftKey::new([2; 32]);
    let mut a: FingerprintTreeMap<u64, u64> = FingerprintTreeMap::with_lift_key(key());
    let mut b: FingerprintTreeMap<u64, u64> = FingerprintTreeMap::with_lift_key(key());
    for i in 0..50u64 {
        a.insert(i, i * 7);
        b.insert(49 - i, (49 - i) * 7); // inserted in the opposite order
    }
    // Peers deriving the identical subkey from the identical cluster key must still converge --
    // keying must not turn the tree order-sensitive or otherwise break the existing contract.
    assert_eq!(a, b);
}

#[test]
fn two_trees_under_different_keys_never_falsely_converge() {
    let mut a: FingerprintTreeMap<u64, u64> =
        FingerprintTreeMap::with_lift_key(LiftKey::new([3; 32]));
    let mut b: FingerprintTreeMap<u64, u64> =
        FingerprintTreeMap::with_lift_key(LiftKey::new([4; 32]));
    for i in 0..20u64 {
        a.insert(i, i);
        b.insert(i, i);
    }
    // A key mismatch (e.g. a rolling upgrade mid-flight, or two clusters with different secrets)
    // must show up as a difference, never as a spurious match -- README "Security model"'s
    // "safely, but wastefully" claim depends on this failing open, not silently.
    assert_ne!(a, b);
}

#[test]
fn check_invariants_holds_for_a_keyed_tree() {
    let mut tree: FingerprintTreeMap<u64, u64> =
        FingerprintTreeMap::with_lift_key(LiftKey::new([5; 32]));
    for i in 0..200u64 {
        tree.insert(i, i * 3);
        tree.check_invariants();
    }
    for i in (0..200u64).step_by(3) {
        tree.remove(&i);
        tree.check_invariants();
    }
}

#[test]
fn with_mut_relifts_under_the_configured_key() {
    let key = LiftKey::new([6; 32]);
    let mut tree: FingerprintTreeMap<u64, u64> = FingerprintTreeMap::with_lift_key(key.clone());
    tree.insert(1, 10);
    tree.with_mut(&1, |v| *v.unwrap() = 99);

    let mut fresh: FingerprintTreeMap<u64, u64> = FingerprintTreeMap::with_lift_key(key);
    fresh.insert(1, 99);
    // The re-lift after `with_mut` must land on the same fingerprint a keyed insert of the
    // post-mutation value would -- i.e. it used the tree's key, not an unkeyed fallback.
    assert_eq!(tree, fresh);
}

#[test]
fn clear_preserves_the_configured_lift_key() {
    let key = LiftKey::new([8; 32]);
    let mut tree: FingerprintTreeMap<u64, u64> = FingerprintTreeMap::with_lift_key(key.clone());
    tree.insert(1, 10);
    tree.clear();
    tree.insert(1, 10);

    let mut fresh: FingerprintTreeMap<u64, u64> = FingerprintTreeMap::with_lift_key(key);
    fresh.insert(1, 10);
    // If `clear` dropped the key (falling back to `Default`'s `None`), this insert would lift
    // unkeyed and disagree with `fresh`, which stayed keyed throughout.
    assert_eq!(tree, fresh);
}
