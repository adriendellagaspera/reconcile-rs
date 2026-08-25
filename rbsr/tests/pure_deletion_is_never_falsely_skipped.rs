// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The shipped comparison map's predicted false-convergence rate is **exactly zero** under a
//! pure-deletion difference.
//!
//! `rbsr` compares the *whole* [`Aggregate`] — `(count, fingerprint)` — never the fingerprint
//! alone. Under a pure-deletion difference (`Y = X ∖ S`, `S` non-empty), any range that actually
//! holds part of `S` has strictly fewer elements on the `Y` side, so `count-agreement` alone
//! (`rbsr::RsosView`'s docs) forbids the driver from ever declaring that range converged — whatever
//! the comparison map does with the fingerprint half.
//!
//! That makes the predicted event rate not merely small but **zero**, with no hypothesis on the
//! lift: a single observed false convergence — a differing range the driver SKIPs — refutes count
//! exactness outright, and no statistics are needed to reject the hypothesis on one witness. That
//! is why this lives in the standard `cargo test --workspace` gate next to
//! `wagner_false_convergence.rs`'s own count-exactness test, rather than in a bench: a bench
//! reports a rate, this asserts a certainty.
//!
//! **Trial count and seeding.** [`TRIALS`] independent pure-deletion instances, each a fresh
//! universe of up to a few hundred keys with a non-empty deleted subset, seeded from the trial
//! index (`StdRng::seed_from_u64`) — a recorded counter, not the process RNG, so a failure
//! reproduces from the printed trial number alone. Convergence is driven to a full fixed point and
//! the discovered enumeration ranges are checked against the *true* symmetric difference: any key
//! in `S` that never surfaces there is exactly the signature of a false SKIP the type system
//! otherwise makes unreachable, so this test is the regression guard on that unreachability
//! actually holding in the driver's own code, not merely in the `Aggregate` type it compares.

#![forbid(unsafe_code)]

use std::collections::HashSet;

use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

use rbsr::{initial_ranges, protocol_round, EnumerationRange, RsosView};
use rsos::{FingerprintTreeMap, Rsos};

/// Independent pure-deletion instances driven. Large enough that a once-in-a-thousand
/// implementation slip (e.g. a comparison that drops the count half) would almost certainly
/// surface, while staying inside the standard test gate's time budget.
const TRIALS: u64 = 500;

/// Past this many rounds the drive is not converging; the cap turns a hang into a failure rather
/// than a timeout with no diagnostic.
const MAX_ROUNDS: usize = 128;

/// Reconcile `a` against `b` to a fixed point, alternating which peer answers, collecting every
/// IDLIST range either side was asked to enumerate.
fn drive<K: Clone + Ord, B: RsosView<K>>(
    a: &B,
    b: &B,
) -> (Vec<EnumerationRange<K>>, Vec<EnumerationRange<K>>) {
    let mut active = initial_ranges(a);
    let mut responder = b;
    let mut advertiser = a;
    let mut a_enumerations = Vec::new();
    let mut b_enumerations = Vec::new();
    let mut rounds = 0;

    // `a` always advertises first, so parity tracks which peer just answered.
    let mut responder_is_b = true;
    while !active.is_empty() && rounds < MAX_ROUNDS {
        let mut children = Vec::new();
        let mut enumerations = Vec::new();
        protocol_round(responder, active, &mut children, &mut enumerations);
        if responder_is_b {
            b_enumerations.extend(enumerations);
        } else {
            a_enumerations.extend(enumerations);
        }
        active = children;
        rounds += 1;
        std::mem::swap(&mut responder, &mut advertiser);
        responder_is_b = !responder_is_b;
    }
    assert!(rounds < MAX_ROUNDS, "the drive did not reach a fixed point");
    (a_enumerations, b_enumerations)
}

/// A random universe `X` and a non-empty deleted subset `S`, `Y = X ∖ S` — the pure-deletion
/// difference the claim is stated over. Sizes are kept modest so `TRIALS` runs stay fast; the
/// claim does not depend on scale.
fn deletion_instance(rng: &mut StdRng) -> (Vec<u64>, HashSet<u64>) {
    let universe_size = rng.gen_range(2..200);
    let mut universe: Vec<u64> = (0..universe_size).map(|_| rng.gen()).collect();
    universe.sort_unstable();
    universe.dedup();
    assert!(!universe.is_empty());

    let deleted_count = rng.gen_range(1..=universe.len());
    universe.shuffle(rng);
    let deleted: HashSet<u64> = universe[..deleted_count].iter().copied().collect();
    universe.sort_unstable();
    (universe, deleted)
}

/// Every key either peer was asked to hand over explicitly, read back through `keys_of` so the
/// check is against ground truth rather than the driver's own bookkeeping.
fn enumerated_keys<K: Ord + Copy + std::hash::Hash>(
    ranges: &[EnumerationRange<K>],
    keys_of: impl Fn(&EnumerationRange<K>) -> Vec<K>,
) -> HashSet<K> {
    ranges.iter().flat_map(keys_of).collect()
}

/// [`FingerprintTreeMap`] compared through its blanket [`RsosView`] impl: the real
/// `rsos::Rsos::aggregate`, no truncation.
#[test]
fn f_p_id_never_declares_false_convergence_on_a_pure_deletion_difference() {
    for trial in 0..TRIALS {
        let mut rng = StdRng::seed_from_u64(trial);
        let (universe, deleted) = deletion_instance(&mut rng);

        let mut a: FingerprintTreeMap<u64, ()> = FingerprintTreeMap::new();
        let mut b: FingerprintTreeMap<u64, ()> = FingerprintTreeMap::new();
        for &key in &universe {
            a.insert(key, ());
            if !deleted.contains(&key) {
                b.insert(key, ());
            }
        }
        assert_ne!(
            Rsos::size(&a),
            Rsos::size(&b),
            "trial {trial}: a non-empty deletion must unbalance the outer range"
        );

        let (a_enum, b_enum) = drive(&a, &b);
        let keys_of = |range: &EnumerationRange<u64>| -> Vec<u64> {
            a.range(*range)
                .chain(b.range(*range))
                .map(|(k, ())| *k)
                .collect()
        };
        let found = enumerated_keys(&a_enum, keys_of)
            .into_iter()
            .chain(enumerated_keys(&b_enum, keys_of))
            .collect::<HashSet<u64>>();

        for key in &deleted {
            assert!(
                found.contains(key),
                "trial {trial}: f_p=id false-converged — deleted key {key} was never enumerated, \
                 which is only possible if some range containing it was SKIPped despite an \
                 unbalanced count"
            );
        }
    }
}
