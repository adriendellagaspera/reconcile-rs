// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Count exactness, mechanically: an unbalanced difference is never SKIPped, whatever the summary
//! does — checked against a comparison map that Wagner's k-tree was used to make genuinely collide
//! on the fingerprint half, so the guarantee is exercised at its hardest case rather than an easy
//! one.
//!
//! The plant here is a *solved* k-sum instance (the additive combiner is a k-sum instance, and the
//! k-tree applies to `ℤ/2^w` *with no error term*, because reduction mod `2^j` is a group
//! homomorphism and merging on low-order bits is therefore exact) with a single key then removed
//! from one peer, so the fingerprints no longer agree and neither do the counts. Pinned because the
//! guarantee is claimed with probability 1 and with no hypothesis on the lift — `POSITIONING.md` §2.1
//! records what it covers and what it does not — so it must hold at every width, not merely be
//! probable at the shipped one.
//!
//! Only the **width** is scaled down. The lift is the shipped [`rsos::digest`]/[`rsos::digest_keyed`]
//! (BLAKE3 over the canonical encoding, unkeyed or keyed) reduced mod `2^w`, the algebra is addition
//! mod `2^w`, and the driver is `rbsr`'s own, unmodified — [`NarrowStore`] enters through
//! [`RsosView`], the third-party-backend seam the trait documents.
//!
//! Two further tests below drive the *balanced* plant `KTree::solve` finds (rather than the
//! deliberately-broken one above) through the unmodified driver twice: once with both peers
//! unkeyed, demonstrating the false SKIP this module's doc describes in prose; once with both
//! peers keyed under a secret the attacker (who ground the plant against the unkeyed lift, having
//! no key) does not hold, demonstrating issue #19's fix defeats exactly that plant.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::ops::{Bound, RangeBounds};

use rbsr::{initial_ranges, protocol_round, RangeAggregate, RsosView};
use rsos::{digest, digest_keyed, Aggregate, Fingerprint, LiftKey};

// ---------------------------------------------------------------------------------------------
// The reduced-width instance of `rsos`'s algebra
// ---------------------------------------------------------------------------------------------

/// Low `width` bits set. `width` is always in `1..=64` here.
fn mask(width: u32) -> u64 {
    if width >= 64 {
        u64::MAX
    } else {
        (1u64 << width) - 1
    }
}

/// The shipped lift, reduced mod `2^width` — unkeyed when `lift_key` is `None` (what an attacker
/// grinding offline, with no cluster key, computes), keyed otherwise (issue #19's fix).
///
/// Reduction is the homomorphism `ℤ/2^256 → ℤ/2^width`, so this is the *same* algebra at a width
/// where the attack is reproducible in a test — not a different construction.
fn lift(key: u64, width: u32, lift_key: Option<&LiftKey>) -> u64 {
    let fp = match lift_key {
        Some(lift_key) => digest_keyed(lift_key, &key),
        None => digest(&key),
    };
    fp.0[0] & mask(width)
}

/// A store summarizing with `Σ mod 2^width` instead of `Σ mod 2^256`.
///
/// Carried in limb 0 of a [`Fingerprint`] so the driver compares the real wire type. Keys are kept
/// sorted and unique, which is all `rank`/`select` need.
struct NarrowStore {
    width: u32,
    keys: Vec<u64>,
    /// `None` matches the vulnerability this module demonstrates; `Some` is issue #19's fix,
    /// exercised by the two tests below that name it directly.
    lift_key: Option<LiftKey>,
}

impl NarrowStore {
    fn new(width: u32, mut keys: Vec<u64>, lift_key: Option<LiftKey>) -> NarrowStore {
        keys.sort_unstable();
        keys.dedup();
        NarrowStore {
            width,
            keys,
            lift_key,
        }
    }

    /// Half-open index span of `range`, empty when the bounds invert.
    fn span<R: RangeBounds<u64>>(&self, range: &R) -> (usize, usize) {
        let start = match range.start_bound() {
            Bound::Unbounded => 0,
            Bound::Included(k) => self.keys.partition_point(|x| x < k),
            Bound::Excluded(k) => self.keys.partition_point(|x| x <= k),
        };
        let end = match range.end_bound() {
            Bound::Unbounded => self.keys.len(),
            Bound::Included(k) => self.keys.partition_point(|x| x <= k),
            Bound::Excluded(k) => self.keys.partition_point(|x| x < k),
        };
        (start, end.max(start))
    }
}

impl RsosView<u64> for NarrowStore {
    fn size(&self) -> usize {
        self.keys.len()
    }

    fn aggregate<R: RangeBounds<u64>>(&self, range: R) -> Aggregate {
        let (start, end) = self.span(&range);
        let slice = &self.keys[start..end];
        let sum = slice.iter().fold(0u64, |acc, &key| {
            acc.wrapping_add(lift(key, self.width, self.lift_key.as_ref())) & mask(self.width)
        });
        Aggregate::new(slice.len(), Fingerprint([sum, 0, 0, 0]))
    }

    fn rank(&self, z: &u64) -> usize {
        self.keys.partition_point(|x| x < z)
    }

    fn select(&self, r: usize) -> &u64 {
        &self.keys[r]
    }
}

// ---------------------------------------------------------------------------------------------
// Wagner's k-tree over ℤ/2^width
// ---------------------------------------------------------------------------------------------

/// A partial sum and the signed keys that produced it.
///
/// `true` in the key list means the key is destined for peer A, `false` for peer B — the sign
/// folded into `value` at level 0.
#[derive(Clone)]
struct Partial {
    value: u64,
    keys: Vec<(u64, bool)>,
}

/// The parameters of one k-tree run.
struct KTree {
    width: u32,
    /// `log2` of the list count, so `k = 2^t` lists and `t` merge levels.
    t: u32,
    /// Bits cancelled per merge level: `⌊width / (t + 1)⌋`.
    j: u32,
    /// Level-0 candidates per list. One bit of oversampling over the `2^j` the analysis asks for.
    list_size: usize,
}

impl KTree {
    fn new(width: u32, t: u32) -> KTree {
        let j = width / (t + 1);
        let list_size = 1usize << (j + 1);
        KTree {
            width,
            t,
            j,
            list_size,
        }
    }

    /// Level-0 keys, namespaced so lists are disjoint from each other and from honest data.
    ///
    /// Bit 63 marks a planted key; `attempt` reseeds the whole search without touching the lift.
    fn key_of(&self, attempt: u64, list: usize, index: usize) -> u64 {
        (1u64 << 63) | (attempt << 52) | ((list as u64) << 40) | (index as u64)
    }

    /// Find `k` distinct keys whose signed lifts sum to zero mod `2^width`, split evenly between
    /// the two peers. `None` if every attempt came up empty.
    ///
    /// Positive lists feed peer A, negative lists peer B, so `|P_A| = |P_B| = k/2` by construction
    /// and the count component of the aggregate matches — Theorem 2 does not stand in the way.
    fn solve(&self, attempts: u64) -> Option<(Vec<u64>, Vec<u64>)> {
        (0..attempts).find_map(|attempt| self.solve_once(attempt))
    }

    fn solve_once(&self, attempt: u64) -> Option<(Vec<u64>, Vec<u64>)> {
        let k = 1usize << self.t;
        let modulus = mask(self.width);

        // Level 0: half the lists positive, half negated, sign folded into the value.
        let mut lists: Vec<Vec<Partial>> = (0..k)
            .map(|list| {
                let positive = list < k / 2;
                (0..self.list_size)
                    .map(|index| {
                        let key = self.key_of(attempt, list, index);
                        // The attacker grinds with no key -- they do not hold one.
                        let raw = lift(key, self.width, None);
                        // Negation mod 2^width, exact for raw == 0 too.
                        let value = if positive {
                            raw
                        } else {
                            raw.wrapping_neg() & modulus
                        };
                        Partial {
                            value,
                            keys: vec![(key, positive)],
                        }
                    })
                    .collect()
            })
            .collect();

        // Merge levels: after level `l`, every surviving sum is zero on its low `l * j` bits.
        for level in 1..=self.t {
            let window = mask(level * self.j);
            lists = lists
                .chunks(2)
                .map(|pair| self.join(&pair[0], &pair[1], window))
                .collect();
        }

        // One list left, zero on `t * j` bits. A full solution is zero on all of them.
        let solution = lists[0].iter().find(|partial| partial.value == 0)?;
        let split = |wanted: bool| -> Vec<u64> {
            solution
                .keys
                .iter()
                .filter(|(_, positive)| *positive == wanted)
                .map(|(key, _)| *key)
                .collect()
        };
        Some((split(true), split(false)))
    }

    /// Keep the sums that cancel on `window`, capped so list sizes stay stable across levels.
    ///
    /// The lookup is exact: `(a + b) mod 2^w ≡ 0 (mod 2^j)` iff `a ≡ −b (mod 2^j)`, because
    /// reduction mod `2^j` is a homomorphism — carries leave the window upward and never re-enter
    /// it. This is the step that must carry no error term for [`KTree::solve`] to actually find a
    /// colliding plant, which the kept test below relies on to exercise the count check against a
    /// genuinely fingerprint-colliding pair rather than an easy one.
    fn join(&self, left: &[Partial], right: &[Partial], window: u64) -> Vec<Partial> {
        let mut index: HashMap<u64, Vec<&Partial>> = HashMap::new();
        for partial in left {
            index
                .entry(partial.value & window)
                .or_default()
                .push(partial);
        }
        let modulus = mask(self.width);
        let mut out = Vec::with_capacity(self.list_size);
        for b in right {
            let wanted = (b.value & window).wrapping_neg() & window;
            let Some(matches) = index.get(&wanted) else {
                continue;
            };
            for a in matches {
                if out.len() == self.list_size {
                    return out;
                }
                let mut keys = a.keys.clone();
                keys.extend_from_slice(&b.keys);
                out.push(Partial {
                    value: a.value.wrapping_add(b.value) & modulus,
                    keys,
                });
            }
        }
        out
    }
}

// ---------------------------------------------------------------------------------------------
// Driving the real protocol
// ---------------------------------------------------------------------------------------------

/// Honest data both peers hold, disjoint from the planted namespace (bit 63 clear).
fn honest() -> Vec<u64> {
    (0..500u64).collect()
}

/// One round of the real driver: peer A advertises its whole store, peer B answers.
///
/// Returns `true` when B SKIPped the outer range — the protocol declaring convergence.
fn declares_convergence(a: &NarrowStore, b: &NarrowStore) -> bool {
    let active: Vec<RangeAggregate<u64>> = initial_ranges(a);
    let mut children = Vec::new();
    let mut enumerations = Vec::new();
    let outcome = protocol_round(b, active, &mut children, &mut enumerations);
    children.is_empty() && enumerations.is_empty() && outcome.skipped() == 1
}

/// Two stores sharing `honest()`, differing only in what was planted on each side, both keyed
/// under `lift_key` (or both unkeyed, if `None`) — never one of each, which would only be
/// re-testing the "different keys never falsely converge" property `rsos`'s own tests already
/// cover, not this module's Wagner-specific claim.
fn planted(
    width: u32,
    on_a: &[u64],
    on_b: &[u64],
    lift_key: Option<LiftKey>,
) -> (NarrowStore, NarrowStore) {
    let build = |extra: &[u64], lift_key: Option<LiftKey>| {
        let mut keys = honest();
        keys.extend_from_slice(extra);
        NarrowStore::new(width, keys, lift_key)
    };
    (build(on_a, lift_key.clone()), build(on_b, lift_key))
}

/// The width E3 runs at, with the k-tree shape used. `j = 8`, so the cost per list is constant
/// and the list count carries the width — kept small so constructing the plant below stays fast.
const CONFIGURATIONS: [(u32, u32); 3] = [(32, 3), (48, 5), (64, 7)];

/// An unbalanced difference is never SKIPped, whatever the summary does.
#[test]
fn an_unbalanced_difference_is_never_skipped() {
    for (width, t) in CONFIGURATIONS {
        let k_tree = KTree::new(width, t);
        let (on_a, mut on_b) = k_tree.solve(8).expect("k-tree found no solution");
        on_b.pop();

        let (a, b) = planted(width, &on_a, &on_b, None);
        assert_ne!(a.size(), b.size());
        assert!(
            !declares_convergence(&a, &b),
            "w={width}: a count mismatch must be detected with certainty"
        );
    }
}

/// The balanced plant `KTree::solve` finds — no `.pop()` this time, so both count *and*
/// fingerprint agree — fools the unmodified driver into declaring convergence between two
/// genuinely different, unkeyed stores. This is the vulnerability this module's doc describes in
/// prose, exercised directly rather than only through the unbalanced safety net above.
#[test]
fn a_balanced_wagner_plant_causes_a_false_skip_under_the_unkeyed_lift() {
    for (width, t) in CONFIGURATIONS {
        let k_tree = KTree::new(width, t);
        let (on_a, on_b) = k_tree.solve(8).expect("k-tree found no solution");

        let (a, b) = planted(width, &on_a, &on_b, None);
        assert_eq!(
            a.size(),
            b.size(),
            "the plant is count-balanced by construction"
        );
        assert_ne!(
            &a.keys, &b.keys,
            "the two stores must genuinely differ for a SKIP here to be false"
        );
        assert!(
            declares_convergence(&a, &b),
            "w={width}: an unkeyed lift must be fooled by a balanced Wagner plant -- this is \
             exactly the gap issue #19 closes for holders of a lift key"
        );
    }
}

/// The identical plant, ground by an attacker with no key against the unkeyed lift above, no
/// longer fools the driver once both peers key their lift under a secret the attacker does not
/// hold (issue #19's fix) — the sums the attacker balanced to zero are, under a different keyed
/// hash, unrelated values that no longer cancel.
#[test]
fn the_same_plant_is_detected_once_the_lift_is_keyed() {
    let lift_key = LiftKey::new([0x42; 32]);
    for (width, t) in CONFIGURATIONS {
        let k_tree = KTree::new(width, t);
        let (on_a, on_b) = k_tree.solve(8).expect("k-tree found no solution");

        let (a, b) = planted(width, &on_a, &on_b, Some(lift_key.clone()));
        assert_eq!(a.size(), b.size(), "the plant is still count-balanced");
        assert!(
            !declares_convergence(&a, &b),
            "w={width}: keying the lift must defeat a plant ground against the unkeyed hash"
        );
    }
}
