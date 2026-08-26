// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use rsos::{Fingerprint, FingerprintTreeMap};

use crate::policy::{EnumerateBelowThreshold, SplitStride, SqrtFanOut};

use super::*;

// ----- Convergence, under every policy and under mixed pairs -----

/// Reconcile two stores to a fixed point, applying IDLISTs the way `reconcile`'s engine does.
/// Policies are supplied separately so a mixed pair can be driven. Returns the message count.
fn drive(
    a: &mut FingerprintTreeMap<i32, i32>,
    b: &mut FingerprintTreeMap<i32, i32>,
    a_policy: &dyn RefinementPolicy,
    b_policy: &dyn RefinementPolicy,
) -> usize {
    let mut active = initial_ranges(&*a);
    let mut responder_is_b = true;
    let mut messages = 0;
    let mut rng = rng();
    while !active.is_empty() {
        messages += 1;
        let mut children = Vec::new();
        let mut enumerations = Vec::new();
        let items: Vec<(i32, i32)> = {
            let (responder, policy) = if responder_is_b {
                (&*b, b_policy)
            } else {
                (&*a, a_policy)
            };
            protocol_round_with_policy(
                responder,
                policy,
                active,
                &mut children,
                &mut enumerations,
                &mut rng,
            );
            enumerations
                .into_iter()
                .flat_map(|range| {
                    responder
                        .range(range)
                        .map(|(k, v)| (*k, *v))
                        .collect::<Vec<_>>()
                })
                .collect()
        };
        let receiver = if responder_is_b { &mut *a } else { &mut *b };
        for (key, value) in items {
            receiver.insert(key, value);
        }
        active = children;
        responder_is_b = !responder_is_b;
        assert!(
            messages < 10_000,
            "reconciliation failed to converge — the refinement is not shrinking"
        );
    }
    messages
}

/// Reconcile two stores under a pair of policies; both must end holding exactly the union.
fn assert_converges(
    keys_a: &[i32],
    keys_b: &[i32],
    a_policy: &dyn RefinementPolicy,
    b_policy: &dyn RefinementPolicy,
) {
    let (mut a, mut b) = (tree(keys_a), tree(keys_b));
    drive(&mut a, &mut b, a_policy, b_policy);

    let mut union: Vec<i32> = keys_a.iter().chain(keys_b).copied().collect();
    union.sort_unstable();
    union.dedup();
    let contents =
        |t: &FingerprintTreeMap<i32, i32>| t.range(..).map(|(k, _)| *k).collect::<Vec<_>>();
    assert_eq!(contents(&a), union, "a did not converge on the union");
    assert_eq!(contents(&b), union, "b did not converge on the union");
    assert_eq!(a.aggregate(..), b.aggregate(..));
}

/// Scattered, clustered and degenerate differences — the shapes that pull policies apart.
fn corpora() -> Vec<(&'static str, Vec<i32>, Vec<i32>)> {
    let full: Vec<i32> = (0..500).collect();
    vec![
        ("both empty", vec![], vec![]),
        ("one side empty", full.clone(), vec![]),
        ("identical", full.clone(), full.clone()),
        (
            "one differing element",
            full.clone(),
            full.iter().copied().filter(|k| *k != 250).collect(),
        ),
        (
            "scattered differences",
            full.clone(),
            full.iter().copied().filter(|k| k % 37 != 0).collect(),
        ),
        (
            "clustered differences",
            full.clone(),
            full.iter()
                .copied()
                .filter(|k| !(200..250).contains(k))
                .collect(),
        ),
        (
            "disjoint halves",
            full.iter().copied().filter(|k| k % 2 == 0).collect(),
            full.iter().copied().filter(|k| k % 2 == 1).collect(),
        ),
    ]
}

/// A policy that behaves like [`FixedFanOut`] except it never actually narrows a range once a
/// real cut is possible (`span() > 1`) — it asks for a stride wider than any span instead.
/// `ARCHITECTURE.md` §5 invariant 13 (#420): included in [`policies`] so the driver's guard,
/// not this policy's own hygiene, is what the convergence matrix below is proving. Without
/// that guard this would hang exactly like the oracle-coupled probe (#356).
#[derive(Clone, Copy, Debug, Default)]
struct NeverNarrows;

impl RefinementPolicy for NeverNarrows {
    fn decide(&self, comparison: Comparison) -> Decision {
        match FixedFanOut::default().decide(comparison) {
            Decision::Split(_) if comparison.span() > 1 => {
                Decision::Split(SplitStride::per_child(usize::MAX))
            }
            other => other,
        }
    }
}

fn policies() -> Vec<(&'static str, Box<dyn RefinementPolicy>)> {
    vec![
        ("SqrtFanOut", Box::new(SqrtFanOut)),
        ("FixedFanOut(2)", Box::new(FixedFanOut::new(FanOut::BINARY))),
        (
            "FixedFanOut(16)",
            Box::new(FixedFanOut::new(FanOut::NEGENTROPY)),
        ),
        (
            "EnumerateBelow(t=32,b=16)",
            Box::new(EnumerateBelowThreshold::PAPER),
        ),
        (
            "EnumerateBelow(t=1,b=2)",
            Box::new(EnumerateBelowThreshold::new(1, FanOut::BINARY)),
        ),
        ("NeverNarrows", Box::new(NeverNarrows)),
    ]
}

/// `ARCHITECTURE.md` §5 invariant 13 (#420), isolated to one round: a policy asking for a
/// stride that would not narrow a `span() > 1` range must not reach the fan-out loop as a
/// `Split` at all — it is answered as an `Enumerate`, counted and bounced back exactly like a
/// policy that had returned `Enumerate` itself.
#[test]
fn non_progressing_split_is_converted_to_enumerate() {
    let store = tree(&(0..10).collect::<Vec<_>>()); // span = 10, so span() > 1
    let segment = RangeAggregate {
        range: KeyRange::new(StartBound::Unbounded, EndBound::Unbounded),
        aggregate: Aggregate::new(5, Fingerprint([9, 9, 9, 9])), // non-empty, disagrees
    };
    let mut child_ranges = Vec::new();
    let mut enumeration_ranges = Vec::new();
    let outcome = protocol_round_with_policy(
        &store,
        &NeverNarrows,
        vec![segment],
        &mut child_ranges,
        &mut enumeration_ranges,
        &mut rng(),
    );
    assert_eq!(outcome.split(), 0, "must not be counted as a SPLIT");
    assert_eq!(
        outcome.enumerated(),
        1,
        "must be counted as an IDLIST instead"
    );
    assert_eq!(enumeration_ranges.len(), 1);
    // The peer's range was non-empty, so IDLIST's one-directional bounce-back applies here
    // exactly as it would for a policy that had returned `Decision::Enumerate` directly.
    assert_eq!(child_ranges.len(), 1);
    assert_eq!(child_ranges[0].aggregate, Aggregate::ZERO);
}

#[test]
fn every_policy_reconciles_every_corpus() {
    for (policy_name, policy) in policies() {
        for (corpus, keys_a, keys_b) in corpora() {
            println!("{policy_name} / {corpus}");
            assert_converges(&keys_a, &keys_b, policy.as_ref(), policy.as_ref());
        }
    }
}

/// Peers running different policies must converge; otherwise the policy has leaked onto the
/// wire.
#[test]
fn peers_running_different_policies_still_converge() {
    for (a_name, a_policy) in policies() {
        for (b_name, b_policy) in policies() {
            for (corpus, keys_a, keys_b) in corpora() {
                println!("{a_name} vs {b_name} / {corpus}");
                assert_converges(&keys_a, &keys_b, a_policy.as_ref(), b_policy.as_ref());
            }
        }
    }
}
