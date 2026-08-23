// Copyright 2026 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use rbsr::{Comparison, Decision, FixedFanOut};
use rsos::FingerprintTreeMap;

use super::*;

#[test]
fn queries_add_sums_componentwise() {
    let a = Queries {
        aggregate: 1,
        rank: 2,
        select: 3,
    };
    let b = Queries {
        aggregate: 10,
        rank: 20,
        select: 30,
    };
    assert_eq!(
        a + b,
        Queries {
            aggregate: 11,
            rank: 22,
            select: 33,
        }
    );
}

#[test]
fn total_bytes_adds_refinement_bytes_to_each_priced_variant() {
    let cost = Cost {
        refinement_bytes: 100,
        enumerated_bytes: vec![10, 20, 30],
        ..Cost::default()
    };
    assert_eq!(cost.total_bytes(), vec![110, 120, 130]);
}

#[test]
fn total_bytes_is_empty_when_nothing_was_priced() {
    let cost = Cost {
        refinement_bytes: 100,
        ..Cost::default()
    };
    assert_eq!(cost.total_bytes(), Vec::<usize>::new());
}

#[test]
fn decisions_copies_the_payload_independent_fields_only() {
    let cost = Cost {
        messages: 3,
        ranges: 7,
        enumerations: 2,
        enumerated_elements: 5,
        queries: Queries {
            aggregate: 1,
            rank: 2,
            select: 3,
        },
        refinement_bytes: 999,
        enumerated_bytes: vec![999],
        ..Cost::default()
    };
    assert_eq!(
        cost.decisions(),
        Decisions {
            messages: 3,
            ranges: 7,
            enumerations: 2,
            enumerated_elements: 5,
            queries: Queries {
                aggregate: 1,
                rank: 2,
                select: 3,
            },
        }
    );
}

#[test]
fn counting_tallies_each_query_kind_separately() {
    let mut map = FingerprintTreeMap::<u64, u64>::new();
    map.insert(1, 1);
    map.insert(2, 2);
    let counting = Counting::new(&map);
    let _ = counting.aggregate(..);
    let _ = counting.rank(&1);
    let _ = counting.rank(&2);
    let _ = counting.select(0);
    assert_eq!(
        counting.queries(),
        Queries {
            aggregate: 1,
            rank: 2,
            select: 1,
        }
    );
}

#[test]
fn reconcile_settles_in_one_round_when_stores_already_agree() {
    let mut a = FingerprintTreeMap::<u64, u64>::new();
    let mut b = FingerprintTreeMap::<u64, u64>::new();
    for key in 0..5u64 {
        a.insert(key, key);
        b.insert(key, key);
    }
    let cost = reconcile(&a, &b, &FixedFanOut::default(), None);
    assert_eq!(
        cost.messages, 1,
        "agreeing stores settle after the single top-level comparison"
    );
    assert_eq!(cost.enumerated_elements, 0);
}

/// Ignores the comparison and always enumerates — deterministic IDLIST traffic to drive the
/// `price_element` wiring without depending on any shipped policy's actual cutoff table.
struct AlwaysEnumerate;

impl rbsr::RefinementPolicy for AlwaysEnumerate {
    fn decide(&self, _comparison: Comparison) -> Decision {
        Decision::Enumerate
    }
}

#[test]
fn reconcile_prices_every_enumerated_element_through_the_closure() {
    let mut a = FingerprintTreeMap::<u64, u64>::new();
    a.insert(1, 1);
    let b = FingerprintTreeMap::<u64, u64>::new();

    let mut price_calls = Vec::new();
    let mut price = |key: u64| {
        price_calls.push(key);
        vec![10, 20]
    };
    let cost = reconcile(&a, &b, &AlwaysEnumerate, Some(&mut price));

    assert_eq!(
        price_calls,
        vec![1],
        "a's one key must be priced exactly once"
    );
    assert_eq!(cost.enumerated_elements, 1);
    assert_eq!(cost.enumerated_bytes, vec![10, 20]);
}

#[test]
fn reconcile_counts_without_pricing_when_no_closure_is_given() {
    let mut a = FingerprintTreeMap::<u64, u64>::new();
    a.insert(1, 1);
    let b = FingerprintTreeMap::<u64, u64>::new();

    let cost = reconcile(&a, &b, &AlwaysEnumerate, None);

    assert_eq!(cost.enumerated_elements, 1);
    assert!(
        cost.enumerated_bytes.is_empty(),
        "no pricing closure means nothing gets priced"
    );
}
