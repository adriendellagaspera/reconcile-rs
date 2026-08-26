// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use super::*;

#[test]
fn zero_is_identity() {
    let f = lift(&42u64, &"hello");
    assert_eq!(f + Fingerprint::ZERO, f);
    assert_eq!(f - Fingerprint::ZERO, f);
    assert_eq!(Fingerprint::ZERO + f, f);
}

#[test]
fn add_then_remove_is_identity() {
    let a = lift(&1u64, &10u64);
    let b = lift(&2u64, &20u64);
    let c = lift(&3u64, &30u64);
    let combined = a + b + c;
    assert_eq!(combined - b - a - c, Fingerprint::ZERO);
    assert_eq!(combined - c, a + b);
}

#[test]
fn add_is_commutative_and_associative() {
    let a = lift(&1u64, &10u64);
    let b = lift(&2u64, &20u64);
    let c = lift(&3u64, &30u64);
    assert_eq!(a + b, b + a);
    assert_eq!((a + b) + c, a + (b + c));
}

#[test]
fn neg_is_additive_inverse() {
    let a = lift(&7u64, &"x");
    assert_eq!(a + (-a), Fingerprint::ZERO);
    assert_eq!(-(-a), a);
}

#[test]
fn add_propagates_carry_across_limbs() {
    let all_ones = Fingerprint([u64::MAX; 4]);
    // (2²⁵⁶ - 1) + 1 wraps to 0.
    assert_eq!(all_ones + Fingerprint([1, 0, 0, 0]), Fingerprint::ZERO);
    // Carry out of limb 0 lands in limb 1.
    assert_eq!(
        Fingerprint([u64::MAX, 0, 0, 0]) + Fingerprint([1, 0, 0, 0]),
        Fingerprint([0, 1, 0, 0])
    );
}

#[test]
fn sub_borrows_across_limbs() {
    // 0 - 1 wraps to 2²⁵⁶ - 1 (all limbs MAX).
    assert_eq!(
        Fingerprint::ZERO - Fingerprint([1, 0, 0, 0]),
        Fingerprint([u64::MAX; 4])
    );
}

// Golden vectors: changing these is a wire break, not a refactor.

#[test]
fn golden_element_hash() {
    assert_eq!(
        lift(&50u64, &"Hello"),
        Fingerprint([
            0x5983_c089_4de2_aacf,
            0xa3b7_5857_a517_c2a4,
            0xf30c_219d_d2d5_d655,
            0xc269_e4a2_cb9e_3aa1,
        ])
    );
}

#[test]
fn golden_combined_fingerprint() {
    let combined = lift(&25u64, &"World!") + lift(&50u64, &"Hello") + lift(&75u64, &"Everyone!");
    assert_eq!(
        combined,
        Fingerprint([
            0x44d8_8232_ba37_b808,
            0x3917_4386_159c_3900,
            0xd744_1273_6509_2edc,
            0x0d4a_f5d8_5402_598c,
        ])
    );
}

// Encoding properties as the protocol sees them; `encoding`'s tests check the bytes.

#[test]
fn framing_is_unambiguous() {
    assert_ne!(lift(&"ab", &"c"), lift(&"a", &"bc"));
    assert_ne!(
        lift(&0u8, &vec![vec![1u8, 2]]),
        lift(&0u8, &vec![vec![1u8], vec![2u8]])
    );
    assert_ne!(lift(&vec![1u8, 2], &()), lift(&(vec![1u8], vec![2u8]), &()));
}

#[test]
fn hash_maps_fingerprint_independently_of_insertion_order() {
    use std::collections::{BTreeMap, HashMap};

    let mut forward = HashMap::new();
    for i in 0..16u32 {
        forward.insert(i, i * 3);
    }
    let mut backward = HashMap::new();
    for i in (0..16u32).rev() {
        backward.insert(i, i * 3);
    }
    assert_eq!(lift(&0u8, &forward), lift(&0u8, &backward));

    let ordered: BTreeMap<u32, u32> = forward.iter().map(|(k, v)| (*k, *v)).collect();
    assert_eq!(lift(&0u8, &forward), lift(&0u8, &ordered));
}

#[test]
fn enum_variants_are_distinguished_by_index() {
    #[derive(Serialize)]
    enum Payload {
        A(u32),
        B(u32),
    }
    assert_ne!(
        lift(&0u8, &Payload::A(7)),
        lift(&0u8, &Payload::B(7)),
        "same payload under different variants must differ"
    );
}

#[test]
fn none_does_not_collide_with_a_value() {
    assert_ne!(lift(&0u8, &None::<u8>), lift(&0u8, &Some(0u8)));
    assert_ne!(
        lift(&0u8, &None::<Vec<u8>>),
        lift(&0u8, &Some(Vec::<u8>::new()))
    );
    assert_ne!(
        lift(&0u8, &Some(None::<u8>)),
        lift(&0u8, &None::<Option<u8>>)
    );

    // Injectivity holds within a type, not across types.
    assert_eq!(lift(&0u8, &None::<u8>), lift(&0u8, &0u8));
}

#[test]
fn integers_of_different_widths_differ() {
    assert_ne!(lift(&0u8, &1u32), lift(&0u8, &1u64));
    assert_ne!(lift(&0u8, &1u8), lift(&0u8, &1u16));
    assert_eq!(lift(&0u8, &1u32), lift(&0u8, &1i32));
}

#[test]
fn floats_follow_bit_patterns_not_partial_eq() {
    assert_ne!(lift(&0u8, &0.0f64), lift(&0u8, &-0.0f64));
    assert_eq!(lift(&0u8, &f64::NAN), lift(&0u8, &f64::NAN));
}

#[test]
fn digest_is_lift_without_a_key_half() {
    assert_eq!(digest(&"Hello"), lift(&(), &"Hello"));
    assert_ne!(digest(&"Hello"), digest(&"Hell"));
}

// Keyed lift: closes the Wagner-grinding gap for holders of the key.

#[test]
fn lift_keyed_is_deterministic_and_injective() {
    let key = LiftKey::new([3u8; 32]);
    assert_eq!(lift_keyed(&key, &1u64, &"a"), lift_keyed(&key, &1u64, &"a"));
    assert_ne!(lift_keyed(&key, &1u64, &"a"), lift_keyed(&key, &2u64, &"a"));
    assert_ne!(lift_keyed(&key, &1u64, &"a"), lift_keyed(&key, &1u64, &"b"));
}

#[test]
fn lift_keyed_differs_across_keys_and_from_unkeyed() {
    let key_a = LiftKey::new([1u8; 32]);
    let key_b = LiftKey::new([2u8; 32]);
    // Two different keys lift the same pair to unrelated fingerprints -- the property that stops
    // an attacker who does not hold the key from predicting, and therefore grinding, a collision.
    assert_ne!(
        lift_keyed(&key_a, &1u64, &"a"),
        lift_keyed(&key_b, &1u64, &"a")
    );
    // Keying changes the output at all -- an implementation that ignored the key and fell back to
    // the unkeyed hash would defeat the whole point silently.
    assert_ne!(lift_keyed(&key_a, &1u64, &"a"), lift(&1u64, &"a"));
}

#[test]
fn digest_keyed_is_lift_keyed_without_a_key_half() {
    let key = LiftKey::new([5u8; 32]);
    assert_eq!(
        digest_keyed(&key, &"Hello"),
        lift_keyed(&key, &(), &"Hello")
    );
    assert_ne!(digest_keyed(&key, &"Hello"), digest_keyed(&key, &"Hell"));
}

#[test]
fn lift_key_debug_never_prints_the_key_material() {
    let key = LiftKey::new([0xAB; 32]);
    assert_eq!(format!("{key:?}"), "LiftKey(\"<redacted>\")");
}

/// Independent oracle: `lift_keyed` must be exactly `blake3::Hasher::new_keyed` over the same
/// canonical encoding `lift` uses -- not some other transform of the key bytes -- so a future
/// refactor of `Blake3Hasher::new` can't silently stop keying without a wire-visible change.
#[test]
fn lift_keyed_matches_an_independently_built_blake3_keyed_hasher() {
    let key_bytes = [9u8; 32];
    let key = LiftKey::new(key_bytes);
    let mut oracle = blake3::Hasher::new_keyed(&key_bytes);
    encoding::encode_into(&mut oracle, &50u64).expect("canonical encoding cannot fail");
    encoding::encode_into(&mut oracle, &"Hello").expect("canonical encoding cannot fail");
    assert_eq!(
        lift_keyed(&key, &50u64, &"Hello"),
        Fingerprint::from_le_bytes(oracle.finalize().as_bytes())
    );
}
