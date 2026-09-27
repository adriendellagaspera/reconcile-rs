use super::*;

#[test]
fn all_profiles_reproduce_the_declared_exact_divergence() {
    for n in [64, 128] {
        for d in [0, 1, 8, 64] {
            for seed in [0, 42, 99] {
                for profile in placement::Profile::ALL {
                    let pair = placement::corpus(n, d, profile, seed);
                    assert_eq!(pair, placement::corpus(n, d, profile, seed));
                    assert_eq!(
                        diff_keys(&pair.left_rows, &pair.right_rows),
                        pair.expected_diff
                    );
                    assert_eq!(pair.expected_diff.len(), d);
                    assert_eq!(
                        set_difference_symbols(&pair.left_rows, &pair.right_rows),
                        2 * d
                    );
                }
                for scenario in mutation::Scenario::ALL {
                    let pair = mutation::corpus(n, d, scenario, seed);
                    assert_eq!(pair, mutation::corpus(n, d, scenario, seed));
                    assert_eq!(diff_keys(&pair.left, &pair.right), pair.expected_diff);
                    assert_eq!(pair.expected_diff.len(), d);
                    assert!((d..=2 * d).contains(&pair.set_difference_symbols));
                }
                for pair in [
                    cold::corpus_equal(n),
                    cold::corpus_outside_insert(n, d),
                    cold::corpus_mixed(n, d, seed),
                ] {
                    assert_eq!(diff_keys(&pair.left, &pair.right), pair.expected);
                    assert!(pair.left.windows(2).all(|w| w[0].0 < w[1].0));
                    assert!(pair.right.windows(2).all(|w| w[0].0 < w[1].0));
                }
            }
        }
    }
}

#[test]
fn reference_distinguishes_value_changes_from_business_keys() {
    let left = [(1, 10), (2, 20)];
    let right = [(1, 11), (3, 30)];
    assert_eq!(diff_keys(&left, &right), [1, 2, 3]);
    assert_eq!(set_difference_symbols(&left, &right), 4);
    let exact = ExactDifference::new(&left, &right);
    assert!(exact.matches(&left, &right));
    assert!(!exact.matches(&right, &left));
}


#[test]
fn mutation_scenarios_preserve_exact_shape_not_only_diff_count() {
    let n = 32;
    let d = 6;
    let seed = 42;

    let update = mutation::corpus(n, d, mutation::Scenario::UpdateRandom, seed);
    assert_eq!((update.left.len(), update.right.len()), (n, n));
    assert_eq!(update.set_difference_symbols, 2 * d);
    assert!(update.expected_diff.iter().all(|key| key % 2 == 0));

    let delete = mutation::corpus(n, d, mutation::Scenario::DeleteRandom, seed);
    assert_eq!((delete.left.len(), delete.right.len()), (n, n - d));
    assert_eq!(delete.set_difference_symbols, d);
    assert!(delete.expected_diff.iter().all(|key| key % 2 == 0));

    let interleaved = mutation::corpus(n, d, mutation::Scenario::InsertInterleaved, seed);
    assert_eq!((interleaved.left.len(), interleaved.right.len()), (n, n + d));
    assert_eq!(interleaved.set_difference_symbols, d);
    assert!(interleaved.expected_diff.iter().all(|key| key % 2 == 1));
    assert!(interleaved.expected_diff.iter().all(|key| *key < 2 * n as u64));

    let outside = mutation::corpus(n, d, mutation::Scenario::InsertOutsideRange, seed);
    assert_eq!((outside.left.len(), outside.right.len()), (n, n + d));
    assert_eq!(outside.set_difference_symbols, d);
    assert_eq!(
        outside.expected_diff,
        (0..d)
            .map(|ordinal| 2 * n as u64 + 1 + 2 * ordinal as u64)
            .collect::<Vec<_>>()
    );

    let balanced = mutation::corpus(n, d, mutation::Scenario::BalancedInsertDelete, seed);
    assert_eq!((balanced.left.len(), balanced.right.len()), (n - d / 2, n + d / 2));
    assert_eq!(balanced.set_difference_symbols, d);
    assert_eq!(
        balanced.expected_diff.iter().filter(|key| **key % 2 == 0).count(),
        d / 2
    );
    assert_eq!(
        balanced.expected_diff.iter().filter(|key| **key % 2 == 1).count(),
        d - d / 2
    );

    let mixed = mutation::corpus(n, d, mutation::Scenario::MixedAutonomous, seed);
    let updates = d / 3;
    let deletes = 2 * d / 3 - updates;
    let inserts = d - 2 * d / 3;
    assert_eq!((mixed.left.len(), mixed.right.len()), (n - deletes, n + inserts));
    assert_eq!(mixed.set_difference_symbols, 2 * updates + deletes + inserts);
}

#[test]
fn placement_profiles_have_exact_small_reference_shapes() {
    assert!(placement::Profile::UniformRandom.is_random());
    for profile in [
        placement::Profile::Contiguous,
        placement::Profile::Clustered4,
        placement::Profile::Clustered16,
        placement::Profile::EvenlySpaced,
        placement::Profile::MaxSpread,
    ] {
        assert!(!profile.is_random(), "{profile} must not be random");
    }

    let clustered4 = placement::corpus(64, 8, placement::Profile::Clustered4, 42);
    assert_eq!(clustered4.expected_diff, vec![7, 8, 23, 24, 39, 40, 55, 56]);

    let clustered16 = placement::corpus(64, 8, placement::Profile::Clustered16, 42);
    assert_eq!(clustered16.expected_diff, vec![3, 11, 19, 27, 35, 43, 51, 59]);

    let contiguous = placement::corpus(16, 4, placement::Profile::Contiguous, 42);
    assert_eq!(contiguous.expected_diff, vec![6, 7, 8, 9]);

    let evenly = placement::corpus(16, 4, placement::Profile::EvenlySpaced, 42);
    assert_eq!(evenly.expected_diff, vec![2, 6, 10, 14]);
}

#[test]
fn placement_value_change_uses_the_declared_digest_transform() {
    let pair = placement::corpus(16, 1, placement::Profile::Contiguous, 42);
    let key = pair.expected_diff[0];
    let left = pair.left_rows[key as usize].1;
    let right = pair.right_rows[key as usize].1;
    let expected_base = match key {
        7 => 0x54ec_826b_3c9d_6f5f,
        other => panic!("unexpected contiguous key {other}"),
    };
    assert_eq!(left, expected_base);
    assert_eq!(right, expected_base ^ 0xa5a5_5a5a_d3c3_b4b4);
    assert_ne!(right, 1);
}


#[test]
fn digest_primitives_have_external_reference_values() {
    assert_eq!(base_digest(0), 0xd6e8_feb8_6659_fd93);
    assert_eq!(changed_digest(0, 1), 0xd6e8_feb8_6659_fd96);
}

#[test]
fn mutation_seed_combination_is_xor_and_shapes_are_exact() {
    let n = 16;
    let d = 4;
    // DeleteRandom has discriminant 1: seed 1 distinguishes XOR (0) from OR (1).
    let ranks = sampled_ranks(n, d, 0);
    let expected_deleted: Vec<_> = ranks.iter().map(|rank| 2 * *rank as u64).collect();
    let delete = mutation::corpus(n, d, mutation::Scenario::DeleteRandom, 1);
    assert_eq!(delete.expected_diff, expected_deleted);

    let ranks = sampled_ranks(n, d, 42 ^ mutation::Scenario::BalancedInsertDelete as u64);
    let split = d / 2;
    let mut expected: Vec<_> = ranks[..split]
        .iter()
        .map(|rank| 2 * *rank as u64)
        .chain(ranks[split..].iter().map(|rank| 2 * *rank as u64 + 1))
        .collect();
    expected.sort_unstable();
    let balanced = mutation::corpus(n, d, mutation::Scenario::BalancedInsertDelete, 42);
    assert_eq!(balanced.expected_diff, expected);
}

#[test]
fn mixed_mutation_values_follow_rank_derived_variants() {
    let n = 18;
    let d = 6;
    let seed = 42;
    let scenario = mutation::Scenario::MixedAutonomous;
    let ranks = sampled_ranks(n, d, seed ^ scenario as u64);
    let a = d / 3;
    let b = 2 * d / 3;
    let pair = mutation::corpus(n, d, scenario, seed);
    let left: std::collections::BTreeMap<_, _> = pair.left.iter().copied().collect();
    let right: std::collections::BTreeMap<_, _> = pair.right.iter().copied().collect();

    for &rank in &ranks[..a] {
        let key = 2 * rank as u64;
        assert_eq!(left[&key], changed_digest(key, rank as u64 + 1));
    }
    for &rank in &ranks[a..b] {
        let key = 2 * rank as u64;
        assert!(!left.contains_key(&key));
        assert!(right.contains_key(&key));
    }
    for &rank in &ranks[b..] {
        let key = 2 * rank as u64 + 1;
        assert_eq!(right[&key], changed_digest(key, rank as u64 + 1));
        assert!(!left.contains_key(&key));
    }
}

#[test]
fn cold_corpora_have_exact_restart_shapes() {
    let outside = cold::corpus_outside_insert(8, 3);
    assert_eq!(outside.expected, vec![17, 19, 21]);
    assert_eq!((outside.left.len(), outside.right.len()), (8, 11));

    let n = 18;
    let d = 6;
    let seed = 42;
    let ranks = sampled_ranks(n, d, seed);
    let a = d / 3;
    let b = 2 * d / 3;
    let mixed = cold::corpus_mixed(n, d, seed);
    assert_eq!((mixed.left.len(), mixed.right.len()), (n - (b - a), n + (d - b)));
    let left: std::collections::BTreeMap<_, _> = mixed.left.iter().copied().collect();
    let right: std::collections::BTreeMap<_, _> = mixed.right.iter().copied().collect();

    for &rank in &ranks[..a] {
        let key = 2 * rank as u64;
        assert_eq!(left[&key], changed_digest(key, rank as u64 + 1));
    }
    for &rank in &ranks[a..b] {
        assert!(!left.contains_key(&(2 * rank as u64)));
    }
    for &rank in &ranks[b..] {
        let key = 2 * rank as u64 + 1;
        assert_eq!(right[&key], changed_digest(key, rank as u64 + 1));
    }
}
