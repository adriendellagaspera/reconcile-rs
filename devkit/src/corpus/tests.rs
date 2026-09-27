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
