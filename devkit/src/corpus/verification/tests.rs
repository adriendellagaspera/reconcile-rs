use super::*;

#[test]
fn verification_rejects_missing_duplicate_wrong_and_reversed_records() {
    let a = (1, 11);
    let b = (2, 22);
    let common = (3, 33);
    let expected = ExactDifference::new(&[a, common], &[b, common]);
    assert!(expected.matches(&[a], &[b]));
    for (plus, minus) in [
        (vec![], vec![b]),
        (vec![a, a], vec![b]),
        (vec![common], vec![b]),
        (vec![b], vec![a]),
        (vec![a], vec![]),
    ] {
        assert!(!expected.matches(&plus, &minus));
    }
}


#[test]
fn verification_helper_accepts_exact_difference_and_panics_on_mismatch() {
    let expected = ExactDifference::new(&[(1, 11), (3, 33)], &[(2, 22), (3, 33)]);
    expected.verify(&[(1, 11)], &[(2, 22)]);

    let mismatch = std::panic::catch_unwind(|| expected.verify(&[], &[(2, 22)]));
    assert!(mismatch.is_err());
}
