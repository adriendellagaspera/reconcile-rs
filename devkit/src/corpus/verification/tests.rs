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
