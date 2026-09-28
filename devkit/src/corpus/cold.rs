//! Restart corpora preserving the original seed and row conventions.
use super::{base_digest, baseline, changed_digest, diff_keys, sampled_ranks};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Corpus {
    pub name: &'static str,
    pub left: Vec<(u64, u64)>,
    pub right: Vec<(u64, u64)>,
    pub expected: Vec<u64>,
}

pub fn corpus_equal(n: usize) -> Corpus {
    let rows = baseline(n);
    Corpus {
        name: "equal",
        left: rows.clone(),
        right: rows,
        expected: Vec::new(),
    }
}

pub fn corpus_outside_insert(n: usize, d: usize) -> Corpus {
    let left = baseline(n);
    let mut right = left.clone();
    right.extend((0..d).map(|ordinal| {
        let key = 2 * n as u64 + 1 + 2 * ordinal as u64;
        (key, base_digest(key))
    }));
    let expected = diff_keys(&left, &right);
    Corpus {
        name: "outside-insert",
        left,
        right,
        expected,
    }
}

pub fn corpus_mixed(n: usize, d: usize, seed: u64) -> Corpus {
    let base = baseline(n);
    let mut left: BTreeMap<_, _> = base.iter().copied().collect();
    let mut right = left.clone();
    let ranks = sampled_ranks(n, d, seed);
    let a = d / 3;
    let b = 2 * d / 3;

    for &rank in &ranks[..a] {
        let key = 2 * rank as u64;
        left.insert(key, changed_digest(key, rank as u64 + 1));
    }
    for &rank in &ranks[a..b] {
        left.remove(&(2 * rank as u64));
    }
    for &rank in &ranks[b..] {
        let key = 2 * rank as u64 + 1;
        right.insert(key, changed_digest(key, rank as u64 + 1));
    }

    let left: Vec<_> = left.into_iter().collect();
    let right: Vec<_> = right.into_iter().collect();
    let expected = diff_keys(&left, &right);
    Corpus {
        name: "mixed-random",
        left,
        right,
        expected,
    }
}
