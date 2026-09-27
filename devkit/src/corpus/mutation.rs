//! Mutations over even baseline keys; odd keys permit interleaved insertions.
use super::{
    base_digest, baseline, changed_digest, diff_keys, sampled_ranks, set_difference_symbols,
};
use std::collections::BTreeMap;
use std::fmt;

#[derive(Clone, Copy, Debug)]
pub enum Scenario {
    UpdateRandom,
    DeleteRandom,
    InsertInterleaved,
    InsertOutsideRange,
    BalancedInsertDelete,
    MixedAutonomous,
}

impl Scenario {
    pub const ALL: [Self; 6] = [
        Self::UpdateRandom,
        Self::DeleteRandom,
        Self::InsertInterleaved,
        Self::InsertOutsideRange,
        Self::BalancedInsertDelete,
        Self::MixedAutonomous,
    ];
}

impl fmt::Display for Scenario {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::UpdateRandom => "update-random",
            Self::DeleteRandom => "delete-random",
            Self::InsertInterleaved => "insert-interleaved",
            Self::InsertOutsideRange => "insert-outside-range",
            Self::BalancedInsertDelete => "balanced-insert-delete",
            Self::MixedAutonomous => "mixed-autonomous",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Corpus {
    pub left: Vec<(u64, u64)>,
    pub right: Vec<(u64, u64)>,
    pub expected_diff: Vec<u64>,
    pub set_difference_symbols: usize,
}

pub fn corpus(n: usize, d: usize, scenario: Scenario, seed: u64) -> Corpus {
    assert!(d <= n, "d must not exceed baseline n");
    let base = baseline(n);
    let mut left: BTreeMap<_, _> = base.iter().copied().collect();
    let mut right = left.clone();
    let ranks = sampled_ranks(n, d, seed ^ scenario as u64);

    match scenario {
        Scenario::UpdateRandom => {
            for &rank in &ranks {
                let key = 2 * rank as u64;
                right.insert(key, changed_digest(key, rank as u64 + 1));
            }
        }
        Scenario::DeleteRandom => {
            for &rank in &ranks {
                right.remove(&(2 * rank as u64));
            }
        }
        Scenario::InsertInterleaved => {
            for &rank in &ranks {
                let key = 2 * rank as u64 + 1;
                right.insert(key, base_digest(key));
            }
        }
        Scenario::InsertOutsideRange => {
            for ordinal in 0..d {
                let key = 2 * n as u64 + 1 + 2 * ordinal as u64;
                right.insert(key, base_digest(key));
            }
        }
        Scenario::BalancedInsertDelete => {
            let deletes = d / 2;
            for &rank in &ranks[..deletes] {
                left.remove(&(2 * rank as u64));
            }
            for &rank in &ranks[deletes..] {
                let key = 2 * rank as u64 + 1;
                right.insert(key, base_digest(key));
            }
        }
        Scenario::MixedAutonomous => {
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
        }
    }

    let left: Vec<_> = left.into_iter().collect();
    let right: Vec<_> = right.into_iter().collect();
    let expected_diff = diff_keys(&left, &right);
    assert_eq!(
        expected_diff.len(),
        d,
        "business-level divergence must stay fixed at d"
    );

    Corpus {
        set_difference_symbols: set_difference_symbols(&left, &right),
        left,
        right,
        expected_diff,
    }
}
