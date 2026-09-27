//! Frozen deterministic benchmark data and independent exact references.
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use std::collections::{BTreeMap, BTreeSet};
pub mod cold;
pub mod mutation;
pub mod placement;
mod verification;
pub use verification::ExactDifference;

pub fn base_digest(key: u64) -> u64 {
    key.wrapping_mul(0x9e37_79b9_7f4a_7c15).rotate_left(17) ^ 0xd6e8_feb8_6659_fd93
}

pub fn changed_digest(key: u64, variant: u64) -> u64 {
    base_digest(key).wrapping_add(variant.wrapping_mul(2).wrapping_add(1))
}

pub fn baseline(n: usize) -> Vec<(u64, u64)> {
    (0..n)
        .map(|rank| {
            let key = 2 * rank as u64;
            (key, base_digest(key))
        })
        .collect()
}

pub fn sampled_ranks(n: usize, count: usize, seed: u64) -> Vec<usize> {
    assert!(count <= n);
    let mut ranks: Vec<_> = (0..n).collect();
    let mut rng = StdRng::seed_from_u64(seed);
    ranks.shuffle(&mut rng);
    ranks.truncate(count);
    ranks.sort_unstable();
    ranks
}

pub fn diff_keys(left: &[(u64, u64)], right: &[(u64, u64)]) -> Vec<u64> {
    let left: BTreeMap<_, _> = left.iter().copied().collect();
    let right: BTreeMap<_, _> = right.iter().copied().collect();
    left.keys()
        .chain(right.keys())
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|key| left.get(key) != right.get(key))
        .collect()
}

pub fn set_difference_symbols(left: &[(u64, u64)], right: &[(u64, u64)]) -> usize {
    let left: BTreeSet<_> = left.iter().copied().collect();
    let right: BTreeSet<_> = right.iter().copied().collect();
    left.symmetric_difference(&right).count()
}

#[cfg(test)]
mod tests;
