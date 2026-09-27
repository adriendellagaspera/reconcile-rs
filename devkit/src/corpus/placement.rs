//! Aligned-key update corpora; max-spread is a stress profile, not a worst-case proof.
use super::base_digest;
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use std::collections::BTreeSet;
use std::fmt;
const MERKLE_FANOUT: usize = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Profile {
    Contiguous,
    Clustered4,
    Clustered16,
    UniformRandom,
    EvenlySpaced,
    MaxSpread,
}

impl Profile {
    pub const ALL: [Self; 6] = [
        Self::Contiguous,
        Self::Clustered4,
        Self::Clustered16,
        Self::UniformRandom,
        Self::EvenlySpaced,
        Self::MaxSpread,
    ];

    pub fn parse(raw: &str) -> Self {
        match raw.trim() {
            "contiguous" => Self::Contiguous,
            "clustered-4" => Self::Clustered4,
            "clustered-16" => Self::Clustered16,
            "uniform-random" => Self::UniformRandom,
            "evenly-spaced" => Self::EvenlySpaced,
            "max-spread" => Self::MaxSpread,
            other => panic!("unknown state-repair profile: {other}"),
        }
    }

    pub fn is_random(self) -> bool {
        self == Self::UniformRandom
    }
}

impl fmt::Display for Profile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Contiguous => "contiguous",
            Self::Clustered4 => "clustered-4",
            Self::Clustered16 => "clustered-16",
            Self::UniformRandom => "uniform-random",
            Self::EvenlySpaced => "evenly-spaced",
            Self::MaxSpread => "max-spread",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Corpus {
    pub expected_diff: Vec<u64>,
    pub left_rows: Vec<(u64, u64)>,
    pub right_rows: Vec<(u64, u64)>,
}

fn changed_digest(key: u64) -> u64 {
    base_digest(key) ^ 0xa5a5_5a5a_d3c3_b4b4
}

fn evenly_spaced_keys(n: usize, d: usize) -> Vec<u64> {
    (0..d)
        .map(|i| (((2 * i + 1) * n) / (2 * d)) as u64)
        .collect()
}

fn contiguous_keys(n: usize, d: usize) -> Vec<u64> {
    let start = (n - d) / 2;
    (start..start + d).map(|key| key as u64).collect()
}

fn clustered_keys(n: usize, d: usize, clusters: usize) -> Vec<u64> {
    let active_clusters = clusters.min(d).min(n);
    if active_clusters == 0 {
        return Vec::new();
    }

    let mut keys = Vec::with_capacity(d);
    let mut remaining = d;
    for cluster in 0..active_clusters {
        let slots_left = active_clusters - cluster;
        let count = remaining.div_ceil(slots_left);
        let partition_start = cluster * n / active_clusters;
        let partition_end = (cluster + 1) * n / active_clusters;
        let partition_len = partition_end - partition_start;
        assert!(
            count <= partition_len,
            "cluster does not fit its key partition"
        );
        let start = partition_start + (partition_len - count) / 2;
        keys.extend((start..start + count).map(|key| key as u64));
        remaining -= count;
    }
    keys.sort_unstable();
    keys
}

fn random_keys(n: usize, d: usize, seed: u64) -> Vec<u64> {
    let mut ranks: Vec<_> = (0..n).collect();
    let mut rng = StdRng::seed_from_u64(seed);
    ranks.shuffle(&mut rng);
    ranks.truncate(d);
    ranks.sort_unstable();
    ranks.into_iter().map(|rank| rank as u64).collect()
}

fn reversed_base16(mut value: usize, digits: usize) -> usize {
    let mut reversed = 0usize;
    for _ in 0..digits {
        reversed = reversed * MERKLE_FANOUT + value % MERKLE_FANOUT;
        value /= MERKLE_FANOUT;
    }
    reversed
}

fn max_spread_keys(n: usize, d: usize) -> Vec<u64> {
    if d == 0 {
        return Vec::new();
    }

    // Minimum hexadecimal width able to represent every rank in 0..n. Computing it directly
    // avoids an equivalent boundary form at exact powers of the fanout.
    let bits = (usize::BITS - (n - 1).leading_zeros()) as usize;
    let digits = bits.div_ceil(4).max(1);
    let space = MERKLE_FANOUT.pow(digits as u32);

    let mut keys = Vec::with_capacity(d);
    for ordinal in 0..space {
        let rank = reversed_base16(ordinal, digits);
        if rank < n {
            keys.push(rank as u64);
            if keys.len() == d {
                break;
            }
        }
    }
    assert_eq!(keys.len(), d, "max-spread permutation did not cover d keys");
    keys.sort_unstable();
    keys
}

fn divergent_keys(n: usize, d: usize, profile: Profile, seed: u64) -> Vec<u64> {
    assert!(d <= n, "d must not exceed n");
    if d == 0 {
        return Vec::new();
    }

    let keys = match profile {
        Profile::Contiguous => contiguous_keys(n, d),
        Profile::Clustered4 => clustered_keys(n, d, 4),
        Profile::Clustered16 => clustered_keys(n, d, 16),
        Profile::UniformRandom => random_keys(n, d, seed),
        Profile::EvenlySpaced => evenly_spaced_keys(n, d),
        Profile::MaxSpread => max_spread_keys(n, d),
    };
    assert_eq!(keys.len(), d);
    assert!(keys.windows(2).all(|window| window[0] < window[1]));
    keys
}

pub fn corpus(n: usize, d: usize, profile: Profile, seed: u64) -> Corpus {
    let expected_diff = divergent_keys(n, d, profile, seed);
    let changed: BTreeSet<_> = expected_diff.iter().copied().collect();
    let left_rows: Vec<_> = (0..n as u64).map(|key| (key, base_digest(key))).collect();
    let right_rows = left_rows
        .iter()
        .map(|&(key, digest)| {
            if changed.contains(&key) {
                (key, changed_digest(key))
            } else {
                (key, digest)
            }
        })
        .collect();
    Corpus {
        expected_diff,
        left_rows,
        right_rows,
    }
}
