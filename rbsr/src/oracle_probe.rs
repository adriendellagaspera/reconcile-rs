// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! **Test-only probe policies (#356), `internal-testing`-gated.** The 2×2 that separates the two
//! properties `tests/oracle_dependent_split_vs_the_union_bound.rs` conflated: whether a stride
//! reads the fingerprint oracle, and whether its magnitude is tied to the range's span.
//!
//! | | span-independent stride | span-relative stride |
//! |---|---|---|
//! | **oracle-independent** | [`ConstantStrideSplit`], [`SpanHashedStrideSplit`] | [`FixedFanOut`](crate::FixedFanOut) (shipped) |
//! | **oracle-coupled** | [`FingerprintDerivedSplit`] | [`SpanRelativeFingerprintSplit`] |
//!
//! Every one of the four shares `shared_cutoffs` with
//! [`FixedFanOut`](crate::FixedFanOut), so the single variable across the table is *how a SPLIT
//! chooses its stride* — never *when* a range is enumerated instead of split.
//!
//! None of these is a shipped policy. Since #352 [`Comparison`] carries no public accessor
//! returning a fingerprint, so the oracle-coupled column cannot be built from outside this crate;
//! it exists only behind `internal-testing`, this crate's test-only door.

use crate::policy::{shared_cutoffs, Comparison, Decision, RefinementPolicy, SplitStride};

/// The width of the span-independent probes' stride range: strides land in `1..=STRIDE_SPREAD`.
///
/// Named once because [`FingerprintDerivedSplit`] and [`SpanHashedStrideSplit`] must draw from the
/// *same* support for the second to be a control for the first.
pub const STRIDE_SPREAD: u64 = 32;

/// Fibonacci hashing: scatter a small count over the whole word, then read the **high** bits.
///
/// Emphatically not a range digest — the input is a count of elements, which
/// [`Comparison::span`] already exposes as a soundness-safe quantity. Both halves are
/// load-bearing and neither is decoration: multiplying by an odd constant leaves the low bits
/// almost unmoved, so the shift is what turns the product into an avalanche.
///
/// Deliberately two operations rather than a full SplitMix64 finalizer. Over a span this small
/// every `x ^= x >> k` step of that finalizer is the identity — `x >> 30` is zero for any span
/// under a billion — so those steps would be unreachable code that no test over a realistic span
/// could distinguish from any mutation of it.
fn mix(x: u64) -> u64 {
    x.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 40
}

/// **Oracle-coupled, span-independent** — the original #356 probe. Deliberately violates the law
/// [`Comparison`]'s docs state: `stride = 1 + fingerprint.low_limb mod 32`, read off the **local**
/// aggregate's fingerprint, so the sequence of ranges an execution compares is correlated with the
/// very oracle the skip rule's collision probability is stated over.
///
/// Its stride is drawn from `1..=32` whatever the span is, which is a *second*, independent defect:
/// see [`ConstantStrideSplit`] for the control that isolates it.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FingerprintDerivedSplit;

impl RefinementPolicy for FingerprintDerivedSplit {
    fn decide(&self, comparison: Comparison) -> Decision {
        if let Some(decision) = shared_cutoffs(comparison) {
            return decision;
        }
        let stride = 1 + comparison.local_fingerprint_limb() % STRIDE_SPREAD;
        Decision::Split(SplitStride::per_child(stride as usize))
    }
}

/// **Oracle-independent, span-independent** — a fixed `stride` for every range, however wide.
///
/// A constant is trivially "a function of the data alone", so this satisfies oracle-independence
/// of shape by construction and cannot be accused of reading the digest. It is nonetheless *not*
/// progress-making once a range's span falls to `stride` or below, which is the point: it is the
/// control that decides whether [`FingerprintDerivedSplit`]'s liveness failure is caused by the
/// oracle coupling or merely by the span-independent magnitude that came with it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConstantStrideSplit(SplitStride);

impl ConstantStrideSplit {
    /// A policy cutting every `elements` keys regardless of the span. `0` is raised to `1`.
    pub const fn per_child(elements: usize) -> ConstantStrideSplit {
        ConstantStrideSplit(SplitStride::per_child(elements))
    }

    /// The constant stride this policy cuts at.
    pub const fn stride(&self) -> SplitStride {
        self.0
    }
}

impl RefinementPolicy for ConstantStrideSplit {
    fn decide(&self, comparison: Comparison) -> Decision {
        if let Some(decision) = shared_cutoffs(comparison) {
            return decision;
        }
        Decision::Split(self.0)
    }
}

/// **Oracle-independent, span-independent** — [`FingerprintDerivedSplit`]'s stride *distribution*,
/// drawn from the span instead of the fingerprint: `stride = 1 + mix(span) mod 32`.
///
/// The tighter of the two controls. [`ConstantStrideSplit`] differs from the oracle-coupled probe
/// in both the source of the stride and its spread; this one differs in the source alone — it is a
/// deterministic function of how many elements fall in the range, which is exactly the
/// "cut by rank" property the soundness union bound needs, and it scatters over the same
/// `1..=32` support.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SpanHashedStrideSplit;

impl RefinementPolicy for SpanHashedStrideSplit {
    fn decide(&self, comparison: Comparison) -> Decision {
        if let Some(decision) = shared_cutoffs(comparison) {
            return decision;
        }
        let stride = 1 + mix(comparison.span() as u64) % STRIDE_SPREAD;
        Decision::Split(SplitStride::per_child(stride as usize))
    }
}

/// **Oracle-coupled, span-relative** — `stride = 1 + fingerprint.low_limb mod (span − 1)`, so the
/// stride lands in `1..=span−1` and every SPLIT emits at least two children.
///
/// The cell the original probe left empty. It reads the same oracle
/// [`FingerprintDerivedSplit`] does — the *choice of cut point* is fingerprint-determined, and the
/// index set an execution compares is still correlated with the digest — while keeping the joint
/// progress property every shipped policy has, so a drive under it terminates and the soundness
/// question can actually be measured on a full-size population.
///
/// `shared_cutoffs` has already returned for `span <= 1`, so `span − 1 >= 1` here.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SpanRelativeFingerprintSplit;

impl RefinementPolicy for SpanRelativeFingerprintSplit {
    fn decide(&self, comparison: Comparison) -> Decision {
        if let Some(decision) = shared_cutoffs(comparison) {
            return decision;
        }
        let stride = 1 + comparison.local_fingerprint_limb() as usize % (comparison.span() - 1);
        Decision::Split(SplitStride::per_child(stride))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rsos::{Aggregate, Fingerprint};

    /// Two aggregates of the given sizes guaranteed not to agree, the local one carrying `limb`.
    fn mismatch(local: usize, remote: usize, limb: u64) -> Comparison {
        Comparison::new(
            Aggregate::new(local, Fingerprint([limb, 0, 0, 0])),
            Aggregate::new(remote, Fingerprint([u64::MAX, 9, 9, 9])),
            0,
        )
    }

    fn stride_of<P: RefinementPolicy>(policy: &P, comparison: Comparison) -> usize {
        let Decision::Split(stride) = policy.decide(comparison) else {
            panic!("span={} must split", comparison.span());
        };
        stride.get()
    }

    /// Pins the exact formula (`1 + limb % 32`) against known limbs, not just "differs from a
    /// rank-cut policy somewhere": the mutation gate needs a witness for `+` rather than `*`, and
    /// for `%` rather than `/`.
    #[test]
    fn fingerprint_derived_stride_is_one_plus_limb_mod_32() {
        // 0 and 32 both reduce to remainder 0 (stride 1, pinning `%` over `/`, which would give 0
        // and 1); 31 and 63 both reduce to 31 (stride 32, pinning `+` over `*`, which would give
        // 0 and 0).
        for (limb, expected) in [(0u64, 1usize), (31, 32), (32, 1), (63, 32)] {
            assert_eq!(
                stride_of(&FingerprintDerivedSplit, mismatch(1_000, 2_000, limb)),
                expected,
                "limb {limb}"
            );
        }
    }

    /// The defect [`ConstantStrideSplit`] is the control for, stated as a property rather than
    /// asserted of one span: a span-independent stride stops refining once the span reaches it.
    #[test]
    fn span_independent_strides_stop_refining_below_their_own_spread() {
        for span in 2..=STRIDE_SPREAD as usize {
            // A constant at the top of the spread never refines anywhere in this window.
            assert!(
                stride_of(
                    &ConstantStrideSplit::per_child(STRIDE_SPREAD as usize),
                    mismatch(span, span + 1, 7)
                ) >= span,
                "span={span}: a constant stride of {STRIDE_SPREAD} must not refine it"
            );
        }
        // Both span-independent probes put *some* limb/span in the no-progress region, which is
        // what a span-relative stride makes impossible.
        assert!(
            (0..64u64).any(|limb| stride_of(&FingerprintDerivedSplit, mismatch(4, 5, limb)) >= 4)
        );
        assert!((2..=STRIDE_SPREAD as usize)
            .any(|span| stride_of(&SpanHashedStrideSplit, mismatch(span, span + 1, 7)) >= span));
    }

    /// Pins [`SpanRelativeFingerprintSplit`]'s exact formula, `1 + limb % (span − 1)`.
    ///
    /// `span_relative_fingerprint_stride_always_refines` below cannot do this on its own: dropping
    /// the `+ 1` leaves the stride inside `1..span` too, because `SplitStride::per_child` raises a
    /// zero stride to one. Distinguishing the two needs a witness where the remainder is non-zero,
    /// so the `+ 1` is observable rather than absorbed by that clamp.
    #[test]
    fn span_relative_fingerprint_stride_is_one_plus_limb_mod_span_minus_one() {
        // (span, limb, expected): remainder 0 pins that the clamp is not what produces the 1;
        // remainders 5 and 98 pin `+` over `*` and over `%`, each of which would drop the offset.
        for (span, limb, expected) in [
            (100usize, 99u64, 1usize),
            (100, 5, 6),
            (100, 98, 99),
            (3, 1, 2),
        ] {
            assert_eq!(
                stride_of(
                    &SpanRelativeFingerprintSplit,
                    mismatch(span, span + 1, limb)
                ),
                expected,
                "span={span}, limb={limb}"
            );
        }
    }

    /// The joint-progress property, as a property over the whole reachable input space rather
    /// than a literal: a span-relative stride always cuts at least two children.
    #[test]
    fn span_relative_fingerprint_stride_always_refines() {
        for span in 2..512usize {
            for limb in [0u64, 1, 7, 31, 32, 1_000_003, u64::MAX / 3, u64::MAX] {
                let stride = stride_of(
                    &SpanRelativeFingerprintSplit,
                    mismatch(span, span + 1, limb),
                );
                assert!(
                    (1..span).contains(&stride),
                    "span={span}, limb={limb}: stride {stride} is outside 1..{span}"
                );
                assert!(span.div_ceil(stride) >= 2, "span={span}, limb={limb}");
            }
        }
    }

    /// The oracle-coupled column must actually read the oracle, or it is not testing what it
    /// claims; the oracle-independent column must actually ignore it, same reason.
    #[test]
    fn only_the_oracle_coupled_column_reacts_to_the_fingerprint() {
        let quiet = mismatch(100, 200, 0);
        let loud = mismatch(100, 200, 17);
        assert_ne!(
            stride_of(&FingerprintDerivedSplit, quiet),
            stride_of(&FingerprintDerivedSplit, loud)
        );
        assert_ne!(
            stride_of(&SpanRelativeFingerprintSplit, quiet),
            stride_of(&SpanRelativeFingerprintSplit, loud)
        );
        assert_eq!(
            stride_of(&SpanHashedStrideSplit, quiet),
            stride_of(&SpanHashedStrideSplit, loud)
        );
        let constant = ConstantStrideSplit::per_child(7);
        assert_eq!(stride_of(&constant, quiet), stride_of(&constant, loud));
        assert_eq!(constant.stride().get(), 7);
    }

    /// `mix` must scatter *near-uniformly*, not merely reach every value: a control for
    /// [`FingerprintDerivedSplit`] has to draw from the same distribution, not just the same
    /// support. Asserted as a two-sided bound on every stride's frequency, which is also what
    /// pins each operation in `mix` — drop or alter either the multiply or the shift and the
    /// whole span range collapses onto one stride.
    #[test]
    fn the_span_hashed_stride_is_near_uniform_over_its_spread() {
        const SPANS: usize = 10_000;
        let mut counts = [0usize; STRIDE_SPREAD as usize];
        for span in 2..2 + SPANS {
            let stride = stride_of(&SpanHashedStrideSplit, mismatch(span, span + 1, 0));
            assert!(
                (1..=STRIDE_SPREAD as usize).contains(&stride),
                "span={span}: stride {stride} is outside 1..={STRIDE_SPREAD}"
            );
            counts[stride - 1] += 1;
        }
        let expected = SPANS as f64 / STRIDE_SPREAD as f64; // 312.5
                                                            // ±20% of uniform. Measured spread over these spans is 310..=315; every arithmetic
                                                            // mutation of `mix` puts all 10,000 spans on a single stride, i.e. one count at 10,000
                                                            // and the other 31 at zero.
        let (lo, hi) = ((expected * 0.8) as usize, (expected * 1.2) as usize);
        for (index, &count) in counts.iter().enumerate() {
            assert!(
                (lo..=hi).contains(&count),
                "stride {} occurred {count} times over {SPANS} spans, outside {lo}..={hi} — \
                 the span-hashed stride is not scattering uniformly",
                index + 1
            );
        }
    }
}
