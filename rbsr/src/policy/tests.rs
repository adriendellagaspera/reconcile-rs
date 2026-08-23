// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use super::*;

use rsos::Fingerprint;

/// Two aggregates of the given sizes guaranteed not to agree, plus a fresh round budget.
fn mismatch(local: usize, remote: usize) -> Comparison {
    Comparison::new(
        Aggregate::new(local, Fingerprint([1, 0, 0, 0])),
        Aggregate::new(remote, Fingerprint([2, 0, 0, 0])),
        0,
    )
}

/// How many children a stride emits over a span, mirroring the driver's loop.
fn children(span: usize, stride: SplitStride) -> usize {
    span.div_ceil(stride.get()).max(1)
}

#[test]
fn agreeing_aggregates_are_skipped_by_every_policy() {
    let aggregate = Aggregate::new(1_000, Fingerprint([9, 9, 9, 9]));
    let agreed = Comparison::new(aggregate, aggregate, 0);
    assert_eq!(SqrtFanOut.decide(agreed), Decision::Skip);
    assert_eq!(FixedFanOut::default().decide(agreed), Decision::Skip);
    assert_eq!(
        EnumerateBelowThreshold::PAPER.decide(agreed),
        Decision::Skip
    );
}

/// The `for_testing` seam (#529) round-trips exactly what `new` was given — the whole point being
/// that a dependent crate's oracle-coupled probe policy sees the same `Aggregate` a driver built.
#[cfg(reconcile_internal_testing)]
#[test]
fn for_testing_accessors_round_trip_the_constructed_aggregates() {
    let local = Aggregate::new(7, Fingerprint([1, 2, 3, 4]));
    let remote = Aggregate::new(9, Fingerprint([5, 6, 7, 8]));
    let comparison = Comparison::new(local, remote, 0);
    assert_eq!(comparison.local_for_testing(), local);
    assert_eq!(comparison.remote_for_testing(), remote);
}

/// Matching fingerprints with mismatched sizes must not be read as agreement.
#[test]
fn matching_fingerprint_with_wrong_size_does_not_agree() {
    let comparison = Comparison::new(Aggregate::new(2, Fingerprint::ZERO), Aggregate::ZERO, 0);
    assert!(!comparison.agrees());
    assert_ne!(SqrtFanOut.decide(comparison), Decision::Skip);
}

#[test]
fn sqrt_fan_out_emits_root_m_children() {
    for span in [100usize, 400, 2_500, 1_000_000] {
        let Decision::Split(stride) = SqrtFanOut.decide(mismatch(span, span)) else {
            panic!("a mismatching range of {span} elements must split");
        };
        assert_eq!(stride.get(), (span as f32).sqrt() as usize);
        let emitted = children(span, stride);
        let root = (span as f64).sqrt() as usize;
        assert!(
            emitted >= root / 2 && emitted <= root * 2,
            "span={span}: {emitted} children, expected ~√span = {root}"
        );
    }
}

/// The child count must stop growing with the range.
#[test]
fn fixed_fan_out_is_constant_in_the_range_size() {
    let policy = FixedFanOut::default();
    for span in [100usize, 400, 2_500, 1_000_000] {
        let Decision::Split(stride) = policy.decide(mismatch(span, span)) else {
            panic!("a mismatching range of {span} elements must split");
        };
        assert!(
            children(span, stride) <= policy.fan_out().get(),
            "span={span}: {} children exceeds b={}",
            children(span, stride),
            policy.fan_out().get()
        );
        assert!(stride.get() < span, "span={span}: the split must refine");
    }
}

#[test]
fn algorithm1_enumerates_at_or_below_the_threshold_and_splits_above() {
    let policy = EnumerateBelowThreshold::new(32, FanOut::NEGENTROPY);
    for span in [0usize, 1, 31, 32] {
        assert_eq!(policy.decide(mismatch(span, 64)), Decision::Enumerate);
    }
    let Decision::Split(stride) = policy.decide(mismatch(33, 64)) else {
        panic!("a range above the threshold must split");
    };
    assert!(stride.get() < 33);
}

/// Neither a zero stride nor a fan-out of one is representable, so neither can hang the
/// protocol.
#[test]
fn degenerate_parameters_are_unrepresentable() {
    assert_eq!(SplitStride::per_child(0), SplitStride::ONE);
    assert_eq!(
        SplitStride::for_fan_out(0, FanOut::BINARY),
        SplitStride::ONE
    );
    assert_eq!(FanOut::new(0), FanOut::BINARY);
    assert_eq!(FanOut::new(1), FanOut::BINARY);
    assert_eq!(
        EnumerateBelowThreshold::new(0, FanOut::BINARY).threshold(),
        1
    );
}

/// `threshold()` reflects the constructed value, not just the degenerate `0 -> 1` case above.
#[test]
fn threshold_accessor_returns_the_constructed_value() {
    assert_eq!(
        EnumerateBelowThreshold::new(32, FanOut::BINARY).threshold(),
        32
    );
}

#[test]
fn for_fan_out_never_exceeds_the_requested_branching_factor() {
    for span in [2usize, 3, 5, 9, 10, 17, 1_000, 999_983] {
        for b in [2usize, 3, 16, 64] {
            let fan_out = FanOut::new(b);
            let stride = SplitStride::for_fan_out(span, fan_out);
            assert!(
                children(span, stride) <= b,
                "span={span}, b={b}: {} children",
                children(span, stride)
            );
            assert!(stride.get() < span, "span={span}, b={b}: must refine");
        }
    }
}
