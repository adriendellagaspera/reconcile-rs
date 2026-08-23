// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! `ARCHITECTURE.md` §5 invariant 13's own coverage (`shipped_policies_always_progress.rs`) only
//! drives *shipped* policies, which all hold the progress law by construction. This test drives
//! two oracle-independent, non-progressing policies instead — the class of policy the driver's
//! guard exists to defend against — and the two cases where only one of two peers runs one, which
//! `shipped_policies_always_progress.rs`'s single-policy convergence matrix cannot reach at all.
//!
//! `ConstantStrideSplit`/`SpanHashedStrideSplit` are `reconcile_internal_testing`-only probe
//! policies (deliberately not shippable) that violate the progress law on their own; the guard
//! (`protocol_round_with_policy` converting a non-progressing `Split` to an `Enumerate`) is what
//! must keep every drive below settling despite that.

#![forbid(unsafe_code)]
#![cfg(reconcile_internal_testing)]

use rand::rngs::StdRng;
use rand::SeedableRng;

use rbsr::{
    balanced_swap, drive, drive_pair, ConstantStrideSplit, FixedFanOut, NarrowStore,
    SpanHashedStrideSplit, Termination, DRIVE_STORE_SIZE, STRIDE_SPREAD,
};

#[test]
fn the_progress_guard_settles_the_two_cases_its_own_tests_miss() {
    let shipped = FixedFanOut::default();
    let deviant = ConstantStrideSplit::per_child(STRIDE_SPREAD as usize);
    for seed in 0..256u64 {
        let mut rng = StdRng::seed_from_u64(seed);
        let (a_keys, b_keys) = balanced_swap(&mut rng, DRIVE_STORE_SIZE, 1);
        let a = NarrowStore::new(16, a_keys);
        let b = NarrowStore::new(16, b_keys);
        for (case, termination) in [
            (
                "oracle-independent constant stride",
                drive(&a, &b, &deviant).termination,
            ),
            (
                "oracle-independent span-hashed stride",
                drive(&a, &b, &SpanHashedStrideSplit).termination,
            ),
            (
                "deviant peer A only",
                drive_pair(&a, &b, &deviant, &shipped).termination,
            ),
            (
                "deviant peer B only",
                drive_pair(&a, &b, &shipped, &deviant).termination,
            ),
        ] {
            assert_eq!(
                termination,
                Termination::Settled,
                "seed {seed} ({case}): the driver's progress guard must settle this"
            );
        }
    }
}
