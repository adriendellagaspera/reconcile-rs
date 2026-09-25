// Copyright 2026 Developers of the reconcile-rs project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Progress-guard tests with deliberately non-progressing policies, including asymmetric peers.
//! The driver must convert non-narrowing split decisions to enumeration and still terminate.

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
                drive(&a, &b, &deviant, &mut StdRng::seed_from_u64(seed)).termination,
            ),
            (
                "oracle-independent span-hashed stride",
                drive(
                    &a,
                    &b,
                    &SpanHashedStrideSplit,
                    &mut StdRng::seed_from_u64(seed),
                )
                .termination,
            ),
            (
                "deviant peer A only",
                drive_pair(&a, &b, &deviant, &shipped, &mut StdRng::seed_from_u64(seed))
                    .termination,
            ),
            (
                "deviant peer B only",
                drive_pair(&a, &b, &shipped, &deviant, &mut StdRng::seed_from_u64(seed))
                    .termination,
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
