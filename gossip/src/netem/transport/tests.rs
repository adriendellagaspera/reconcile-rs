// Copyright 2026 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use super::*;

#[test]
fn pending_equality_is_by_due_and_seq_only() {
    let due = Instant::now();
    let a = Pending {
        due,
        seq: 1,
        destination: "127.0.0.1:1000".parse().unwrap(),
        bytes: vec![1, 2, 3],
    };
    let same_due_and_seq = Pending {
        due,
        seq: 1,
        destination: "127.0.0.1:2000".parse().unwrap(),
        bytes: vec![9],
    };
    assert_eq!(
        a, same_due_and_seq,
        "destination/bytes must not affect ordering-derived equality"
    );

    let different_seq = Pending {
        due,
        seq: 2,
        destination: a.destination,
        bytes: a.bytes.clone(),
    };
    assert_ne!(a, different_seq);

    let different_due = Pending {
        due: due + Duration::from_millis(1),
        seq: a.seq,
        destination: a.destination,
        bytes: a.bytes.clone(),
    };
    assert_ne!(a, different_due);
}

#[test]
fn coarse_wait_worthwhile_is_false_at_and_before_the_tie() {
    let now = Instant::now();
    assert!(!coarse_wait_worthwhile(now, now));
    assert!(!coarse_wait_worthwhile(now - Duration::from_nanos(1), now));
    assert!(coarse_wait_worthwhile(now + Duration::from_nanos(1), now));
}
