// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Receipt, even checkpointed locally, cannot exclude first delivery of stale application data.

use super::*;

#[test]
fn unseen_stale_signed_payload_after_receipt_quorum_cannot_resurrect_a_delete() {
    let a_id = signer(201).peer_id();
    let b_id = signer(202).peer_id();
    let mut a = make(201, SignedConfig::new([b_id]), memory());
    let mut b = make(202, SignedConfig::new([a_id]), memory());
    let now = Instant::now();
    let (a_addr, b_addr) = ((71, 1), (72, 2));
    connect(&mut a, a_addr, &mut b, b_addr, now);
    a.insert(7, "old".into()).unwrap();
    deliver(&mut a, a_addr, &mut b, b_addr, now);
    let delayed = b.reconcile(now).unwrap();
    assert!(!delayed.is_empty());

    a.remove(7).unwrap();
    deliver(&mut a, a_addr, &mut b, b_addr, now);
    assert!(a.has_all_tombstone_acks(&7));
    a.snapshot_now().unwrap();
    for frame in delayed {
        // This frame's counter has never been seen; transport replay rejection alone is
        // insufficient. The held deletion must still dominate its older application stamp.
        let replies = a.receive(b_addr, &frame.bytes, now).unwrap();
        assert!(
            !replies.is_empty(),
            "the stale payload actually reached application handling"
        );
    }
    assert!(a.get(&7).is_none());
    assert!(a.is_tombstone(&7));
}
