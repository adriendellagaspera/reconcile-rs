// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::collections::BTreeSet;

use crate::clock::{Hlc, LogicalCounter, NodeId, PhysicalTime, Timestamp};
use crate::entry::{Entry, State};
use crate::replica::Replica;
use crate::replicated_map::Config;

use super::super::{Message, MAX_MESSAGES_PER_DATAGRAM};

async fn engine(addr: &str) -> Replica<i32, i32> {
    let config = Config::default()
        .with_port(5000)
        .with_listen_addr(addr.parse().unwrap())
        .with_insecure_no_key();
    super::in_memory_test_replica(config)
}

/// `resend_held_tombstone_acks`' return value is discarded by its only caller
/// (`start_reconciliation`) and otherwise feeds only a metrics counter — so a wrong count is
/// invisible short of decoding the wire output back out, which is what this asserts: the
/// returned count must equal the number of `Ack` messages actually appended to `send_buf`.
#[tokio::test]
async fn returned_count_matches_acks_actually_appended() {
    let eng = engine("127.0.0.160").await;
    let n: i32 = 5;
    for key in 0..n {
        eng.just_insert(
            key,
            Entry::tombstone(Timestamp::new(
                Hlc::new(
                    PhysicalTime::from_millis(key as u64 + 1),
                    LogicalCounter::new(0),
                ),
                NodeId::new(0),
            )),
        );
    }

    let mut send_buf = Vec::new();
    let appended = eng.resend_held_tombstone_acks(&mut send_buf);

    assert_eq!(
        appended, n as usize,
        "every held tombstone must be reported as appended when well under the byte budget"
    );

    let decoded: Vec<Message<i32, Entry<Timestamp, i32>, State<i32>>> =
        gossip::bincode::decode_stream(&send_buf, MAX_MESSAGES_PER_DATAGRAM)
            .expect("resend_held_tombstone_acks writes valid Message encodings");
    let acks = decoded
        .iter()
        .filter(|m| matches!(m, Message::TombstoneAck(_)))
        .count();
    assert_eq!(
        acks, appended,
        "the returned count must equal the number of Ack messages actually written to send_buf"
    );
}

fn decoded_ack_keys(send_buf: &[u8]) -> Vec<i32> {
    let decoded: Vec<Message<i32, Entry<Timestamp, i32>, State<i32>>> =
        gossip::bincode::decode_stream(send_buf, MAX_MESSAGES_PER_DATAGRAM)
            .expect("resend_held_tombstone_acks writes valid Message encodings");
    decoded
        .into_iter()
        .filter_map(|message| match message {
            Message::TombstoneAck((key, _)) => Some(key),
            _ => None,
        })
        .collect()
}

fn insert_tombstones(eng: &Replica<i32, i32>, keys: impl IntoIterator<Item = i32>) {
    for key in keys {
        eng.just_insert(
            key,
            Entry::tombstone(Timestamp::new(
                Hlc::new(
                    PhysicalTime::from_millis(key as u64 + 1),
                    LogicalCounter::new(0),
                ),
                NodeId::new(0),
            )),
        );
    }
}

/// A truncated resend must resume at the first key that did not fit, rather than shifting by one
/// key per reconciliation round. With fixed-size integer keys, a stable set is therefore covered
/// in roughly `ceil(n / window_capacity)` rounds.
#[tokio::test]
async fn resend_window_resumes_at_first_uncovered_key() {
    let eng = engine("127.0.0.161").await;
    let n: i32 = 2000;
    insert_tombstones(&eng, 0..n);

    let mut send_buf = Vec::new();
    let first_count = eng.resend_held_tombstone_acks(&mut send_buf);
    let first_keys = decoded_ack_keys(&send_buf);
    assert_eq!(first_keys.len(), first_count);
    assert!(
        first_count < n as usize,
        "test setup: the 8 KiB resend budget must truncate the first window"
    );
    let expected_second_start = *first_keys.last().expect("first window must contain acks") + 1;

    send_buf.clear();
    eng.resend_held_tombstone_acks(&mut send_buf);
    let second_keys = decoded_ack_keys(&send_buf);
    assert_eq!(
        second_keys.first().copied(),
        Some(expected_second_start),
        "the next window must start at the first key not covered by the previous one"
    );

    let mut seen: BTreeSet<_> = first_keys.into_iter().chain(second_keys).collect();
    let max_rounds = (n as usize).div_ceil(first_count) + 1;
    let mut rounds = 2;
    while seen.len() < n as usize && rounds < max_rounds {
        send_buf.clear();
        eng.resend_held_tombstone_acks(&mut send_buf);
        seen.extend(decoded_ack_keys(&send_buf));
        rounds += 1;
    }

    assert_eq!(
        seen.len(),
        n as usize,
        "every stable tombstone must be covered"
    );
    assert!(
        rounds <= max_rounds,
        "coverage must scale with the number of byte-bounded windows, not with n"
    );
}

/// If every tombstone at or after the cursor disappears, the insertion point is one past the
/// shortened live set and the resend cycle must wrap to its first key.
#[tokio::test]
async fn resend_cursor_wraps_when_successor_range_disappears() {
    let eng = engine("127.0.0.162").await;
    let n: i32 = 2000;
    insert_tombstones(&eng, 0..n);

    let mut send_buf = Vec::new();
    eng.resend_held_tombstone_acks(&mut send_buf);
    let first_keys = decoded_ack_keys(&send_buf);
    let cursor_key = *first_keys.last().expect("first window must contain acks") + 1;

    for key in cursor_key..n {
        eng.just_insert(
            key,
            Entry::present(
                Timestamp::new(
                    Hlc::new(
                        PhysicalTime::from_millis(10_000 + key as u64),
                        LogicalCounter::new(0),
                    ),
                    NodeId::new(0),
                ),
                7,
            ),
        );
    }

    send_buf.clear();
    eng.resend_held_tombstone_acks(&mut send_buf);
    let wrapped_keys = decoded_ack_keys(&send_buf);
    assert_eq!(
        wrapped_keys.first().copied(),
        Some(0),
        "a cursor beyond the shortened live set must wrap to the first tombstone"
    );
}

/// The cursor is a key rather than an index: if its target is resurrected before the next round,
/// ordered lookup resumes at the target's successor, while a tombstone inserted behind the cursor
/// is still covered after the window wraps.
#[tokio::test]
async fn resend_cursor_survives_live_set_changes_without_skipping_keys() {
    let eng = engine("127.0.0.162").await;
    let n: i32 = 2000;
    insert_tombstones(&eng, 0..n);

    let mut send_buf = Vec::new();
    eng.resend_held_tombstone_acks(&mut send_buf);
    let first_keys = decoded_ack_keys(&send_buf);
    let resurrected = *first_keys.last().expect("first window must contain acks") + 1;

    eng.just_insert(
        resurrected,
        Entry::present(
            Timestamp::new(
                Hlc::new(PhysicalTime::from_millis(10_000), LogicalCounter::new(0)),
                NodeId::new(0),
            ),
            7,
        ),
    );
    let inserted = n;
    insert_tombstones(&eng, [inserted]);

    send_buf.clear();
    eng.resend_held_tombstone_acks(&mut send_buf);
    let resumed_keys = decoded_ack_keys(&send_buf);
    assert_eq!(
        resumed_keys.first().copied(),
        Some(resurrected + 1),
        "a disappeared cursor key must resume at its ordered successor, not restart the cycle"
    );

    let expected: BTreeSet<_> = (0..n)
        .filter(|key| *key != resurrected)
        .chain(std::iter::once(inserted))
        .collect();
    let mut seen: BTreeSet<_> = first_keys.into_iter().chain(resumed_keys).collect();

    for _ in 0..16 {
        if expected.is_subset(&seen) {
            break;
        }
        send_buf.clear();
        eng.resend_held_tombstone_acks(&mut send_buf);
        seen.extend(decoded_ack_keys(&send_buf));
    }

    assert!(
        expected.is_subset(&seen),
        "resurrection at the cursor and insertion behind it must not permanently skip a tombstone"
    );
    assert!(
        !seen.contains(&resurrected),
        "a resurrected key must no longer be resent as a tombstone"
    );
}
