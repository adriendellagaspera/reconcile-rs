// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::clock::{Hlc, LogicalCounter, NodeId, PhysicalTime, Timestamp};
use crate::entry::{Entry, State};
use crate::replica::Replica;
use crate::replicated_map::Config;

use super::super::{Message, MAX_MESSAGES_PER_DATAGRAM};

async fn engine(addr: &str) -> Replica<i32, i32> {
    let config = Config::default()
        .with_port(crate::replica::tests::next_ephemeral_test_port())
        .with_listen_addr(addr.parse().unwrap())
        .with_insecure_no_key();
    Replica::new(config).await.expect("bind failed")
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
    let appended = eng.resend_held_tombstone_acks(&mut send_buf, 0);

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

/// The resend window starts at `round % n` across the *sorted* tombstone keys, not some other
/// index arithmetic that happens to coincide with it when the whole window fits in one round
/// (`returned_count_matches_acks_actually_appended` above never truncates, so it can't tell
/// `round % n` apart from e.g. `round / n` or `round + n` -- both reduce to the same visited
/// key set when nothing is dropped). Forcing a real byte-budget truncation here, with `n` large
/// enough that only a slice of the window fits, makes the *first* key actually resent depend on
/// which arithmetic computed the start.
#[tokio::test]
async fn resend_window_starts_at_round_modulo_tombstone_count() {
    let eng = engine("127.0.0.161").await;
    // Comfortably more than TOMBSTONE_ACK_RESEND_BYTE_BUDGET (8 KiB) worth of Ack messages, so
    // the byte budget truncates the window well before it wraps back around.
    let n: i32 = 2000;
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

    let round: u32 = 733; // 733 % 2000 == 733; neither 733 / 2000 (== 0) nor 733 + 2000 matches it.
    let mut send_buf = Vec::new();
    let appended = eng.resend_held_tombstone_acks(&mut send_buf, round);
    assert!(
        appended < n as usize,
        "test setup: expected the byte budget to truncate the window well before {n} keys"
    );

    let decoded: Vec<Message<i32, Entry<Timestamp, i32>, State<i32>>> =
        gossip::bincode::decode_stream(&send_buf, MAX_MESSAGES_PER_DATAGRAM)
            .expect("resend_held_tombstone_acks writes valid Message encodings");
    let first_key = decoded
        .iter()
        .find_map(|m| match m {
            Message::TombstoneAck((k, _)) => Some(*k),
            _ => None,
        })
        .expect("at least one Ack must have been written");
    assert_eq!(
        first_key,
        round as i32 % n,
        "the resend window must start at round % n across the sorted tombstone keys"
    );
}
