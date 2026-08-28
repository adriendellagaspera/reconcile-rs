// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! #463 reserved wire tags 5 and 6 as skippable slots; #23 has since consumed tag 5 for a real
//! [`Message::ConvergenceAck`](super::super::Message::ConvergenceAck) (own tests:
//! [`convergence_ack`](super::convergence_ack)), leaving tag 6 as the one still reserved. What this file
//! pins for that remaining tag:
//!
//! 1. Its own encoding (golden vector) — a reordering of `Message`'s variants would move it (and
//!    every tag past it) silently, breaking the reservation.
//! 2. That a `Reserved6` message packed *alongside* a real message in one datagram does not stop
//!    the real message from being processed — the whole point of reserving a tag rather than
//!    leaving an unknown one to drop the datagram wholesale.
//! 3. That the opaque `Vec<u8>` payload's decode is bounded by the actual bytes available, not by
//!    whatever length it claims — a lying length prefix must fail cleanly, not allocate on the
//!    strength of an attacker's say-so (the #463 acceptance item this file exists to discharge).

use bincode::{DefaultOptions, Deserializer, Serializer};
use gossip::auth;
use serde::{Deserialize, Serialize};

use crate::clock::{Hlc, LogicalCounter, NodeId, PhysicalTime, Timestamp};
use crate::entry::{Entry, State};
use crate::replica::Replica;
use crate::replicated_map::Config;
use std::net::SocketAddr;

use super::super::Message;

type Msg = Message<i32, Entry<Timestamp, u8>, State<u8>>;

fn future_stamp() -> Timestamp {
    Timestamp::new(
        Hlc::new(PhysicalTime::from_millis(u64::MAX), LogicalCounter::new(0)),
        NodeId::new(0),
    )
}

/// Serialize several messages back-to-back the way `send_messages_paced` packs one datagram,
/// prefixed with the wire-version byte a real datagram always carries.
fn datagram_bytes(messages: &[Msg]) -> Vec<u8> {
    let mut buf = vec![gossip::auth::WIRE_VERSION];
    for message in messages {
        message
            .serialize(&mut Serializer::new(&mut buf, DefaultOptions::new()))
            .unwrap();
    }
    buf
}

async fn feed_datagram(engine: &Replica<i32, u8>, messages: &[Msg]) -> bool {
    let bytes = datagram_bytes(messages);
    let payload = auth::Authenticator::new(None, false)
        .unwrap()
        .open(&bytes)
        .expect("unauthenticated mode clears any datagram")
        .check_version()
        .expect("datagram_bytes stamps the current wire version");
    let peer: SocketAddr = "127.0.0.62:9".parse().unwrap();
    let payload = payload
        .verify_replay(&engine.replay_filter, peer.ip())
        .expect("unauthenticated mode is exempt from the replay check");
    let mut send_buf = Vec::new();
    engine.handle_messages(payload, peer, &mut send_buf).await
}

/// Reordering `Message`'s variants moves every wire tag past whichever moved — this pins tag 6
/// specifically so that drift is caught here, not discovered by a peer failing to decode a
/// future addition at it.
#[test]
fn reserved_tag_6_pins_its_own_encoding() {
    const RESERVED_6_GOLDEN: &[u8] = &[6, 2, 9, 9];

    let six: Msg = Message::Reserved6(vec![9, 9]);
    let mut buf = Vec::new();
    six.serialize(&mut Serializer::new(&mut buf, DefaultOptions::new()))
        .unwrap();
    assert_eq!(
        buf, RESERVED_6_GOLDEN,
        "Message::Reserved6's wire encoding changed — this is a protocol break, not a refactor"
    );
    let mut deserializer = Deserializer::from_slice(RESERVED_6_GOLDEN, DefaultOptions::new());
    match Msg::deserialize(&mut deserializer).unwrap() {
        Message::Reserved6(payload) => assert_eq!(payload, vec![9, 9]),
        other => panic!("expected Reserved6, got {other:?}"),
    }
}

/// The reservation's entire point: a peer that does not understand a message at a reserved tag
/// must still apply every *other* message the same datagram carried, not drop the datagram whole.
#[tokio::test]
async fn a_reserved_message_does_not_block_the_rest_of_the_datagram() {
    let config = Config::default()
        .with_port(crate::replica::tests::next_ephemeral_test_port())
        .with_listen_addr("127.0.0.62".parse().unwrap())
        .with_insecure_no_key();
    let engine = Replica::<i32, u8>::new(config).await.expect("bind failed");

    let real_update = Message::EntryUpdate((1, Entry::present(future_stamp(), 7)));
    let reserved = Message::Reserved6(vec![0xff; 16]);

    let spoke_dated = feed_datagram(&engine, &[reserved, real_update]).await;
    assert!(
        spoke_dated,
        "a Reserved6 message packed ahead of a real EntryUpdate must not stop the EntryUpdate \
         from being processed — the whole datagram must not be dropped for one \
         unrecognised-content tag"
    );
    assert_eq!(
        engine
            .map
            .load_full()
            .get(&1)
            .and_then(|v| v.value().copied()),
        Some(7),
        "the real EntryUpdate alongside the reserved message must actually be applied, not just \
         reported as spoke_dated"
    );
}

/// #463's acceptance item: the opaque payload's decode must be bounded by the bytes actually
/// present, not by whatever length it claims. A `Vec<u8>` field decodes through serde's own
/// `size_hint::cautious` (it never preallocates past a small, fixed cap regardless of a claimed
/// length), so a lying length prefix must fail cleanly rather than attempt a multi-gigabyte
/// allocation from a few real bytes.
#[test]
fn reserved_payload_with_a_lying_length_prefix_fails_cleanly() {
    // Tag 6, then a bincode varint claiming a payload of 10_000_000 bytes, then far fewer actual
    // bytes than that.
    let mut crafted = vec![6u8];
    crafted.extend_from_slice(&bincode::serialize(&10_000_000u64).unwrap());
    crafted.extend_from_slice(&[1, 2, 3]);

    let mut deserializer = Deserializer::from_slice(&crafted, DefaultOptions::new());
    let result = Msg::deserialize(&mut deserializer);
    assert!(
        result.is_err(),
        "a Reserved6 payload claiming far more bytes than are actually present must fail to \
         decode, not succeed with truncated or garbage data"
    );
}
