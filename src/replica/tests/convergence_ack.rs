// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! #23: an `EntryFingerprint` round that converges with nothing else to send back now gets a real
//! [`Message::ConvergenceAck`] reply, instead of leaving the sender to ride out a bounded,
//! unacknowledged retry on `repair_interval`.

use bincode::{DefaultOptions, Deserializer, Serializer};
use gossip::auth;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

use crate::clock::Timestamp;
use crate::entry::{Entry, State};
use crate::replica::Replica;
use crate::replicated_map::Config;

use super::super::Message;

type Msg = Message<i32, Entry<Timestamp, u8>, State<u8>>;

/// `ConvergenceAck` carries no payload, so its wire encoding is exactly its tag byte — pinned the
/// same way [`reserved_wire_tags`](super::reserved_wire_tags) pins tag 6's, so a future reordering
/// of `Message`'s variants is caught here rather than by a peer silently misreading tag 5.
#[test]
fn convergence_ack_pins_its_own_wire_tag() {
    const CONVERGENCE_ACK_GOLDEN: &[u8] = &[5];

    let ack: Msg = Message::ConvergenceAck;
    let mut buf = Vec::new();
    ack.serialize(&mut Serializer::new(&mut buf, DefaultOptions::new()))
        .unwrap();
    assert_eq!(
        buf, CONVERGENCE_ACK_GOLDEN,
        "Message::ConvergenceAck's wire encoding changed — this is a protocol break, not a refactor"
    );
    let mut deserializer = Deserializer::from_slice(CONVERGENCE_ACK_GOLDEN, DefaultOptions::new());
    match Msg::deserialize(&mut deserializer).unwrap() {
        Message::ConvergenceAck => {}
        other => panic!("expected ConvergenceAck, got {other:?}"),
    }
}

fn message_bytes(message: &Msg) -> Vec<u8> {
    let mut buf = vec![gossip::auth::WIRE_VERSION];
    message
        .serialize(&mut Serializer::new(&mut buf, DefaultOptions::new()))
        .unwrap();
    buf
}

/// Feed `message` to `engine` as if it had arrived from `peer`, returning the raw bytes
/// `handle_messages` wrote into its scratch send buffer -- unsealed, since `send_to_retry` seals
/// a copy rather than mutating its input (`pacing::send_messages_paced`).
async fn feed_and_capture_reply(
    engine: &Replica<i32, u8>,
    message: &Msg,
    peer: SocketAddr,
) -> Vec<u8> {
    let bytes = message_bytes(message);
    let payload = auth::Authenticator::new(None, false)
        .open(&bytes)
        .expect("unauthenticated mode clears any datagram")
        .check_version()
        .expect("message_bytes stamps the current wire version");
    let payload = payload
        .verify_replay(&engine.replay_filter, peer.ip())
        .expect("unauthenticated mode is exempt from the replay check");
    let mut send_buf = Vec::new();
    engine.handle_messages(payload, peer, &mut send_buf).await;
    send_buf
}

/// The core #23 behavior: an `EntryFingerprint` describing exactly what this (empty) engine
/// already holds converges as a pure SKIP -- `rbsr` itself has nothing to send back -- and must
/// still get an explicit `ConvergenceAck` reply rather than silence.
#[tokio::test]
async fn a_converged_comparison_round_is_acked() {
    let config = Config::default()
        .with_port(crate::replica::tests::next_ephemeral_test_port())
        .with_listen_addr("127.0.0.63".parse().unwrap())
        .with_insecure_no_key();
    let engine = Replica::<i32, u8>::new(config).await.expect("bind failed");

    // The engine's own initial_ranges, fed straight back to it, describes exactly what it
    // already holds -- guaranteed to compare equal and SKIP, whatever `rbsr`'s aggregate shape is.
    let segment = {
        let guard = engine.map.load_full();
        rbsr::initial_ranges(&*guard)
            .into_iter()
            .next()
            .expect("initial_ranges always yields exactly one whole-store segment")
    };
    let message = Message::EntryFingerprint(segment);
    let peer: SocketAddr = "127.0.0.64:9".parse().unwrap();

    let reply = feed_and_capture_reply(&engine, &message, peer).await;
    let decoded: Vec<Msg> = gossip::bincode::decode_stream(&reply, 8)
        .expect("a converged round's reply must decode as valid Message bytes");
    assert_eq!(
        decoded.len(),
        1,
        "a converged round must reply with exactly one message, not silence or a burst: {decoded:?}"
    );
    assert!(
        matches!(decoded[0], Message::ConvergenceAck),
        "a converged comparison round must ack with Message::ConvergenceAck, got {:?}",
        decoded[0]
    );
}

/// Pins `converged_with_nothing_to_send`'s `&&`, not `||`: a round that finds a real difference
/// (so `differences` is non-empty) but needs no further refinement (`out_comparison` stays empty,
/// the common case when the peer's claimed range was empty) must not *also* get a
/// `ConvergenceAck` alongside the real dump -- only a genuine pure SKIP (both empty) does. `||`
/// would ack this case too, since one side alone being empty is already enough for it.
#[tokio::test]
async fn a_round_that_finds_a_real_difference_is_not_also_acked() {
    // A cold peer's initial probe: the whole universe, claiming to hold nothing -- exactly what
    // an empty store's own `initial_ranges` looks like.
    let probe_config = Config::default()
        .with_port(crate::replica::tests::next_ephemeral_test_port())
        .with_listen_addr("127.0.0.67".parse().unwrap())
        .with_insecure_no_key();
    let empty_probe_engine = Replica::<i32, u8>::new(probe_config)
        .await
        .expect("bind failed");
    let probe_segment = {
        let guard = empty_probe_engine.map.load_full();
        rbsr::initial_ranges(&*guard)
            .into_iter()
            .next()
            .expect("initial_ranges always yields exactly one whole-store segment")
    };

    let b_config = Config::default()
        .with_port(crate::replica::tests::next_ephemeral_test_port())
        .with_listen_addr("127.0.0.68".parse().unwrap())
        .with_insecure_no_key();
    let b = Replica::<i32, u8>::new(b_config)
        .await
        .expect("bind failed");
    b.just_insert(1, Entry::present(b.clock_now(), 7));

    let message = Message::EntryFingerprint(probe_segment);
    let peer: SocketAddr = "127.0.0.69:9".parse().unwrap();
    let reply = feed_and_capture_reply(&b, &message, peer).await;
    let decoded: Vec<Msg> = gossip::bincode::decode_stream(&reply, 8)
        .expect("reply must decode as valid Message bytes");
    assert!(
        decoded.is_empty(),
        "a round that finds a real difference must not synchronously reply with anything here -- \
         the actual EntryUpdate dump goes out on a background task, and a ConvergenceAck must \
         not be added on top of it: {decoded:?}"
    );
}

/// The receiving side of the same mechanism: a `ConvergenceAck` is proof the peer engages with
/// the dated comparison protocol, exactly like a real
/// `EntryFingerprint`/`EntryUpdate`/`TombstoneAck` -- `run()` only grants peers/members
/// membership when `spoke_dated` is true (mirrors
/// [`handle_messages_return_value`](super::handle_messages_return_value)'s coverage of the other
/// message shapes).
#[tokio::test]
async fn a_convergence_ack_reports_spoke_dated() {
    let config = Config::default()
        .with_port(crate::replica::tests::next_ephemeral_test_port())
        .with_listen_addr("127.0.0.65".parse().unwrap())
        .with_insecure_no_key();
    let engine = Replica::<i32, u8>::new(config).await.expect("bind failed");
    let peer: SocketAddr = "127.0.0.66:9".parse().unwrap();

    let bytes = message_bytes(&Message::ConvergenceAck);
    let payload = auth::Authenticator::new(None, false)
        .open(&bytes)
        .expect("unauthenticated mode clears any datagram")
        .check_version()
        .expect("message_bytes stamps the current wire version");
    let payload = payload
        .verify_replay(&engine.replay_filter, peer.ip())
        .expect("unauthenticated mode is exempt from the replay check");
    let mut send_buf = Vec::new();
    let spoke_dated = engine.handle_messages(payload, peer, &mut send_buf).await;

    assert!(
        spoke_dated,
        "a ConvergenceAck must report spoke_dated = true, or run() will never grant its sender \
         peers/members membership"
    );
}
