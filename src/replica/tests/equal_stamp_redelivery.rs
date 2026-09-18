// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use gossip::auth;

use super::deadlock_regressions::update_message_bytes;
use crate::clock::{Hlc, LogicalCounter, NodeId, PhysicalTime, Timestamp};
use crate::entry::{Entry, State};
use crate::replica::{version_hash, Message, Replica, MAX_MESSAGES_PER_DATAGRAM};
use crate::replicated_map::Config;

/// A remote `Update` whose stamp exactly equals the locally-held stamp must not be treated as
/// newer — `handle_messages` only re-applies on a *strictly greater* stamp. Equal stamps only
/// arise from an exact re-delivery of the same write (a retried or duplicated datagram); treating
/// them as newer would make every such duplicate an unbounded re-apply. Asserted via the
/// pre-insert hook and the stored value, the only externally observable effects of the internal
/// `>` comparison.
#[tokio::test]
async fn equal_stamp_update_is_not_reapplied() {
    let config = Config::default()
        .with_port(crate::replica::tests::next_ephemeral_test_port())
        .with_listen_addr("127.0.0.150".parse().unwrap())
        .with_insecure_no_key();
    let engine = Replica::<i32, u8>::new(config).await.expect("bind failed");

    let stamp = Timestamp::new(
        Hlc::new(PhysicalTime::from_millis(1_000), LogicalCounter::new(0)),
        NodeId::new(0),
    );
    let key = 7;
    engine.just_insert(key, Entry::present(stamp, 1));

    let calls = Arc::new(AtomicUsize::new(0));
    let calls2 = calls.clone();
    *engine.pre_insert.write() = Box::new(move |_: &i32, _: &Entry<Timestamp, u8>| {
        calls2.fetch_add(1, Ordering::SeqCst);
    });

    // Same key, same stamp, a different value: only the stamp comparison decides whether this
    // is re-applied, so the value must not matter.
    let bytes = update_message_bytes(key, Entry::present(stamp, 2));
    let payload = auth::Authenticator::new(None, false)
        .unwrap()
        .open(&bytes)
        .expect("unauthenticated mode clears any datagram")
        .check_version()
        .expect("update_message_bytes stamps the current wire version");
    let peer: SocketAddr = "127.0.0.151:9".parse().unwrap();
    let payload = payload
        .verify_replay(&engine.replay_filter, peer.ip())
        .expect("unauthenticated mode is exempt from the replay check");
    let mut send_buf = Vec::new();
    engine.handle_messages(payload, peer, &mut send_buf).await;

    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "an equal-stamp Update must not be re-applied (pre-insert hook must not run)"
    );
    assert_eq!(
        engine
            .map
            .load_full()
            .get(&key)
            .and_then(|v| v.value().copied()),
        Some(1),
        "an equal-stamp Update must not overwrite the locally-held value"
    );
}

/// A newer remote tombstone is integrated and acknowledged immediately. This pins the outbound
/// ack guard in `apply_dated_updates`: a non-empty ack batch must be sent, not suppressed.
#[tokio::test]
async fn newer_remote_tombstone_is_acked() {
    let config = Config::default()
        .with_port(crate::replica::tests::next_ephemeral_test_port())
        .with_listen_addr("127.0.0.152".parse().unwrap())
        .with_insecure_no_key();
    let engine = Replica::<i32, u8>::new(config).await.expect("bind failed");

    let old_stamp = Timestamp::new(
        Hlc::new(PhysicalTime::from_millis(1_000), LogicalCounter::new(0)),
        NodeId::new(0),
    );
    let new_stamp = Timestamp::new(
        Hlc::new(PhysicalTime::from_millis(2_000), LogicalCounter::new(0)),
        NodeId::new(1),
    );
    let key = 9;
    engine.just_insert(key, Entry::present(old_stamp, 1));

    let tombstone = Entry::tombstone(new_stamp);
    let expected_version = version_hash(&tombstone);
    let bytes = update_message_bytes(key, tombstone);
    let payload = auth::Authenticator::new(None, false)
        .unwrap()
        .open(&bytes)
        .expect("unauthenticated mode clears any datagram")
        .check_version()
        .expect("update_message_bytes stamps the current wire version");
    let peer: SocketAddr = "127.0.0.153:9".parse().unwrap();
    let payload = payload
        .verify_replay(&engine.replay_filter, peer.ip())
        .expect("unauthenticated mode is exempt from the replay check");

    let mut send_buf = Vec::new();
    engine.handle_messages(payload, peer, &mut send_buf).await;

    let decoded: Vec<Message<i32, Entry<Timestamp, u8>, State<u8>>> =
        gossip::bincode::decode_stream(&send_buf, MAX_MESSAGES_PER_DATAGRAM)
            .expect("the tombstone acknowledgement must be a valid message");
    assert!(
        decoded.iter().any(|message| matches!(
            message,
            Message::TombstoneAck((ack_key, version))
                if *ack_key == key && *version == expected_version
        )),
        "a newly integrated remote tombstone must be acknowledged immediately: {decoded:?}"
    );
}
