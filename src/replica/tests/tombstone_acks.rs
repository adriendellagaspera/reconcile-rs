// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::net::IpAddr;

use bincode::{DefaultOptions, Deserializer};
use serde::Deserialize;

use crate::clock::{Hlc, LogicalCounter, NodeId, PhysicalTime, Timestamp};
use crate::entry::{Entry, State};
use crate::replica::{version_hash, Message, Replica};
use crate::replicated_map::Config;

type Tombstoned = Entry<Timestamp, i32>;

async fn engine(addr: &str) -> Replica<i32, i32> {
    let config = Config::default()
        .with_port(5000)
        .with_listen_addr(addr.parse().unwrap())
        .with_insecure_no_key();
    super::in_memory_test_replica(config)
}

/// Every reconciliation round must resend an `Ack` for each tombstone the node currently
/// holds, so the tombstone receipt-ack matrix keeps converging at three or more nodes (where a
/// held tombstone is otherwise never re-advertised once two replicas agree on it).
/// Deterministic, socket-free: insert a tombstone, run one round, and inspect the datagram.
#[tokio::test]
async fn reconciliation_round_resends_acks_for_held_tombstones() {
    let eng = engine("127.0.0.66").await;
    let live_key = 1;
    let tombstone_key = 2;
    // A live value's ack must NOT be resent; a tombstone's must be.
    eng.just_insert(
        live_key,
        Entry::present(
            Timestamp::new(
                Hlc::new(PhysicalTime::from_millis(1), LogicalCounter::new(0)),
                NodeId::new(0),
            ),
            11,
        ),
    );
    let tombstone: Tombstoned = Entry::tombstone(Timestamp::new(
        Hlc::new(PhysicalTime::from_millis(2), LogicalCounter::new(0)),
        NodeId::new(0),
    ));
    let expected_version = version_hash(&tombstone);
    eng.just_insert(tombstone_key, tombstone);

    let mut buf = Vec::new();
    eng.start_reconciliation(&mut buf).await;

    // Decode every message in the datagram and collect the acks.
    let mut acks: Vec<(i32, u64)> = Vec::new();
    let mut de = Deserializer::from_slice(&buf, DefaultOptions::new());
    while let Ok(msg) = Message::<i32, Tombstoned, State<i32>>::deserialize(&mut de) {
        if let Message::TombstoneAck(ack) = msg {
            acks.push(ack);
        }
    }

    assert!(
        acks.contains(&(tombstone_key, expected_version)),
        "the round must resend the ack for the held tombstone (key {tombstone_key}); got \
         {acks:?}"
    );
    assert!(
        !acks.iter().any(|(k, _)| *k == live_key),
        "a live value's ack must never be resent; got {acks:?}"
    );
}

#[tokio::test]
async fn decommission_releases_a_silent_peer() {
    let eng = engine("127.0.0.63").await;
    let live: IpAddr = "127.0.0.64".parse().unwrap();
    let gone: IpAddr = "127.0.0.65".parse().unwrap();
    let key = 9;
    let tombstone: Tombstoned = Entry::tombstone(Timestamp::new(
        Hlc::new(PhysicalTime::from_millis(1), LogicalCounter::new(0)),
        NodeId::new(0),
    ));
    let version = version_hash(&tombstone);

    eng.members.write().extend([live, gone]);
    eng.tombstone_acks
        .write()
        .entry(key)
        .or_default()
        .insert(live, version);
    eng.tombstone_acks
        .write()
        .entry(key)
        .or_default()
        .insert(gone, version);

    eng.decommission_peer(gone);

    assert!(!eng.members.read().contains(&gone));
    let acks = eng.tombstone_acks.read();
    let key_acks = acks.get(&key).unwrap();
    assert!(!key_acks.contains_key(&gone));
    assert_eq!(key_acks.get(&live), Some(&version));
}
