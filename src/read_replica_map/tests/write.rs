// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::super::*;
use super::{ephemeral_config, wait_until};
use crate::clock::Timestamp;
use crate::entry::{Entry, State};
use crate::replicated_map::Config;
use gossip::replay;

/// The on-update hook fires for every integrated value, including tombstones.
#[tokio::test]
async fn on_update_hook_fires() {
    let read_replica = ReadReplicaMap::<i32, i32>::new(ephemeral_config())
        .await
        .expect("bind failed");
    let count = Arc::new(AtomicUsize::new(0));
    let count2 = count.clone();
    read_replica.set_on_update(move |_, _| {
        count2.fetch_add(1, Ordering::SeqCst);
    });
    read_replica.integrate(vec![(1, State::Present(10)), (2, State::Tombstone)]);
    assert_eq!(count.load(Ordering::SeqCst), 2);
}

/// A second `set_on_update` call replaces the first hook rather than adding to it.
#[tokio::test]
async fn set_on_update_replaces_previous_hook() {
    let read_replica = ReadReplicaMap::<i32, i32>::new(ephemeral_config())
        .await
        .expect("bind failed");
    let first_count = Arc::new(AtomicUsize::new(0));
    let second_count = Arc::new(AtomicUsize::new(0));

    let first_count2 = first_count.clone();
    read_replica.set_on_update(move |_, _| {
        first_count2.fetch_add(1, Ordering::SeqCst);
    });
    let second_count2 = second_count.clone();
    read_replica.set_on_update(move |_, _| {
        second_count2.fetch_add(1, Ordering::SeqCst);
    });

    read_replica.integrate(vec![(1, State::Present(10))]);

    assert_eq!(first_count.load(Ordering::SeqCst), 0);
    assert_eq!(second_count.load(Ordering::SeqCst), 1);
}

/// #294: `start_reconciliation` (the public, buffer-owning wrapper) must actually send a
/// value-only comparison round to the seeded peer — a mutant that no-ops its body would leave
/// the peer's socket silent forever, which this test's `recv_from` would then time out on. No
/// `run()` loop is spawned on either side, so nothing but this call can produce the datagram.
#[tokio::test]
async fn start_reconciliation_wrapper_actually_transmits() {
    use crate::transport::InMemoryNetwork;

    let net = InMemoryNetwork::new();
    let port = crate::replica::tests::next_ephemeral_test_port();
    let read_replica_addr: IpAddr = "127.0.5.2".parse().unwrap();
    let peer_addr: IpAddr = "127.0.5.3".parse().unwrap();
    let peer_transport = net.bind(SocketAddr::new(peer_addr, port));

    let read_replica = ReadReplicaMap::<i32, String>::new_with_transport(
        ephemeral_config()
            .with_port(port)
            .with_listen_addr(read_replica_addr),
        Arc::new(net.bind(SocketAddr::new(read_replica_addr, port))),
    )
    .with_seed(peer_addr);

    read_replica.start_reconciliation().await;

    let mut buf = [0u8; 65536];
    let (size, from) =
        tokio::time::timeout(Duration::from_secs(5), peer_transport.recv_from(&mut buf))
            .await
            .expect("start_reconciliation never sent anything to the seeded peer")
            .expect("recv_from failed");
    assert!(size > 0, "the datagram sent to the peer was empty");
    assert_eq!(from.ip(), read_replica_addr);
}

/// A datagram exactly [`super::super::write::BUFFER_SIZE`]-worth of message bytes is received and
/// integrated, not discarded as "too small" — the whole reason the receive buffer is sized
/// `BUFFER_SIZE + 1`, one byte more than the largest legitimate UDP payload, so a maximum-size
/// datagram never exactly fills it (which `recv_from` cannot distinguish from truncation).
#[tokio::test]
async fn a_maximum_size_datagram_is_received_not_discarded_as_too_small() {
    const BUFFER_SIZE: usize = 65507;

    let port = crate::replica::tests::next_ephemeral_test_port();
    let addr: std::net::IpAddr = "127.0.0.1".parse().unwrap();
    let read_replica = ReadReplicaMap::<i32, String>::new(
        Config::default()
            .with_port(port)
            .with_listen_addr(addr)
            .with_insecure_no_key(),
    )
    .await
    .expect("bind failed");
    let task = tokio::spawn(read_replica.clone().run());

    // Pad a `StateUpdate` so the *sealed* datagram (the version-byte-framed wire form
    // `Authenticator::seal` produces, matching what the receive loop actually expects) is
    // exactly `BUFFER_SIZE`. bincode's varint length-prefix grows with the string's own length,
    // so probe against a same-order-of-magnitude candidate and correct by the exact observed
    // delta, rather than a single fixed-overhead guess that undercounts the prefix.
    let seal = |value: &str| -> Vec<u8> {
        let message: crate::replica::Message<i32, Entry<Timestamp, String>, State<String>> =
            crate::replica::Message::StateUpdate((1, State::Present(value.to_string())));
        let mut buf = Vec::new();
        gossip::bincode::encode(&message, &mut buf).unwrap();
        read_replica
            .authenticator
            .seal(replay::Seq::NONE, replay::Stamp::NONE, &buf)
    };
    let padding_len = BUFFER_SIZE - seal("").len();
    let overshoot = seal(&"x".repeat(padding_len)).len() as i64 - BUFFER_SIZE as i64;
    let padding_len = (padding_len as i64 - overshoot) as usize;
    let padded_value = "x".repeat(padding_len);
    let payload = seal(&padded_value);
    assert_eq!(
        payload.len(),
        BUFFER_SIZE,
        "test setup: padding miscalculated"
    );

    let sender = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    sender
        .send_to(&payload, (addr, port))
        .await
        .expect("send_to a real socket should not fail");

    assert!(
        wait_until(|| read_replica.get(&1).as_deref() == Some(&padded_value)).await,
        "a maximum-size datagram must be received and integrated, not discarded as too small"
    );

    task.abort();
}
