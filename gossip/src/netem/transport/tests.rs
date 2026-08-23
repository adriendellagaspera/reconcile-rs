// Copyright 2026 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::netem::{Link, Rtt, Seed};
use crate::InMemoryNetwork;

use super::*;

#[tokio::test]
async fn dropping_netem_transport_stops_the_pump() {
    let network = InMemoryNetwork::new();
    let sender_addr: SocketAddr = "127.0.0.1:9101".parse().unwrap();
    let receiver_addr: SocketAddr = "127.0.0.1:9102".parse().unwrap();
    let sender_inner = Arc::new(network.bind(sender_addr));
    let receiver = network.bind(receiver_addr);

    let netem = Netem::uniform(Link::at(Rtt::from_millis(20.0)), Seed::new(1));
    let sender = NetemTransport::new(sender_inner, netem);
    sender.send_to(b"hello", &receiver_addr).await.unwrap();
    // Drop before the queued datagram is due: with the pump actually aborted, it never arrives.
    drop(sender);

    let mut buf = [0u8; 16];
    let delivered =
        tokio::time::timeout(Duration::from_millis(200), receiver.recv_from(&mut buf)).await;
    assert!(
        delivered.is_err(),
        "dropping NetemTransport must cancel its pump, not just the handle"
    );
}

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
