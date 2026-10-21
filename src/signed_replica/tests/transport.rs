// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::clock::{HlcClock, NodeId};
use crate::persistence::InMemoryPersistence;
use crate::signed_replica::{PeerSigner, RoutedDatagram, SignedConfig, SignedReplica};
use crate::{ClusterKey, InMemoryNetwork, PeerId, Transport};

// Deliberately neither Copy nor an IP/socket encoding.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct OpticalSlot {
    terminal: String,
    wavelength: u8,
}

type Replica = SignedReplica<u64, String, OpticalSlot>;
const MTU: usize = 256;

fn slot(terminal: &str, wavelength: u8) -> OpticalSlot {
    OpticalSlot {
        terminal: terminal.into(),
        wavelength,
    }
}

fn replica(seed: u8, remote: PeerId) -> Replica {
    let mut config = SignedConfig::new([remote]);
    config.framing.datagram_payload_budget = MTU;
    SignedReplica::new(
        config,
        ClusterKey::new([0x7c; 32]),
        PeerSigner::from_seed([seed; 32]),
        Arc::new(HlcClock::new(NodeId::new(seed.into()))),
        Arc::new(InMemoryPersistence::new()),
    )
    .unwrap()
}

// Object-safe public I/O, carrying signed bytes without interpreting the endpoint as identity.
async fn transfer(
    sender: &dyn Transport<OpticalSlot>,
    receiver: &dyn Transport<OpticalSlot>,
    recipient: &mut Replica,
    outgoing: Vec<RoutedDatagram<OpticalSlot>>,
    now: Instant,
) -> Vec<RoutedDatagram<OpticalSlot>> {
    let mut responses = Vec::new();
    for datagram in outgoing {
        assert!(datagram.bytes.len() <= MTU);
        assert_eq!(datagram.destination, receiver.local_addr().unwrap());
        assert_eq!(
            sender
                .send_to(&datagram.bytes, &datagram.destination)
                .await
                .unwrap(),
            datagram.bytes.len()
        );
        let mut buffer = [0; MTU];
        let (read, source) =
            tokio::time::timeout(Duration::from_secs(1), receiver.recv_from(&mut buffer))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(source, sender.local_addr().unwrap());
        responses.extend(recipient.receive(source, &buffer[..read], now).unwrap());
    }
    responses
}

async fn exchange(
    a: &mut Replica,
    a_port: &dyn Transport<OpticalSlot>,
    b: &mut Replica,
    b_port: &dyn Transport<OpticalSlot>,
    mut outgoing: Vec<RoutedDatagram<OpticalSlot>>,
    now: Instant,
) {
    for _ in 0..8 {
        outgoing = transfer(a_port, b_port, b, outgoing, now).await;
        outgoing = transfer(b_port, a_port, a, outgoing, now).await;
        if outgoing.is_empty() {
            return;
        }
    }
    panic!("signed exchange did not quiesce");
}

async fn connect(
    a: &mut Replica,
    a_port: &dyn Transport<OpticalSlot>,
    b: &mut Replica,
    b_port: &dyn Transport<OpticalSlot>,
    now: Instant,
) {
    let hello = a.probe(b_port.local_addr().unwrap()).unwrap();
    exchange(a, a_port, b, b_port, vec![hello], now).await;
    let hello = b.probe(a_port.local_addr().unwrap()).unwrap();
    exchange(b, b_port, a, a_port, vec![hello], now).await;
}

#[tokio::test]
async fn signed_replication_survives_non_ip_outage_and_route_move() {
    let network = InMemoryNetwork::with_datagram_limit(NonZeroUsize::new(MTU).unwrap());
    let a_port = network.bind(slot("surface", 3));
    let b_port = network.bind(slot("vehicle", 7));
    let a_id = PeerSigner::from_seed([11; 32]).peer_id();
    let b_id = PeerSigner::from_seed([12; 32]).peer_id();
    let mut a = replica(11, b_id);
    let mut b = replica(12, a_id);
    let now = Instant::now();
    connect(&mut a, &a_port, &mut b, &b_port, now).await;

    let value = "fragmented optical contact ".repeat(64);
    a.insert(7, value.clone()).unwrap();
    let frames = a.reconcile(now).unwrap();
    assert!(frames.len() > 1, "exercise bounded non-IP fragmentation");
    exchange(&mut a, &a_port, &mut b, &b_port, frames, now).await;
    assert_eq!(b.get(&7), Some(&value));

    // The old physical link disappears. Sends are still accepted, but no ACK may release GC.
    let old_endpoint = b_port.local_addr().unwrap();
    drop(b_port);
    a.remove(7).unwrap();
    for frame in a.reconcile(now).unwrap() {
        assert_eq!(frame.destination, old_endpoint);
        a_port
            .send_to(&frame.bytes, &frame.destination)
            .await
            .unwrap();
    }
    assert!(a.is_tombstone(&7));
    assert!(!a.collect_if_stable(&7));
    assert!(b.get(&7).is_some());

    // Restore the same signer at a different endpoint; returnability must be proved again.
    let b_port = network.bind(slot("vehicle", 19));
    connect(&mut a, &a_port, &mut b, &b_port, now).await;
    let frames = a.reconcile(now).unwrap();
    assert!(!frames.is_empty());
    exchange(&mut a, &a_port, &mut b, &b_port, frames, now).await;
    assert_eq!(a.members(), HashSet::from([b_id]));
    assert_eq!(b.members(), HashSet::from([a_id]));
    assert!(b.is_tombstone(&7));
    assert!(a.acknowledged_version(&7, b_id).is_some());
    assert!(a.collect_if_stable(&7));

    assert_eq!(
        a_port
            .send_to(&vec![0; MTU + 1], &b_port.local_addr().unwrap())
            .await
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::InvalidInput
    );
}
