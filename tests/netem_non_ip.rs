// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use gossip::netem::{Link, LinkPolicy, NetemTransport, Probability, Seed};
use reconcile::{InMemoryNetwork, Transport};

// Owns its label and is deliberately not Copy or convertible to an IP address.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct Slot(String);

fn slot(name: &str) -> Slot {
    Slot(name.into())
}

struct OpticalPolicy {
    outage: Arc<AtomicBool>,
}

impl LinkPolicy<Slot> for OpticalPolicy {
    fn link(&self, source: &Slot, destination: &Slot) -> (Link, Seed) {
        let seed = match (source.0.as_str(), destination.0.as_str()) {
            ("surface", "vehicle") => 31,
            ("vehicle", "surface") => 53,
            (_, "other") => 71,
            _ => panic!("unexpected optical route"),
        };
        let loss = if self.outage.load(Ordering::Relaxed) {
            Probability::ALWAYS
        } else {
            Probability::percent(40.0)
        };
        (Link::PERFECT.with_loss(loss), Seed::new(seed))
    }
}

async fn loss_trace(source: &str, destination: &str, interleave: bool) -> Vec<u32> {
    let network = InMemoryNetwork::<Slot>::new();
    let sender = NetemTransport::new(
        Arc::new(network.bind(slot(source))),
        OpticalPolicy {
            outage: Arc::new(AtomicBool::new(false)),
        },
    );
    let receiver = network.bind(slot(destination));
    let _other = (destination != "other").then(|| network.bind(slot("other")));
    let port: &dyn Transport<Slot> = &sender;
    let mut expected = 0;
    for i in 0u32..256 {
        let before = sender.impairments().dropped();
        assert_eq!(
            port.send_to(&i.to_le_bytes(), &slot(destination))
                .await
                .unwrap(),
            4
        );
        expected += usize::from(sender.impairments().dropped() == before);
        if interleave {
            port.send_to(b"unrelated", &slot("other")).await.unwrap();
        }
    }
    assert!(expected > 0 && expected < 256, "exercise delivery and loss");
    assert_eq!(
        sender.impairments().offered(),
        if interleave { 512 } else { 256 }
    );
    let mut received = Vec::new();
    let mut buffer = [0; 4];
    for _ in 0..expected {
        let (n, from) =
            tokio::time::timeout(Duration::from_secs(5), receiver.recv_from(&mut buffer))
                .await
                .expect("every non-dropped datagram must reach its optical destination")
                .unwrap();
        assert_eq!(n, 4);
        assert_eq!(from, slot(source));
        received.push(u32::from_le_bytes(buffer));
    }
    received
}

#[tokio::test]
async fn opaque_directed_link_loss_is_reproducible_and_independent() {
    let forward = loss_trace("surface", "vehicle", false).await;
    assert_eq!(forward, loss_trace("surface", "vehicle", false).await);
    assert_eq!(forward, loss_trace("surface", "vehicle", true).await);
    assert_ne!(forward, loss_trace("vehicle", "surface", false).await);
    assert_ne!(forward, loss_trace("surface", "other", false).await);
}

#[tokio::test]
async fn opaque_policy_can_change_link_conditions_without_rebinding() {
    let network = InMemoryNetwork::<Slot>::new();
    let outage = Arc::new(AtomicBool::new(true));
    let sender = NetemTransport::new(
        Arc::new(network.bind(slot("surface"))),
        OpticalPolicy {
            outage: outage.clone(),
        },
    );
    let receiver = network.bind(slot("vehicle"));
    for _ in 0..16 {
        assert_eq!(
            sender.send_to(b"outage", &slot("vehicle")).await.unwrap(),
            6
        );
    }
    assert_eq!(sender.impairments().dropped(), 16);
    outage.store(false, Ordering::Relaxed);
    let mut delivered = 0;
    for _ in 0..64 {
        let before = sender.impairments().dropped();
        sender.send_to(b"restored", &slot("vehicle")).await.unwrap();
        delivered += usize::from(sender.impairments().dropped() == before);
    }
    assert!(delivered > 0);
    let mut buffer = [0; 16];
    for _ in 0..delivered {
        let (n, source) =
            tokio::time::timeout(Duration::from_secs(5), receiver.recv_from(&mut buffer))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(&buffer[..n], b"restored");
        assert_eq!(source, slot("surface"));
    }
    assert_eq!(sender.local_addr().unwrap(), slot("surface"));
    assert_eq!(sender.impairments().offered(), 80);
}
