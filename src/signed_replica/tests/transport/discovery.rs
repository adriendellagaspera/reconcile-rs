// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use super::*;
use gossip::discovery::{
    EndpointDiscovery, EndpointDiscoveryConfig, EndpointDiscoveryFuture, EndpointDiscoveryState,
};

struct Hints(Vec<OpticalSlot>);
impl EndpointDiscovery<OpticalSlot> for Hints {
    fn discover(&self, _limit: NonZeroUsize) -> EndpointDiscoveryFuture<'_, OpticalSlot> {
        Box::pin(async { Ok(self.0.clone()) })
    }
}

struct Failure;
impl EndpointDiscovery<OpticalSlot> for Failure {
    fn discover(&self, _limit: NonZeroUsize) -> EndpointDiscoveryFuture<'_, OpticalSlot> {
        Box::pin(async { Err(std::io::Error::other("optical directory unavailable").into()) })
    }
}

fn discovery() -> EndpointDiscoveryState<OpticalSlot> {
    EndpointDiscoveryState::new(
        EndpointDiscoveryConfig::new(
            NonZeroUsize::new(4).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .unwrap(),
    )
}

#[tokio::test]
async fn discovered_optical_routes_remain_untrusted_and_absences_cannot_release_gc() {
    let network = InMemoryNetwork::with_datagram_limit(NonZeroUsize::new(MTU).unwrap());
    let a_port = network.bind(slot("surface", 3));
    let b_port = network.bind(slot("vehicle", 7));
    let a_id = PeerSigner::from_seed([11; 32]).peer_id();
    let b_id = PeerSigner::from_seed([12; 32]).peer_id();
    let mut a = replica(11, b_id);
    let mut b = replica(12, a_id);
    let mut a_discovery = discovery();
    let mut b_discovery = discovery();
    let now = Instant::now();
    assert!(a
        .probe_discovered(&mut a_discovery, now)
        .unwrap()
        .is_empty());
    a_discovery
        .refresh(&Hints(vec![b_port.local_addr().unwrap(); 2]))
        .await
        .unwrap();
    b_discovery
        .refresh(&Hints(vec![a_port.local_addr().unwrap()]))
        .await
        .unwrap();
    assert_eq!(a_discovery.candidate_count(), 1);
    let value = "contact found by optical discovery".repeat(32);
    a.insert(7, value.clone()).unwrap();
    let hello = a.probe_discovered(&mut a_discovery, now).unwrap();
    assert_eq!(hello.len(), 1);
    assert_eq!(hello[0].destination, b_port.local_addr().unwrap());
    assert!(a
        .probe_discovered(&mut a_discovery, now)
        .unwrap()
        .is_empty());
    // Merely resolving and probing an endpoint does not admit outbound protocol traffic.
    assert!(a.reconcile(now).unwrap().is_empty());
    let outsider = replica(13, a_id);
    let forged = outsider.probe(a_port.local_addr().unwrap()).unwrap();
    assert!(a
        .receive(slot("spoofed", 99), &forged.bytes, now)
        .unwrap()
        .is_empty());
    assert_eq!(a.members(), HashSet::from([b_id]));
    assert!(a.reconcile(now).unwrap().is_empty());

    exchange(&mut a, &a_port, &mut b, &b_port, hello, now).await;
    let hello = b.probe_discovered(&mut b_discovery, now).unwrap();
    exchange(&mut b, &b_port, &mut a, &a_port, hello, now).await;
    let frames = a.reconcile(now).unwrap();
    assert!(frames.len() > 1);
    exchange(&mut a, &a_port, &mut b, &b_port, frames, now).await;
    assert_eq!(b.get(&7), Some(&value));

    let old_endpoint = b_port.local_addr().unwrap();
    drop(b_port);
    a.remove(7).unwrap();
    a_discovery.refresh(&Hints(vec![])).await.unwrap();
    assert!(a
        .probe_discovered(&mut a_discovery, now)
        .unwrap()
        .is_empty());
    assert!(a_discovery.refresh(&Failure).await.is_err());
    assert!(a_discovery
        .refresh(&Hints(vec![slot("overflow", 88); 5]))
        .await
        .is_err());
    assert_eq!(a_discovery.candidate_count(), 0);
    assert_eq!(a.members(), HashSet::from([b_id]));
    assert!(a.is_tombstone(&7));
    assert!(!a.collect_if_stable(&7));
    // Directory omissions do not remove a previously confirmed route either.
    let frames = a.reconcile(now).unwrap();
    assert!(!frames.is_empty());
    for frame in frames {
        assert_eq!(frame.destination, old_endpoint);
        a_port
            .send_to(&frame.bytes, &frame.destination)
            .await
            .unwrap();
    }
    assert!(b.get(&7).is_some());

    let b_port = network.bind(slot("vehicle", 19));
    let later = now + Duration::from_secs(1);
    a_discovery
        .refresh(&Hints(vec![b_port.local_addr().unwrap()]))
        .await
        .unwrap();
    let hello = a.probe_discovered(&mut a_discovery, later).unwrap();
    assert_eq!(hello.len(), 1);
    exchange(&mut a, &a_port, &mut b, &b_port, hello, later).await;
    let hello = b.probe_discovered(&mut b_discovery, later).unwrap();
    exchange(&mut b, &b_port, &mut a, &a_port, hello, later).await;
    let frames = a.reconcile(later).unwrap();
    assert!(!frames.is_empty());
    exchange(&mut a, &a_port, &mut b, &b_port, frames, later).await;
    assert_eq!(a.members(), HashSet::from([b_id]));
    assert_eq!(b.members(), HashSet::from([a_id]));
    assert!(b.is_tombstone(&7));
    assert!(a.acknowledged_version(&7, b_id).is_some());
    assert!(a.collect_if_stable(&7));
}
