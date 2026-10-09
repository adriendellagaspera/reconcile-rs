// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. You may not use this file except according to those terms.

//! Ensure the read replica actually drives periodic idle recovery.
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::read_replica_map::ReadReplicaMap;
use crate::replicated_map::{Config, FramingConfig};
use crate::transport::{InMemoryNetwork, Transport};
use gossip::auth::Authenticator;
use gossip::framing::{self, RecoveryControl};
use gossip::replay::{ReplayFilter, SenderCounter, FRESHNESS_WINDOW_DEFAULT};

#[tokio::test]
async fn running_read_replica_requests_missing_without_terminal_fragment() {
    let network = InMemoryNetwork::new();
    let addr: SocketAddr = "127.26.8.1:9286".parse().unwrap();
    let peer: SocketAddr = "127.26.8.2:9286".parse().unwrap();
    let source = network.bind(peer);
    let framing = FramingConfig::default().with_reassembly_ttl(Duration::from_secs(90));
    let cfg = Config::new(addr.port())
        .with_listen_addr(addr.ip())
        .with_reconcile_interval(Duration::from_secs(20))
        .with_framing(framing)
        .with_insecure_no_key();
    let replica =
        ReadReplicaMap::<u64, Vec<u8>>::new_with_transport(cfg, Arc::new(network.bind(addr)))
            .unwrap();
    replica
        .recovery
        .lock()
        .record_capability(peer.ip(), Instant::now(), framing, 4);
    let task = tokio::spawn(replica.clone().run());
    let auth = Authenticator::new(None, false).unwrap();
    let counter = SenderCounter::new();
    let id = framing::transfer_id(b"abcdefghij");
    let mut frame = Vec::new();
    framing::write_fragment(id, 10, 0, b"abc", &mut frame).unwrap();
    let wire = auth.seal(counter.next_seq(), counter.next_stamp(), &frame);
    source.send_to(&wire, &addr).await.unwrap();

    let mut buf = [0u8; 2048];
    let seen = tokio::time::timeout(Duration::from_secs(7), async {
        loop {
            let (size, from) = source.recv_from(&mut buf).await.unwrap();
            assert_eq!(from, addr);
            let opened = auth
                .open(&buf[..size])
                .unwrap()
                .check_version()
                .unwrap()
                .verify_replay(
                    &ReplayFilter::new(FRESHNESS_WINDOW_DEFAULT, false),
                    addr.ip(),
                )
                .unwrap();
            if let Some(RecoveryControl::Missing {
                transfer_id,
                ranges,
            }) = framing::parse_recovery_control(opened.as_bytes(), 16).unwrap()
            {
                return (transfer_id, ranges);
            }
        }
    })
    .await
    .expect("read replica must drive periodic idle recovery");
    task.abort();
    assert_eq!(seen.0, id);
    assert_eq!(seen.1, vec![(3, 7)]);
}
