// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

#![cfg(feature = "metrics")]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use tokio_util::sync::CancellationToken;

use gossip::auth::Authenticator;
use gossip::replay::SenderCounter;
use reconcile::{
    metrics as names,
    replicated_map::{Config, FramingConfig},
    InMemoryNetwork, ReplicatedMap, Transport,
};

fn counter(snapshotter: &Snapshotter, name: &str, label: Option<(&str, &str)>) -> u64 {
    snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter_map(|(composite, _unit, _desc, value)| {
            let key = composite.key();
            if key.name() != name {
                return None;
            }
            if let Some((label_key, label_value)) = label {
                if !key
                    .labels()
                    .any(|item| item.key() == label_key && item.value() == label_value)
                {
                    return None;
                }
            }
            match value {
                DebugValue::Counter(value) => Some(value),
                _ => None,
            }
        })
        .sum()
}

fn metric_exists(snapshotter: &Snapshotter, name: &str, label: Option<(&str, &str)>) -> bool {
    snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .any(|(composite, _unit, _desc, _value)| {
            let key = composite.key();
            key.name() == name
                && label.is_none_or(|(label_key, label_value)| {
                    key.labels()
                        .any(|item| item.key() == label_key && item.value() == label_value)
                })
        })
}

fn gauge(snapshotter: &Snapshotter, name: &str) -> Option<f64> {
    snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .find_map(|(composite, _unit, _desc, value)| {
            (composite.key().name() == name).then_some(value)
        })
        .and_then(|value| match value {
            DebugValue::Gauge(value) => Some(*value),
            _ => None,
        })
}

fn config(addr: SocketAddr) -> Config {
    Config::new(addr.port())
        .with_listen_addr(addr.ip())
        .with_reconcile_interval(Duration::from_millis(20))
        .with_repair_interval(Duration::from_millis(50))
        .with_framing(
            FramingConfig::default()
                .with_max_incomplete_transfers_per_peer(1)
                .with_reassembly_ttl(Duration::from_millis(20)),
        )
        .with_insecure_no_key()
}

#[tokio::test(flavor = "multi_thread")]
async fn fragmentation_completion_eviction_rejection_and_retained_bytes_are_observable() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder
        .install()
        .expect("this integration-test binary installs one global recorder");

    let network = InMemoryNetwork::new();
    let source_addr: SocketAddr = "127.28.0.1:9280".parse().unwrap();
    let target_addr: SocketAddr = "127.28.0.2:9280".parse().unwrap();
    let raw_addr: SocketAddr = "127.28.0.3:9280".parse().unwrap();

    let source = ReplicatedMap::<u64, Vec<u8>>::new_with_transport(
        config(source_addr),
        Arc::new(network.bind(source_addr)),
    )
    .expect("source config");
    let target = ReplicatedMap::<u64, Vec<u8>>::new_with_transport(
        config(target_addr),
        Arc::new(network.bind(target_addr)),
    )
    .expect("target config");

    source.load_bulk(&[(1, vec![0x5a; 64 * 1024])]);
    let fingerprint = source.fingerprint(..);
    target.seed_peer(source_addr.ip());

    let source_task = tokio::spawn(source.clone().run(CancellationToken::new()));
    let target_task = tokio::spawn(target.clone().run(CancellationToken::new()));
    tokio::time::timeout(Duration::from_secs(5), async {
        while target.fingerprint(..) != fingerprint {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("large value must converge through fragments");

    assert!(counter(&snapshotter, names::FRAGMENTED_MESSAGES_TOTAL, None) > 0);
    assert!(counter(&snapshotter, names::FRAGMENTS_SENT_TOTAL, None) > 0);
    assert!(counter(&snapshotter, names::FRAGMENTS_RECEIVED_TOTAL, None) > 0);
    assert!(counter(&snapshotter, names::REASSEMBLIES_COMPLETED_TOTAL, None) > 0);
    assert_eq!(
        gauge(&snapshotter, names::REASSEMBLY_BYTES_CURRENT),
        Some(0.0)
    );

    assert!(
        !metric_exists(
            &snapshotter,
            names::REASSEMBLY_EVICTIONS_TOTAL,
            Some(("reason", "capacity"))
        ),
        "zero capacity-eviction reports must not create a metric series"
    );

    source_task.abort();

    let raw = network.bind(raw_addr);
    let auth = Authenticator::new(None, false).unwrap();
    let counter_seq = SenderCounter::new();
    let mut frame = Vec::new();

    for (index, message) in [b"first".as_slice(), b"second".as_slice()]
        .into_iter()
        .enumerate()
    {
        gossip::framing::write_fragment(
            gossip::framing::transfer_id(message),
            message.len(),
            0,
            &message[..1],
            &mut frame,
        )
        .unwrap();
        let wire = auth.seal(counter_seq.next_seq(), counter_seq.next_stamp(), &frame);
        raw.send_to(&wire, &target_addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;

        if index == 0 {
            let duplicate = auth.seal(counter_seq.next_seq(), counter_seq.next_stamp(), &frame);
            raw.send_to(&duplicate, &target_addr).await.unwrap();
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    let wire = auth.seal(counter_seq.next_seq(), counter_seq.next_stamp(), &[0xff]);
    raw.send_to(&wire, &target_addr).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    assert!(counter(&snapshotter, names::REASSEMBLY_DUPLICATES_TOTAL, None) > 0);
    assert!(
        counter(
            &snapshotter,
            names::REASSEMBLY_EVICTIONS_TOTAL,
            Some(("reason", "capacity"))
        ) > 0
    );
    assert!(
        counter(
            &snapshotter,
            names::REASSEMBLY_EVICTIONS_TOTAL,
            Some(("reason", "ttl"))
        ) > 0
    );
    assert!(
        counter(
            &snapshotter,
            names::REASSEMBLY_REJECTIONS_TOTAL,
            Some(("reason", "unknown_tag"))
        ) > 0
    );
    assert_eq!(
        gauge(&snapshotter, names::REASSEMBLY_BYTES_CURRENT),
        Some(0.0)
    );

    target_task.abort();
}
