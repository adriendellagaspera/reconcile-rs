// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. You may not use this file except according to those terms.

use super::*;
use crate::metrics as names;
use crate::transport::InMemoryNetwork;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};

fn fallbacks(snapshot: &Snapshotter, reason: &str) -> u64 {
    snapshot
        .snapshot()
        .into_vec()
        .into_iter()
        .filter_map(|(key, _, _, value)| {
            if key.key().name() != names::SELECTIVE_RECOVERY_FALLBACKS_TOTAL
                || !key
                    .key()
                    .labels()
                    .any(|x| x.key() == "reason" && x.value() == reason)
            {
                return None;
            }
            match value {
                DebugValue::Counter(count) => Some(count),
                _ => None,
            }
        })
        .sum()
}

#[test]
fn fragmented_send_reports_state_limit_only_if_retention_actually_fails() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let recorder = DebuggingRecorder::new();
    let snapshot = recorder.snapshotter();
    ::metrics::with_local_recorder(&recorder, || {
        // Wrong metric with matching reason and right metric with wrong reason
        // must never contribute to the state_limit fallback count.
        ::metrics::counter!("issue286_unrelated_fallback_metric", "reason" => "state_limit")
            .increment(11);
        ::metrics::counter!(names::SELECTIVE_RECOVERY_FALLBACKS_TOTAL, "reason" => "unrelated")
            .increment(7);
        runtime.block_on(async {
            let network = InMemoryNetwork::new();
            let source: SocketAddr = "127.20.11.1:9286".parse().unwrap();
            let peer: SocketAddr = "127.20.11.2:9286".parse().unwrap();
            let transport = network.bind(source);
            let _receiver = network.bind(peer);
            let authenticator = auth::Authenticator::new(None, false).unwrap();
            let counter = replay::SenderCounter::new();
            let recovery = Mutex::new(RecoveryBook::default());
            let cfg = FramingConfig::default()
                .with_max_outbound_recovery_bytes_per_peer(8192)
                .with_max_total_outbound_recovery_bytes(8192);
            recovery
                .lock()
                .record_capability(peer.ip(), Instant::now(), cfg, 4);
            let payload = vec![0x5a; 8192];
            let ports = SendPorts {
                transport: &transport,
                authenticator: &authenticator,
                sender_counter: &counter,
                framing: cfg,
                recovery: &recovery,
            };
            let mut frame = Vec::new();
            let mut pacer = Pacer::new(None);
            assert!(
                send_logical_payload(&ports, peer, &payload, &mut frame, &mut pacer)
                    .await
                    .unwrap()
                    > 0
            );
            assert_eq!(recovery.lock().occupancy(), (1, payload.len()));
            assert_eq!(
                fallbacks(&snapshot, "state_limit"),
                0,
                "successful retained transfers must not emit state_limit"
            );

            let restricted = SendPorts {
                framing: cfg.with_max_total_outbound_recovery_bytes(1),
                ..ports
            };
            let other = vec![0x19; 8192];
            assert!(
                send_logical_payload(&restricted, peer, &other, &mut frame, &mut pacer)
                    .await
                    .unwrap()
                    > 0
            );
            assert_eq!(
                fallbacks(&snapshot, "state_limit"),
                1,
                "failed retention must record explicit fallback"
            );
        })
    });
}
