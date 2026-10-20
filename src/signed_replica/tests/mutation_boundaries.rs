// Copyright 2026 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use super::*;
use crate::signed_replica::network::{serialize, SignedMessage};

#[test]
fn exposed_record_counts_and_exact_ack_matrix_track_real_causal_state() {
    let peer = signer(152).peer_id();
    let mut store = make(151, SignedConfig::new([peer]), memory());
    assert_eq!(store.len(), 0);
    assert!(store.is_empty());
    assert!(store.tombstone_ack_snapshot().is_empty());
    assert_eq!(store.acknowledged_version(&1, peer), None);
    store.insert(1, "one".into()).unwrap();
    store.insert(2, "two".into()).unwrap();
    assert_eq!(store.len(), 2);
    assert!(!store.is_empty());
    store.remove(1).unwrap();
    assert_eq!(store.len(), 2);
    assert!(!store.collect_if_stable(&1));
    assert_eq!(store.acknowledged_version(&1, peer), None);

    let self_id = signer(151).peer_id();
    let mut replica = make(152, SignedConfig::new([self_id]), memory());
    let now = Instant::now();
    connect(&mut store, (21, 1), &mut replica, (22, 2), now);
    // The second key must be sent on the second reconciliation round.
    deliver(&mut store, (21, 1), &mut replica, (22, 2), now);
    deliver(&mut store, (21, 1), &mut replica, (22, 2), now);
    let exact = crate::replica::version_hash(store.entries.get(&1).unwrap());
    assert_ne!(exact, 0);
    assert_eq!(store.acknowledged_version(&1, peer), Some(exact));
    assert_eq!(
        store.tombstone_ack_snapshot().get(&1).unwrap().get(&peer),
        Some(&exact)
    );
    assert!(store.is_tombstone_stable(&1));
    assert!(store.collect_if_stable(&1));
    assert_eq!(store.len(), 1);
    assert!(!store.is_empty());
    assert!(store.tombstone_ack_snapshot().is_empty());
}

#[test]
fn signed_message_length_limit_is_inclusive_and_fragment_count_has_exact_bounds() {
    let value = "abcxyz".repeat(200);
    let temp = make(154, SignedConfig::new([]), memory());
    let entry = Entry::present(temp.clock.now(), value.clone());
    let encoded = serialize(&SignedMessage::Update(17_u64, entry)).unwrap();
    let exact = encoded.len();

    let mut cfg = SignedConfig::new([]);
    cfg.framing = cfg.framing.with_max_logical_message_size(exact);
    let mut fits = make(154, cfg.clone(), memory());
    fits.insert(17, value.clone()).unwrap();
    assert_eq!(fits.get(&17).map(String::as_str), Some(value.as_str()));

    cfg.framing = cfg.framing.with_max_logical_message_size(exact - 1);
    let mut too_small = make(154, cfg, memory());
    assert_eq!(
        too_small.insert(17, value.clone()).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert!(too_small.is_empty());

    let mut cfg = SignedConfig::new([]);
    cfg.framing = cfg.framing.with_datagram_payload_budget(300);
    let example = make(154, cfg.clone(), memory());
    let chunk = example.boundary.payload_budget() - gossip::framing::FRAGMENT_HEADER_LEN;
    let fragments = exact.div_ceil(chunk);
    assert!(fragments >= 2);
    cfg.framing = cfg.framing.with_max_fragments_per_message(fragments);
    cfg.max_frames_per_tick = fragments;
    let mut at_limit = make(154, cfg.clone(), memory());
    at_limit.insert(17, value.clone()).unwrap();

    cfg.framing = cfg.framing.with_max_fragments_per_message(fragments - 1);
    let mut above_fragment_limit = make(154, cfg.clone(), memory());
    assert_eq!(
        above_fragment_limit
            .insert(17, value.clone())
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    cfg.framing = cfg.framing.with_max_fragments_per_message(fragments);
    cfg.max_frames_per_tick = fragments - 1;
    let mut above_tick_limit = make(154, cfg, memory());
    assert_eq!(
        above_tick_limit.insert(17, value).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn multiple_dated_keys_advance_in_order_without_repeating_the_previous_cursor() {
    let target = signer(162).peer_id();
    let origin = signer(161).peer_id();
    let now = Instant::now();
    let mut a = make(161, SignedConfig::new([target]), memory());
    let mut b = make(162, SignedConfig::new([origin]), memory());
    connect(&mut a, (31, 1), &mut b, (32, 2), now);

    for (key, value) in [(10, "first"), (20, "second"), (30, "third")] {
        a.insert(key, value.to_owned()).unwrap();
    }
    for (key, value) in [(10, "first"), (20, "second"), (30, "third")] {
        assert_eq!(b.get(&key), None);
        let frames = a.reconcile(now).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].destination, (32, 2));
        let replies = b.receive((31, 1), &frames[0].bytes, now).unwrap();
        assert!(replies.is_empty());
        assert_eq!(b.get(&key).map(String::as_str), Some(value));
    }
    assert_eq!(b.len(), 3);
    for _ in 0..3 {
        let frames = a.reconcile(now).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].destination, (32, 2));
        assert!(b
            .receive((31, 1), &frames[0].bytes, now)
            .unwrap()
            .is_empty());
    }
    assert_eq!(b.len(), 3);
}

#[test]
fn frame_budget_rotates_service_between_confirmed_peers_and_respects_the_ceiling() {
    let a_id = signer(171).peer_id();
    let b_id = signer(172).peer_id();
    let c_id = signer(173).peer_id();
    let now = Instant::now();
    let mut cfg = SignedConfig::new([b_id, c_id]);
    cfg.max_frames_per_tick = 1;
    let mut a = make(171, cfg, memory());
    let mut b = make(172, SignedConfig::new([a_id]), memory());
    let mut c = make(173, SignedConfig::new([a_id]), memory());
    connect(&mut a, (41, 1), &mut b, (42, 2), now);
    connect(&mut a, (41, 1), &mut c, (43, 3), now);
    a.insert(5, "both".into()).unwrap();

    let mut destinations = Vec::new();
    for _ in 0..4 {
        let frames = a.reconcile(now).unwrap();
        assert_eq!(frames.len(), 1);
        let frame = &frames[0];
        destinations.push(frame.destination);
        match frame.destination {
            (42, 2) => {
                assert!(b.receive((41, 1), &frame.bytes, now).unwrap().is_empty());
            }
            (43, 3) => {
                assert!(c.receive((41, 1), &frame.bytes, now).unwrap().is_empty());
            }
            other => panic!("unexpected routed destination {other:?}"),
        }
    }
    assert_ne!(destinations[0], destinations[1]);
    assert_eq!(destinations[0], destinations[2]);
    assert_eq!(destinations[1], destinations[3]);
    assert_eq!(b.get(&5).map(String::as_str), Some("both"));
    assert_eq!(c.get(&5).map(String::as_str), Some("both"));

    // A complete frame exactly fills the configured tick; no extra packet can be emitted.
    assert_eq!(a.reconcile(now).unwrap().len(), 1);
}

#[test]
fn multi_fragment_missing_span_is_retransmitted_at_the_exact_nonzero_offsets() {
    let a_id = signer(181).peer_id();
    let b_id = signer(182).peer_id();
    let mut cfg_a = SignedConfig::new([b_id]);
    let mut cfg_b = SignedConfig::new([a_id]);
    cfg_a.framing = cfg_a.framing.with_datagram_payload_budget(300);
    cfg_b.framing = cfg_b.framing.with_datagram_payload_budget(300);
    let now = Instant::now();
    let mut a = make(181, cfg_a, memory());
    let mut b = make(182, cfg_b, memory());
    connect(&mut a, (51, 1), &mut b, (52, 2), now);

    let value = "reconciliation-retransmission".repeat(95);
    a.insert(123, value.clone()).unwrap();
    let frames = a.reconcile(now).unwrap();
    assert!(frames.len() >= 6);
    let missing = 1..4;
    for (i, frame) in frames.iter().enumerate() {
        if !missing.contains(&i) {
            assert_eq!(frame.destination, (52, 2));
            assert!(b.receive((51, 1), &frame.bytes, now).unwrap().is_empty());
        }
    }
    assert_eq!(b.get(&123), None);
    let requests = b.reconcile(now + Duration::from_secs(3)).unwrap();
    assert!(!requests.is_empty());
    let mut total_retransmitted = 0;
    for request in requests {
        let resent = a
            .receive((52, 2), &request.bytes, now + Duration::from_secs(3))
            .unwrap();
        total_retransmitted += resent.len();
        for frame in resent {
            assert_eq!(frame.destination, (52, 2));
            let replies = b
                .receive((51, 1), &frame.bytes, now + Duration::from_secs(3))
                .unwrap();
            for reply in replies {
                assert_eq!(reply.destination, (51, 1));
                a.receive((52, 2), &reply.bytes, now + Duration::from_secs(3))
                    .unwrap();
            }
        }
    }
    assert_eq!(total_retransmitted, missing.len(), "NACK retry must retransmit only the missing full-size fragment chunks");
    assert_eq!(b.get(&123).map(String::as_str), Some(value.as_str()));
}

#[test]
fn signed_frame_headers_cover_every_byte_at_exact_nonoverlapping_offsets() {
    use gossip::framing::{fragment_metadata, FRAGMENT_HEADER_LEN};

    let a_id = signer(191).peer_id();
    let b_id = signer(192).peer_id();
    let mut cfg_a = SignedConfig::new([b_id]);
    let mut cfg_b = SignedConfig::new([a_id]);
    cfg_a.framing = cfg_a.framing.with_datagram_payload_budget(300);
    cfg_b.framing = cfg_b.framing.with_datagram_payload_budget(300);
    let now = Instant::now();
    let mut a = make(191, cfg_a, memory());
    let mut b = make(192, cfg_b, memory());
    connect(&mut a, (61, 1), &mut b, (62, 2), now);

    let text = "signed-fragment-offset".repeat(175);
    a.insert(345, text).unwrap();
    let frames = a.reconcile(now).unwrap();
    assert!(frames.len() >= 4);
    let chunk = a.boundary.payload_budget() - FRAGMENT_HEADER_LEN;
    let mut observed_total = None;
    let mut observed_transfer = None;
    let mut covered = 0;
    for (index, routed) in frames.iter().enumerate() {
        assert_eq!(routed.destination, (62, 2));
        let checked = b
            .boundary
            .open(&routed.bytes, |_| (true, 1))
            .expect("each outgoing fragment must authenticate under the same verified signer");
        assert_eq!(checked.peer().peer_id(), a_id);
        let payload = checked.into_payload();
        let metadata = fragment_metadata(payload.as_bytes())
            .expect("a multi-fragment record must carry a valid fragment header");
        assert_eq!(metadata.offset, index * chunk);
        assert_eq!(metadata.payload_len, (metadata.total_len - metadata.offset).min(chunk));
        assert!(metadata.payload_len > 0);
        if let Some(total) = observed_total {
            assert_eq!(metadata.total_len, total);
        } else {
            observed_total = Some(metadata.total_len);
        }
        if let Some(id) = observed_transfer {
            assert_eq!(metadata.transfer_id, id);
        } else {
            observed_transfer = Some(metadata.transfer_id);
        }
        covered += metadata.payload_len;
    }
    assert_eq!(Some(covered), observed_total);
}
