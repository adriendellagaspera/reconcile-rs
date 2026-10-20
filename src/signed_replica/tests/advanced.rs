// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use super::*;
use crate::signed_replica::network::{serialize, SignedMessage};

#[test]
fn signed_stale_ack_and_trailing_protocol_bytes_cannot_mutate_causal_state() {
    let a_id = signer(101).peer_id();
    let b_id = signer(102).peer_id();
    let now = Instant::now();
    let a_addr = (1, 3);
    let b_addr = (2, 7);
    let mut a = make(101, SignedConfig::new([b_id]), memory());
    let mut b = make(102, SignedConfig::new([a_id]), memory());
    connect(&mut a, a_addr, &mut b, b_addr, now);

    a.remove(55).unwrap();
    let current_version = crate::replica::version_hash(a.entries.get(&55).unwrap());
    assert!(!a.is_tombstone_stable(&55));
    let stale = SignedMessage::<u64, String>::Ack(55, current_version.wrapping_add(1));
    let mut frame = Vec::new();
    gossip::framing::write_complete(&serialize(&stale).unwrap(), &mut frame);
    let signed = b.boundary.seal(&frame).unwrap();
    assert!(a.receive(b_addr, &signed, now).unwrap().is_empty());
    assert_eq!(a.acknowledged_version(&55, b_id), None);
    assert!(!a.collect_if_stable(&55));

    // A complete signed frame must be exactly one fully decoded message: even a truncated
    // second message at the end must not authorize the first (valid) update.
    let malicious = SignedMessage::Update(
        999,
        Entry::present(b.clock.now(), "must-not-apply".to_owned()),
    );
    let mut bytes = serialize(&malicious).unwrap();
    bytes.push(0xff);
    gossip::framing::write_complete(&bytes, &mut frame);
    let signed = b.boundary.seal(&frame).unwrap();
    assert!(a.receive(b_addr, &signed, now).unwrap().is_empty());
    assert!(a.get(&999).is_none());
    assert_eq!(a.members(), HashSet::from([b_id]));
    assert!(!a.is_tombstone_stable(&55));
}

#[test]
fn a_signed_read_only_peer_cannot_forge_an_authoritative_ack() {
    let b_id = signer(112).peer_id();
    let reader_id = signer(113).peer_id();
    let a_id = signer(111).peer_id();
    let mut config = SignedConfig::new([b_id]);
    config.readers.insert(reader_id);
    let mut reader_config = SignedConfig::new([a_id]);
    reader_config.read_only = true;
    let now = Instant::now();
    let mut authority = make(111, config, memory());
    let mut reader = make(113, reader_config, memory());
    connect(&mut authority, (3, 3), &mut reader, (4, 4), now);
    authority.remove(77).unwrap();
    let version = crate::replica::version_hash(authority.entries.get(&77).unwrap());

    // The reader owns a genuine allowlisted signing key and a confirmed returnable route,
    // but cannot claim the role of an authoritative tombstone holder by choosing an ACK tag.
    let fake = SignedMessage::<u64, String>::Ack(77, version);
    let mut frame = Vec::new();
    gossip::framing::write_complete(&serialize(&fake).unwrap(), &mut frame);
    let bytes = reader.boundary.seal(&frame).unwrap();
    assert!(authority.receive((4, 4), &bytes, now).unwrap().is_empty());
    assert_eq!(authority.acknowledged_version(&77, reader_id), None);
    assert_eq!(authority.acknowledged_version(&77, b_id), None);
    assert!(!authority.collect_if_stable(&77));
    assert_eq!(authority.members(), HashSet::from([b_id]));
}

#[test]
fn signer_not_on_the_allowlist_is_rejected_despite_a_valid_cluster_mac() {
    use gossip::auth::Authenticator;
    use gossip::peer::identity::PeerAllowlist;
    use gossip::peer::runtime::SignedDatagramBoundary;
    use gossip::replay::FRESHNESS_WINDOW_DEFAULT;

    let a_id = signer(121).peer_id();
    let b_id = signer(122).peer_id();
    let now = Instant::now();
    let mut a = make(121, SignedConfig::new([b_id]), memory());
    let mut b = make(122, SignedConfig::new([a_id]), memory());
    connect(&mut a, (5, 3), &mut b, (6, 3), now);
    let intruder = SignedDatagramBoundary::new(
        Authenticator::new(Some(key()), false).unwrap(),
        signer(123),
        PeerAllowlist::new([]),
        FRESHNESS_WINDOW_DEFAULT,
        4,
        4,
        1200,
    )
    .unwrap();
    let fabricated =
        SignedMessage::Update(321_u64, Entry::present(b.clock.now(), "forged".to_owned()));
    let mut frame = Vec::new();
    gossip::framing::write_complete(&serialize(&fabricated).unwrap(), &mut frame);
    let signed = intruder.seal(&frame).unwrap();
    assert!(a.receive((6, 3), &signed, now).unwrap().is_empty());
    assert!(a.get(&321).is_none());
    assert_eq!(a.members(), HashSet::from([b_id]));
}

#[test]
fn deliberate_member_decommission_requires_matching_roster_on_reboot() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("decommission.snapshot");
    let backend: Backend = Arc::new(FileSnapshot::new(&path));
    let b_id = signer(132).peer_id();
    let c_id = signer(133).peer_id();
    let mut a = make(131, SignedConfig::new([b_id, c_id]), backend.clone());
    a.remove(88).unwrap();
    a.snapshot_now().unwrap();
    assert!(!a.is_tombstone_stable(&88));
    assert!(a.decommission(c_id));
    a.snapshot_now().unwrap();
    drop(a);

    let restored = make(131, SignedConfig::new([b_id]), backend.clone());
    assert_eq!(restored.members(), HashSet::from([b_id]));
    assert!(restored.is_tombstone(&88));
    assert!(!restored.is_tombstone_stable(&88));
    assert!(restored.acknowledged_version(&88, c_id).is_none());

    // Restoring an old provisioned roster would reinstate an allegedly retired blocker:
    // it must fail explicitly rather than deciding to discard persisted causal history.
    let error = SignedReplica::<u64, String, Slot>::new(
        SignedConfig::new([b_id, c_id]),
        key(),
        signer(131),
        Arc::new(HlcClock::new(NodeId::new(131))),
        backend,
    )
    .err()
    .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
}
