// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use super::*;

use crate::clock::{HlcClock, NodeId};
use crate::persistence::{FileSnapshot, InMemoryPersistence};
use std::net::IpAddr;

type Slot = (u16, u8);
type Store = SignedReplica<u64, String, Slot>;
type Backend = Arc<dyn Persistence<u64, String, PeerId>>;

fn signer(seed: u8) -> PeerSigner {
    PeerSigner::from_seed([seed; 32])
}

fn key() -> ClusterKey {
    ClusterKey::new([0x7c; 32])
}

fn make(seed: u8, cfg: SignedConfig, persistence: Backend) -> Store {
    SignedReplica::new(
        cfg,
        key(),
        signer(seed),
        Arc::new(HlcClock::new(NodeId::new(seed as u64))),
        persistence,
    )
    .unwrap()
}

fn memory() -> Backend {
    Arc::new(InMemoryPersistence::new())
}

fn handshake(a: &mut Store, a_addr: Slot, b: &mut Store, b_addr: Slot, now: Instant) {
    let hello = a.probe(b_addr).unwrap();
    let mut challenge = b.receive(a_addr, &hello.bytes, now).unwrap();
    assert_eq!(challenge.len(), 1);
    assert_eq!(challenge[0].destination, a_addr);
    let challenge = challenge.pop().unwrap();
    let mut response = a.receive(b_addr, &challenge.bytes, now).unwrap();
    assert_eq!(response.len(), 1);
    let response = response.pop().unwrap();
    assert_eq!(response.destination, b_addr);
    assert!(b.receive(a_addr, &response.bytes, now).unwrap().is_empty());
}

fn connect(a: &mut Store, a_addr: Slot, b: &mut Store, b_addr: Slot, now: Instant) {
    handshake(a, a_addr, b, b_addr, now);
    handshake(b, b_addr, a, a_addr, now);
}

fn deliver(
    sender: &mut Store,
    sender_addr: Slot,
    recipient: &mut Store,
    recipient_addr: Slot,
    now: Instant,
) {
    for outgoing in sender.reconcile(now).unwrap() {
        assert_eq!(outgoing.destination, recipient_addr);
        let responses = recipient
            .receive(sender_addr, &outgoing.bytes, now)
            .unwrap();
        for response in responses {
            assert_eq!(response.destination, sender_addr);
            let final_responses = sender
                .receive(recipient_addr, &response.bytes, now)
                .unwrap();
            assert!(final_responses.is_empty());
        }
    }
}

#[test]
fn two_signed_authorities_converge_and_ack_the_exact_tombstone() {
    let a_id = signer(11).peer_id();
    let b_id = signer(12).peer_id();
    let mut a = make(11, SignedConfig::new([b_id]), memory());
    let mut b = make(12, SignedConfig::new([a_id]), memory());
    let now = Instant::now();
    let a_addr = (1, 2);
    let b_addr = (8, 5);
    assert_eq!(a.members(), HashSet::from([b_id]));
    assert_eq!(b.members(), HashSet::from([a_id]));

    a.insert(7, "value".into()).unwrap();
    assert_eq!(a.get(&7).map(String::as_str), Some("value"));
    assert!(!a.is_tombstone_stable(&7));
    connect(&mut a, a_addr, &mut b, b_addr, now);
    deliver(&mut a, a_addr, &mut b, b_addr, now);
    assert_eq!(b.get(&7).map(String::as_str), Some("value"));

    a.remove(7).unwrap();
    assert!(a.is_tombstone(&7));
    assert!(!a.is_tombstone_stable(&7));
    assert!(!a.collect_if_stable(&7));
    deliver(&mut a, a_addr, &mut b, b_addr, now);
    assert!(b.is_tombstone(&7));
    assert!(a.is_tombstone_stable(&7));
    assert!(a.acknowledged_version(&7, b_id).is_some());
    assert!(a.collect_if_stable(&7));
    assert!(!a.is_tombstone(&7));
}

#[test]
fn disconnected_third_authority_blocks_gc_until_explicit_decommission() {
    let a_id = signer(21).peer_id();
    let b_id = signer(22).peer_id();
    let c_id = signer(23).peer_id();
    let now = Instant::now();
    let mut a = make(21, SignedConfig::new([b_id, c_id]), memory());
    let mut b = make(22, SignedConfig::new([a_id, c_id]), memory());
    connect(&mut a, (1, 1), &mut b, (2, 2), now);
    a.remove(42).unwrap();
    deliver(&mut a, (1, 1), &mut b, (2, 2), now);
    assert_eq!(a.members(), HashSet::from([b_id, c_id]));
    assert!(!a.is_tombstone_stable(&42));
    assert!(!a.collect_if_stable(&42));
    assert!(a.decommission(c_id));
    assert_eq!(a.members(), HashSet::from([b_id]));
    assert!(a.is_tombstone_stable(&42));
    assert!(a.collect_if_stable(&42));
}

#[test]
fn changing_non_ip_route_preserves_authenticated_peer_and_ack_history() {
    let a_id = signer(31).peer_id();
    let b_id = signer(32).peer_id();
    let now = Instant::now();
    let mut a = make(31, SignedConfig::new([b_id]), memory());
    let mut b = make(32, SignedConfig::new([a_id]), memory());
    let old = (3, 1);
    let moved = (3, 9);
    let b_addr = (7, 2);
    connect(&mut a, old, &mut b, b_addr, now);
    a.remove(18).unwrap();
    deliver(&mut a, old, &mut b, b_addr, now);
    assert!(a.is_tombstone_stable(&18));
    let ack = a.acknowledged_version(&18, b_id);
    assert!(ack.is_some());

    // The first datagram on a new optical wavelength must not dispatch data.
    let moved_hello = a.probe(b_addr).unwrap();
    let challenges = b.receive(moved, &moved_hello.bytes, now).unwrap();
    assert_eq!(challenges.len(), 1);
    assert_eq!(challenges[0].destination, moved);
    assert_eq!(b.routes.route(a_id), Some(&old));
    assert_eq!(b.members(), HashSet::from([a_id]));
    let responses = a.receive(b_addr, &challenges[0].bytes, now).unwrap();
    assert_eq!(responses.len(), 1);
    b.receive(moved, &responses[0].bytes, now).unwrap();
    assert_eq!(b.routes.route(a_id), Some(&moved));
    assert_eq!(a.acknowledged_version(&18, b_id), ack);
    assert!(a.is_tombstone_stable(&18));
}

#[test]
fn readers_get_values_but_cannot_write_ack_or_join_causal_membership() {
    let authority_id = signer(41).peer_id();
    let reader_id = signer(42).peer_id();
    let mut cfg_a = SignedConfig::new([]);
    cfg_a.readers.insert(reader_id);
    let mut cfg_reader = SignedConfig::new([authority_id]);
    cfg_reader.read_only = true;
    let mut a = make(41, cfg_a, memory());
    let mut reader = make(42, cfg_reader, memory());
    let now = Instant::now();
    connect(&mut a, (1, 5), &mut reader, (2, 6), now);
    assert_eq!(a.members().len(), 0);
    assert!(reader.members().is_empty());
    assert_eq!(
        reader.insert(77, "fake".into()).unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(
        reader.remove(77).unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );

    a.insert(77, "authored".into()).unwrap();
    deliver(&mut a, (1, 5), &mut reader, (2, 6), now);
    assert_eq!(reader.get(&77).map(String::as_str), Some("authored"));
    a.remove(77).unwrap();
    deliver(&mut a, (1, 5), &mut reader, (2, 6), now);
    assert!(reader.is_tombstone(&77));
    assert!(a.members().is_empty());
    assert!(a.tombstone_ack_snapshot().is_empty());
    assert!(!reader.collect_if_stable(&77));
}

#[test]
fn signed_v2_snapshot_reloads_causal_blockers_and_rejects_v1_or_roster_drift() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("signed.snapshot");
    let other = signer(52).peer_id();
    let backend: Backend = Arc::new(FileSnapshot::new(&path));
    let mut original = make(51, SignedConfig::new([other]), backend.clone());
    original.insert(4, "durable".into()).unwrap();
    original.remove(9).unwrap();
    assert!(!original.is_tombstone_stable(&9));
    original.snapshot_now().unwrap();
    original.insert(5, "delta".into()).unwrap();
    original.snapshot_now().unwrap();
    drop(original);
    let mut restored = make(51, SignedConfig::new([other]), backend);
    assert_eq!(restored.get(&4).map(String::as_str), Some("durable"));
    assert_eq!(restored.get(&5).map(String::as_str), Some("delta"));
    assert!(restored.is_tombstone(&9));
    assert_eq!(restored.members(), HashSet::from([other]));
    assert!(!restored.collect_if_stable(&9));

    let drift_backend: Backend = Arc::new(FileSnapshot::new(&path));
    let err = SignedReplica::<u64, String, Slot>::new(
        SignedConfig::new([]),
        key(),
        signer(51),
        Arc::new(HlcClock::new(NodeId::new(51))),
        drift_backend,
    )
    .err()
    .unwrap();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);

    let old_path = dir.path().join("old.snapshot");
    let legacy = FileSnapshot::new(&old_path);
    let old_state = PersistedState::<u64, String, IpAddr>::default();
    Persistence::<u64, String, IpAddr>::save(&legacy, &old_state).unwrap();
    let signed_backend: Backend = Arc::new(FileSnapshot::new(&old_path));
    let err = SignedReplica::<u64, String, Slot>::new(
        SignedConfig::new([other]),
        key(),
        signer(51),
        Arc::new(HlcClock::new(NodeId::new(51))),
        signed_backend,
    )
    .err()
    .unwrap();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn missing_signed_fragment_recovers_by_verified_peer_id_after_selective_nack() {
    let a_id = signer(61).peer_id();
    let b_id = signer(62).peer_id();
    let now = Instant::now();
    let a_addr = (11, 3);
    let b_addr = (12, 8);
    let mut a = make(61, SignedConfig::new([b_id]), memory());
    let mut b = make(62, SignedConfig::new([a_id]), memory());
    connect(&mut a, a_addr, &mut b, b_addr, now);

    let large = "large-bounded-value".repeat(500);
    a.insert(13, large.clone()).unwrap();
    let frames = a.reconcile(now).unwrap();
    assert!(frames.len() >= 3, "large dated record should require signed fragmentation");
    let lost = frames.len() / 2;
    for (i, frame) in frames.into_iter().enumerate() {
        if i != lost {
            assert_eq!(frame.destination, b_addr);
            assert!(b.receive(a_addr, &frame.bytes, now).unwrap().is_empty());
        }
    }
    assert!(b.get(&13).is_none());

    let requests = b.reconcile(now + Duration::from_secs(3)).unwrap();
    assert!(!requests.is_empty(), "incomplete signed transfer should issue bounded NACKs");
    for request in requests {
        assert_eq!(request.destination, a_addr);
        let missing_frames = a.receive(b_addr, &request.bytes, now + Duration::from_secs(3)).unwrap();
        assert!(!missing_frames.is_empty(), "sender retains requested data under verified identity");
        for frame in missing_frames {
            assert_eq!(frame.destination, b_addr);
            let acks = b.receive(a_addr, &frame.bytes, now + Duration::from_secs(3)).unwrap();
            for ack in acks {
                assert_eq!(ack.destination, a_addr);
                a.receive(b_addr, &ack.bytes, now + Duration::from_secs(3)).unwrap();
            }
        }
    }
    assert_eq!(b.get(&13).map(String::as_str), Some(large.as_str()));
    assert_eq!(b.members(), HashSet::from([a_id]));
}

#[test]
fn signed_read_only_sender_cannot_inject_a_dated_entry_even_with_a_confirmed_route() {
    use crate::signed_replica::network::{serialize, SignedMessage};

    let authority_id = signer(71).peer_id();
    let reader_id = signer(72).peer_id();
    let mut config = SignedConfig::new([]);
    config.readers.insert(reader_id);
    let mut reader_config = SignedConfig::new([authority_id]);
    reader_config.read_only = true;
    let mut authority = make(71, config, memory());
    let mut reader = make(72, reader_config, memory());
    let now = Instant::now();
    let author_addr = (31, 2);
    let reader_addr = (32, 2);
    connect(&mut authority, author_addr, &mut reader, reader_addr, now);

    let rogue = SignedMessage::<u64, String>::Update(
        999,
        Entry::present(reader.clock.now(), "injected".into()),
    );
    let mut frame = Vec::new();
    gossip::framing::write_complete(&serialize(&rogue).unwrap(), &mut frame);
    let signed_by_reader = reader.boundary.seal(&frame).unwrap();
    let replies = authority.receive(reader_addr, &signed_by_reader, now).unwrap();
    assert!(replies.is_empty());
    assert!(authority.get(&999).is_none());
    assert!(authority.members().is_empty());
    assert!(authority.tombstone_ack_snapshot().is_empty());

    // The normal crypto boundary rejects raw, unsigned application bytes entirely.
    assert!(authority.receive(reader_addr, &frame, now).unwrap().is_empty());
    assert!(authority.members().is_empty());
}

#[test]
fn version_exact_ack_bookkeeping_survives_v2_incremental_snapshot_and_reboot() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ack-v2.snapshot");
    let a_id = signer(81).peer_id();
    let b_id = signer(82).peer_id();
    let a_backend: Backend = Arc::new(FileSnapshot::new(&path));
    let mut a = make(81, SignedConfig::new([b_id]), a_backend.clone());
    let mut b = make(82, SignedConfig::new([a_id]), memory());
    let now = Instant::now();
    connect(&mut a, (41, 1), &mut b, (42, 1), now);

    a.remove(123).unwrap();
    a.snapshot_now().unwrap();
    assert!(!a.is_tombstone_stable(&123));
    deliver(&mut a, (41, 1), &mut b, (42, 1), now);
    let exact = a.acknowledged_version(&123, b_id).unwrap();
    assert!(a.is_tombstone_stable(&123));
    a.snapshot_now().unwrap();
    drop(a);

    let mut reloaded = make(81, SignedConfig::new([b_id]), a_backend);
    assert_eq!(reloaded.acknowledged_version(&123, b_id), Some(exact));
    assert_eq!(reloaded.members(), HashSet::from([b_id]));
    assert!(reloaded.is_tombstone_stable(&123));
    assert!(reloaded.collect_if_stable(&123));
    reloaded.snapshot_now().unwrap();
}

#[test]
fn failed_signed_persistence_write_retries_the_exact_uncommitted_generation() {
    use std::sync::atomic::{AtomicBool, Ordering};

    struct FlakyBackend {
        inner: InMemoryPersistence<u64, String, PeerId>,
        fail_once: AtomicBool,
    }

    impl Persistence<u64, String, PeerId> for FlakyBackend {
        fn load(&self) -> io::Result<Option<PersistedState<u64, String, PeerId>>> {
            Persistence::load(&self.inner)
        }

        fn save(&self, state: &PersistedState<u64, String, PeerId>) -> io::Result<()> {
            if self.fail_once.swap(false, Ordering::SeqCst) {
                return Err(io::Error::other("simulated interrupted snapshot write"));
            }
            Persistence::save(&self.inner, state)
        }
    }

    let peer = signer(92).peer_id();
    let backend: Backend = Arc::new(FlakyBackend {
        inner: InMemoryPersistence::new(),
        fail_once: AtomicBool::new(true),
    });
    let mut store = make(91, SignedConfig::new([peer]), backend.clone());
    store.insert(44, "kept".into()).unwrap();
    store.remove(45).unwrap();
    assert_eq!(store.snapshot_now().unwrap_err().kind(), io::ErrorKind::Other);
    let frozen = store.generations.freeze().unwrap();
    assert!(frozen.entries.contains_key(&44));
    assert!(frozen.entries.contains_key(&45));
    assert!(store.snapshot_now().is_ok());
    let restored = make(91, SignedConfig::new([peer]), backend);
    assert_eq!(restored.get(&44).map(String::as_str), Some("kept"));
    assert!(restored.is_tombstone(&45));
    assert!(!restored.is_tombstone_stable(&45));
}
