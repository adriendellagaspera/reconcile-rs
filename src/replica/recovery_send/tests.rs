use super::*;
use crate::entry::State;
use crate::replica::RecoveryBook;
use crate::transport::{InMemoryNetwork, Transport};
use gossip::auth::Authenticator;
use gossip::replay::{ReplayFilter, SenderCounter, FRESHNESS_WINDOW_DEFAULT};

type Msg = Message<u64, Vec<u8>, State<u8>>;

#[test]
fn capability_advertisement_is_exact_and_disabled_peers_remain_silent() {
    let mut wire = Vec::new();
    append_capability::<u64, Vec<u8>, State<u8>>(FramingConfig::default(), &mut wire);
    let messages: Vec<Msg> = gossip::bincode::decode_stream(&wire, 4).unwrap();
    assert_eq!(messages.len(), 1);
    match &messages[0] {
        Message::Reserved6(data) => assert_eq!(data.as_slice(), SELECTIVE_RECOVERY_CAPABILITY),
        other => panic!("selective capability missing: {other:?}"),
    }
    wire.clear();
    append_capability::<u64, Vec<u8>, State<u8>>(
        FramingConfig::default().with_selective_recovery(false),
        &mut wire,
    );
    assert!(wire.is_empty());
}

#[tokio::test]
async fn authenticated_missing_ranges_emit_only_requested_offsets_and_validate_bounds() {
    let network = InMemoryNetwork::new();
    let local: SocketAddr = "127.20.8.1:9286".parse().unwrap();
    let peer: SocketAddr = "127.20.8.2:9286".parse().unwrap();
    let sender = network.bind(local);
    let receiver = network.bind(peer);
    let authenticator = Authenticator::new(None, false).unwrap();
    let counter = SenderCounter::new();
    let replay = ReplayFilter::new(FRESHNESS_WINDOW_DEFAULT, false);
    let recovery = Mutex::new(RecoveryBook::default());
    let cfg = FramingConfig::default()
        .with_max_selective_recovery_rounds(32)
        .with_recovery_ttl(Duration::from_secs(60));
    let id = [0xa1; 32];
    let payload = b"abcdefghijklmnopqrst";
    let now = Instant::now();
    recovery.lock().record_capability(peer.ip(), now, cfg, 4);
    assert!(
        recovery
            .lock()
            .retain(peer.ip(), id, payload, now, cfg)
            .retained
    );
    let ports = SendPorts {
        transport: &sender,
        authenticator: &authenticator,
        sender_counter: &counter,
        recovery: &recovery,
        framing: cfg,
    };

    for report in [
        vec![(0, 0)],
        vec![(0, 21)],
        vec![(6, 4), (8, 3)],
        vec![(u32::MAX, 4)],
    ] {
        assert!(!retransmit_missing_to(&ports, peer, id, &report).await);
    }
    assert!(!retransmit_missing_to(&ports, local, id, &[(0, 1)]).await);
    assert!(!retransmit_missing_to(&ports, peer, [0; 32], &[(0, 1)]).await);

    // Valid missing ranges remain distinct: no whole-message retry, and offsets are exact.
    assert!(retransmit_missing_to(&ports, peer, id, &[(0, 3), (3, 3), (8, 4), (16, 4)]).await);
    let mut observed = Vec::new();
    let mut buf = [0u8; 2048];
    while let Ok(Ok((size, source))) =
        tokio::time::timeout(Duration::from_millis(25), receiver.recv_from(&mut buf)).await
    {
        assert_eq!(source, local);
        let verified = authenticator
            .open(&buf[..size])
            .unwrap()
            .check_version()
            .unwrap()
            .verify_replay(&replay, local.ip())
            .unwrap();
        let metadata = framing::fragment_metadata(verified.as_bytes()).unwrap();
        assert_eq!(metadata.transfer_id, id);
        assert_eq!(metadata.total_len, payload.len());
        observed.push((metadata.offset, metadata.payload_len));
    }
    assert_eq!(observed, vec![(0, 3), (3, 3), (8, 4), (16, 4)]);
    assert_eq!(recovery.lock().occupancy(), (1, payload.len()));

    // A peer-scoped completion ACK releases retained state exactly once.
    complete_recovery(&ports, local, id);
    assert_eq!(recovery.lock().occupancy(), (1, payload.len()));
    complete_recovery(&ports, peer, id);
    assert_eq!(recovery.lock().occupancy(), (0, 0));
    assert!(!retransmit_missing_to(&ports, peer, id, &[(0, 3)]).await);
}

#[tokio::test]
async fn terminal_free_idle_nack_is_authenticated_rate_limited_and_capability_gated() {
    let network = InMemoryNetwork::new();
    let local: SocketAddr = "127.20.9.1:9286".parse().unwrap();
    let peer: SocketAddr = "127.20.9.2:9286".parse().unwrap();
    let sender = network.bind(local);
    let receiver = network.bind(peer);
    let authenticator = Authenticator::new(None, false).unwrap();
    let counter = SenderCounter::new();
    let recovery = Mutex::new(RecoveryBook::default());
    let cfg = FramingConfig::default().with_reassembly_ttl(Duration::from_secs(90));
    let reassembler = Mutex::new(framing::Reassembler::new(cfg.reassembly_limits()));
    let id = framing::transfer_id(b"abcdefghij");
    let mut fragment = Vec::new();
    framing::write_fragment(id, 10, 0, b"abc", &mut fragment).unwrap();
    let wire = authenticator.seal(counter.next_seq(), counter.next_stamp(), &fragment);
    let replay = ReplayFilter::new(FRESHNESS_WINDOW_DEFAULT, false);
    let verified = authenticator
        .open(&wire)
        .unwrap()
        .check_version()
        .unwrap()
        .verify_replay(&replay, peer.ip())
        .unwrap();
    let stale = Instant::now() - Duration::from_secs(2);
    assert!(matches!(
        reassembler
            .lock()
            .accept(peer.ip(), verified, stale)
            .outcome,
        Ok(framing::ReceiveOutcome::Pending)
    ));
    let ports = SendPorts {
        transport: &sender,
        authenticator: &authenticator,
        sender_counter: &counter,
        recovery: &recovery,
        framing: cfg,
    };
    // An unknown capability must not emit control.
    retry_idle_incomplete(&ports, &reassembler, local.port()).await;
    let mut buf = [0u8; 2048];
    assert!(
        tokio::time::timeout(Duration::from_millis(25), receiver.recv_from(&mut buf))
            .await
            .is_err()
    );

    // Re-seed the transfer: the earlier unknown-capability poll consumed the request timer.
    let fresh_wire = authenticator.seal(counter.next_seq(), counter.next_stamp(), &fragment);
    let fresh = authenticator
        .open(&fresh_wire)
        .unwrap()
        .check_version()
        .unwrap()
        .verify_replay(&replay, peer.ip())
        .unwrap();
    assert!(matches!(
        reassembler.lock().accept(peer.ip(), fresh, stale).outcome,
        Ok(framing::ReceiveOutcome::Duplicate)
    ));
    // Ensure the due interval has elapsed without any sleeping.
    let due = stale + Duration::from_secs(6);
    let seeded = reassembler.lock().poll_idle_missing(
        due,
        Duration::from_secs(1),
        Duration::from_secs(3),
        8,
        1,
    );
    assert_eq!(seeded.len(), 1);
    assert_eq!(seeded[0].ranges, vec![(3, 7)]);
    recovery
        .lock()
        .record_capability(peer.ip(), Instant::now(), cfg, 4);
    // The prior synthetic poll was deliberately in the past; a real poll remains rate-limited.
    retry_idle_incomplete(&ports, &reassembler, local.port()).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(25), receiver.recv_from(&mut buf))
            .await
            .is_err()
    );

    // A second independent transfer is due; the eligible peer gets exactly one control.
    let second_id = framing::transfer_id(b"1234567890");
    framing::write_fragment(second_id, 10, 0, b"123", &mut fragment).unwrap();
    let second_wire = authenticator.seal(counter.next_seq(), counter.next_stamp(), &fragment);
    let second = authenticator
        .open(&second_wire)
        .unwrap()
        .check_version()
        .unwrap()
        .verify_replay(&replay, peer.ip())
        .unwrap();
    assert!(matches!(
        reassembler.lock().accept(peer.ip(), second, stale).outcome,
        Ok(framing::ReceiveOutcome::Pending)
    ));
    retry_idle_incomplete(&ports, &reassembler, local.port()).await;
    let (size, _) = tokio::time::timeout(Duration::from_secs(1), receiver.recv_from(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let decoded = authenticator
        .open(&buf[..size])
        .unwrap()
        .check_version()
        .unwrap()
        .verify_replay(
            &ReplayFilter::new(FRESHNESS_WINDOW_DEFAULT, false),
            local.ip(),
        )
        .unwrap();
    assert_eq!(
        framing::parse_recovery_control(decoded.as_bytes(), 8).unwrap(),
        Some(framing::RecoveryControl::Missing {
            transfer_id: second_id,
            ranges: vec![(3, 7)]
        })
    );
    retry_idle_incomplete(&ports, &reassembler, local.port()).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(25), receiver.recv_from(&mut buf))
            .await
            .is_err()
    );
}

#[test]
fn idle_expiration_gate_actually_retires_stale_sender_bytes() {
    let network = InMemoryNetwork::new();
    let local: SocketAddr = "127.20.10.1:9286".parse().unwrap();
    let peer: SocketAddr = "127.20.10.2:9286".parse().unwrap();
    let transport = network.bind(local);
    let authenticator = Authenticator::new(None, false).unwrap();
    let counter = SenderCounter::new();
    let recovery = Mutex::new(RecoveryBook::default());
    let cfg = FramingConfig::default().with_recovery_ttl(Duration::from_secs(1));
    let old = Instant::now() - Duration::from_secs(3);
    assert!(
        recovery
            .lock()
            .retain(peer.ip(), [9; 32], b"stale", old, cfg)
            .retained
    );
    let ports = SendPorts {
        transport: &transport,
        authenticator: &authenticator,
        sender_counter: &counter,
        recovery: &recovery,
        framing: cfg,
    };
    assert_eq!(recovery.lock().occupancy(), (1, 5));
    expire_recovery_state(&ports);
    assert_eq!(recovery.lock().occupancy(), (0, 0));
}

#[tokio::test]
async fn multi_fragment_retransmission_tracks_every_exact_chunk_offset() {
    let network = InMemoryNetwork::new();
    let local: SocketAddr = "127.20.8.3:9286".parse().unwrap();
    let peer: SocketAddr = "127.20.8.4:9286".parse().unwrap();
    let sender = network.bind(local);
    let receiver = network.bind(peer);
    let authenticator = Authenticator::new(None, false).unwrap();
    let counter = SenderCounter::new();
    let replay = ReplayFilter::new(FRESHNESS_WINDOW_DEFAULT, false);
    let recovery = Mutex::new(RecoveryBook::default());
    let cfg = FramingConfig::default().with_recovery_ttl(Duration::from_secs(60));
    let capacity =
        framing::fragment_payload_capacity(cfg.datagram_payload_budget, authenticator.overhead())
            .unwrap();
    assert!(capacity > 0);
    let payload = vec![23u8; capacity * 2 + 17];
    let id = [0xa2; 32];
    let now = Instant::now();
    recovery.lock().record_capability(peer.ip(), now, cfg, 4);
    assert!(
        recovery
            .lock()
            .retain(peer.ip(), id, &payload, now, cfg)
            .retained
    );
    let ports = SendPorts {
        transport: &sender,
        authenticator: &authenticator,
        sender_counter: &counter,
        recovery: &recovery,
        framing: cfg,
    };
    assert!(retransmit_missing_to(&ports, peer, id, &[(0, payload.len() as u32)]).await);
    let mut observed = Vec::new();
    let mut buf = [0u8; 2048];
    while let Ok(Ok((size, _))) =
        tokio::time::timeout(Duration::from_millis(25), receiver.recv_from(&mut buf)).await
    {
        let opened = authenticator
            .open(&buf[..size])
            .unwrap()
            .check_version()
            .unwrap()
            .verify_replay(&replay, local.ip())
            .unwrap();
        let metadata = framing::fragment_metadata(opened.as_bytes()).unwrap();
        observed.push((metadata.offset, metadata.payload_len));
    }
    assert_eq!(
        observed,
        vec![(0, capacity), (capacity, capacity), (2 * capacity, 17)]
    );
}
