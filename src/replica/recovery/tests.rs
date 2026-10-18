use super::*;

fn cfg() -> FramingConfig {
    FramingConfig::default()
        .with_max_outbound_recovery_transfers_per_peer(1)
        .with_max_total_outbound_recovery_transfers(2)
        .with_max_outbound_recovery_bytes_per_peer(8)
        .with_max_total_outbound_recovery_bytes(12)
        .with_max_selective_recovery_rounds(2)
        .with_recovery_ttl(Duration::from_millis(10))
        .with_recovery_capability_ttl(Duration::from_millis(10))
}

#[test]
fn capability_is_bounded_and_expires() {
    let now = Instant::now();
    let mut book = RecoveryBook::default();
    book.record_capability("127.0.0.1".parse().unwrap(), now, cfg(), 1);
    book.record_capability("127.0.0.2".parse().unwrap(), now, cfg(), 1);
    assert!(book.supports("127.0.0.1".parse().unwrap(), now, cfg()));
    assert!(!book.supports("127.0.0.2".parse().unwrap(), now, cfg()));
    assert!(!book.supports(
        "127.0.0.1".parse().unwrap(),
        now + Duration::from_millis(10),
        cfg()
    ));
}

#[test]
fn per_peer_bound_evicts_oldest_deterministically() {
    let now = Instant::now();
    let peer = "127.0.0.1".parse().unwrap();
    let mut book = RecoveryBook::default();
    assert!(book.retain(peer, [1; 32], &[1u8; 4], now, cfg()).retained);
    let report = book.retain(
        peer,
        [2; 32],
        &[2u8; 4],
        now + Duration::from_millis(1),
        cfg(),
    );
    assert_eq!(report.evicted, 1);
    assert!(book
        .payload_for_report(peer, [1; 32], now + Duration::from_millis(2), cfg())
        .is_none());
    assert!(book
        .payload_for_report(peer, [2; 32], now + Duration::from_millis(2), cfg())
        .is_some());
}

#[test]
fn recovery_rounds_are_bounded() {
    let now = Instant::now();
    let peer = "127.0.0.1".parse().unwrap();
    let mut book = RecoveryBook::default();
    book.retain(peer, [3; 32], &[3u8; 4], now, cfg());
    assert!(book.payload_for_report(peer, [3; 32], now, cfg()).is_some());
    assert!(book.payload_for_report(peer, [3; 32], now, cfg()).is_some());
    assert!(book.payload_for_report(peer, [3; 32], now, cfg()).is_none());
}

#[test]
fn retained_transfer_defers_identical_full_send_but_preserves_timeout_fallback() {
    let now = Instant::now();
    let peer = "127.0.0.1".parse().unwrap();
    let config = cfg().with_recovery_ttl(Duration::from_secs(40));
    let mut book = RecoveryBook::default();
    let first = book.retain(peer, [9; 32], &[2; 4], now, config);
    assert!(first.retained);
    assert!(!first.skip_full_send);
    let immediate = book.retain(peer, [9; 32], &[2; 4], now + Duration::from_secs(1), config);
    assert!(immediate.skip_full_send);
    assert!(!immediate.fallback_full_retry);
    let retry = book.retain(
        peer,
        [9; 32],
        &[2; 4],
        now + Duration::from_secs(12),
        config,
    );
    assert!(!retry.skip_full_send);
    assert!(retry.fallback_full_retry);
    let next = book.retain(
        peer,
        [9; 32],
        &[2; 4],
        now + Duration::from_secs(13),
        config,
    );
    assert!(next.skip_full_send);
}

#[test]
fn ttl_releases_retained_bytes() {
    let now = Instant::now();
    let peer = "127.0.0.1".parse().unwrap();
    let mut book = RecoveryBook::default();
    book.retain(peer, [4; 32], &[4u8; 4], now, cfg());
    let report = book.expire(now + Duration::from_millis(10), cfg());
    assert_eq!(report.transfers, 1);
    assert_eq!(report.bytes, 4);
    assert_eq!(report.remaining_bytes, 0);
}

#[test]
fn selective_capability_requires_exact_payload_and_enablement() {
    assert!(is_selective_recovery_capability(
        SELECTIVE_RECOVERY_CAPABILITY
    ));
    for payload in [&b"sr"[..], &b"sr\\x00"[..], &b"sr\\x01x"[..], &b""[..]] {
        assert!(!is_selective_recovery_capability(payload));
    }
    let now = Instant::now();
    let peer = "127.0.0.1".parse().unwrap();
    let mut book = RecoveryBook::default();
    book.record_capability(peer, now, cfg().with_selective_recovery(false), 2);
    assert!(!book.supports(peer, now, cfg()));
    book.record_capability(peer, now, cfg(), 2);
    assert!(book.supports(peer, now, cfg()));
    assert!(!book.supports(peer, now, cfg().with_selective_recovery(false)));
    assert!(book.supports(peer, now + Duration::from_millis(9), cfg()));
    assert!(!book.supports(peer, now + Duration::from_millis(10), cfg()));
}

#[test]
fn sender_rejects_each_independent_capacity_limit_without_evicting_valid_state() {
    let now = Instant::now();
    let peer = "127.0.0.11".parse().unwrap();
    let caps = cfg()
        .with_recovery_ttl(Duration::from_secs(60))
        .with_max_outbound_recovery_transfers_per_peer(3)
        .with_max_total_outbound_recovery_transfers(4);
    let mut book = RecoveryBook::default();
    // Equality to both byte ceilings is valid; only strictly larger payloads are rejected.
    let accepted = book.retain(peer, [1; 32], &[1; 8], now, caps);
    assert!(accepted.retained);
    assert_eq!((accepted.transfers, accepted.bytes), (1, 8));
    for rejected_cfg in [
        caps.with_selective_recovery(false),
        caps.with_max_outbound_recovery_bytes_per_peer(7),
        caps.with_max_total_outbound_recovery_bytes(7),
        caps.with_max_outbound_recovery_transfers_per_peer(0),
        caps.with_max_total_outbound_recovery_transfers(0),
    ] {
        let rejected = book.retain(peer, [2; 32], &[2; 8], now, rejected_cfg);
        assert!(!rejected.retained);
        assert_eq!((rejected.transfers, rejected.bytes), (1, 8));
        assert_eq!(rejected.evicted, 0);
    }
    assert_eq!(book.occupancy(), (1, 8));
    assert_eq!(book.peer_bytes(peer), 8);
    assert_eq!(book.peer_transfer_count(peer), 1);
}

#[test]
fn global_and_peer_byte_budgets_evict_oldest_and_account_exactly() {
    let now = Instant::now();
    let a = "127.0.0.21".parse().unwrap();
    let b = "127.0.0.22".parse().unwrap();
    let c = cfg()
        .with_recovery_ttl(Duration::from_secs(60))
        .with_max_outbound_recovery_transfers_per_peer(4)
        .with_max_total_outbound_recovery_transfers(8)
        .with_max_outbound_recovery_bytes_per_peer(8)
        .with_max_total_outbound_recovery_bytes(12);
    let mut book = RecoveryBook::default();
    assert!(book.retain(a, [1; 32], &[1; 8], now, c).retained);
    assert!(
        book.retain(b, [2; 32], &[2; 4], now + Duration::from_millis(1), c)
            .retained
    );
    assert_eq!(book.occupancy(), (2, 12));
    // Exact total threshold is accepted; going above evicts the globally oldest peer.
    let result = book.retain(b, [3; 32], &[3; 4], now + Duration::from_millis(2), c);
    assert_eq!((result.evicted, result.transfers, result.bytes), (1, 2, 8));
    assert_eq!((book.peer_bytes(a), book.peer_bytes(b)), (0, 8));
    assert!(!book.complete(a, [1; 32]));
    // A new transfer from b exceeds the per-peer cap and must remove b's oldest.
    let second = book.retain(b, [4; 32], &[4; 4], now + Duration::from_millis(3), c);
    assert_eq!(second.evicted, 1);
    assert_eq!(book.peer_bytes(b), 8);
    assert!(!book.complete(b, [2; 32]));
    assert!(book.complete(b, [3; 32]));
    assert_eq!(book.occupancy(), (1, 4));
    assert!(!book.complete(b, [3; 32]));
    assert!(book.forget(b, [4; 32]));
    assert!(!book.forget(b, [4; 32]));
    assert_eq!(book.occupancy(), (0, 0));
}

#[test]
fn global_transfer_count_and_per_peer_count_are_independently_bounded() {
    let now = Instant::now();
    let a = "127.0.0.31".parse().unwrap();
    let b = "127.0.0.32".parse().unwrap();
    let caps = cfg()
        .with_recovery_ttl(Duration::from_secs(60))
        .with_max_outbound_recovery_transfers_per_peer(2)
        .with_max_total_outbound_recovery_transfers(2)
        .with_max_outbound_recovery_bytes_per_peer(64)
        .with_max_total_outbound_recovery_bytes(128);
    let mut book = RecoveryBook::default();
    book.retain(a, [1; 32], &[1], now, caps);
    book.retain(a, [2; 32], &[2], now + Duration::from_millis(1), caps);
    let r = book.retain(b, [3; 32], &[3], now + Duration::from_millis(2), caps);
    assert_eq!(r.evicted, 1);
    assert_eq!(book.occupancy(), (2, 2));
    assert!(!book.complete(a, [1; 32]));
    assert!(book.complete(a, [2; 32]));
    assert!(book.complete(b, [3; 32]));
    let mut book = RecoveryBook::default();
    let caps = caps
        .with_max_total_outbound_recovery_transfers(9)
        .with_max_outbound_recovery_transfers_per_peer(1);
    book.retain(a, [1; 32], &[1], now, caps);
    let r = book.retain(a, [2; 32], &[2], now + Duration::from_millis(1), caps);
    assert_eq!(r.evicted, 1);
    assert_eq!(book.occupancy(), (1, 1));
}

#[test]
fn exhausted_rounds_cannot_be_replayed_and_completion_is_peer_scoped() {
    let now = Instant::now();
    let a = "127.0.0.41".parse().unwrap();
    let b = "127.0.0.42".parse().unwrap();
    let config = cfg().with_recovery_ttl(Duration::from_secs(30));
    let mut book = RecoveryBook::default();
    book.retain(a, [4; 32], &[9; 6], now, config);
    assert!(book.payload_for_report(b, [4; 32], now, config).is_none());
    let first = book.payload_for_report(a, [4; 32], now, config).unwrap();
    assert_eq!(first.payload.as_ref(), &[9; 6]);
    assert!(!first.exhausted_after_this_round);
    let second = book.payload_for_report(a, [4; 32], now, config).unwrap();
    assert!(second.exhausted_after_this_round);
    assert!(book.payload_for_report(a, [4; 32], now, config).is_none());
    assert!(!book.complete(b, [4; 32]));
    assert_eq!(book.occupancy(), (1, 6));
    assert!(book.complete(a, [4; 32]));
    assert_eq!(book.occupancy(), (0, 0));
    assert!(!book.complete(a, [4; 32]));
    assert!(book.payload_for_report(a, [4; 32], now, config).is_none());
    // A zero-round configuration may retain a payload but never authorize a retransmission.
    book.retain(a, [5; 32], &[3; 2], now, config);
    assert!(book
        .payload_for_report(
            a,
            [5; 32],
            now,
            config.with_max_selective_recovery_rounds(0)
        )
        .is_none());
}

#[test]
fn expiry_is_exact_and_reports_live_occupancy_without_touching_other_peers() {
    let now = Instant::now();
    let a = "127.0.0.51".parse().unwrap();
    let b = "127.0.0.52".parse().unwrap();
    let c = cfg()
        .with_recovery_ttl(Duration::from_millis(10))
        .with_max_outbound_recovery_transfers_per_peer(3)
        .with_max_total_outbound_recovery_transfers(4)
        .with_max_outbound_recovery_bytes_per_peer(16)
        .with_max_total_outbound_recovery_bytes(32);
    let mut book = RecoveryBook::default();
    book.retain(a, [1; 32], &[1; 3], now, c);
    book.retain(b, [2; 32], &[2; 5], now + Duration::from_millis(5), c);
    let report = book.expire(now + Duration::from_millis(9), c);
    assert_eq!(
        (
            report.transfers,
            report.bytes,
            report.remaining_transfers,
            report.remaining_bytes
        ),
        (0, 0, 2, 8)
    );
    let report = book.expire(now + Duration::from_millis(10), c);
    assert_eq!(
        (
            report.transfers,
            report.bytes,
            report.remaining_transfers,
            report.remaining_bytes
        ),
        (1, 3, 1, 5)
    );
    assert!(!book.forget(a, [1; 32]));
    assert!(book.forget(b, [2; 32]));
    assert_eq!(book.occupancy(), (0, 0));
}

#[test]
fn exact_global_recovery_byte_ceiling_accepts_transfer() {
    let now = Instant::now();
    let peer = "127.0.0.67".parse().unwrap();
    let limits = cfg()
        .with_max_outbound_recovery_bytes_per_peer(16)
        .with_max_total_outbound_recovery_bytes(12);
    let mut book = RecoveryBook::default();
    let report = book.retain(peer, [71; 32], &[7; 12], now, limits);
    assert!(
        report.retained,
        "the configured global byte ceiling is inclusive"
    );
    assert_eq!((report.transfers, report.bytes), (1, 12));
    assert_eq!(book.occupancy(), (1, 12));
}

#[test]
fn verified_signers_isolate_recovery_even_with_identical_transfer_id_and_route_migration() {
    use gossip::auth::{Authenticator, ClusterKey};
    use gossip::peer::identity::{PeerAllowlist, PeerSigner, VerifiedPeerId};
    use gossip::peer::runtime::SignedDatagramBoundary;
    use gossip::replay::FRESHNESS_WINDOW_DEFAULT;

    let alice = PeerSigner::from_seed([0x51; 32]);
    let bob = PeerSigner::from_seed([0x52; 32]);
    let verifier = SignedDatagramBoundary::new(
        Authenticator::new(Some(ClusterKey::new([0x53; 32])), false).unwrap(),
        PeerSigner::from_seed([0x54; 32]),
        PeerAllowlist::new([alice.peer_id(), bob.peer_id()]),
        FRESHNESS_WINDOW_DEFAULT,
        2,
        2,
        1200,
    )
    .unwrap();
    let send = |signer: PeerSigner| {
        SignedDatagramBoundary::new(
            Authenticator::new(Some(ClusterKey::new([0x53; 32])), false).unwrap(),
            signer,
            PeerAllowlist::new([]),
            FRESHNESS_WINDOW_DEFAULT,
            2,
            2,
            1200,
        )
        .unwrap()
    };
    let alice = send(alice);
    let bob = send(bob);
    let a_packet = alice.seal(b"entry").unwrap();
    let b_packet = bob.seal(b"entry").unwrap();
    let a: VerifiedPeerId = verifier.open(&a_packet, |_| (false, 0)).unwrap().peer();
    let b: VerifiedPeerId = verifier.open(&b_packet, |_| (false, 1)).unwrap().peer();
    assert_ne!(a, b);

    let now = Instant::now();
    let policy = cfg().with_recovery_ttl(Duration::from_secs(30));
    let mut book = RecoveryBook::<VerifiedPeerId>::for_identity();
    book.record_capability(a, now, policy, 2);
    book.record_capability(b, now, policy, 2);
    assert!(book.supports(a, now, policy));
    assert!(book.supports(b, now, policy));

    let transfer = [0x56; 32];
    assert!(book.retain(a, transfer, b"aaaa", now, policy).retained);
    assert!(book.retain(b, transfer, b"bbbb", now, policy).retained);
    assert_eq!(book.occupancy(), (2, 8));
    assert_eq!(
        book.payload_for_report(a, transfer, now, policy)
            .unwrap()
            .payload
            .as_ref(),
        b"aaaa"
    );
    assert_eq!(
        book.payload_for_report(b, transfer, now, policy)
            .unwrap()
            .payload
            .as_ref(),
        b"bbbb"
    );
    // The same authenticated identity survives independent re-verification; no retained
    // state or recovery-round budget is tied to the (separately managed) transport route.
    let moved = alice.seal(b"after-route-move").unwrap();
    let peer_after_move = verifier.open(&moved, |_| (true, 2)).unwrap().peer();
    assert_eq!(peer_after_move, a);
    assert!(book.complete(peer_after_move, transfer));
    assert_eq!(book.occupancy(), (1, 4));
    assert!(book.payload_for_report(a, transfer, now, policy).is_none());
    assert!(book
        .payload_for_report(b, transfer, now, policy)
        .is_none_or(|payload| payload.payload.as_ref() == b"bbbb"));
    assert!(book.complete(b, transfer));
    assert_eq!(book.occupancy(), (0, 0));
}
