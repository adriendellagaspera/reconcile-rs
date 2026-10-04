// Copyright 2023 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::UdpSocket;

use crate::entry::State;
use crate::replica::{send_control_batch_to, send_messages_paced, Message, SendPorts};
use crate::transport::UdpTransport;
use gossip::auth::Authenticator;

type Msg = Message<u64, Vec<u8>, State<u8>>;

/// `n` Update messages with `value_len`-byte values — enough to span several 64 KiB datagrams.
fn bulk_updates(n: u64, value_len: usize) -> Vec<Msg> {
    (0..n)
        .map(|k| Message::EntryUpdate((k, vec![0u8; value_len])))
        .collect()
}

/// Send `messages` (unauthenticated, to a discard address — the datagrams go nowhere on an
/// unconnected UDP socket) at `rate` and return how long it took.
/// Bounded by an outer timeout well above any legitimate pacing delay this module exercises:
/// a broken pacing calculation (e.g. a duration derived from multiplying instead of dividing)
/// produces an astronomically large but finite `Duration`, which `sleep` then waits out
/// literally rather than erroring — an unbounded `.await` here would hang the test for as
/// long as the surrounding harness lets it, instead of failing fast and readably.
async fn time_send(messages: &[Msg], rate: Option<usize>) -> Duration {
    let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let transport = UdpTransport::new(socket);
    let authenticator = Authenticator::new(None, false).unwrap();
    let sender_counter = gossip::replay::SenderCounter::new();
    let ports = SendPorts {
        transport: &transport,
        authenticator: &authenticator,
        sender_counter: &sender_counter,
        framing: crate::replicated_map::FramingConfig::default(),
    };
    let peer: SocketAddr = "127.0.0.1:9".parse().unwrap(); // discard port
    let mut send_buf = Vec::new();
    let start = Instant::now();
    tokio::time::timeout(
        Duration::from_secs(10),
        send_messages_paced(messages, &ports, &peer, &mut send_buf, rate),
    )
    .await
    .expect("send_messages_paced took over 10s — pacing duration math is almost certainly broken");
    start.elapsed()
}

/// `bulk_send_rate` actually meters the transfer: a multi-datagram payload sent at a
/// low rate takes substantially longer than the same payload sent unpaced. Anchored to
/// wall-clock, so we only assert a generous lower bound on the paced run (sleeping can only
/// lengthen it) and an upper bound on the unpaced run — robust to CI scheduler jitter.
#[tokio::test]
async fn bulk_send_rate_meters_the_transfer() {
    // ~265 KiB => ~5 datagrams of 64 KiB, i.e. ~4 inter-datagram pacing points.
    let messages = bulk_updates(256, 1024);

    let unpaced = time_send(&messages, None).await;
    assert!(
        unpaced < Duration::from_millis(200),
        "unpaced send should be near-instant, took {unpaced:?}"
    );

    // 512 KiB/s over ~256 KiB of leading datagrams => ~0.5 s of cumulative sleeps.
    let paced = time_send(&messages, Some(512 * 1024)).await;
    assert!(
        paced >= Duration::from_millis(300),
        "paced send should be metered to ~0.5 s, took {paced:?}"
    );
}

/// A `None` rate is the unpaced behaviour, and an explicit `0` is treated as "no
/// pacing" rather than dividing by zero.
#[tokio::test]
async fn zero_or_none_rate_does_not_pace() {
    let messages = bulk_updates(256, 1024);
    assert!(time_send(&messages, None).await < Duration::from_millis(200));
    assert!(time_send(&messages, Some(0)).await < Duration::from_millis(200));
}

/// A nonzero `bulk_send_rate` below the floor is clamped up to it rather than holding the
/// per-peer in-flight mark across an effectively unbounded sleep. Zero and `None`
/// still pass through unpaced.
#[tokio::test]
async fn tiny_bulk_send_rate_is_clamped_to_the_floor() {
    use crate::replica::Replica;
    use crate::replicated_map::{Config, MIN_BULK_SEND_RATE};

    async fn engine(addr: &str, bulk_send_rate: Option<usize>) -> Replica<i32, i32> {
        let config = Config {
            bulk_send_rate,
            ..Config::default()
                .with_port(5000)
                .with_listen_addr(addr.parse().unwrap())
                .with_insecure_no_key()
        };
        super::in_memory_test_replica(config)
    }

    let tiny = engine("127.0.0.80", Some(1)).await;
    assert_eq!(tiny.bulk_send_rate, Some(MIN_BULK_SEND_RATE));

    let none = engine("127.0.0.81", None).await;
    assert_eq!(none.bulk_send_rate, None);

    let zero = engine("127.0.0.82", Some(0)).await;
    assert_eq!(zero.bulk_send_rate, Some(0));

    let above_floor = engine("127.0.0.83", Some(MIN_BULK_SEND_RATE * 2)).await;
    assert_eq!(above_floor.bulk_send_rate, Some(MIN_BULK_SEND_RATE * 2));
}

/// A single message above the configured logical-message ceiling is rejected even though the
/// framing layer could otherwise split it. A normal-sized sibling still goes out, before or after
/// the rejected message, and no empty or oversized datagram is emitted.
#[tokio::test]
async fn message_above_logical_limit_is_dropped_without_harming_siblings() {
    use crate::transport::{InMemoryNetwork, Transport};

    async fn observe_sends(messages: &[Msg]) -> Vec<usize> {
        let net = InMemoryNetwork::new();
        let sender_addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let receiver_addr: SocketAddr = "127.0.0.1:2".parse().unwrap();
        let sender_transport = net.bind(sender_addr);
        let receiver_transport = net.bind(receiver_addr);

        let authenticator = Authenticator::new(None, false).unwrap();
        let sender_counter = gossip::replay::SenderCounter::new();
        let ports = SendPorts {
            transport: &sender_transport,
            authenticator: &authenticator,
            sender_counter: &sender_counter,
            framing: crate::replicated_map::FramingConfig::default()
                .with_max_logical_message_size(1024),
        };
        let mut send_buf = Vec::new();
        send_messages_paced(messages, &ports, &receiver_addr, &mut send_buf, None).await;

        let mut sizes = Vec::new();
        let mut buf = [0u8; 1 << 17];
        while let Ok(Ok((n, _))) = tokio::time::timeout(
            Duration::from_millis(50),
            receiver_transport.recv_from(&mut buf),
        )
        .await
        {
            sizes.push(n);
        }
        sizes
    }

    // One message far above the configured 1 KiB logical ceiling, flanked by ordinary ones.
    let oversized = vec![Message::EntryUpdate((
        999u64,
        vec![0u8; super::super::BUFFER_SIZE * 2],
    ))];
    let small_before = bulk_updates(1, 16);
    let small_after = bulk_updates(1, 16);

    // Rejected first in the batch: it must not produce an empty frame.
    let mut messages = oversized.clone();
    messages.extend(small_after.clone());
    let sizes = observe_sends(&messages).await;
    assert_eq!(
        sizes.len(),
        1,
        "expected exactly the one normal-sized datagram, got {sizes:?}"
    );
    assert!(
        sizes[0] < super::super::BUFFER_SIZE,
        "unexpected datagram size {sizes:?}"
    );

    // Rejected in the middle of the batch: the preceding valid message still flushes normally.
    let mut messages = small_before;
    messages.extend(oversized);
    let sizes = observe_sends(&messages).await;
    assert_eq!(
        sizes.len(),
        1,
        "expected exactly the one normal-sized datagram, got {sizes:?}"
    );
    assert!(
        sizes[0] < super::super::BUFFER_SIZE,
        "unexpected datagram size {sizes:?}"
    );
}

#[tokio::test]
async fn logical_limit_splits_valid_small_messages_instead_of_dropping_the_batch() {
    use crate::transport::{InMemoryNetwork, Transport};

    let net = InMemoryNetwork::new();
    let sender_addr: SocketAddr = "127.0.0.1:11".parse().unwrap();
    let receiver_addr: SocketAddr = "127.0.0.1:12".parse().unwrap();
    let sender_transport = net.bind(sender_addr);
    let receiver_transport = net.bind(receiver_addr);
    let authenticator = Authenticator::new(None, false).unwrap();
    let sender_counter = gossip::replay::SenderCounter::new();
    let ports = SendPorts {
        transport: &sender_transport,
        authenticator: &authenticator,
        sender_counter: &sender_counter,
        framing: crate::replicated_map::FramingConfig::default().with_max_logical_message_size(600),
    };
    let messages = bulk_updates(2, 400);
    let mut send_buf = Vec::new();
    send_messages_paced(&messages, &ports, &receiver_addr, &mut send_buf, None).await;

    let mut datagrams = 0;
    let mut buf = [0u8; 2048];
    while let Ok(Ok((_len, _))) = tokio::time::timeout(
        Duration::from_millis(50),
        receiver_transport.recv_from(&mut buf),
    )
    .await
    {
        datagrams += 1;
    }
    assert_eq!(
        datagrams, 2,
        "two individually valid messages must be split rather than dropped as one oversized batch"
    );
}

/// A refinement *round* above the UDP payload ceiling remains one logical workset.
///
/// Physical datagrams may fragment it, but the receiver must expose exactly one complete logical
/// payload so bulk admission and dump-slot accounting see the same range set as `protocol_round`.
#[tokio::test]
async fn oversized_refinement_batch_is_split_without_dropping_ranges() {
    use crate::transport::{InMemoryNetwork, Transport};

    let empty = rsos::FingerprintTreeMap::<u64, u64>::new();
    let segment = rbsr::initial_ranges(&empty).pop().unwrap();
    let mut messages = Vec::new();
    let mut encoded = Vec::new();
    while encoded.len() < 67_794 {
        let message: Msg = Message::EntryFingerprint(segment.clone());
        gossip::bincode::encode(&message, &mut encoded).unwrap();
        messages.push(message);
    }
    assert!(encoded.len() > super::super::BUFFER_SIZE);

    let net = InMemoryNetwork::new();
    let sender_addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let receiver_addr: SocketAddr = "127.0.0.1:2".parse().unwrap();
    let sender_transport = net.bind(sender_addr);
    let receiver_transport = net.bind(receiver_addr);
    let authenticator = Authenticator::new(None, false).unwrap();
    let sender_counter = gossip::replay::SenderCounter::new();
    let framing = crate::replicated_map::FramingConfig::default();
    let ports = SendPorts {
        transport: &sender_transport,
        authenticator: &authenticator,
        sender_counter: &sender_counter,
        framing,
    };
    let mut send_buf = Vec::new();
    send_control_batch_to(&messages, &ports, &receiver_addr, &mut send_buf).await;

    let replay_filter =
        gossip::replay::ReplayFilter::new(gossip::replay::FRESHNESS_WINDOW_DEFAULT, false);
    let mut reassembler = gossip::framing::Reassembler::new(framing.reassembly_limits());
    let mut datagrams = 0;
    let mut completed_payloads = 0;
    let mut received = 0;
    let mut buf = [0u8; 1 << 17];
    while let Ok(Ok((len, from))) = tokio::time::timeout(
        Duration::from_millis(50),
        receiver_transport.recv_from(&mut buf),
    )
    .await
    {
        datagrams += 1;
        assert!(
            len <= crate::replicated_map::DEFAULT_DATAGRAM_PAYLOAD_BUDGET,
            "datagram length {len}"
        );
        let payload = authenticator
            .open(&buf[..len])
            .unwrap()
            .check_version()
            .unwrap()
            .verify_replay(&replay_filter, from.ip())
            .unwrap();
        let report = reassembler.accept(from.ip(), payload, Instant::now());
        match report.outcome.unwrap() {
            gossip::framing::ReceiveOutcome::Complete(logical) => {
                completed_payloads += 1;
                let decoded: Vec<Msg> =
                    gossip::bincode::decode_stream(logical.as_bytes(), messages.len()).unwrap();
                assert!(decoded
                    .iter()
                    .all(|msg| matches!(msg, Message::EntryFingerprint(_))));
                received += decoded.len();
            }
            gossip::framing::ReceiveOutcome::Pending => {}
            gossip::framing::ReceiveOutcome::Duplicate => {
                panic!("fresh control-batch fragments must not duplicate")
            }
        }
    }
    assert!(datagrams >= 2, "batch was not physically fragmented");
    assert_eq!(
        completed_payloads, 1,
        "one refinement round must decode as one logical workset"
    );
    assert_eq!(
        received,
        messages.len(),
        "lost a fingerprint during framing"
    );
}

fn encoded_len(message: &Msg) -> usize {
    let mut encoded = Vec::new();
    gossip::bincode::encode(message, &mut encoded).unwrap();
    encoded.len()
}

fn update_reaching_encoded_len(key: u64, target: usize) -> Msg {
    for value_len in 0..=target {
        let message = Message::EntryUpdate((key, vec![0u8; value_len]));
        if encoded_len(&message) >= target {
            return message;
        }
    }
    panic!("could not construct an encoded update reaching {target} bytes");
}

async fn count_in_memory_sends(
    messages: &[Msg],
    framing: crate::replicated_map::FramingConfig,
) -> usize {
    use crate::transport::{InMemoryNetwork, Transport};

    let net = InMemoryNetwork::new();
    let sender_addr: SocketAddr = "127.0.0.1:31".parse().unwrap();
    let receiver_addr: SocketAddr = "127.0.0.1:32".parse().unwrap();
    let sender_transport = net.bind(sender_addr);
    let receiver_transport = net.bind(receiver_addr);
    let authenticator = Authenticator::new(None, false).unwrap();
    let sender_counter = gossip::replay::SenderCounter::new();
    let ports = SendPorts {
        transport: &sender_transport,
        authenticator: &authenticator,
        sender_counter: &sender_counter,
        framing,
    };
    let mut send_buf = Vec::new();
    send_messages_paced(messages, &ports, &receiver_addr, &mut send_buf, None).await;

    let mut datagrams = 0;
    let mut buf = [0u8; 1 << 17];
    while let Ok(Ok((_len, _))) = tokio::time::timeout(
        Duration::from_millis(50),
        receiver_transport.recv_from(&mut buf),
    )
    .await
    {
        datagrams += 1;
    }
    datagrams
}

#[tokio::test]
async fn exact_logical_and_fragment_count_limits_are_sendable() {
    let authenticator = Authenticator::new(None, false).unwrap();
    let framing = crate::replicated_map::FramingConfig::default();
    let complete_capacity = gossip::framing::complete_payload_capacity(
        framing.datagram_payload_budget,
        authenticator.overhead(),
    )
    .unwrap();
    let fragment_capacity = gossip::framing::fragment_payload_capacity(
        framing.datagram_payload_budget,
        authenticator.overhead(),
    )
    .unwrap();

    let message = update_reaching_encoded_len(1, complete_capacity + 1);
    let logical_len = encoded_len(&message);
    let fragment_count = logical_len.div_ceil(fragment_capacity);
    assert!(fragment_count >= 2);

    let at_limits = framing
        .with_max_logical_message_size(logical_len)
        .with_max_fragments_per_message(fragment_count);
    assert_eq!(
        count_in_memory_sends(std::slice::from_ref(&message), at_limits).await,
        fragment_count
    );

    let below_fragment_limit = framing
        .with_max_logical_message_size(logical_len)
        .with_max_fragments_per_message(fragment_count - 1);
    assert_eq!(
        count_in_memory_sends(std::slice::from_ref(&message), below_fragment_limit).await,
        0
    );
}

#[tokio::test]
async fn batching_uses_each_encoded_message_length_not_accumulated_batch_length() {
    let authenticator = Authenticator::new(None, false).unwrap();
    let framing = crate::replicated_map::FramingConfig::default();
    let capacity = gossip::framing::complete_payload_capacity(
        framing.datagram_payload_budget,
        authenticator.overhead(),
    )
    .unwrap();

    let first = update_reaching_encoded_len(1, capacity * 7 / 10);
    let second = update_reaching_encoded_len(2, capacity * 4 / 10);
    let third = update_reaching_encoded_len(3, capacity * 4 / 10);
    let first_len = encoded_len(&first);
    let second_len = encoded_len(&second);
    let third_len = encoded_len(&third);

    assert!(first_len <= capacity && second_len <= capacity && third_len <= capacity);
    assert!(first_len + second_len > capacity);
    assert!(second_len + third_len <= capacity);

    assert_eq!(
        count_in_memory_sends(&[first, second, third], framing).await,
        2
    );
}
