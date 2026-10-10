use super::*;
use reconcile::InMemoryNetwork;

#[test]
fn ingress_budget_is_shared_and_has_only_one_datagram_of_burst() {
    let start = Instant::now();
    let mut bucket = Bucket {
        credit: 0.0,
        last: start,
        burst: 1200,
    };
    let mut admitted = 0;
    for millis in 0..5000 {
        let now = start + Duration::from_millis(millis);
        for _sender in 0..12 {
            if bucket.admit(100, now, 1250) {
                admitted += 100;
            }
        }
        assert!(admitted as f64 <= millis as f64 * 1.25);
    }
    assert!(admitted >= 6100);
    assert!(bucket.admit(1200, start + Duration::from_secs(100), 1250));
    assert!(!bucket.admit(1, start + Duration::from_secs(100), 1250));
}

#[tokio::test]
async fn outgoing_neighbors_share_serialization_and_overflow_is_bounded() {
    let fabric = InMemoryNetwork::new();
    let a: SocketAddr = "127.0.0.1:9000".parse().unwrap();
    let b: SocketAddr = "127.0.0.2:9000".parse().unwrap();
    let c: SocketAddr = "127.0.0.3:9000".parse().unwrap();
    let receiver_b = fabric.bind(b);
    let receiver_c = fabric.bind(c);
    let stats = Arc::new(Counters::default());
    let transport =
        BandwidthTransport::glider(Arc::new(fabric.bind(a)), 10, 1200, stats.clone(), Some(c));
    let start = Instant::now();
    transport.send_to(&[0; 250], &b).await.unwrap();
    transport.send_to(&[0; 250], &c).await.unwrap();
    let mut buf = [0; 1200];
    receiver_b.recv_from(&mut buf).await.unwrap();
    assert!(start.elapsed() >= Duration::from_millis(200));
    receiver_c.recv_from(&mut buf).await.unwrap();
    assert!(start.elapsed() >= Duration::from_millis(400));
    assert_eq!(stats.snapshot().tx_bytes, 500);
    for _ in 0..100 {
        transport.send_to(&[0; 1200], &b).await.unwrap();
    }
    assert!(stats.snapshot().queued_bytes <= QUEUE_BYTES);
    assert!(stats.snapshot().tx_dropped > 0);
}

#[tokio::test]
async fn receiver_fan_in_is_policed_before_protocol_consumption() {
    let fabric = InMemoryNetwork::new();
    let a: SocketAddr = "127.0.0.1:9000".parse().unwrap();
    let b: SocketAddr = "127.0.0.2:9000".parse().unwrap();
    let c: SocketAddr = "127.0.0.3:9000".parse().unwrap();
    let stats = Arc::new(Counters::default());
    let receiver = BandwidthTransport::new(Arc::new(fabric.bind(a)), 10, 1200, stats.clone());
    let sender_b = fabric.bind(b);
    let sender_c = fabric.bind(c);
    tokio::time::sleep(Duration::from_secs(1)).await;
    sender_b.send_to(&[1; 1200], &a).await.unwrap();
    sender_c.send_to(&[2; 1200], &a).await.unwrap();
    let mut buf = [0; 1200];
    receiver.recv_from(&mut buf).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), receiver.recv_from(&mut buf))
            .await
            .is_err()
    );
    assert_eq!(stats.snapshot().rx_bytes, 1200);
    assert_eq!(stats.snapshot().rx_dropped, 1);
}

#[test]
fn station_reservation_is_byte_fair_and_idle_share_is_borrowed() {
    let destination = "127.0.0.1:9000".parse().unwrap();
    let mut pending = Pending::default();
    for _ in 0..40 {
        pending.packets[0].push_back(Packet {
            bytes: vec![0; 1000],
            destination,
        });
        for _ in 0..10 {
            pending.packets[1].push_back(Packet {
                bytes: vec![0; 100],
                destination,
            });
        }
    }
    let mut served = [0usize; 2];
    for _ in 0..200 {
        let (class, packet) = pending.pop().unwrap();
        served[class] += packet.bytes.len();
        assert!(served[0].abs_diff(served[1]) <= 1000);
    }
    pending.packets[0].clear();
    while let Some((class, _)) = pending.pop() {
        assert_eq!(class, 1);
    }
    pending.packets[0].push_back(Packet {
        bytes: vec![0; 1000],
        destination,
    });
    assert_eq!(pending.pop().unwrap().0, 0);
}

#[tokio::test]
async fn fleet_backlog_cannot_fill_station_admission_reservation() {
    let fabric = InMemoryNetwork::new();
    let a = "127.0.0.1:9000".parse().unwrap();
    let fleet = "127.0.0.2:9000".parse().unwrap();
    let center = "127.0.0.3:9000".parse().unwrap();
    let stats = Arc::new(Counters::default());
    let sender = BandwidthTransport::glider(
        Arc::new(fabric.bind(a)),
        10,
        1200,
        stats.clone(),
        Some(center),
    );
    for _ in 0..100 {
        sender.send_to(&[0; 1000], &fleet).await.unwrap();
    }
    let before = stats.snapshot();
    sender.send_to(&[0; 1000], &center).await.unwrap();
    let after = stats.snapshot();
    assert_eq!(after.tx_dropped, before.tx_dropped);
    assert_eq!(after.queued_bytes, before.queued_bytes + 1000);
    assert!(after.queued_bytes <= QUEUE_BYTES);
}

#[tokio::test]
async fn station_has_independent_transmit_lanes_and_unlimited_receive_fan_in() {
    let fabric = InMemoryNetwork::new();
    let a = "127.0.0.1:9000".parse().unwrap();
    let b = "127.0.0.2:9000".parse().unwrap();
    let c = "127.0.0.3:9000".parse().unwrap();
    let peer_b = fabric.bind(b);
    let peer_c = fabric.bind(c);
    let stats = Arc::new(Counters::default());
    let center = BandwidthTransport::command_center(
        Arc::new(fabric.bind(a)),
        10,
        1200,
        stats.clone(),
        vec![b, c],
    );
    let start = Instant::now();
    center.send_to(&[0; 250], &b).await.unwrap();
    center.send_to(&[0; 250], &c).await.unwrap();
    assert_eq!(stats.snapshot().queued_bytes, 500);
    assert_eq!(stats.snapshot().max_lane_queued_bytes, 250);
    tokio::time::timeout(Duration::from_millis(350), async {
        let mut buf = [0; 1200];
        peer_b.recv_from(&mut buf).await.unwrap();
        peer_c.recv_from(&mut buf).await.unwrap();
    })
    .await
    .expect("CC lanes must run concurrently, not share 400 ms serialization");
    assert!(start.elapsed() >= Duration::from_millis(200));
    peer_b.send_to(&[1; 1200], &a).await.unwrap();
    peer_c.send_to(&[2; 1200], &a).await.unwrap();
    let mut buf = [0; 1200];
    center.recv_from(&mut buf).await.unwrap();
    center.recv_from(&mut buf).await.unwrap();
    assert_eq!(stats.snapshot().rx_bytes, 2400);
    assert_eq!(stats.snapshot().rx_dropped, 0);
}

#[tokio::test]
async fn reserved_class_accepts_a_full_configured_datagram() {
    let fabric = InMemoryNetwork::new();
    let a = "127.0.0.1:9000".parse().unwrap();
    let center = "127.0.0.2:9000".parse().unwrap();
    let receiver = fabric.bind(center);
    let stats = Arc::new(Counters::default());
    let sender = BandwidthTransport::glider(
        Arc::new(fabric.bind(a)),
        0,
        16384,
        stats.clone(),
        Some(center),
    );
    sender.send_to(&[0; 16384], &center).await.unwrap();
    let mut buf = [0; 16384];
    tokio::time::timeout(Duration::from_secs(1), receiver.recv_from(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stats.snapshot().tx_dropped, 0);
}

#[tokio::test]
async fn tiny_datagrams_cannot_bypass_bounded_queue_memory() {
    let fabric = InMemoryNetwork::new();
    let a = "127.0.0.1:9000".parse().unwrap();
    let b = "127.0.0.2:9000".parse().unwrap();
    let stats = Arc::new(Counters::default());
    let sender = BandwidthTransport::new(Arc::new(fabric.bind(a)), 10, 1200, stats.clone());
    for _ in 0..100 {
        sender.send_to(&[], &b).await.unwrap();
    }
    assert_eq!(stats.snapshot().queued_bytes, 0);
    assert_eq!(stats.snapshot().tx_dropped, 36);
}
