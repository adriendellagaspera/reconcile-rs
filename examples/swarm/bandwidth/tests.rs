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
    let transport = BandwidthTransport::new(Arc::new(fabric.bind(a)), 10, 1200, stats.clone());
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
