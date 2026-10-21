// Copyright 2026 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::num::NonZeroUsize;
use std::time::Duration;

use reconcile::{InMemoryNetwork, Transport};

#[tokio::test]
async fn public_non_ip_transport_enforces_mtu_and_preserves_source() {
    let network = InMemoryNetwork::<String>::with_datagram_limit(NonZeroUsize::new(8).unwrap());
    let sender = network.bind("serial-a".into());
    let receiver = network.bind("serial-b".into());
    let erased: &dyn Transport<String> = &sender;
    assert_eq!(
        erased
            .send_to(b"12345678", &"serial-b".into())
            .await
            .unwrap(),
        8
    );
    let mut buffer = [0; 8];
    let (n, source) = receiver.recv_from(&mut buffer).await.unwrap();
    assert_eq!(&buffer[..n], b"12345678");
    assert_eq!(source, "serial-a");
    assert_eq!(receiver.local_addr().unwrap(), "serial-b");
    assert_eq!(
        erased
            .send_to(b"123456789", &"serial-b".into())
            .await
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::InvalidInput
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(10), receiver.recv_from(&mut buffer))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn non_ip_truncation_consumes_only_one_datagram() {
    let network = InMemoryNetwork::<String>::new();
    let sender = network.bind("bus-a".into());
    let receiver = network.bind("bus-b".into());
    sender.send_to(b"12345678", &"bus-b".into()).await.unwrap();
    sender.send_to(b"ok", &"bus-b".into()).await.unwrap();
    let mut buffer = [0; 3];
    let (n, _) = receiver.recv_from(&mut buffer).await.unwrap();
    assert_eq!(&buffer[..n], b"123");
    let (n, _) = receiver.recv_from(&mut buffer).await.unwrap();
    assert_eq!(&buffer[..n], b"ok");
    assert_eq!(sender.send_to(b"lost", &"unbound".into()).await.unwrap(), 4);
}
