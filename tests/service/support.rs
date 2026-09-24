// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Polling helpers shared by every test in this binary: convergence is asynchronous, so
//! assertions need to retry rather than check once.

use std::time::Duration;

pub(crate) async fn wait_until<F: FnMut() -> bool>(mut f: F) -> bool {
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        if f() {
            return true;
        }
    }
    false
}

macro_rules! assert_until {
    ( $x:expr ) => {
        assert!(crate::support::wait_until(|| $x).await, stringify!($x))
    };
}
pub(crate) use assert_until;

/// Like [`wait_until`] but waits up to ~10 s. Tombstone GC is gated on a 1 s scan loop
/// (`TOMBSTONE_CLEARING`) plus the wall-clock tombstone timeout, so events that depend on a
/// completed GC need a longer budget than the 1 s [`wait_until`].
pub(crate) async fn wait_until_slow<F: FnMut() -> bool>(mut f: F) -> bool {
    for _ in 0..1000 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        if f() {
            return true;
        }
    }
    false
}

macro_rules! assert_until_slow {
    ( $x:expr ) => {
        assert!(crate::support::wait_until_slow(|| $x).await, stringify!($x))
    };
}
pub(crate) use assert_until_slow;

/// Bind two distinct loopback endpoints to one OS-selected protocol port without releasing
/// either endpoint between selection and use. Retry if the second address is already occupied.
pub(crate) async fn bind_udp_pair(
    first: std::net::IpAddr,
    second: std::net::IpAddr,
) -> (
    u16,
    std::sync::Arc<tokio::net::UdpSocket>,
    std::sync::Arc<tokio::net::UdpSocket>,
) {
    use std::io::ErrorKind;
    use std::net::SocketAddr;
    use std::sync::Arc;

    assert_ne!(first, second, "each cluster node must bind a distinct IP");
    for _ in 0..128 {
        let a = Arc::new(
            tokio::net::UdpSocket::bind(SocketAddr::new(first, 0))
                .await
                .expect("bind first UDP endpoint"),
        );
        let port = a.local_addr().expect("bound UDP address").port();
        match tokio::net::UdpSocket::bind(SocketAddr::new(second, port)).await {
            Ok(b) => return (port, a, Arc::new(b)),
            Err(error) if error.kind() == ErrorKind::AddrInUse => continue,
            Err(error) => panic!("bind second UDP endpoint: {error}"),
        }
    }
    panic!("could not reserve a shared UDP port for both cluster endpoints");
}
