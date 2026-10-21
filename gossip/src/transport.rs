// Copyright 2023 Developers of the reconcile project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The [`Transport`] port and its [`UdpTransport`]/[`InMemoryTransport`] adapters.

use std::hash::Hash;
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;

use async_trait::async_trait;
use socket2::SockRef;
use tokio::net::UdpSocket;
use tracing::{debug, warn};

/// Connectionless datagram I/O over an opaque substrate endpoint `A`.
///
/// Endpoints are untrusted routing metadata, never authenticated peer identities. No IP,
/// connection, reliable delivery, ordering, or independent streams are required. `SocketAddr`
/// is the default for the legacy runtime and UDP; signed replicas can use any endpoint type.
/// Receive consumes one message and may truncate it to the supplied buffer. A successful send
/// reports the bytes accepted by the substrate, not an acknowledgement by the remote peer.
/// Callers must size receive buffers for their substrate and enforce their protocol MTU.
/// # Implementing your own
/// All supporting types (`io::Result` and the `#[async_trait]` macro) are either from
/// `std` or re-exported from this crate (or `reconcile`). An external implementation
/// chooses its endpoint type and never has to independently depend on
/// `async-trait` and match its version to this crate's:
// async-trait lowers these async methods to boxed Future return types that are already
// must-use; current Clippy otherwise reports double_must_use on the macro expansion.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait Transport<A: Send + Sync + 'static = SocketAddr>: Send + Sync + 'static {
    /// Receive one datagram into `buf`, returning the number of bytes read and the sender address.
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, A)>;

    /// Send one datagram to `dst`, returning the number of bytes written.
    async fn send_to(&self, buf: &[u8], dst: &A) -> io::Result<usize>;

    /// The local address this transport is bound to.
    fn local_addr(&self) -> io::Result<A>;
}

/// The default [`Transport`] adapter: a tokio UDP socket.
#[derive(Clone, Debug)]
pub struct UdpTransport(Arc<UdpSocket>);

impl UdpTransport {
    /// Wrap an already-bound UDP socket.
    pub fn new(socket: Arc<UdpSocket>) -> Self {
        UdpTransport(socket)
    }

    /// Bind a UDP socket at `addr` and size its kernel send/receive buffers, returning the ready
    /// transport. `recv_buffer_size` / `send_buffer_size` size `SO_RCVBUF` / `SO_SNDBUF`
    /// respectively; `None` leaves the inherited OS default.
    /// # Errors
    /// Returns an `io::Error` if the socket cannot be bound to `addr` (e.g. the port is in use).
    pub async fn bind(
        addr: SocketAddr,
        recv_buffer_size: Option<usize>,
        send_buffer_size: Option<usize>,
    ) -> io::Result<Self> {
        let socket = UdpSocket::bind(addr).await?;
        set_socket_buffers(&socket, recv_buffer_size, send_buffer_size);
        Ok(UdpTransport(Arc::new(socket)))
    }

    /// Borrow the underlying socket (e.g. to tune `SO_RCVBUF` / `SO_SNDBUF`).
    pub fn socket(&self) -> &UdpSocket {
        &self.0
    }
}

/// Apply the requested `SO_RCVBUF` / `SO_SNDBUF` sizes; `None` leaves the OS default.
/// The kernel clamps an over-large request rather than failing, so clamping is a `debug`, not a
/// warning. Linux `getsockopt` reports the doubled value, so a honoured request reads back larger
/// than asked.
fn set_socket_buffers(
    socket: &UdpSocket,
    recv_buffer_size: Option<usize>,
    send_buffer_size: Option<usize>,
) {
    let sock = SockRef::from(socket);
    if let Some(size) = recv_buffer_size {
        match sock.set_recv_buffer_size(size) {
            Ok(()) => match sock.recv_buffer_size() {
                Ok(actual) => debug!(
                    "gossip socket SO_RCVBUF: requested {size} B, OS granted {actual} B \
                     (raise net.core.rmem_max if a larger buffer is needed)"
                ),
                Err(e) => debug!("could not read back SO_RCVBUF: {e}"),
            },
            Err(e) => warn!("failed to set gossip socket SO_RCVBUF to {size} B: {e}"),
        }
    }
    if let Some(size) = send_buffer_size {
        match sock.set_send_buffer_size(size) {
            Ok(()) => match sock.send_buffer_size() {
                Ok(actual) => debug!(
                    "gossip socket SO_SNDBUF: requested {size} B, OS granted {actual} B \
                     (raise net.core.wmem_max if a larger buffer is needed)"
                ),
                Err(e) => debug!("could not read back SO_SNDBUF: {e}"),
            },
            Err(e) => warn!("failed to set gossip socket SO_SNDBUF to {size} B: {e}"),
        }
    }
}

#[async_trait]
impl Transport for UdpTransport {
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.0.recv_from(buf).await
    }

    async fn send_to(&self, buf: &[u8], dst: &SocketAddr) -> io::Result<usize> {
        self.0.send_to(buf, dst).await
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.0.local_addr()
    }
}

/// An in-process [`Transport`] over a shared [`InMemoryNetwork`]: reliable and FIFO per
/// sender→receiver pair, so convergence is deterministic on a single-threaded runtime. A datagram
/// to an unbound address is dropped, as with UDP.
/// Public, not test-gated, so downstream crates can drive a deterministic cluster of their own.
pub use in_memory::{InMemoryNetwork, InMemoryTransport};

mod in_memory {
    use super::*;
    use crate::substrate::{DatagramFabric, DatagramPort};

    /// A shared in-process datagram fabric over opaque substrate endpoints.
    /// The default endpoint type preserves the legacy socket-addressed API. Pending messages
    /// use unbounded in-process queues; the optional limit bounds individual datagrams only.
    ///
    /// ```
    /// use reconcile_gossip::InMemoryNetwork;
    /// let network = InMemoryNetwork::<String>::new();
    /// let port = network.bind("optical-slot-a".into());
    /// ```
    #[derive(Clone, Debug)]
    pub struct InMemoryNetwork<A = SocketAddr> {
        fabric: DatagramFabric<A>,
    }

    impl<A> Default for InMemoryNetwork<A> {
        fn default() -> Self {
            Self {
                fabric: DatagramFabric::default(),
            }
        }
    }

    impl<A> InMemoryNetwork<A> {
        /// An empty fabric with no bound transports. Equivalent to [`Default::default`].
        pub fn new() -> Self {
            InMemoryNetwork::default()
        }

        /// Construct a fabric that rejects sends exceeding `limit` bytes, including wire
        /// authentication and framing overhead. Receive buffers may still truncate messages.
        pub fn with_datagram_limit(limit: NonZeroUsize) -> Self {
            Self {
                fabric: DatagramFabric::new(limit.get()),
            }
        }
    }

    impl<A: Clone + Eq + Hash> InMemoryNetwork<A> {
        /// Bind a transport at `addr`. A datagram sent to `addr` by any peer on this network is
        /// delivered to the returned transport's [`recv_from`](Transport::recv_from).
        pub fn bind(&self, addr: A) -> InMemoryTransport<A> {
            InMemoryTransport {
                port: self.fabric.bind(addr),
            }
        }
    }

    /// One endpoint on an [`InMemoryNetwork`].
    #[derive(Debug)]
    pub struct InMemoryTransport<A = SocketAddr> {
        port: DatagramPort<A>,
    }

    #[async_trait]
    impl<A: Clone + Eq + Hash + Send + Sync + 'static> Transport<A> for InMemoryTransport<A> {
        async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, A)> {
            self.port.recv_from(buf).await
        }

        async fn send_to(&self, buf: &[u8], dst: &A) -> io::Result<usize> {
            self.port.send_to(buf, dst)
        }

        fn local_addr(&self) -> io::Result<A> {
            Ok(self.port.local_addr())
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn in_memory_delivers_between_endpoints() {
        let net = InMemoryNetwork::new();
        let a: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let b: SocketAddr = "127.0.0.1:2".parse().unwrap();
        let ta = net.bind(a);
        let tb = net.bind(b);

        ta.send_to(b"hello", &b).await.unwrap();
        let mut buf = [0u8; 16];
        let (n, src) = tb.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"hello");
        assert_eq!(src, a);
        assert_eq!(tb.local_addr().unwrap(), b);
    }

    #[tokio::test]
    async fn in_memory_drops_datagram_to_unknown_address() {
        let net = InMemoryNetwork::new();
        let a: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let ta = net.bind(a);
        let unknown: SocketAddr = "127.0.0.1:9".parse().unwrap();
        let n = ta.send_to(b"lost", &unknown).await.unwrap();
        assert_eq!(n, 4);
    }
}
