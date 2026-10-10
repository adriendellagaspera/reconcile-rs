// Copyright 2026 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Address-agnostic, datagram-only in-process fabric.
//!
//! Addresses are opaque to the fabric: neither routing nor delivery requires IP, a port number,
//! or a session. This is an internal primitive used by the legacy socket-addressed
//! `InMemoryTransport` and by non-IP tests, not yet the public runtime transport boundary.
//! Sender addresses are untrusted metadata, never authenticated logical peer identities.

use std::collections::HashMap;
use std::hash::Hash;
use std::io;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex as AsyncMutex;

type Datagram<A> = (A, Vec<u8>);

/// An in-process message medium, parameterized by its substrate endpoint address.
#[derive(Debug)]
pub(crate) struct DatagramFabric<A> {
    routes: Arc<Mutex<HashMap<A, UnboundedSender<Datagram<A>>>>>,
    max_datagram_bytes: usize,
}

impl<A> Default for DatagramFabric<A> {
    fn default() -> Self {
        Self::new(usize::MAX)
    }
}

impl<A> Clone for DatagramFabric<A> {
    fn clone(&self) -> Self {
        Self {
            routes: Arc::clone(&self.routes),
            max_datagram_bytes: self.max_datagram_bytes,
        }
    }
}

impl<A> DatagramFabric<A> {
    /// Construct a medium with a nonzero maximum datagram size. The default has no added cap,
    /// matching the legacy in-memory adapter's behavior.
    pub(crate) fn new(max_datagram_bytes: usize) -> Self {
        assert!(max_datagram_bytes > 0, "datagram capacity must be nonzero");
        Self {
            routes: Arc::new(Mutex::new(HashMap::new())),
            max_datagram_bytes,
        }
    }
}

impl<A: Clone + Eq + Hash> DatagramFabric<A> {
    /// Bind an endpoint. Rebinding replaces that endpoint's delivery route.
    pub(crate) fn bind(&self, address: A) -> DatagramPort<A> {
        let (tx, rx) = unbounded_channel();
        self.routes.lock().insert(address.clone(), tx);
        DatagramPort {
            fabric: self.clone(),
            address,
            rx: AsyncMutex::new(rx),
        }
    }
}

/// One bound datagram endpoint. Receives from any other endpoint on its fabric.
#[derive(Debug)]
pub(crate) struct DatagramPort<A> {
    fabric: DatagramFabric<A>,
    address: A,
    rx: AsyncMutex<UnboundedReceiver<Datagram<A>>>,
}

impl<A: Clone + Eq + Hash> DatagramPort<A> {
    /// Return the substrate-specific endpoint address, not a logical peer identity.
    pub(crate) fn local_addr(&self) -> A {
        self.address.clone()
    }

    /// Send one bounded datagram. Unknown/unbound endpoints silently drop it, as with UDP.
    /// A successful send does not guarantee delivery.
    pub(crate) fn send_to(&self, bytes: &[u8], destination: &A) -> io::Result<usize> {
        if bytes.len() > self.fabric.max_datagram_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "datagram exceeds substrate payload limit",
            ));
        }
        if let Some(tx) = self.fabric.routes.lock().get(destination) {
            let _ = tx.send((self.address.clone(), bytes.to_vec()));
        }
        Ok(bytes.len())
    }

    /// Receive one datagram, retaining the sender's substrate endpoint address.
    ///
    /// Like a datagram socket, a message larger than `buf` is truncated on receive.
    pub(crate) async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, A)> {
        let (src, bytes) =
            self.rx.lock().await.recv().await.ok_or_else(|| {
                io::Error::new(io::ErrorKind::BrokenPipe, "in-memory network closed")
            })?;
        let n = bytes.len().min(buf.len());
        buf[..n].copy_from_slice(&bytes[..n]);
        Ok((n, src))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// These coordinates represent optical switching fabric slots, not disguised IP addresses.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    struct OpticalSlot {
        terminal: u16,
        wavelength: u8,
    }

    #[tokio::test]
    async fn non_ip_endpoints_exchange_bounded_datagrams() {
        let fabric = DatagramFabric::new(8);
        let a = OpticalSlot {
            terminal: 1,
            wavelength: 3,
        };
        let b = OpticalSlot {
            terminal: 2,
            wavelength: 3,
        };
        let sender = fabric.bind(a);
        let receiver = fabric.bind(b);

        assert_eq!(sender.send_to(b"contact", &b).unwrap(), 7);
        let mut buffer = [0; 8];
        let (read, source) = receiver.recv_from(&mut buffer).await.unwrap();
        assert_eq!(source, a);
        assert_eq!(&buffer[..read], b"contact");
        assert_eq!(receiver.local_addr(), b);

        // A datagram exactly at the MTU must be admitted; only larger messages fail.
        // This also protects the strict '>' comparison against a '>=' regression.
        assert_eq!(sender.send_to(b"capacity", &b).unwrap(), 8);
        let (read, source) = receiver.recv_from(&mut buffer).await.unwrap();
        assert_eq!(source, a);
        assert_eq!(&buffer[..read], b"capacity");

        assert_eq!(
            sender.send_to(b"exceeds mtu", &b).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        let unbound = OpticalSlot {
            terminal: 99,
            wavelength: 5,
        };
        assert_eq!(sender.send_to(b"lost", &unbound).unwrap(), 4);
    }

    #[tokio::test]
    async fn endpoint_rebinding_does_not_require_an_ip_identity() {
        let fabric = DatagramFabric::new(4);
        let source = OpticalSlot {
            terminal: 1,
            wavelength: 2,
        };
        let old_route = OpticalSlot {
            terminal: 2,
            wavelength: 2,
        };
        let new_route = OpticalSlot {
            terminal: 2,
            wavelength: 7,
        };
        let sender = fabric.bind(source);
        let _old = fabric.bind(old_route);
        let relocated = fabric.bind(new_route);

        // A higher-level peer directory can change its route independently of the logical
        // peer identifier, without translating either route into a fake SocketAddr.
        let logical_peer_id: u128 = 0xfeed;
        let mut directory = HashMap::from([(logical_peer_id, old_route)]);
        directory.insert(logical_peer_id, new_route);
        sender.send_to(b"ok", &directory[&logical_peer_id]).unwrap();

        let mut buffer = [0; 4];
        let (read, origin) = relocated.recv_from(&mut buffer).await.unwrap();
        assert_eq!(&buffer[..read], b"ok");
        assert_eq!(origin, source);
        assert_eq!(directory.len(), 1);
    }

    #[tokio::test]
    async fn buffer_truncation_matches_legacy_socket_behavior() {
        let fabric = DatagramFabric::default();
        let source = OpticalSlot {
            terminal: 1,
            wavelength: 1,
        };
        let target = OpticalSlot {
            terminal: 2,
            wavelength: 1,
        };
        let sender = fabric.bind(source);
        let receiver = fabric.bind(target);
        sender.send_to(b"12345678", &target).unwrap();

        let mut buffer = [0; 3];
        let (read, peer) = receiver.recv_from(&mut buffer).await.unwrap();
        assert_eq!(read, 3);
        assert_eq!(peer, source);
        assert_eq!(&buffer, b"123");
    }
}
