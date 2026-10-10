// Browser-only UDP boundary; callers supply their own Transport.
use super::*;
/// UDP requires an operating-system socket; browser callers supply a transport instead.
#[derive(Clone, Debug)]
pub struct UdpTransport;
fn unavailable() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "UDP sockets are unavailable in a browser; use new_with_transport",
    )
}
impl UdpTransport {
    /// Browser UDP binding returns Unsupported.
    pub async fn bind(_: SocketAddr, _: Option<usize>, _: Option<usize>) -> io::Result<Self> {
        Err(unavailable())
    }
}
#[async_trait]
impl Transport for UdpTransport {
    async fn recv_from(&self, _: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        Err(unavailable())
    }
    async fn send_to(&self, _: &[u8], _: &SocketAddr) -> io::Result<usize> {
        Err(unavailable())
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Err(unavailable())
    }
}
