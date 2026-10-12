use std::io;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

use reconcile::runtime as tokio;
use reconcile::{
    async_trait, replicated_map::Config, ClusterKey, Discovery, DnsDiscovery, DnsDiscoveryError,
    InMemoryNetwork, InMemoryTransport, NodeId, ReplicatedMap, Transport, UdpTransport,
};
use tokio_util::sync::CancellationToken;
use wasm_bindgen::prelude::*;

struct Link {
    inner: InMemoryTransport,
    connected: Arc<AtomicBool>,
}

#[async_trait]
impl Transport for Link {
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.inner.recv_from(buf).await
    }
    async fn send_to(&self, buf: &[u8], dst: &SocketAddr) -> io::Result<usize> {
        if self.connected.load(Ordering::Relaxed) {
            self.inner.send_to(buf, dst).await
        } else {
            Ok(buf.len())
        }
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}

/// Browser integration probe: timers, cancellation and two authenticated real replicas.
#[wasm_bindgen]
pub async fn verify_runtime() -> Result<String, JsValue> {
    console_error_panic_hook::set_once();
    let network = InMemoryNetwork::new();
    let connected = Arc::new(AtomicBool::new(false));
    let cancel = CancellationToken::new();
    let mut nodes = Vec::new();
    let mut tasks = Vec::new();
    for id in 1..=2 {
        let addr: SocketAddr = format!("127.0.0.{id}:7000").parse().unwrap();
        let config = Config::default()
            .with_port(7000)
            .with_listen_addr(addr.ip())
            .with_net("127.0.0.0/30".parse().unwrap())
            .map_err(|e| JsValue::from_str(&e.to_string()))?
            .with_cluster_key(ClusterKey::new([0x42; 32]))
            .with_node_id(NodeId::new(id));
        let transport = Link {
            inner: network.bind(addr),
            connected: connected.clone(),
        };
        let node = ReplicatedMap::<String, u64>::new_with_transport(config, Arc::new(transport))
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        let running = node.clone();
        let token = cancel.clone();
        tasks.push(tokio::spawn(async move { running.run(token).await }));
        nodes.push(node);
    }
    let result = async {
        let error = UdpTransport::bind("127.0.0.1:7000".parse().unwrap(), None, None).await.unwrap_err();
        if error.kind() != io::ErrorKind::Unsupported { return Err(JsValue::from_str("browser UDP did not report Unsupported")); }
        let resolver = DnsDiscovery::new("fleet.invalid", 7000);
        let error = resolver.discover().await.unwrap_err();
        if !matches!(error.downcast_ref::<DnsDiscoveryError>(), Some(DnsDiscoveryError::Resolve(e)) if e.kind() == io::ErrorKind::Unsupported) {
            return Err(JsValue::from_str("browser DNS did not report Unsupported"));
        }
        tokio::time::timeout(Duration::ZERO, async {}).await.map_err(|e| JsValue::from_str(&e.to_string()))?;
        if tokio::time::timeout(Duration::from_millis(1), std::future::pending::<()>()).await.is_ok() {
            return Err(JsValue::from_str("browser timeout did not expire"));
        }
        let aborted = tokio::spawn(std::future::pending::<()>());
        aborted.abort();
        if aborted.await.is_ok() { return Err(JsValue::from_str("task cancellation did not abort")); }
        nodes[0].insert("left".into(), 1);
        nodes[1].insert("right".into(), 2);
        nodes[0].insert("concurrent".into(), 11);
        nodes[1].insert("concurrent".into(), 22);
        tokio::time::sleep(Duration::from_millis(300)).await;
        if nodes[1].get_cloned(&"left".into()).is_some() || nodes[0].get_cloned(&"right".into()).is_some() {
            return Err(JsValue::from_str("isolated replicas leaked knowledge"));
        }
        connected.store(true, Ordering::Relaxed);
        nodes[0].seed_peer("127.0.0.2".parse().unwrap());
        nodes[1].seed_peer("127.0.0.1".parse().unwrap());
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                for node in &nodes { node.start_reconciliation().await; }
                let a = nodes[0].snapshot();
                let b = nodes[1].snapshot();
                if a.len() == 3 && a.iter().eq(b.iter()) { break; }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }).await.map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok("two replicas: isolated writes, concurrent LWW, exact dated-entry convergence; timers, cancellation and unsupported socket/DNS errors passed".to_string())
    }.await;
    cancel.cancel();
    for task in tasks {
        task.abort();
    }
    result
}
