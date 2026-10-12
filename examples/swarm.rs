#![forbid(unsafe_code)]
#[path = "swarm/cluster.rs"]
mod cluster;
#[path = "swarm/controls.rs"]
mod controls;
#[path = "swarm/http.rs"]
mod http;
#[path = "swarm/transport.rs"]
mod transport;
use clap::Parser;
#[derive(Parser)]
#[command(about = "Three real replicas: local writes, partition and reconciliation")]
struct Args {
    /// Loopback HTTP port; 0 chooses an available port.
    #[arg(long, default_value_t = 8088)]
    port: u16,
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .init();
    http::serve(cluster::Cluster::new()?, args.port).await
}
