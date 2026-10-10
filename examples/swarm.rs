#![forbid(unsafe_code)]

#[path = "swarm/bandwidth.rs"]
mod bandwidth;
#[path = "swarm/cluster.rs"]
mod cluster;
#[path = "swarm/http.rs"]
mod http;
#[path = "swarm/telemetry.rs"]
mod telemetry;
#[path = "swarm/transport.rs"]
mod transport;
#[path = "swarm/world.rs"]
mod world;

use clap::Parser;
use cluster::Cluster;
use std::io;

#[derive(Parser)]
#[command(about = "Offline visual demonstration of real reconcile-rs replicas")]
struct Args {
    /// Per-datagram packet loss percentage.
    #[arg(long, default_value_t = 35.0)]
    loss: f64,
    /// Loopback HTTP port; 0 selects an available port.
    #[arg(long, default_value_t = 8088)]
    port: u16,
    /// Number of drones (2–20), plus one command-center replica.
    #[arg(long, default_value_t = 12)]
    nodes: usize,
    /// Accelerate application time only (1–20); protocol timers stay real.
    #[arg(long, default_value_t = 1.0)]
    speed: f64,
    /// In-memory datagram budget in bytes; use 1200 for default UDP framing.
    #[arg(long, default_value_t = 1200)]
    mtu: usize,
    /// Glider aggregate TX/RX and CC per-glider TX in kbit/s; 0 disables shaping.
    #[arg(long, default_value_t = 10)]
    bandwidth_kbps: usize,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> io::Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .init();
    telemetry::install()?;
    if !args.loss.is_finite() || !(0.0..=100.0).contains(&args.loss) {
        return Err(io::Error::other("loss must be between 0 and 100 percent"));
    }
    if !args.speed.is_finite() || !(1.0..=20.0).contains(&args.speed) {
        return Err(io::Error::other("speed must be between 1 and 20"));
    }
    http::serve(
        Cluster::with_limits(args.loss, args.nodes, args.mtu, args.bandwidth_kbps)?,
        args.port,
        args.speed,
    )
    .await
}
