#![forbid(unsafe_code)]

#[path = "swarm/cluster.rs"]
mod cluster;
#[path = "swarm/http.rs"]
mod http;

use cluster::Cluster;
use std::io;

#[tokio::main(flavor = "current_thread")]
async fn main() -> io::Result<()> {
    let mut loss: f64 = 35.0;
    let mut port = 8088;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--loss" => loss = args.next().and_then(|s| s.parse().ok()).unwrap_or(-1.0),
            "--port" => port = args.next().and_then(|s| s.parse().ok()).unwrap_or(0),
            _ => {
                return Err(io::Error::other(
                    "usage: swarm [--loss 0..100] [--port PORT]",
                ))
            }
        }
    }
    if !loss.is_finite() || !(0.0..=100.0).contains(&loss) {
        return Err(io::Error::other("loss must be between 0 and 100 percent"));
    }
    http::serve(Cluster::new(loss)?, port).await
}
