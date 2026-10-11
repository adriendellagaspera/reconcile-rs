use super::cluster::Cluster;
use std::io;
pub fn apply(cluster: &mut Cluster, path: &str) -> io::Result<()> {
    match path.trim_start_matches('/') {
        "next" => cluster.next(),
        "reset" => {
            *cluster = Cluster::new()?;
            Ok(())
        }
        _ => Err(io::Error::other("invalid control")),
    }
}
