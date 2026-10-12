use super::cluster::{Cluster, KEY_LIMIT};
use super::transport::SIZE;
use std::io;

pub struct KeyRange {
    pub start: u32,
    pub end: u32,
}
impl KeyRange {
    fn parse(start: &str, end: &str) -> io::Result<Self> {
        let start = start.parse().map_err(io::Error::other)?;
        let end = end.parse().map_err(io::Error::other)?;
        if start >= end || end > KEY_LIMIT {
            return Err(io::Error::other(
                "range must satisfy 0 <= start < end <= 1024",
            ));
        }
        Ok(Self { start, end })
    }
}
fn peer(text: &str) -> io::Result<usize> {
    let id = text.parse::<usize>().map_err(io::Error::other)?;
    if id >= SIZE {
        return Err(io::Error::other("invalid replica"));
    }
    Ok(id)
}
fn flag(text: &str) -> io::Result<bool> {
    match text {
        "0" => Ok(false),
        "1" => Ok(true),
        _ => Err(io::Error::other("flag must be 0 or 1")),
    }
}
fn value(hex: &str) -> io::Result<String> {
    if hex.len() > 512 || hex.len() % 2 != 0 {
        return Err(io::Error::other("value must be at most 256 UTF-8 bytes"));
    }
    let bytes = hex
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).map_err(io::Error::other)?;
            u8::from_str_radix(text, 16).map_err(io::Error::other)
        })
        .collect::<io::Result<Vec<_>>>()?;
    String::from_utf8(bytes).map_err(io::Error::other)
}
pub fn apply(cluster: &mut Cluster, path: &str) -> io::Result<()> {
    let parts: Vec<_> = path.trim_start_matches('/').split('/').collect();
    let name = |id: usize| ["A", "B", "C"][id];
    let message = match parts.as_slice() {
        ["reset"] => {
            let revision = cluster.revision;
            *cluster = Cluster::new()?;
            cluster.revision = revision;
            "Reset: three empty replicas".into()
        }
        ["heal"] => {
            for id in 0..SIZE {
                cluster.connect(id, true);
            }
            for a in 0..SIZE {
                for b in a + 1..SIZE {
                    cluster.link(a, b, true);
                }
            }
            "All replicas and links connected".into()
        }
        ["connect", id, online] => {
            let id = peer(id)?;
            let online = flag(online)?;
            cluster.connect(id, online);
            format!(
                "{}: {}",
                name(id),
                if online {
                    "connected"
                } else {
                    "disconnected; local writes remain available"
                }
            )
        }
        ["link", a, b, open] => {
            let a = peer(a)?;
            let b = peer(b)?;
            let open = flag(open)?;
            if a == b {
                return Err(io::Error::other("select two different replicas"));
            }
            cluster.link(a, b, open);
            format!(
                "{} ↔ {}: {}",
                name(a),
                name(b),
                if open { "open" } else { "cut" }
            )
        }
        [action @ ("write" | "delete"), id, start, end, rest @ ..] => {
            let id = peer(id)?;
            let range = KeyRange::parse(start, end)?;
            match (*action, rest) {
                ("write", [hex]) => {
                    let value = value(hex)?;
                    let entries: Vec<_> = (range.start..range.end)
                        .map(|key| (key, value.clone()))
                        .collect();
                    cluster.nodes[id].insert_bulk(&entries);
                    format!(
                        "{}: wrote {} keys [{}, {}) = {:?}",
                        name(id),
                        entries.len(),
                        range.start,
                        range.end,
                        value
                    )
                }
                ("delete", []) => {
                    let count = cluster.nodes[id].range_to_vec(range.start..range.end).len();
                    cluster.nodes[id].delete_range(range.start..range.end);
                    format!(
                        "{}: deleted {} live keys [{}, {})",
                        name(id),
                        count,
                        range.start,
                        range.end
                    )
                }
                _ => return Err(io::Error::other("invalid range action")),
            }
        }
        _ => return Err(io::Error::other("invalid control")),
    };
    cluster.acknowledge(message);
    Ok(())
}
