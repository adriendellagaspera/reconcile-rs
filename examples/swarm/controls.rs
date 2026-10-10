use super::cluster::Cluster;
use super::world::{OrderAction, PeerState};
use std::io;

pub fn apply(cluster: &mut Cluster, path: &str) -> io::Result<()> {
    match path.trim_start_matches('/') {
        "heal" => {
            cluster.world.scripted = false;
            cluster.heal();
            return Ok(());
        }
        "partition" => {
            cluster.world.scripted = false;
            cluster.partition(true);
            return Ok(());
        }
        "jammer" => {
            cluster.toggle_jammer();
            return Ok(());
        }
        "weather" => {
            cluster.world.scripted = false;
            let storm = cluster.state()["storm"].as_bool().unwrap();
            cluster.partition(!storm);
            return Ok(());
        }
        "observe" => {
            cluster.world.scripted = false;
            cluster.observe();
            return Ok(());
        }
        "contact" => {
            cluster.world.scripted = false;
            cluster.world.reveal_contact();
            cluster.observe();
            return Ok(());
        }
        "play" => {
            cluster.world.playing = true;
            return Ok(());
        }
        "pause" => {
            cluster.world.playing = false;
            return Ok(());
        }
        action @ ("reset" | "demo") => {
            *cluster = Cluster::with_limits(
                cluster.loss,
                cluster.center(),
                cluster.datagram_budget,
                cluster.bandwidth_kbps,
            )?;
            cluster.world.playing = true;
            if action == "demo" {
                cluster.start_demo();
            }
            return Ok(());
        }
        _ => {}
    }
    let parts: Vec<_> = path.trim_start_matches('/').split('/').collect();
    let index = |value: &str| value.parse::<usize>().map_err(io::Error::other);
    let result = match parts.as_slice() {
        ["peer", id, action] => {
            let state = match *action {
                "online" => PeerState::Active,
                "offline" => PeerState::Offline,
                "stop" => PeerState::Stopped,
                _ => return Err(io::Error::other("unknown peer action")),
            };
            cluster.set_peer(index(id)?, state)
        }
        ["link", a, b, "toggle"] => cluster.toggle_link(index(a)?, index(b)?),
        ["order", id, action] => cluster.issue_order(
            index(id)?,
            match *action {
                "scan" => OrderAction::Scan,
                "hold" => OrderAction::Hold,
                "patrol" => OrderAction::Patrol,
                _ => return Err(io::Error::other("unknown order")),
            },
        ),
        ["range", value] => cluster.set_range(value.parse().map_err(io::Error::other)?),
        _ => Err(io::Error::other("invalid control")),
    };
    if result.is_ok() {
        cluster.world.scripted = false;
    }
    result
}
