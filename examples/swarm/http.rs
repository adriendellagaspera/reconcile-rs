use std::io;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

use super::cluster::Cluster;
use super::world::{PeerState, STEP_SECONDS};

pub async fn serve(cluster: Cluster, port: u16, speed: f64) -> io::Result<()> {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)).await?;
    println!("Swarm demo: http://{}/", listener.local_addr()?);
    println!(
        "{} actual replicas; seeded loss {}%; 160 ms RTT. Ctrl-C to stop.",
        cluster.nodes.len(),
        cluster.loss
    );
    let cluster = Arc::new(Mutex::new(cluster));
    let connections = Arc::new(Semaphore::new(16));
    let mut ticks = tokio::time::interval(Duration::from_secs_f64(STEP_SECONDS / speed));
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = ticks.tick() => cluster.lock().advance(),
            connection = listener.accept() => {
                let (socket, _) = connection?;
                if let Ok(permit) = connections.clone().try_acquire_owned() {
                    let cluster = cluster.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        let _ = tokio::time::timeout(Duration::from_secs(3), respond(socket, cluster)).await;
                    });
                }
            }
        }
    }
}

async fn respond(mut socket: TcpStream, shared: Arc<Mutex<Cluster>>) -> io::Result<()> {
    let (method, path) = request(&mut socket).await?;
    let (status, kind, body) = match (method.as_str(), path.as_str()) {
        ("GET", "/") => (
            "200 OK",
            "text/html; charset=utf-8",
            include_str!("index.html").to_string(),
        ),
        ("GET", "/app.js") => (
            "200 OK",
            "text/javascript; charset=utf-8",
            include_str!("app.js").to_string(),
        ),
        ("GET", "/knowledge.js") => (
            "200 OK",
            "text/javascript; charset=utf-8",
            include_str!("knowledge.js").to_string(),
        ),
        ("GET", "/state") => (
            "200 OK",
            "application/json",
            shared.lock().state().to_string(),
        ),
        (
            "POST",
            action @ ("/heal" | "/partition" | "/observe" | "/reset" | "/play" | "/pause" | "/demo"
            | "/contact" | "/weather"),
        ) => {
            let mut cluster = shared.lock();
            match action {
                "/heal" => {
                    cluster.world.scripted = false;
                    cluster.heal();
                }
                "/partition" => {
                    cluster.world.scripted = false;
                    cluster.partition(true);
                }
                "/weather" => {
                    cluster.world.scripted = false;
                    let storm = cluster.state()["storm"].as_bool().unwrap();
                    cluster.partition(!storm);
                }
                "/observe" => {
                    cluster.world.scripted = false;
                    cluster.observe();
                }
                "/contact" => {
                    cluster.world.scripted = false;
                    cluster.world.reveal_contact();
                    cluster.observe();
                }
                "/play" => cluster.world.playing = true,
                "/pause" => cluster.world.playing = false,
                "/reset" | "/demo" => {
                    *cluster = Cluster::with_datagram_budget(
                        cluster.loss,
                        cluster.center(),
                        cluster.datagram_budget,
                    )?;
                    if action == "/demo" {
                        cluster.start_demo();
                    }
                }
                _ => unreachable!(),
            }
            ("200 OK", "application/json", cluster.state().to_string())
        }
        ("POST", action)
            if action.starts_with("/peer/")
                || action.starts_with("/link/")
                || action.starts_with("/range/") =>
        {
            let mut cluster = shared.lock();
            let result = control(&mut cluster, action);
            match result {
                Ok(()) => {
                    cluster.world.scripted = false;
                    ("200 OK", "application/json", cluster.state().to_string())
                }
                Err(error) => ("400 Bad Request", "text/plain", error.to_string()),
            }
        }
        _ => ("404 Not Found", "text/plain", "Not found".into()),
    };
    let response = format!("HTTP/1.1 {status}\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n{body}", body.len());
    socket.write_all(response.as_bytes()).await
}

fn control(cluster: &mut Cluster, path: &str) -> io::Result<()> {
    let parts: Vec<_> = path.trim_start_matches('/').split('/').collect();
    let index = |value: &str| value.parse::<usize>().map_err(io::Error::other);
    match parts.as_slice() {
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
        ["range", value] => cluster.set_range(value.parse().map_err(io::Error::other)?),
        _ => Err(io::Error::other("invalid control")),
    }
}

async fn request(socket: &mut TcpStream) -> io::Result<(String, String)> {
    let mut bytes = Vec::new();
    let mut chunk = [0; 1024];
    loop {
        let n = socket.read(&mut chunk).await?;
        if n == 0 {
            return Err(io::Error::other("incomplete request"));
        }
        bytes.extend_from_slice(&chunk[..n]);
        if bytes.len() > 8192 {
            return Err(io::Error::other("request too large"));
        }
        if bytes.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }
    let text = std::str::from_utf8(&bytes).map_err(io::Error::other)?;
    let mut words = text.lines().next().unwrap_or("").split_whitespace();
    Ok((
        words.next().unwrap_or("").into(),
        words.next().unwrap_or("").into(),
    ))
}
