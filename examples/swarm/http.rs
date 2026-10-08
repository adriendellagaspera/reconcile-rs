use std::io;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::cluster::Cluster;

pub async fn serve(mut cluster: Cluster, port: u16) -> io::Result<()> {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)).await?;
    println!("Swarm vertical slice: http://{}/", listener.local_addr()?);
    println!(
        "Two real replicas; seeded loss {}%; 160 ms RTT. Ctrl-C to stop.",
        cluster.loss
    );
    loop {
        let (mut socket, _) = listener.accept().await?;
        // Small loopback-only server. Bound request size and wait time so a stalled browser
        // connection cannot stop controls or monopolize the demonstration indefinitely.
        match tokio::time::timeout(Duration::from_secs(2), request(&mut socket)).await {
            Ok(Ok((method, path))) => {
                let (status, kind, body) = match (method.as_str(), path.as_str()) {
                    ("GET", "/") => (
                        "200 OK",
                        "text/html; charset=utf-8",
                        include_str!("index.html").to_string(),
                    ),
                    ("GET", "/state") => {
                        ("200 OK", "application/json", cluster.state().to_string())
                    }
                    ("POST", action @ ("/heal" | "/partition" | "/observe" | "/reset")) => {
                        match action {
                            "/heal" => cluster.partition(false),
                            "/partition" => cluster.partition(true),
                            "/observe" => cluster.observe(),
                            "/reset" => cluster = Cluster::new(cluster.loss)?,
                            _ => unreachable!(),
                        }
                        ("200 OK", "application/json", cluster.state().to_string())
                    }
                    _ => ("404 Not Found", "text/plain", "Not found".into()),
                };
                let response = format!("HTTP/1.1 {status}\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n{body}", body.len());
                let _ = tokio::time::timeout(
                    Duration::from_secs(2),
                    socket.write_all(response.as_bytes()),
                )
                .await;
            }
            _ => {
                let _ = socket.shutdown().await;
            }
        }
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
