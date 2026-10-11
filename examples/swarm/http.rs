use std::io;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

use super::cluster::Cluster;

pub async fn serve(cluster: Cluster, port: u16) -> io::Result<()> {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)).await?;
    println!("Reconciliation lab: http://{}/", listener.local_addr()?);
    let cluster = Arc::new(Mutex::new(cluster));
    let connections = Arc::new(Semaphore::new(16));
    loop {
        let (socket, _) = listener.accept().await?;
        if let Ok(permit) = connections.clone().try_acquire_owned() {
            let cluster = cluster.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let _ = tokio::time::timeout(
                    std::time::Duration::from_secs(3),
                    respond(socket, cluster),
                )
                .await;
            });
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
        ("GET", "/api.js") => (
            "200 OK",
            "text/javascript; charset=utf-8",
            include_str!("api.js").to_string(),
        ),
        ("GET", "/config.js") => (
            "200 OK",
            "text/javascript; charset=utf-8",
            include_str!("config.js").to_string(),
        ),
        ("GET", "/state") => (
            "200 OK",
            "application/json",
            shared.lock().state().to_string(),
        ),
        ("POST", action @ ("/next" | "/reset")) => {
            let mut cluster = shared.lock();
            match super::controls::apply(&mut cluster, action) {
                Ok(()) => ("200 OK", "application/json", cluster.state().to_string()),
                Err(error) => ("400 Bad Request", "text/plain", error.to_string()),
            }
        }
        _ => ("404 Not Found", "text/plain", "Not found".into()),
    };
    let response = format!("HTTP/1.1 {status}\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n{body}", body.len());
    socket.write_all(response.as_bytes()).await
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
