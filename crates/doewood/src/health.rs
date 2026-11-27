//! `doewood health`: asks the ops server's `/readyz` whether this instance is ready, for a
//! container `HEALTHCHECK` or Compose's `condition: service_healthy` (the image has no curl).
//!
//! One HTTP/1.1 GET over a plain TCP connection, with a 2 s limit on the whole exchange: exit 0
//! on 200, otherwise 1 with a one-line reason on stderr.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;

/// The limit on connecting, sending and reading the answer, together.
pub const TIMEOUT: Duration = Duration::from_secs(2);

/// The most of the answer read: the status line and `/readyz`'s short body fit easily.
const MAX_RESPONSE: usize = 16 * 1024;

/// Where to connect for an ops server bound to `listen`: an unspecified address
/// (`0.0.0.0`, `[::]`) is reached on loopback.
pub fn target(listen: SocketAddr) -> SocketAddr {
    let ip = match listen.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        ip => ip,
    };
    SocketAddr::new(ip, listen.port())
}

/// Asks `http://<addr>/readyz` once. `Ok` when it answers 200; otherwise why not, in one line.
pub async fn check(addr: SocketAddr, timeout: Duration) -> Result<(), String> {
    match tokio::time::timeout(timeout, readyz(addr)).await {
        Ok(result) => result,
        Err(_) => Err(format!(
            "http://{addr}/readyz did not answer within {} ms",
            timeout.as_millis()
        )),
    }
}

async fn readyz(addr: SocketAddr) -> Result<(), String> {
    let mut stream = TcpStream::connect(addr)
        .await
        .map_err(|e| format!("cannot connect to {addr}: {e}"))?;
    let request = format!(
        "GET /readyz HTTP/1.1\r\nHost: {addr}\r\nUser-Agent: doewood-health\r\n\
         Connection: close\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|e| format!("cannot send to {addr}: {e}"))?;
    let mut response = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let n = stream
            .read(&mut chunk)
            .await
            .map_err(|e| format!("cannot read from {addr}: {e}"))?;
        if n == 0 {
            break;
        }
        response.extend_from_slice(&chunk[..n]);
        if response.len() >= MAX_RESPONSE {
            break;
        }
    }
    let text = String::from_utf8_lossy(&response);
    let status =
        status_code(&text).ok_or_else(|| format!("http://{addr}/readyz did not answer HTTP"))?;
    if status == 200 {
        return Ok(());
    }
    // `/readyz` says why it is not ready in its body (never a secret, see `OpsState`).
    let body = text.split_once("\r\n\r\n").map_or("", |(_, b)| b);
    let why = body.lines().next().unwrap_or("").trim();
    Err(if why.is_empty() {
        format!("http://{addr}/readyz answered {status}")
    } else {
        format!("http://{addr}/readyz answered {status}: {}", one_line(why))
    })
}

/// The status code of an HTTP/1.x status line.
fn status_code(response: &str) -> Option<u16> {
    let line = response.lines().next()?;
    let mut parts = line.split(' ');
    if !parts.next()?.starts_with("HTTP/1.") {
        return None;
    }
    let code = parts.next()?;
    (code.len() == 3).then(|| code.parse().ok()).flatten()
}

/// At most 200 printable characters.
fn one_line(text: &str) -> String {
    text.chars().filter(|c| !c.is_control()).take(200).collect()
}

#[cfg(test)]
mod tests {
    use tokio::net::TcpListener;

    use super::*;

    /// A server that answers one connection with `response` (after `delay`).
    async fn serve_once(response: &'static str, delay: Duration) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let n = s.read(&mut buf).await.unwrap();
            let request = String::from_utf8_lossy(&buf[..n]).into_owned();
            assert!(request.starts_with("GET /readyz HTTP/1.1\r\n"), "{request}");
            tokio::time::sleep(delay).await;
            let _ = s.write_all(response.as_bytes()).await;
        });
        addr
    }

    #[tokio::test]
    async fn ready_is_ok() {
        let addr = serve_once(
            "HTTP/1.1 200 OK\r\ncontent-length: 6\r\n\r\nready\n",
            Duration::ZERO,
        )
        .await;
        assert_eq!(check(addr, TIMEOUT).await, Ok(()));
    }

    #[tokio::test]
    async fn not_ready_says_why() {
        let addr = serve_once(
            "HTTP/1.1 503 Service Unavailable\r\ncontent-length: 20\r\n\r\nnot ready: starting\n",
            Duration::ZERO,
        )
        .await;
        let err = check(addr, TIMEOUT).await.unwrap_err();
        assert!(err.contains("answered 503: not ready: starting"), "{err}");
        assert!(!err.contains('\n'), "{err}");
    }

    #[tokio::test]
    async fn nothing_listening_fails() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let err = check(addr, TIMEOUT).await.unwrap_err();
        assert!(err.starts_with("cannot connect to"), "{err}");
    }

    #[tokio::test]
    async fn a_slow_answer_times_out() {
        let addr = serve_once("HTTP/1.1 200 OK\r\n\r\n", Duration::from_secs(5)).await;
        let err = check(addr, Duration::from_millis(200)).await.unwrap_err();
        assert!(err.contains("did not answer within 200 ms"), "{err}");
    }

    #[tokio::test]
    async fn not_http_fails() {
        let addr = serve_once("SSH-2.0-x\r\n", Duration::ZERO).await;
        let err = check(addr, TIMEOUT).await.unwrap_err();
        assert!(err.contains("did not answer HTTP"), "{err}");
    }

    #[test]
    fn unspecified_addresses_mean_loopback() {
        let t = |s: &str| target(s.parse().unwrap()).to_string();
        assert_eq!(t("0.0.0.0:7444"), "127.0.0.1:7444");
        assert_eq!(t("[::]:7444"), "[::1]:7444");
        assert_eq!(t("10.1.2.3:9000"), "10.1.2.3:9000");
        assert_eq!(t("[::1]:1"), "[::1]:1");
    }

    #[test]
    fn status_lines() {
        assert_eq!(status_code("HTTP/1.1 200 OK\r\n"), Some(200));
        assert_eq!(status_code("HTTP/1.0 503 x"), Some(503));
        assert_eq!(status_code("HTTP/2 200"), None);
        assert_eq!(status_code("HTTP/1.1 20"), None);
        assert_eq!(status_code(""), None);
    }
}
