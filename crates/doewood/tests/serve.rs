//! Starts the real binary with an empty environment in a temporary directory, checks the ops
//! endpoints and (on Unix) a clean exit on SIGTERM.

use std::io::{BufRead as _, BufReader, Read as _, Write as _};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::process::{Child, ChildStderr, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_doewood");

/// A command with the environment cleared, except what the OS needs to start a process
/// (Windows needs `SystemRoot` for sockets).
fn bare(dir: &Path) -> Command {
    let mut cmd = Command::new(BIN);
    cmd.env_clear().current_dir(dir);
    #[cfg(windows)]
    for var in ["SystemRoot", "SYSTEMROOT"] {
        if let Some(v) = std::env::var_os(var) {
            cmd.env(var, v);
        }
    }
    cmd
}

fn get(addr: SocketAddr, path: &str) -> (u16, String) {
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    write!(
        s,
        "GET {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut resp = String::new();
    s.read_to_string(&mut resp).unwrap();
    let status = resp
        .split(' ')
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let body = resp
        .split_once("\r\n\r\n")
        .map_or("", |(_, b)| b)
        .to_owned();
    (status, body)
}

/// Forwards the child's JSON log lines to a channel.
fn log_lines(stderr: ChildStderr) -> mpsc::Receiver<serde_json::Value> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines() {
            let Ok(line) = line else { break };
            let v = serde_json::from_str(&line)
                .unwrap_or_else(|_| panic!("log line is not JSON: {line}"));
            if tx.send(v).is_err() {
                break;
            }
        }
    });
    rx
}

/// Waits for a log line with this message and returns it.
fn wait_for(
    rx: &mpsc::Receiver<serde_json::Value>,
    seen: &mut Vec<serde_json::Value>,
    message: &str,
) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let line = rx
            .recv_timeout(left)
            .unwrap_or_else(|_| panic!("no {message:?} log line; saw {seen:#?}"));
        seen.push(line.clone());
        if line["message"].as_str() == Some(message) {
            return line;
        }
    }
}

struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn serve_starts_with_nothing_configured() {
    let tmp = tempfile::tempdir().unwrap();
    // Port 0: the test must not depend on 7444 being free. Everything else is unset.
    let mut child = KillOnDrop(
        bare(tmp.path())
            .arg("serve")
            .env("DOEWOOD_OPS_LISTEN", "127.0.0.1:0")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let rx = log_lines(child.0.stderr.take().unwrap());
    let mut seen = Vec::new();

    let ready = wait_for(&rx, &mut seen, "ready");
    let addr: SocketAddr = ready["addr"].as_str().unwrap().parse().unwrap();

    // Defaults applied, the dev key generated loudly, no secrets logged.
    let data = tmp.path().join("doewood-data");
    assert!(data.join("doewood.sqlite").is_file());
    let key = std::fs::read_to_string(data.join("master.key")).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(data.join("master.key"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }
    let all = seen.iter().map(ToString::to_string).collect::<String>();
    assert!(!all.contains(key.trim()), "the key was logged");
    assert!(
        seen.iter().any(|l| l["level"] == "WARN"
            && l["message"]
                .as_str()
                .unwrap_or("")
                .contains("DEVELOPMENT ONLY")),
        "{seen:#?}"
    );
    assert!(seen.iter().any(|l| l["var"] == "DOEWOOD_POSTGRES_URL"
        && l["value"].as_str().unwrap_or("").contains("not configured")));

    assert_eq!(get(addr, "/healthz"), (200, "ok\n".to_owned()));
    assert_eq!(get(addr, "/readyz"), (200, "ready\n".to_owned()));
    let (status, metrics) = get(addr, "/metrics");
    assert_eq!(status, 200);
    assert!(metrics.contains("doewood_ready 1"), "{metrics}");
    assert!(metrics.contains("doewood_build_info{"), "{metrics}");
    assert_eq!(get(addr, "/nope").0, 404);

    #[cfg(unix)]
    {
        let pid = child.0.id().to_string();
        let killed = Command::new("kill").args(["-TERM", &pid]).status().unwrap();
        assert!(killed.success());
        wait_for(&rx, &mut seen, "store closed; bye");
        let deadline = Instant::now() + Duration::from_secs(20);
        let status = loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                break status;
            }
            assert!(Instant::now() < deadline, "no exit after SIGTERM");
            std::thread::sleep(Duration::from_millis(50));
        };
        assert!(status.success(), "{status:?}");
        let stopped = seen
            .iter()
            .find(|l| l["message"] == "servers stopped")
            .unwrap();
        assert_eq!(stopped["backend"], "sqlite", "standalone backend");
        // The WAL was checkpointed into the main file on close.
        let wal = data.join("doewood.sqlite-wal");
        assert!(!wal.exists() || std::fs::metadata(&wal).unwrap().len() == 0);
        // A second start reuses the same key and store.
        let out = bare(tmp.path()).args(["config", "check"]).output().unwrap();
        let text = String::from_utf8(out.stdout).unwrap();
        assert!(text.contains("key file: exists"), "{text}");
    }
}

const OPS_LISTENING: &str = "ops server listening (/healthz, /readyz, /metrics)";

/// Sends SIGTERM and checks a clean exit.
#[cfg(unix)]
fn terminate(
    child: &mut KillOnDrop,
    rx: &mpsc::Receiver<serde_json::Value>,
    seen: &mut Vec<serde_json::Value>,
) {
    let pid = child.0.id().to_string();
    assert!(
        Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .unwrap()
            .success()
    );
    wait_for(rx, seen, "store closed; bye");
    let deadline = Instant::now() + Duration::from_secs(20);
    let status = loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline, "no exit after SIGTERM");
        std::thread::sleep(Duration::from_millis(50));
    };
    assert!(status.success(), "{status:?}");
}

#[test]
fn serve_runs_degraded_while_postgres_is_unreachable() {
    let tmp = tempfile::tempdir().unwrap();
    // Nothing listens on port 1: every attempt is refused.
    let mut child = KillOnDrop(
        bare(tmp.path())
            .arg("serve")
            .env("DOEWOOD_OPS_LISTEN", "127.0.0.1:0")
            .env(
                "DOEWOOD_POSTGRES_URL",
                "postgres://dw:hunter2@127.0.0.1:1/doewood?connect_timeout=2",
            )
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let rx = log_lines(child.0.stderr.take().unwrap());
    let mut seen = Vec::new();
    let listening = wait_for(&rx, &mut seen, OPS_LISTENING);
    let addr: SocketAddr = listening["addr"].as_str().unwrap().parse().unwrap();
    wait_for(&rx, &mut seen, "shared Postgres unavailable; retrying");
    // A second attempt follows the backoff: the instance keeps trying.
    wait_for(&rx, &mut seen, "shared Postgres unavailable; retrying");

    assert_eq!(get(addr, "/healthz"), (200, "ok\n".to_owned()));
    let (status, body) = get(addr, "/readyz");
    assert_eq!(status, 503, "{body}");
    assert!(
        body.starts_with("not ready: shared Postgres unreachable"),
        "{body}"
    );
    assert!(body.contains("retrying"), "{body}");
    let (_, metrics) = get(addr, "/metrics");
    assert!(metrics.contains("doewood_ready 0"), "{metrics}");
    assert!(metrics.contains("mode=\"shared\""), "{metrics}");
    assert!(!seen.iter().any(|l| l["message"] == "ready"), "{seen:#?}");

    #[cfg(unix)]
    terminate(&mut child, &rx, &mut seen);
    let all = seen.iter().map(ToString::to_string).collect::<String>();
    assert!(
        !all.contains("hunter2") && !body.contains("hunter2"),
        "{all}"
    );
}

/// Runs only when `DOEWOOD_TEST_POSTGRES_URL` is set (the postgres CI job sets it).
#[test]
fn serve_uses_postgres_when_reachable() {
    let Some(url) = std::env::var("DOEWOOD_TEST_POSTGRES_URL")
        .ok()
        .filter(|v| !v.trim().is_empty())
    else {
        eprintln!("skipping: DOEWOOD_TEST_POSTGRES_URL is not set");
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let mut child = KillOnDrop(
        bare(tmp.path())
            .arg("serve")
            .env("DOEWOOD_OPS_LISTEN", "127.0.0.1:0")
            .env("DOEWOOD_POSTGRES_URL", &url)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let rx = log_lines(child.0.stderr.take().unwrap());
    let mut seen = Vec::new();
    let listening = wait_for(&rx, &mut seen, OPS_LISTENING);
    let addr: SocketAddr = listening["addr"].as_str().unwrap().parse().unwrap();
    wait_for(&rx, &mut seen, "shared Postgres connected");
    wait_for(&rx, &mut seen, "ready");
    assert_eq!(get(addr, "/readyz"), (200, "ready\n".to_owned()));
    #[cfg(unix)]
    {
        terminate(&mut child, &rx, &mut seen);
        let stopped = seen
            .iter()
            .find(|l| l["message"] == "servers stopped")
            .unwrap();
        assert_eq!(stopped["backend"], "postgres");
    }
}

#[test]
fn an_unusable_data_dir_is_fatal() {
    let tmp = tempfile::tempdir().unwrap();
    let file = tmp.path().join("not-a-dir");
    std::fs::write(&file, b"x").unwrap();
    let out = bare(tmp.path())
        .arg("serve")
        .env("DOEWOOD_DATA_DIR", &file)
        .env("DOEWOOD_OPS_LISTEN", "127.0.0.1:0")
        .output()
        .unwrap();
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("unusable"), "{stderr}");
}

#[test]
fn config_check_with_empty_env_creates_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let out = bare(tmp.path()).args(["config", "check"]).output().unwrap();
    assert!(out.status.success());
    let text = String::from_utf8(out.stdout).unwrap();
    for var in [
        "DOEWOOD_DATA_DIR",
        "DOEWOOD_LISTEN",
        "DOEWOOD_OPS_LISTEN",
        "DOEWOOD_POSTGRES_URL",
        "DOEWOOD_MASTER_KEY",
        "DOEWOOD_INSTANCE_ID",
    ] {
        assert!(text.contains(var), "{var} missing from {text}");
    }
    assert!(text.contains("127.0.0.1:7443"));
    assert!(text.contains("will be generated on first start"));
    assert!(!tmp.path().join("doewood-data").exists());
}

#[test]
fn config_check_redacts_secrets() {
    let tmp = tempfile::tempdir().unwrap();
    let key = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";
    let out = bare(tmp.path())
        .args(["config", "check"])
        .env("DOEWOOD_MASTER_KEY", key)
        .env("DOEWOOD_POSTGRES_URL", "postgres://dw:hunter2@db/doewood")
        .output()
        .unwrap();
    let text = String::from_utf8(out.stdout).unwrap();
    assert!(!text.contains(key) && !text.contains("hunter2"), "{text}");
    assert!(text.contains("postgres://dw:***@db/doewood"), "{text}");
    assert!(text.contains("set (redacted)"), "{text}");
}

#[test]
fn version_prints_the_crate_version() {
    let out = Command::new(BIN).arg("version").output().unwrap();
    let text = String::from_utf8(out.stdout).unwrap();
    assert!(
        text.starts_with(&format!("doewood {}", env!("CARGO_PKG_VERSION"))),
        "{text}"
    );
}
