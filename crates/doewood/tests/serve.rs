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

/// An HTTP/1.1 POST; the status and the body as (lossy) text.
fn post(addr: SocketAddr, path: &str, content_type: &str, body: &[u8]) -> (u16, String) {
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    write!(
        s,
        "POST {path} HTTP/1.1\r\nHost: test\r\nContent-Type: {content_type}\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .unwrap();
    s.write_all(body).unwrap();
    let mut resp = Vec::new();
    s.read_to_end(&mut resp).unwrap();
    let resp = String::from_utf8_lossy(&resp).into_owned();
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

/// Runs `doewood health` against an ops server at `ops`: whether it passed, and its stderr.
fn health(dir: &Path, ops: &str) -> (bool, String) {
    let out = bare(dir)
        .arg("health")
        .env("DOEWOOD_OPS_LISTEN", ops)
        .output()
        .unwrap();
    assert!(out.stdout.is_empty());
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
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
            .env("DOEWOOD_LISTEN", "127.0.0.1:0")
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
    // `doewood health` (issue #144): ready, also through an unspecified address (loopback).
    assert_eq!(health(tmp.path(), &addr.to_string()), (true, String::new()));
    assert_eq!(
        health(tmp.path(), &format!("0.0.0.0:{}", addr.port())),
        (true, String::new())
    );
    let (status, metrics) = get(addr, "/metrics");
    assert_eq!(status, 200);
    assert!(metrics.contains("doewood_ready 1"), "{metrics}");
    assert!(metrics.contains("doewood_build_info{"), "{metrics}");
    assert_eq!(get(addr, "/nope").0, 404);

    let api = started_in_order(&seen, &ready);

    // The gRPC API answers on its own port, here over gRPC-Web (HTTP/1.1), as a browser
    // calls it: an empty ListSourcesRequest frame.
    let api_addr: SocketAddr = api["addr"].as_str().unwrap().parse().unwrap();
    let (status, body) = post(
        api_addr,
        "/doewood.v1.ConfigService/ListSources",
        "application/grpc-web+proto",
        &[0, 0, 0, 0, 0],
    );
    assert_eq!(status, 200);
    assert!(body.contains("grpc-status:0"), "{body}");
    // The call is on the ops port's /metrics.
    let (_, metrics) = get(addr, "/metrics");
    for line in [
        "doewood_grpc_calls_total{code=\"OK\",method=\"doewood.v1.ConfigService/ListSources\"} 1",
        "doewood_grpc_call_duration_seconds_count{method=\"doewood.v1.ConfigService/ListSources\"} 1",
        "doewood_grpc_calls_in_flight{method=\"doewood.v1.ConfigService/ListSources\"} 0",
        "doewood_grpc_connections_total 1",
        "doewood_grpc_shed_total 0",
    ] {
        assert!(metrics.lines().any(|l| l == line), "{line} in {metrics}");
    }
    // The S3 webhook is on the ops server.
    let (status, _) = post(
        addr,
        "/events/s3/0192f5b2-8a7e-7cc4-9b5e-4d1b2e3f4a5b",
        "application/json",
        b"{\"Records\":[]}",
    );
    assert_eq!(status, 404, "unknown source");

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

/// Checks that `ready` came only once the gRPC API listened, and that the start was timed step
/// by step (issue #118); returns the API's `listening` line.
fn started_in_order(seen: &[serde_json::Value], ready: &serde_json::Value) -> serde_json::Value {
    let api = seen
        .iter()
        .find(|l| l["message"] == API_LISTENING)
        .cloned()
        .unwrap_or_else(|| panic!("ready before the gRPC API listened: {seen:#?}"));
    let steps: Vec<&str> = seen
        .iter()
        .filter(|l| l["message"] == "startup step")
        .map(|l| l["step"].as_str().unwrap())
        .collect();
    for step in [
        "config",
        "listeners",
        "master key",
        "store open",
        "ingest",
        "api",
    ] {
        assert!(steps.contains(&step), "{step} in {steps:?}");
    }
    assert!(ready["startup_ms"].is_u64(), "{ready}");
    #[cfg(target_os = "linux")]
    assert!(ready["rss_mib"].as_u64().unwrap() > 0, "{ready}");
    api
}

const OPS_LISTENING: &str = "ops server listening (/healthz, /readyz, /metrics)";
const API_LISTENING: &str = "gRPC API listening (gRPC and gRPC-Web, health, reflection)";

/// `doewood health` fails fast when nothing listens on the ops port.
#[test]
fn health_fails_when_nothing_listens() {
    let tmp = tempfile::tempdir().unwrap();
    let free = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = free.local_addr().unwrap();
    drop(free);
    let started = Instant::now();
    let (ok, why) = health(tmp.path(), &addr.to_string());
    assert!(!ok);
    assert!(
        why.starts_with(&format!(
            "doewood health: not ready: cannot connect to {addr}"
        )),
        "{why}"
    );
    assert!(started.elapsed() < Duration::from_secs(5));
    // It creates nothing: no data directory, no key.
    assert_eq!(std::fs::read_dir(tmp.path()).unwrap().count(), 0);
}

/// `dev add-source` goes through the API's code (validation, claim), and `serve` then starts
/// a scanner for the source.
#[test]
fn dev_add_source_then_serve_scans_it() {
    let tmp = tempfile::tempdir().unwrap();
    let add = |extra: &[&str]| {
        bare(tmp.path())
            .args([
                "dev",
                "add-source",
                "--access-key-env",
                "K",
                "--secret-key-env",
                "S",
            ])
            .args(extra)
            .env("K", "test-key-id")
            .env("S", "test-secret-value")
            .output()
            .unwrap()
    };
    let out = add(&[
        "--endpoint",
        "http://127.0.0.1:1",
        "--bucket",
        "docs",
        "--interval-secs",
        "3600",
    ]);
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "{text} {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(text.starts_with("added source "), "{text}");
    // Validation is the API's: every problem, with field paths, and no secret echoed.
    let out = add(&["--endpoint", "http://u:pw-in-url@x", "--bucket", "B!"]);
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("InvalidArgument") && err.contains("endpoint") && err.contains("bucket"),
        "{err}"
    );
    assert!(!err.contains("pw-in-url"), "{err}");
    // The same source twice.
    let out = add(&["--endpoint", "http://127.0.0.1:1", "--bucket", "docs"]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("AlreadyExists"));

    let mut child = KillOnDrop(
        bare(tmp.path())
            .arg("serve")
            .env("DOEWOOD_OPS_LISTEN", "127.0.0.1:0")
            .env("DOEWOOD_LISTEN", "127.0.0.1:0")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let rx = log_lines(child.0.stderr.take().unwrap());
    let mut seen = Vec::new();
    let started = wait_for(&rx, &mut seen, "starting scanner");
    assert_eq!(started["bucket"], "docs");
    wait_for(&rx, &mut seen, API_LISTENING);
    #[cfg(unix)]
    terminate(&mut child, &rx, &mut seen);
    let all = seen.iter().map(ToString::to_string).collect::<String>();
    assert!(!all.contains("test-secret-value"), "{all}");
}

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
            .env("DOEWOOD_LISTEN", "127.0.0.1:0")
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
    // `doewood health` fails while not ready, with the reason on one line.
    let (ok, why) = health(tmp.path(), &addr.to_string());
    assert!(!ok);
    assert!(
        why.starts_with("doewood health: not ready: ")
            && why.contains("answered 503: not ready: shared Postgres unreachable")
            && why.trim_end().lines().count() == 1,
        "{why}"
    );
    assert!(!why.contains("hunter2"), "{why}");
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
            .env("DOEWOOD_LISTEN", "127.0.0.1:0")
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
    assert!(
        seen.iter().any(|l| l["message"] == API_LISTENING),
        "ready before the gRPC API listened: {seen:#?}"
    );
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

/// `dev add-source` into `dir`, with keys from the environment; the shared Postgres only when
/// `pg` is given. Returns whether it worked, and stderr.
fn dev_add(dir: &Path, instance: &str, pg: Option<&str>, bucket: &str) -> (bool, String) {
    let mut cmd = bare(dir);
    cmd.args([
        "dev",
        "add-source",
        "--access-key-env",
        "K",
        "--secret-key-env",
        "S",
        "--endpoint",
        "http://127.0.0.1:1",
        "--bucket",
        bucket,
        "--interval-secs",
        "3600",
    ])
    .env("K", "test-key-id")
    .env("S", "test-secret-value")
    .env("DOEWOOD_INSTANCE_ID", instance);
    if let Some(url) = pg {
        cmd.env("DOEWOOD_POSTGRES_URL", url);
    }
    let out = cmd.output().unwrap();
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// Runs `serve` on `dir` against the shared Postgres until its sources are re-claimed; returns
/// the log lines up to then, and the summary line. Stops it with SIGTERM (Unix).
fn serve_until_reclaimed(
    dir: &Path,
    instance: &str,
    url: &str,
) -> (Vec<serde_json::Value>, serde_json::Value) {
    let mut child = KillOnDrop(
        bare(dir)
            .arg("serve")
            .env("DOEWOOD_OPS_LISTEN", "127.0.0.1:0")
            .env("DOEWOOD_LISTEN", "127.0.0.1:0")
            .env("DOEWOOD_INSTANCE_ID", instance)
            .env("DOEWOOD_POSTGRES_URL", url)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let rx = log_lines(child.0.stderr.take().unwrap());
    let mut seen = Vec::new();
    let listening = wait_for(&rx, &mut seen, OPS_LISTENING);
    let addr: SocketAddr = listening["addr"].as_str().unwrap().parse().unwrap();
    let summary = wait_for(&rx, &mut seen, "source claims checked in the shared store");
    // The start did not fail: the instance is ready.
    assert_eq!(get(addr, "/readyz"), (200, "ready\n".to_owned()));
    let upto = seen.clone();
    #[cfg(unix)]
    terminate(&mut child, &rx, &mut seen);
    (upto, summary)
}

/// Runs only when `DOEWOOD_TEST_POSTGRES_URL` is set (the postgres CI job sets it). `serve`
/// re-claims the sources of its store at start: its own are claimed and scanned, one another
/// instance claimed is reported and not scanned, and a restart keeps its claims (issue #12).
#[test]
fn serve_reclaims_its_sources_and_reports_conflicts() {
    let Some(url) = std::env::var("DOEWOOD_TEST_POSTGRES_URL")
        .ok()
        .filter(|v| !v.trim().is_empty())
    else {
        eprintln!("skipping: DOEWOOD_TEST_POSTGRES_URL is not set");
        return;
    };
    // Unique names: the database is shared with other tests and earlier runs.
    let run = uuid::Uuid::now_v7().simple().to_string();
    let run = &run[run.len() - 12..];
    let (a, b) = (format!("dw-a-{run}"), format!("dw-b-{run}"));
    let (mine, taken) = (format!("mine-{run}"), format!("taken-{run}"));
    let (dir_a, dir_b, dir_c) = (
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    );
    // A claims `taken` in the shared store. B has both sources in its own store only, as a
    // standalone run left them.
    let (ok, err) = dev_add(dir_a.path(), &a, Some(&url), &taken);
    assert!(ok, "{err}");
    for bucket in [&mine, &taken] {
        let (ok, err) = dev_add(dir_b.path(), &b, None, bucket);
        assert!(ok, "{err}");
    }

    for round in ["first start", "restart"] {
        let (seen, summary) = serve_until_reclaimed(dir_b.path(), &b, &url);
        assert_eq!(summary["claimed"], 1, "{round}: {summary}");
        assert_eq!(summary["conflicts"], 1, "{round}: {summary}");
        let scanned: Vec<_> = seen
            .iter()
            .filter(|l| l["message"] == "starting scanner")
            .map(|l| l["bucket"].as_str().unwrap_or_default().to_owned())
            .collect();
        assert_eq!(scanned, std::slice::from_ref(&mine), "{round}: {seen:#?}");
        let warn = seen
            .iter()
            .find(|l| l["message"] == "source claimed by another instance; not scanning it")
            .unwrap_or_else(|| panic!("{round}: no conflict warning in {seen:#?}"));
        assert_eq!(warn["level"], "WARN");
        assert_eq!(warn["owner"], a.as_str());
        let reason = warn["reason"].as_str().unwrap();
        assert!(
            reason.starts_with(&format!("claimed by instance \"{a}\" since ")),
            "{reason}"
        );
    }

    // B's claim on `mine` is in the shared store: a third instance is refused, B named.
    let (ok, err) = dev_add(dir_c.path(), "dw-c", Some(&url), &mine);
    assert!(!ok);
    assert!(err.contains("AlreadyExists") && err.contains(&b), "{err}");
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
        "DOEWOOD_API_MAX_MESSAGE_MIB",
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
