use super::*;
use std::io::{BufRead, BufReader};
use std::os::unix::process::ExitStatusExt;
use std::process::{Child, Command, Stdio};

/// A shell that has printed `ready`, so its first command has run.
fn spawn_ready(script: &str) -> Child {
    let mut child = Command::new("sh")
        .arg("-c")
        .arg(format!("{script}; echo ready; while :; do sleep 0.1; done"))
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn test process");
    let mut line = String::new();
    BufReader::new(child.stdout.as_mut().expect("piped stdout"))
        .read_line(&mut line)
        .expect("read readiness");
    assert_eq!(line, "ready\n");
    child
}

#[tokio::test]
async fn terminate_returns_once_sigterm_ends_the_process() {
    let mut child = spawn_ready("true");
    let process = launch::ProcessHandle::open(child.id()).expect("pin test process");
    let started = std::time::Instant::now();
    terminate(&process, Duration::from_secs(3), Duration::from_secs(2))
        .await
        .expect("terminate");
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(child.wait().expect("reap").signal(), Some(libc::SIGTERM));
}

#[tokio::test]
async fn terminate_escalates_to_sigkill_when_sigterm_is_ignored() {
    let mut child = spawn_ready("trap '' TERM");
    let process = launch::ProcessHandle::open(child.id()).expect("pin test process");
    terminate(&process, Duration::from_millis(100), Duration::from_secs(2))
        .await
        .expect("terminate");
    assert_eq!(child.wait().expect("reap").signal(), Some(libc::SIGKILL));
}
