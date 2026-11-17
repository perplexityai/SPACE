use super::*;
use std::io::{BufRead, BufReader};
use std::os::unix::process::ExitStatusExt;
use std::process::{Command, Stdio};

/// A test child that is killed and reaped however the test ends.
pub(crate) struct ReapedChild(std::process::Child);

impl ReapedChild {
    pub(crate) fn spawn(command: &mut std::process::Command) -> Self {
        Self(command.spawn().expect("spawn test process"))
    }
}

impl std::ops::Deref for ReapedChild {
    type Target = std::process::Child;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for ReapedChild {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for ReapedChild {
    fn drop(&mut self) {
        // A child the test already reaped is not signalled again.
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A shell script that runs `setup`, says `ready` and then waits forever.
pub(crate) fn ready_script(setup: &str) -> String {
    format!("{setup}; echo ready; while :; do sleep 0.1; done")
}

/// A child that has printed `ready`, so its setup has run.
pub(crate) fn spawn_ready(command: &mut Command) -> ReapedChild {
    let mut child = ReapedChild::spawn(command.stdout(Stdio::piped()));
    let mut line = String::new();
    BufReader::new(child.stdout.as_mut().expect("piped stdout"))
        .read_line(&mut line)
        .expect("read readiness");
    assert_eq!(line, "ready\n");
    child
}

#[tokio::test]
async fn terminate_returns_once_sigterm_ends_the_process() {
    let mut child = spawn_ready(Command::new("sh").arg("-c").arg(ready_script("true")));
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
    let mut child = spawn_ready(
        Command::new("sh")
            .arg("-c")
            .arg(ready_script("trap '' TERM")),
    );
    let process = launch::ProcessHandle::open(child.id()).expect("pin test process");
    terminate(&process, Duration::from_millis(100), Duration::from_secs(2))
        .await
        .expect("terminate");
    assert_eq!(child.wait().expect("reap").signal(), Some(libc::SIGKILL));
}
