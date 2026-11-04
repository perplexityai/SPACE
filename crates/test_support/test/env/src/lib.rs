//! Isolated configuration values and subprocess environments for tests.
//!
//! Configuration parsers take a read-only `Environment`. Tests of actual
//! startup environment behavior re-execute one libtest case with `Command::env`.
//! Neither path mutates the environment of a running process.

#![forbid(unsafe_code)]

use std::ffi::{OsStr, OsString};
use std::io::{self, Read, Seek, SeekFrom};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const CHILD: &str = "SPACE_TEST_ENV_CHILD";
const ACK: &str = "SPACE_TEST_ENV_CHILD_ACK";

/// Run the current libtest case with values set before the child starts.
/// Returns true in the child; the parent waits for success and returns false.
/// Call once at the start of a test, before constructing its fixtures. Return
/// from the test when this returns false. Use `TestEnv` for multiple in-process
/// configurations. Child failure, timeout, or failure to enter this function
/// fails the parent test as well.
pub fn enter_test_process(vars: &[(&str, Option<&OsStr>)]) -> bool {
    let thread = std::thread::current();
    let name = std::env::var("NEXTEST_TEST_NAME")
        .ok()
        .or_else(|| thread.name().map(str::to_owned))
        .expect("environment subprocess must be entered from a named test thread");
    assert_ne!(name, "main", "call from a libtest test thread");
    if std::env::var(CHILD).ok().as_deref() == Some(name.as_str()) {
        std::fs::write(
            std::env::var_os(ACK).expect("child acknowledgement path"),
            &name,
        )
        .expect("acknowledge selected test");
        return true;
    }
    run_test(&name, vars);
    false
}

#[allow(
    clippy::panic,
    reason = "Child failure must fail the parent libtest case"
)]
fn run_test(name: &str, vars: &[(&str, Option<&OsStr>)]) {
    let ack = tempfile::NamedTempFile::new().expect("create child acknowledgement");
    let mut command = Command::new(std::env::current_exe().expect("test executable"));
    command.args(["--exact", name, "--nocapture", "--include-ignored"]);
    command.env(CHILD, name).env(ACK, ack.path());
    for (key, value) in vars {
        assert!(
            ![CHILD, ACK, "NEXTEST_TEST_NAME"].contains(key),
            "reserved test variable"
        );
        match value {
            Some(value) => {
                command.env(key, value);
            }
            None => {
                command.env_remove(key);
            }
        }
    }
    let (_, output) = run_command(&mut command, Duration::from_secs(120))
        .unwrap_or_else(|error| panic!("environment test child {name}: {error}"));
    assert!(
        output.status.success(),
        "environment test child failed: {}\n{}",
        output.status,
        output_detail(&output.stdout, &output.stderr)
    );
    assert_eq!(
        std::fs::read_to_string(ack.path()).expect("read child acknowledgement"),
        name,
        "child did not execute the selected test"
    );
}

/// Set by GitHub CI: a test that needs root then fails without it instead of
/// skipping, so a privileged test that drifts out of the `privileged` nextest
/// group fails the unprivileged step. An empty value or `0` leaves it off.
pub const REQUIRE_TEST_PRIVILEGE: &str = "SPACE_CI_REQUIRE_TEST_PRIVILEGE";

/// Whether a test that needs root can run its root path. Returns true as root.
/// Without root it fails the test when [`REQUIRE_TEST_PRIVILEGE`] has any value
/// other than empty or `0`, and otherwise reports the skip on stderr and
/// returns false. Return from the test when this returns false.
#[cfg(unix)]
#[must_use = "return from the test when this returns false"]
pub fn root_or_skip(requirement: &str) -> bool {
    if nix::unistd::geteuid().is_root() {
        return true;
    }
    let required = std::env::var_os(REQUIRE_TEST_PRIVILEGE)
        .is_some_and(|value| !value.is_empty() && value != "0");
    assert!(
        !required,
        "requires root to {requirement}, and {REQUIRE_TEST_PRIVILEGE} is set to require it: \
         list this test in the privileged nextest group"
    );
    eprintln!(
        "SKIPPED: requires root to {requirement}; {REQUIRE_TEST_PRIVILEGE} is unset, empty or 0"
    );
    false
}

/// Whether a test that needs an ordinary user can run: root bypasses
/// permission checks, so a test of what an unprivileged caller is refused
/// passes as root without testing anything. Returns true when not root, and
/// otherwise reports the skip on stderr and returns false. CI runs such tests
/// in its unprivileged step. Return from the test when this returns false.
#[cfg(unix)]
#[must_use = "return from the test when this returns false"]
pub fn unprivileged_or_skip(requirement: &str) -> bool {
    if !nix::unistd::geteuid().is_root() {
        return true;
    }
    eprintln!("SKIPPED: requires a non-root user to {requirement}");
    false
}

/// Run a test command with a deadline, returning its PID and captured output.
/// Each returned output stream contains at most its final MiB. On Unix, timeout and
/// unwinding terminate the owned process group; the direct child is reaped.
pub fn run_command(command: &mut Command, timeout: Duration) -> io::Result<(u32, Output)> {
    let mut stdout = tempfile::tempfile()?;
    let mut stderr = tempfile::tempfile()?;
    command.stdout(Stdio::from(stdout.try_clone()?));
    command.stderr(Stdio::from(stderr.try_clone()?));
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let mut child = TestChild {
        child: command.spawn()?,
        reaped: false,
    };
    let pid = child.child.id();
    let deadline = Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child.child.try_wait()? {
            child.reaped = true;
            break status;
        }
        if Instant::now() >= deadline {
            child.terminate()?;
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "test command exceeded {timeout:?}\n{}",
                    output_detail(&output_tail(&mut stdout)?, &output_tail(&mut stderr)?)
                ),
            ));
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    Ok((
        pid,
        Output {
            status,
            stdout: output_tail(&mut stdout)?,
            stderr: output_tail(&mut stderr)?,
        },
    ))
}

fn output_tail(file: &mut std::fs::File) -> io::Result<Vec<u8>> {
    const LIMIT: u64 = 1024 * 1024;
    let len = file.metadata()?.len();
    file.seek(SeekFrom::Start(len.saturating_sub(LIMIT)))?;
    let mut bytes = Vec::new();
    file.take(LIMIT).read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn output_detail(stdout: &[u8], stderr: &[u8]) -> String {
    format!(
        "stdout (tail):\n{}\nstderr (tail):\n{}",
        String::from_utf8_lossy(stdout),
        String::from_utf8_lossy(stderr)
    )
}

struct TestChild {
    child: Child,
    reaped: bool,
}

impl TestChild {
    fn terminate(&mut self) -> io::Result<()> {
        if self.reaped {
            return Ok(());
        }
        // The child has not been reaped, so its PID cannot be reused as an
        // unrelated process group while this guard owns it.
        #[cfg(unix)]
        {
            use nix::{
                sys::signal::{Signal, killpg},
                unistd::Pid,
            };
            let pid = i32::try_from(self.child.id()).map_err(io::Error::other)?;
            match killpg(Pid::from_raw(pid), Signal::SIGKILL) {
                Ok(()) | Err(nix::errno::Errno::ESRCH) => {}
                Err(error) => return Err(error.into()),
            }
        }
        // Also kill the owned child directly if it changed process groups.
        // A group kill may already have completed; still reap that child.
        if let Err(error) = self.child.kill()
            && self.child.try_wait()?.is_none()
        {
            return Err(error);
        }
        self.child.wait()?;
        self.reaped = true;
        Ok(())
    }
}

impl Drop for TestChild {
    fn drop(&mut self) {
        if let Err(error) = self.terminate() {
            eprintln!("failed to clean up test child: {error}");
        }
    }
}

/// Isolated environment values for testing configuration parsers. It neither
/// reads nor changes ambient process state and can be passed across threads.
#[derive(Default, Clone)]
pub struct TestEnv(std::collections::BTreeMap<String, OsString>);

impl TestEnv {
    pub fn set(&mut self, name: &str, value: impl AsRef<OsStr>) {
        self.0.insert(name.to_owned(), value.as_ref().to_owned());
    }

    pub fn remove(&mut self, name: &str) {
        self.0.remove(name);
    }
}

impl pplx_space_env_vars::Environment for TestEnv {
    fn var_os(&self, name: &str) -> Option<OsString> {
        self.0.get(name).cloned()
    }
}

#[cfg(test)]
mod tests;
