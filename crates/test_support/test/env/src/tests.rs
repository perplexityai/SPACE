use super::*;
use pplx_space_env_vars::Environment;

#[test]
fn configuration_values_are_owned_and_preserve_non_unicode() {
    let mut first = TestEnv::default();
    first.set("SETTING", "first");
    let mut second = first.clone();
    second.set("SETTING", "second");
    assert_eq!(first.var("SETTING").unwrap(), "first");
    assert_eq!(second.var("SETTING").unwrap(), "second");
    second.remove("SETTING");
    assert!(matches!(
        second.var("SETTING"),
        Err(std::env::VarError::NotPresent)
    ));
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;
        let value = OsString::from_vec(vec![0xff]);
        first.set("NON_UNICODE", &value);
        assert_eq!(first.var_os("NON_UNICODE"), Some(value.clone()));
        assert_eq!(
            first.var("NON_UNICODE"),
            Err(std::env::VarError::NotUnicode(value))
        );
    }
}

#[test]
fn child_environment_is_configured_without_changing_the_parent() {
    const KEY: &str = "SPACE_TEST_ENV_OWNED_VALUE";
    let before = std::env::var_os(KEY);
    if enter_test_process(&[(KEY, Some(OsStr::new("child"))), ("PATH", None)]) {
        assert_eq!(std::env::var(KEY).unwrap(), "child");
        assert!(std::env::var_os("PATH").is_none());
        return;
    }
    assert_eq!(std::env::var_os(KEY), before);
}

#[test]
fn child_failure_fails_the_parent() {
    if std::env::var_os(CHILD).is_some() {
        assert!(enter_test_process(&[]));
        panic!("intentional child failure");
    }
    let failure = std::panic::catch_unwind(|| enter_test_process(&[])).unwrap_err();
    let message = panic_message(&failure);
    assert!(
        message.contains("environment test child failed"),
        "{message}"
    );
    assert!(message.contains("intentional child failure"), "{message}");
}

fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> &str {
    payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .expect("string panic")
}

#[test]
fn missing_test_cannot_report_success() {
    let failure = std::panic::catch_unwind(|| run_test("no_such_test_case", &[])).unwrap_err();
    assert!(panic_message(&failure).contains("child did not execute the selected test"));
}

#[cfg(unix)]
#[test]
fn command_output_keeps_non_utf8_failure_tail() {
    let mut command = Command::new("/bin/sh");
    command.args([
        "-c",
        "head -c 1100000 /dev/zero; printf '\\377failure-tail'; exit 7",
    ]);
    let (_, output) = run_command(&mut command, Duration::from_secs(10)).unwrap();
    assert_eq!(output.status.code(), Some(7));
    assert_eq!(output.stdout.len(), 1024 * 1024);
    assert!(output.stdout.ends_with(b"\xfffailure-tail"));
    assert!(output_detail(&output.stdout, &output.stderr).contains("�failure-tail"));
}

#[cfg(unix)]
#[test]
fn command_timeout_reports_output_and_terminates_child_group() {
    let mut command = Command::new("/bin/sh");
    let descendant = tempfile::NamedTempFile::new().unwrap();
    command
        .args([
            "-c",
            "printf 'before-timeout'; sleep 30 & echo $! > \"$1\"; wait",
            "timeout-test",
        ])
        .arg(descendant.path());
    let started = Instant::now();
    let error = run_command(&mut command, Duration::from_secs(1)).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert!(error.to_string().contains("before-timeout"), "{error}");
    assert!(started.elapsed() < Duration::from_secs(5));
    let pid: i32 = std::fs::read_to_string(descendant.path())
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    // A container's PID 1 may leave the orphan as a zombie; that is terminated,
    // even though reaping it belongs to its new parent.
    let terminated = || {
        #[cfg(target_os = "linux")]
        if let Ok(status) = std::fs::read_to_string(format!("/proc/{pid}/status")) {
            return status.lines().any(|line| line.starts_with("State:\tZ"));
        }
        matches!(
            nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None),
            Err(nix::errno::Errno::ESRCH)
        )
    };
    let deadline = Instant::now() + Duration::from_secs(2);
    while !terminated() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(terminated(), "descendant survived the command timeout");
}
