#![cfg(test)]

//! The peer-process check-mode switch is validated at startup for every
//! role: a set-but-unrecognized value must fail the process before role
//! dispatch, so an agent pod with a typo'd value cannot start green and
//! silently evaluate nothing.

use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn run_role_with_bad_mode(role: &str) -> (bool, String) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_space-credmgr-svc"))
        .args(["--role", role])
        .env("SPACE_UDS_PEER_PROCESS_CHECK", "enfroce")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn space-credmgr-svc");

    let deadline = Instant::now() + Duration::from_secs(60);
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll child") {
            break status;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "space-credmgr-svc --role {role} did not exit within 60s; \
                 a bad check-mode value must fail startup"
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    };

    let mut output = String::new();
    if let Some(mut stdout) = child.stdout.take() {
        let _ = stdout.read_to_string(&mut output);
    }
    if let Some(mut stderr) = child.stderr.take() {
        let _ = stderr.read_to_string(&mut output);
    }
    (status.success(), output)
}

#[test]
fn agent_role_rejects_an_unparseable_check_mode() {
    let (success, output) = run_role_with_bad_mode("agent");
    assert!(!success, "startup must fail on a typo'd mode: {output}");
    assert!(
        output.contains("SPACE_UDS_PEER_PROCESS_CHECK"),
        "the failure must name the mis-set switch, not an unrelated later error: {output}"
    );
}

#[test]
fn authority_role_rejects_an_unparseable_check_mode() {
    let (success, output) = run_role_with_bad_mode("authority");
    assert!(!success, "startup must fail on a typo'd mode: {output}");
    assert!(
        output.contains("SPACE_UDS_PEER_PROCESS_CHECK"),
        "the failure must name the mis-set switch, not an unrelated later error: {output}"
    );
}
