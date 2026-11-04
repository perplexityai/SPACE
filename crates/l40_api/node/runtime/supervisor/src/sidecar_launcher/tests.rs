use super::*;
use std::collections::HashMap;
use std::os::fd::IntoRawFd;

#[test]
fn launcher_child_receives_only_the_explicit_environment() {
    let output = launcher_command("/usr/bin/env").output().unwrap();
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "PATH=/usr/sbin:/usr/bin:/sbin:/bin\n"
    );
}

async fn outcome_stream(bytes: &[u8]) -> Result<(), String> {
    let (mut writer, mut reader) = UnixStream::pair().expect("create test socketpair");
    writer.write_all(bytes).await.expect("write test outcome");
    drop(writer);
    read_launcher_outcome(&mut reader).await
}

fn encoded_error(message: &str) -> Vec<u8> {
    let mut encoded = vec![ERROR_TAG];
    encoded.extend_from_slice(&(message.len() as u32).to_be_bytes());
    encoded.extend_from_slice(message.as_bytes());
    encoded
}

#[tokio::test]
async fn prepared_then_exec_close_is_success() {
    outcome_stream(&[PREPARED_TAG])
        .await
        .expect("CLOEXEC close confirms target exec");
}

#[tokio::test]
async fn exec_errors_are_preserved_after_prepare() {
    let mut after_prepare = vec![PREPARED_TAG];
    after_prepare.extend_from_slice(&encoded_error("exec failed"));
    assert_eq!(
        outcome_stream(&after_prepare)
            .await
            .expect_err("exec must fail"),
        "exec failed"
    );
}

#[tokio::test]
async fn launcher_entrypoint_reports_setup_failure() {
    let (parent_stream, child_stream) = StdUnixStream::pair().expect("create test socketpair");
    let raw_fd = child_stream.into_raw_fd();
    let launcher = std::thread::spawn(move || run(raw_fd));
    parent_stream
        .set_nonblocking(true)
        .expect("configure test socket");
    let mut parent_stream = UnixStream::from_std(parent_stream).expect("adopt test socket");
    let config = SidecarLauncherConfig {
        ns_path: "/path/that/does/not/exist".to_string(),
        program: "/bin/true".to_string(),
        args: Vec::new(),
        env: HashMap::new(),
        uid: 1,
        gid: 1,
        cgroup_path: "/path/that/does/not/exist".to_string(),
        pre_exec_sockets: Vec::new(),
        grant_cap_net_admin: false,
    };
    let launch_spec = serde_json::to_vec(&config).expect("serialize test launch spec");

    write_frame_async(&mut parent_stream, &launch_spec)
        .await
        .expect("send test launch spec");
    parent_stream.shutdown().await.expect("finish test request");
    let error = read_launcher_outcome(&mut parent_stream)
        .await
        .expect_err("invalid namespace must fail");
    let launch_error = launcher
        .join()
        .expect("launcher thread must not panic")
        .expect_err("launcher setup must fail");
    assert_eq!(error, launch_error);
}

#[tokio::test]
async fn close_before_prepare_is_not_success() {
    let error = outcome_stream(&[])
        .await
        .expect_err("silent launcher exit must fail closed");
    assert!(error.contains("read sidecar launcher outcome"));
}

#[tokio::test]
async fn contained_launch_requires_actual_runtime_identity_and_confirmed_exec() {
    let mut prefix = vec![RUNTIME_PID_TAG];
    prefix.extend_from_slice(&42_u32.to_be_bytes());
    let mut success = prefix.clone();
    success.push(PREPARED_TAG);
    let mut failed_exec = success.clone();
    failed_exec.extend(encoded_error("exec refused"));
    for (bytes, expected) in [
        (success, Some(42)),
        (prefix, None),
        (failed_exec, None),
        (vec![PREPARED_TAG], None),
        (encoded_error("namespace setup refused"), None),
        (vec![RUNTIME_PID_TAG, 0, 0, 0, 1, PREPARED_TAG], None),
    ] {
        let (mut writer, mut reader) = UnixStream::pair().unwrap();
        writer.write_all(&bytes).await.unwrap();
        drop(writer);
        match expected {
            Some(pid) => assert_eq!(read_contained_outcome(&mut reader).await.unwrap(), pid),
            None => {
                read_contained_outcome(&mut reader).await.unwrap_err();
            }
        }
    }
}
