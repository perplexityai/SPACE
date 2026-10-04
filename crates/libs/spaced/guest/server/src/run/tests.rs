#[cfg(target_os = "linux")]
use super::*;

// Exercise the actual service entry point in an isolated process: sending a
// signal to the test runner would alter the signal policy of unrelated tests.
#[cfg(target_os = "linux")]
#[test]
fn unix_stop_signals_drain_the_service() {
    const CHILD: &str = "SPACE_SPACED_SIGNAL_TEST_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let mut service_config = service_config_from_environment();
        service_config.local_socket_path = Some(
            std::path::PathBuf::from(
                std::env::var_os("NOTIFY_SOCKET").expect("test notification path"),
            )
            .with_file_name("local.sock"),
        );
        run(SpacedGuestRunConfig {
            service_config: Some(service_config),
        })
        .expect("graceful service stop");
        return;
    }

    struct ChildGuard(std::process::Child);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    for signal in [
        nix::sys::signal::Signal::SIGINT,
        nix::sys::signal::Signal::SIGTERM,
    ] {
        let directory = tempfile::tempdir().expect("test sockets");
        let notify_path = directory.path().join("notify.sock");
        let ready = UnixDatagram::bind(&notify_path).expect("readiness receiver");
        ready
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .expect("read timeout");
        let mut child = ChildGuard(
            std::process::Command::new(std::env::current_exe().expect("test executable"))
                .args([
                    "--exact",
                    "run::tests::unix_stop_signals_drain_the_service",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .env("NOTIFY_SOCKET", notify_path)
                .env(
                    "SPACE_SPACED_GUEST_BRIDGE_SOCKET",
                    directory.path().join("bridge.sock"),
                )
                .stdout(std::process::Stdio::null())
                .spawn()
                .expect("child service"),
        );
        let mut notification = [0; 128];
        ready.recv(&mut notification).expect("service readiness");
        let pid = nix::unistd::Pid::from_raw(i32::try_from(child.0.id()).expect("child PID"));
        nix::sys::signal::kill(pid, signal).expect("send stop signal");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        loop {
            if let Some(status) = child.0.try_wait().expect("child status") {
                assert!(
                    status.success(),
                    "{signal} bypassed graceful shutdown: {status}"
                );
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "service did not stop after {signal}"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}

#[test]
fn bounded_runtime_shutdown_does_not_wait_for_stuck_blocking_threads() {
    let (release, wait) = std::sync::mpsc::channel::<()>();
    let started = std::time::Instant::now();
    super::block_on_bounded(
        async move {
            // Stands in for a thread stuck inside a child `spawn`.
            tokio::task::spawn_blocking(move || {
                let _ = wait.recv_timeout(std::time::Duration::from_secs(30));
            });
        },
        std::time::Duration::from_millis(50),
    )
    .unwrap();
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "runtime shutdown waited for a stuck blocking thread"
    );
    drop(release);
}
