use super::*;
use std::time::Duration;

// As above, in a re-executed child.
#[test]
fn a_second_stop_signal_forces_and_none_is_lost_between_the_two() {
    use nix::sys::signal::Signal;

    const CHILD: &str = "SPACE_COMMON_SVC_SECOND_STOP_SIGNAL_TEST_CHILD";
    if let Some(signal) = std::env::var_os(CHILD) {
        let signal: Signal = signal
            .to_str()
            .expect("signal name")
            .parse()
            .expect("stop signal");
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime")
            .block_on(async {
                // A signal delivered before `recv` is polled is kept, for the
                // second call as for the first.
                let mut signals = StopSignals::install().expect("stop-signal handlers");
                for _ in 0..2 {
                    nix::sys::signal::raise(signal).expect("raise stop signal");
                    tokio::time::timeout(Duration::from_secs(5), signals.recv("testing"))
                        .await
                        .expect("a signal raised before recv is kept");
                }

                let stop = stop_tokens();
                nix::sys::signal::raise(signal).expect("raise the first stop signal");
                tokio::time::timeout(Duration::from_secs(5), stop.shutdown.cancelled())
                    .await
                    .expect("the first stop signal cancels shutdown");
                assert!(!stop.forced.is_cancelled(), "one signal doesn't force");
                nix::sys::signal::raise(signal).expect("raise the second stop signal");
                tokio::time::timeout(Duration::from_secs(5), stop.forced.cancelled())
                    .await
                    .expect("the second stop signal cancels forced");
            });
        return;
    }

    let test_executable = std::env::current_exe().expect("test executable");
    for signal in [Signal::SIGINT, Signal::SIGTERM] {
        let status = std::process::Command::new(&test_executable)
            .args([
                "--exact",
                "shutdown::tests::a_second_stop_signal_forces_and_none_is_lost_between_the_two",
                "--nocapture",
            ])
            .env(CHILD, signal.as_str())
            .stdout(std::process::Stdio::null())
            .status()
            .expect("child test process");
        assert!(status.success(), "{signal}: {status}");
    }
}

// Signal a re-executed child rather than the test runner: installing stop
// handlers in the runner would change how unrelated tests respond to them.
#[test]
fn stop_signals_are_handled_once_shutdown_token_returns() {
    use nix::sys::signal::Signal;

    const CHILD: &str = "SPACE_COMMON_SVC_SHUTDOWN_SIGNAL_TEST_CHILD";
    if let Some(signal) = std::env::var_os(CHILD) {
        let signal: Signal = signal
            .to_str()
            .expect("signal name")
            .parse()
            .expect("stop signal");
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime")
            .block_on(async {
                let shutdown = shutdown_token();
                // A current-thread runtime cannot run the spawned signal task
                // before this block yields, so the signal is observed only if
                // `shutdown_token` installed the handlers before returning.
                nix::sys::signal::raise(signal).expect("raise stop signal");
                tokio::time::timeout(Duration::from_secs(5), shutdown.cancelled())
                    .await
                    .expect("stop signal cancels the shutdown token");
            });
        return;
    }

    let test_executable = std::env::current_exe().expect("test executable");
    for signal in [Signal::SIGINT, Signal::SIGTERM] {
        let status = std::process::Command::new(&test_executable)
            .args([
                "--exact",
                "shutdown::tests::stop_signals_are_handled_once_shutdown_token_returns",
                "--nocapture",
            ])
            .env(CHILD, signal.as_str())
            .stdout(std::process::Stdio::null())
            .status()
            .expect("child test process");
        assert!(
            status.success(),
            "{signal} raised after shutdown_token returned was not handled: {status}"
        );
    }
}
