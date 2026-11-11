use super::*;
use crate::host_command::Command;
use std::process::{Command as StdCommand, Stdio};
use std::time::{Duration, Instant};

const CHILD_MODE: &str = "SPACE_TEST_STOP_SIGNAL_ROUTING";
const TEST_NAME: &str =
    "process_signals::tests::shutdown_routing_preserves_workers_children_and_cleanup";

fn assert_worker_mask(before: SigSet) {
    let mask = SigSet::thread_get_mask().unwrap();
    assert!(mask.contains(Signal::SIGINT));
    assert!(mask.contains(Signal::SIGTERM));
    assert!(mask.contains(Signal::SIGUSR1));
    for untouched in [Signal::SIGPROF, Signal::SIGSEGV, Signal::SIGBUS] {
        assert_eq!(mask.contains(untouched), before.contains(untouched));
    }
}

fn assert_child_mask(bytes: &[u8]) {
    let status = std::str::from_utf8(bytes).unwrap();
    let mask = status
        .lines()
        .find_map(|line| line.strip_prefix("SigBlk:\t"))
        .map(|hex| u64::from_str_radix(hex, 16).unwrap())
        .expect("exec target reports its signal mask");
    for signal in [Signal::SIGINT, Signal::SIGTERM] {
        assert_eq!(mask & (1 << (signal as i32 - 1)), 0, "{signal}");
    }
    assert_ne!(mask & (1 << (libc::SIGUSR1 - 1)), 0);
}

#[test]
fn shutdown_routing_preserves_workers_children_and_cleanup() {
    if let Ok(mode) = std::env::var(CHILD_MODE) {
        // The harness's main thread stays blocked; exercise installation on
        // the test/startup thread with an otherwise unblocked stop-signal mask.
        if mode != "queued" {
            shutdown_signals().thread_unblock().unwrap();
        }
        let before = SigSet::thread_get_mask().unwrap();
        if mode == "queued" {
            nix::sys::signal::kill(nix::unistd::getpid(), Signal::SIGTERM).unwrap();
        }
        let signals = ProcessShutdownSignals::install().unwrap();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            tokio::spawn(async move { assert_worker_mask(before) })
                .await
                .unwrap();
            tokio::task::spawn_blocking(move || assert_worker_mask(before))
                .await
                .unwrap();
        });
        std::thread::spawn(move || assert_worker_mask(before))
            .join()
            .unwrap();
        assert_worker_mask(before);

        // Inspect exec targets directly: shells can reset their own masks.
        let output = crate::host_command::output(
            Command::new("cat").arg("/proc/self/status"),
            Duration::from_secs(2),
            &[],
        )
        .unwrap();
        assert!(output.status.success());
        assert_child_mask(&output.stdout);
        let output = crate::host_command::stream_in_namespace(
            StdCommand::new("cat").arg("/proc/self/status"),
            Duration::from_secs(2),
            Stdio::null(),
            Stdio::piped(),
            None,
        )
        .unwrap();
        assert!(output.status.success());
        assert_child_mask(&output.stdout);
        // A temporary send-like mask must not leak into child tools.
        let previous = SigSet::thread_get_mask().unwrap();
        let mut temporary = SigSet::empty();
        temporary.add(Signal::SIGUSR2);
        temporary.thread_block().unwrap();
        let _temporary_mask = StartupMask {
            previous,
            _thread: PhantomData,
        };
        let output = crate::host_command::output(
            Command::new("cat").arg("/proc/self/status"),
            Duration::from_secs(2),
            &[],
        )
        .unwrap();
        assert_child_mask(&output.stdout);
        let status = std::str::from_utf8(&output.stdout).unwrap();
        let blocked = status
            .lines()
            .find_map(|line| line.strip_prefix("SigBlk:\t"))
            .unwrap();
        assert_eq!(
            u64::from_str_radix(blocked, 16).unwrap() & (1 << (libc::SIGUSR2 - 1)),
            0
        );
        drop(_temporary_mask);

        if mode == "none" {
            assert!(!signals.shutdown_token().is_cancelled());
        } else {
            if mode != "queued" {
                let signal = mode.parse::<Signal>().unwrap();
                nix::sys::signal::kill(nix::unistd::getpid(), signal).unwrap();
            }
            runtime.block_on(async {
                tokio::time::timeout(Duration::from_secs(2), signals.shutdown_token().cancelled())
                    .await
                    .expect("routed signal must cancel the service token");
            });
        }
        drop(runtime);
        drop(signals);
        assert_eq!(SigSet::thread_get_mask().unwrap(), before);
        return;
    }

    // Block before exec so even the child test harness's own threads inherit
    // the startup mask. The parent test process's mask is never changed.
    for mode in ["SIGINT", "SIGTERM", "queued", "none"] {
        let mut command = StdCommand::new(std::env::current_exe().unwrap());
        command.args(["--exact", TEST_NAME, "--nocapture"]);
        command.env(CHILD_MODE, mode);
        use std::os::unix::process::CommandExt;
        // SAFETY: only async-signal-safe signal-set and mask operations run.
        unsafe {
            command.pre_exec(|| {
                let mut signals = shutdown_signals();
                signals.add(Signal::SIGUSR1);
                signals.thread_block().map_err(io::Error::from)
            });
        }
        let mut child = command.spawn().unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("signal-routing subprocess hung: {mode}");
            }
            std::thread::sleep(Duration::from_millis(5));
        };
        assert!(status.success(), "signal-routing mode {mode}: {status}");
    }
}
