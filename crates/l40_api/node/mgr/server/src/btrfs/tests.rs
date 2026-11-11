use super::*;
use std::io::Read;
use std::os::fd::{AsFd, AsRawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use nix::sys::signal::{SaFlags, SigAction, SigHandler, SigSet, Signal};

static SIGNAL_HANDLED: AtomicBool = AtomicBool::new(false);

extern "C" fn on_signal(_: libc::c_int) {
    SIGNAL_HANDLED.store(true, Ordering::SeqCst);
}

struct Subvolumes<'a>(&'a File);

impl Drop for Subvolumes<'_> {
    fn drop(&mut self) {
        // Attempt both removals even if one fails, and do this before TempDir
        // tries ordinary directory removal on read-only subvolumes.
        let cleanup =
            ["child", "parent"].map(|name| crate::clone_cleanup::remove_at(self.0, name.as_ref()));
        for result in cleanup {
            if std::thread::panicking() {
                if let Err(error) = result {
                    eprintln!("btrfs test cleanup failed: {error}");
                }
            } else {
                result.expect("failed to clean up btrfs test subvolume");
            }
        }
    }
}

struct SignalHandler(SigAction);

impl Drop for SignalHandler {
    fn drop(&mut self) {
        // SAFETY: senders are joined before the handler guard is dropped.
        if let Err(error) = unsafe { nix::sys::signal::sigaction(Signal::SIGUSR2, &self.0) } {
            eprintln!("failed to restore test signal handler: {error}");
        }
    }
}

fn wait_until(mut ready: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !ready() {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    true
}

#[test]
fn signal_during_backpressured_send_does_not_replay_the_ioctl() {
    let Some(root) = std::env::var_os("SPACE_NOCOW_TEST_BTRFS_DIR") else {
        assert!(
            std::env::var_os("CI").is_none(),
            "CI requires the btrfs fixture"
        );
        eprintln!("requires SPACE_NOCOW_TEST_BTRFS_DIR on a disposable btrfs filesystem");
        return;
    };
    assert!(nix::unistd::geteuid().is_root());
    let temp = tempfile::tempdir_in(root).unwrap();
    let root = File::open(temp.path()).unwrap();
    let _subvolumes = Subvolumes(&root);
    create_subvolume(&root, "parent".as_ref()).unwrap();
    let parent_path = temp.path().join("parent");
    let parent = File::open(&parent_path).unwrap();
    set_readonly(&parent, true).unwrap();
    snapshot(&parent, &root, "child".as_ref(), false).unwrap();
    let child_path = temp.path().join("child");
    let child = File::open(&child_path).unwrap();
    std::fs::write(child_path.join("rootfs.ext4"), vec![0x5a; 256 * 1024]).unwrap();
    set_readonly(&child, true).unwrap();

    // nextest isolates this privileged test in its own process. Target just
    // the sending thread; ordinary process delivery could choose the reader.
    let action = SigAction::new(
        SigHandler::Handler(on_signal),
        SaFlags::SA_RESTART,
        SigSet::empty(),
    );
    // SAFETY: the handler only stores into a lock-free atomic.
    let _handler =
        SignalHandler(unsafe { nix::sys::signal::sigaction(Signal::SIGUSR2, &action) }.unwrap());
    let (read, write) = nix::unistd::pipe().unwrap();
    nix::fcntl::fcntl(write.as_fd(), nix::fcntl::FcntlArg::F_SETPIPE_SZ(4096)).unwrap();
    let mut reader = File::from(read);
    let output = File::from(write);
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (result, bytes, coordinated, mask_restored, failed_send) = std::thread::scope(|scope| {
        let sender = scope.spawn(|| {
            let mut initial = SigSet::empty();
            initial.add(Signal::SIGUSR1);
            initial.thread_block().unwrap();
            let before = SigSet::thread_get_mask().unwrap();
            // SAFETY: these calls return the calling thread's identities.
            let tid = unsafe { libc::syscall(libc::SYS_gettid) };
            ready_tx.send(tid).unwrap();
            let result = send(&parent, &child, &output);
            drop(output);
            let after_success = SigSet::thread_get_mask().unwrap();
            let (closed_reader, broken_writer) = nix::unistd::pipe().unwrap();
            drop(closed_reader);
            let failed = send(&parent, &child, &File::from(broken_writer)).is_err();
            let after_error = SigSet::thread_get_mask().unwrap();
            let restored = before == after_success && before == after_error;
            (result, restored, failed)
        });
        let tid = ready_rx.recv().unwrap();
        let blocked = wait_until(|| {
            let mut queued: libc::c_int = 0;
            // SAFETY: FIONREAD writes one int; the reader stays open.
            (unsafe { libc::ioctl(reader.as_raw_fd(), libc::FIONREAD, &mut queued) }) == 0
                && queued > 0
                // A partially occupied pipe can already have no free buffer
                // slots. Wait for the writer itself, not a byte-count guess.
                && std::fs::read_to_string(format!("/proc/self/task/{tid}/wchan"))
                    .is_ok_and(|name| matches!(name.trim(), "pipe_write" | "anon_pipe_write"))
        });
        let pid = nix::unistd::getpid().as_raw();
        // SAFETY: the sender remains alive while its full pipe cannot drain.
        let signalled =
            blocked && unsafe { libc::syscall(libc::SYS_tgkill, pid, tid, libc::SIGUSR2) } == 0;
        let observed = signalled
            && wait_until(|| {
                if SIGNAL_HANDLED.load(Ordering::SeqCst) {
                    return true;
                }
                // With masking the signal stays pending; without it the handler
                // runs and the kernel replays SEND. Drain only after either event.
                std::fs::read_to_string(format!("/proc/self/task/{tid}/status"))
                    .ok()
                    .and_then(|status| {
                        status
                            .lines()
                            .find_map(|line| line.strip_prefix("SigPnd:\t"))
                            .and_then(|mask| u64::from_str_radix(mask, 16).ok())
                    })
                    .is_some_and(|mask| mask & (1 << (libc::SIGUSR2 - 1)) != 0)
            });
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).unwrap();
        let (result, restored, failed) = sender.join().unwrap();
        (result, bytes, observed, restored, failed)
    });
    assert!(
        coordinated,
        "signal must arrive while SEND is blocked in pipe_write"
    );
    result.unwrap();
    assert!(SIGNAL_HANDLED.load(Ordering::SeqCst));
    assert!(
        mask_restored,
        "blocking threads must retain their prior signal mask"
    );
    assert!(failed_send, "sending to a closed pipe must fail");
    let stream_path = temp.path().join("rootfs.btrfs-send");
    std::fs::write(&stream_path, bytes).unwrap();
    let dump = std::process::Command::new("btrfs")
        .args(["receive", "--dump", "-f"])
        .arg(&stream_path)
        .output()
        .unwrap();
    assert!(
        dump.status.success(),
        "{}",
        String::from_utf8_lossy(&dump.stderr)
    );
}
