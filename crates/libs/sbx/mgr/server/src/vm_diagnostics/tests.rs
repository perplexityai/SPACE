use std::io::Write as _;
use std::path::Path;
use std::sync::mpsc;

use pplx_space_observability::test_capture::{CapturedEventsLayer, field_value};
use tracing_subscriber::layer::SubscriberExt as _;

use super::*;

/// Readers whose background tasks never run: the runtime is entered, so they
/// can be spawned, but never driven. Only the polls a test makes read a log.
fn readers() -> (tokio::runtime::Runtime, VmOutputReaders) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let mut readers = {
        let _entered = runtime.enter();
        VmOutputReaders::new(CancellationToken::new())
    };
    // File times come from the kernel's coarse clock, so a log created right
    // after the readers can be stamped before them; a second back makes every
    // log a test writes count as new.
    readers.started_at -= Duration::from_secs(1);
    (runtime, readers)
}

fn attach(
    runtime: &tokio::runtime::Runtime,
    readers: &VmOutputReaders,
    id: &str,
    pid: u32,
    jail: &Path,
) {
    let _entered = runtime.enter();
    readers.attach_at(id, pid, jail.to_path_buf());
}

fn append_error(jail: &Path, message: &str) {
    let mut log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(jail.join(FIRECRACKER_LOG_FILE))
        .unwrap();
    writeln!(
        log,
        "2026-10-19T00:00:00 [anonymous-instance:main:ERROR] {message}"
    )
    .unwrap();
}

/// `(sandbox_id, firecracker_message)` of every line one poll re-logged.
fn poll(readers: &VmOutputReaders) -> Vec<(String, String)> {
    let capture = Capture::new();
    tracing::dispatcher::with_default(&capture.dispatch, || readers.poll_logs_now());
    capture.logged()
}

/// The re-logged lines of every thread that installs `dispatch`.
struct Capture {
    dispatch: tracing::Dispatch,
    events: pplx_space_observability::test_capture::CapturedEvents,
}

impl Capture {
    fn new() -> Self {
        let layer = CapturedEventsLayer::new();
        let events = layer.events();
        Self {
            dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)),
            events,
        }
    }

    /// `(sandbox_id, firecracker_message)` of every line re-logged so far.
    fn logged(&self) -> Vec<(String, String)> {
        let mut logged: Vec<_> = self
            .events
            .events()
            .iter()
            .filter_map(|event| {
                Some((
                    field_value(event, "sandbox_id")?.to_string(),
                    field_value(event, "firecracker_message")?.to_string(),
                ))
            })
            .collect();
        logged.sort();
        logged
    }
}

fn logged(entries: &[(&str, &str)]) -> Vec<(String, String)> {
    entries
        .iter()
        .map(|(id, message)| (id.to_string(), message.to_string()))
        .collect()
}

/// Every attached sandbox's log is read on each poll, under its own id, and a
/// detached sandbox's is no longer read.
#[test]
fn one_poll_tails_every_attached_sandbox_until_detached() {
    let (runtime, readers) = readers();
    let jails: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    for (index, jail) in jails.iter().enumerate() {
        attach(&runtime, &readers, &format!("sbx-{index}"), 1, jail.path());
        append_error(jail.path(), &format!("first {index}"));
    }

    assert_eq!(
        poll(&readers),
        logged(&[
            ("sbx-0", "first 0"),
            ("sbx-1", "first 1"),
            ("sbx-2", "first 2")
        ])
    );

    readers.detach("sbx-1");
    for (index, jail) in jails.iter().enumerate() {
        append_error(jail.path(), &format!("second {index}"));
    }

    assert_eq!(
        poll(&readers),
        logged(&[("sbx-0", "second 0"), ("sbx-2", "second 2")])
    );
}

/// One sandbox's log path holding a FIFO, which a blocking open would wait on
/// for a writer, or a symlink, neither holds up nor feeds the poll: it returns,
/// and every other sandbox's line is re-logged, in any iteration order.
#[test]
fn the_poll_reads_only_regular_files() {
    let (runtime, readers) = readers();
    let fifo = tempfile::tempdir().unwrap();
    nix::unistd::mkfifo(
        &fifo.path().join(FIRECRACKER_LOG_FILE),
        nix::sys::stat::Mode::from_bits_truncate(0o600),
    )
    .unwrap();
    let elsewhere = tempfile::tempdir().unwrap();
    append_error(elsewhere.path(), "not this sandbox's log");
    let symlink = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(
        elsewhere.path().join(FIRECRACKER_LOG_FILE),
        symlink.path().join(FIRECRACKER_LOG_FILE),
    )
    .unwrap();
    let regular: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    attach(&runtime, &readers, "fifo", 1, fifo.path());
    attach(&runtime, &readers, "symlink", 1, symlink.path());
    for (index, jail) in regular.iter().enumerate() {
        attach(
            &runtime,
            &readers,
            &format!("regular-{index}"),
            1,
            jail.path(),
        );
        append_error(jail.path(), &format!("still read {index}"));
    }

    let (polled, logged_lines) = mpsc::channel();
    // A poll stuck on the FIFO never returns, so it runs where a test can stop
    // waiting for it.
    std::thread::spawn(move || {
        let _ = polled.send(poll(&readers));
    });

    assert_eq!(
        logged_lines.recv_timeout(Duration::from_secs(10)),
        Ok(logged(&[
            ("regular-0", "still read 0"),
            ("regular-1", "still read 1")
        ]))
    );
}

/// A re-attach for the same Firecracker process keeps the tail where it was;
/// one for a new process reads that process's log from its start.
#[test]
fn a_new_runtime_pid_restarts_the_tail() {
    let (runtime, readers) = readers();
    let jail = tempfile::tempdir().unwrap();
    attach(&runtime, &readers, "sbx", 1, jail.path());
    append_error(jail.path(), "old process");
    assert_eq!(poll(&readers), logged(&[("sbx", "old process")]));

    attach(&runtime, &readers, "sbx", 1, jail.path());
    assert_eq!(poll(&readers), Vec::new());

    // The new process's log is longer than what was read of the old one, so
    // only a restarted tail reads it from its first byte.
    std::fs::remove_file(jail.path().join(FIRECRACKER_LOG_FILE)).unwrap();
    append_error(
        jail.path(),
        "new process, a longer first line than the old one",
    );
    attach(&runtime, &readers, "sbx", 2, jail.path());

    assert_eq!(
        poll(&readers),
        logged(&[("sbx", "new process, a longer first line than the old one")])
    );
}

/// A job that read a log before its sandbox was re-attached for a new
/// Firecracker process, and reaches admission only after the replace has
/// returned, re-logs nothing; the replacement logs the new process's line
/// exactly once. The replace does not wait for the job, which holds the log.
#[test]
fn a_read_admitted_after_a_replace_relogs_nothing() {
    let (runtime, readers) = readers();
    let jail = tempfile::tempdir().unwrap();
    attach(&runtime, &readers, "sbx", 1, jail.path());
    append_error(jail.path(), "old process, first");
    append_error(jail.path(), "old process, second");
    assert_eq!(
        poll(&readers),
        logged(&[
            ("sbx", "old process, first"),
            ("sbx", "old process, second")
        ])
    );
    // Shorter than what the old tail read, so the old tail reads it from 0.
    std::fs::remove_file(jail.path().join(FIRECRACKER_LOG_FILE)).unwrap();
    append_error(jail.path(), "new process");

    let stale = readers.registration("sbx");
    let mut state = lock(&stale.state);
    assert!(!*lock(&stale.retired));
    let read = state.tail.read().unwrap();
    attach(&runtime, &readers, "sbx", 2, jail.path());
    let capture = Capture::new();
    tracing::dispatcher::with_default(&capture.dispatch, || stale.admit(&mut state, read));
    drop(state);

    assert_eq!(capture.logged(), Vec::new());
    assert_eq!(poll(&readers), logged(&[("sbx", "new process")]));
    assert_eq!(poll(&readers), Vec::new());
}

// The capture a driven runtime thread installs, held until the thread stops.
thread_local! {
    static CAPTURING: std::cell::RefCell<Option<tracing::dispatcher::DefaultGuard>> =
        const { std::cell::RefCell::new(None) };
}

/// A runtime whose clock moves only when every task is idle and no blocking
/// job is running, and whose threads, the blocking pool's included, re-log to
/// `capture`.
fn driven_runtime(capture: &Capture) -> tokio::runtime::Runtime {
    let dispatch = capture.dispatch.clone();
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .on_thread_start(move || CAPTURING.set(Some(tracing::dispatcher::set_default(&dispatch))))
        .on_thread_stop(|| CAPTURING.set(None))
        .build()
        .unwrap()
}

/// Readers whose log poll runs, after it has taken its first, empty tick.
async fn driven_readers(shutdown: CancellationToken) -> VmOutputReaders {
    let mut readers = VmOutputReaders::new(shutdown);
    readers.started_at -= Duration::from_secs(1);
    tokio::time::sleep(Duration::from_millis(1)).await;
    readers
}

/// Yield until `done`. The paused clock stands still meanwhile, so no tick
/// fires while this waits.
async fn until(mut done: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(std::time::Instant::now() < deadline, "condition never held");
        tokio::task::yield_now().await;
    }
}

/// The running poll re-logs a line appended to an attached sandbox's log on a
/// later tick, and nothing once it is shut down.
#[test]
fn the_running_poll_relogs_appended_errors_until_shut_down() {
    let capture = Capture::new();
    driven_runtime(&capture).block_on(async {
        let shutdown = CancellationToken::new();
        let readers = driven_readers(shutdown.clone()).await;
        let jail = tempfile::tempdir().unwrap();
        readers.attach_at("sbx", 1, jail.path().to_path_buf());
        append_error(jail.path(), "first");
        until(|| capture.logged() == logged(&[("sbx", "first")])).await;

        append_error(jail.path(), "second");
        tokio::time::sleep(POLL_INTERVAL).await;
        until(|| capture.logged().len() == 2).await;
        assert_eq!(
            capture.logged(),
            logged(&[("sbx", "first"), ("sbx", "second")])
        );

        shutdown.cancel();
        append_error(jail.path(), "after shutdown");
        tokio::time::sleep(POLL_INTERVAL * 3).await;
        assert_eq!(
            capture.logged(),
            logged(&[("sbx", "first"), ("sbx", "second")])
        );
    });
}

/// An attach wakes the poll, so a line already in a new sandbox's log is
/// re-logged even when the sandbox is detached before the next regular tick,
/// as a start that fails and is cleaned up within one is.
#[test]
fn an_attach_reads_the_log_before_the_next_tick() {
    let capture = Capture::new();
    driven_runtime(&capture).block_on(async {
        let readers = driven_readers(CancellationToken::new()).await;
        let jail = tempfile::tempdir().unwrap();
        append_error(jail.path(), "before attach");
        let attached_at = Instant::now();

        readers.attach_at("sbx", 1, jail.path().to_path_buf());
        until(|| !capture.logged().is_empty()).await;
        readers.detach("sbx");

        assert!(attached_at.elapsed() < POLL_INTERVAL);
        tokio::time::sleep(POLL_INTERVAL * 2).await;
        assert_eq!(capture.logged(), logged(&[("sbx", "before attach")]));
    });
}

/// A job that copied the attachments before a sandbox was detached, or
/// re-attached for a new Firecracker process, skips the registrations it holds
/// for them: the detached log is not read, and the replaced one is read once,
/// by its new registration.
#[test]
fn a_running_job_skips_registrations_detached_or_replaced_since_its_copy() {
    let capture = Capture::new();
    driven_runtime(&capture).block_on(async {
        let readers = driven_readers(CancellationToken::new()).await;
        let detached = tempfile::tempdir().unwrap();
        let replaced = tempfile::tempdir().unwrap();
        readers.attach_at("detached", 1, detached.path().to_path_buf());
        readers.attach_at("replaced", 1, replaced.path().to_path_buf());
        append_error(detached.path(), "detached");
        append_error(replaced.path(), "replaced");
        let copied = [
            readers.registration("detached"),
            readers.registration("replaced"),
        ];

        // Holding both registrations stops the job the attaches woke at
        // whichever it reaches first, before it reads either log.
        let (held, holding) = mpsc::channel();
        let (release, released) = mpsc::channel::<()>();
        let holder = {
            let copied = copied.clone();
            std::thread::spawn(move || {
                let _held: Vec<_> = copied.iter().map(|log| lock(&log.state)).collect();
                held.send(()).unwrap();
                let _ = released.recv();
            })
        };
        holding.recv().unwrap();
        // The readers' map, `copied`, the holder's and the job's copy.
        until(|| copied.iter().all(|log| Arc::strong_count(log) == 4)).await;

        readers.detach("detached");
        readers.attach_at("replaced", 2, replaced.path().to_path_buf());
        drop(release);
        holder.join().unwrap();
        until(|| copied.iter().all(|log| Arc::strong_count(log) == 1)).await;
        until(|| !capture.logged().is_empty()).await;

        assert_eq!(capture.logged(), logged(&[("replaced", "replaced")]));
    });
}
