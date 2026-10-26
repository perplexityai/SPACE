use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use futures_util::StreamExt;
use pplx_space_spaced_guest_protocol::proto::bridge_guest::{
    PtyConfig, StartRequest, StatRequest, filesystem_server::Filesystem, process_event::Event,
    process_exit::Outcome, process_server::Process,
};

struct UnrelatedSpawns {
    stop: Arc<AtomicBool>,
    count: Arc<AtomicUsize>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl UnrelatedSpawns {
    fn start(probe: &Path) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let count = Arc::new(AtomicUsize::new(0));
        let child_stop = stop.clone();
        let child_count = count.clone();
        let probe = probe.to_owned();
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let thread = std::thread::spawn(move || {
            ready_tx.send(()).expect("parent waits for spawner");
            while !child_stop.load(Ordering::Relaxed) {
                let output = Command::new(&probe)
                    .arg("--descriptor-probe")
                    .stdin(Stdio::null())
                    .stderr(Stdio::inherit())
                    .output()
                    .expect("concurrent caller spawn");
                assert!(output.status.success());
                for target in std::str::from_utf8(&output.stdout).unwrap().lines() {
                    assert!(
                        !target.starts_with("/dev/pts/") && target != "/dev/ptmx",
                        "PTY inherited during startup: {target}"
                    );
                    assert!(
                        !target.ends_with("/cgroup.procs") && !target.starts_with("socket:"),
                        "placement or worker descriptor inherited during startup: {target}"
                    );
                }
                child_count.fetch_add(1, Ordering::Relaxed);
            }
        });
        ready_rx.recv().expect("spawner started");
        Self {
            stop,
            count,
            thread: Some(thread),
        }
    }

    fn finish(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.thread
            .take()
            .unwrap()
            .join()
            .expect("unrelated spawn checks");
        assert!(
            self.count.load(Ordering::Relaxed) > 1,
            "must exercise repeated unrelated execs"
        );
    }
}

impl Drop for UnrelatedSpawns {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take()
            && thread.join().is_err()
            && !std::thread::panicking()
        {
            panic!("unrelated spawner failed");
        }
    }
}

pub(super) async fn during_startup(boundary: &super::Boundary, probe: &Path) {
    let unrelated = UnrelatedSpawns::start(probe);
    // A fresh service forces actual socketpair/worker startup each round. The
    // independent OS thread execs while both worker and PTY setup run, rather
    // than starting only after their temporary descriptor windows have closed.
    for _ in 0..16 {
        let service = boundary.service();
        let filesystem = service.filesystem_service();
        let (worker, process) = tokio::join!(
            filesystem.stat_info(tonic::Request::new(StatRequest {
                path: "/".into(),
                user: None
            })),
            service.start(tonic::Request::new(StartRequest {
                command: vec!["/bin/true".into()],
                cwd: Some("/".into()),
                pty: Some(PtyConfig { size: None }),
                idempotency_key: Some("startup-pty".into()),
                ..Default::default()
            })),
        );
        worker.expect("fresh identity worker");
        let mut events = process.expect("concurrent PTY start").into_inner();
        let mut exited = false;
        while let Some(event) = events.next().await {
            if let Some(Event::Exit(exit)) = event.expect("PTY event").event {
                assert!(matches!(exit.outcome, Some(Outcome::Exited(exit)) if exit.exit_code == 0));
                exited = true;
            }
        }
        assert!(exited);
        service.drain().await.expect("startup round cleanup");
    }
    unrelated.finish();
}
