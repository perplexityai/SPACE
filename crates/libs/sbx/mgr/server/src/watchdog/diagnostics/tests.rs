use std::collections::HashMap;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

use super::super::tests::{sample, test_health};
use super::super::{ComposedProbeResult, apply_composed_health_result};
use super::*;

async fn apply_guest_outcome(
    state: &Arc<Mutex<HashMap<String, SandboxHealth>>>,
    db: &Arc<std::sync::Mutex<rusqlite::Connection>>,
    sandbox_id: &str,
    guest: HealthProbeOutcome,
) -> Option<HealthTransition> {
    let observations = HealthComponent::ALL.map(|component| {
        sample(if component == HealthComponent::SpacedGuest {
            guest
        } else {
            HealthProbeOutcome::Success
        })
    });
    apply_composed_health_result(
        state,
        db,
        Some(Ok((
            sandbox_id.to_string(),
            Some(crate::state::SandboxState::Started),
            0,
            ComposedProbeResult {
                observations,
                guest_status: None,
            },
        ))),
    )
    .await
}

#[tokio::test]
async fn only_degrading_transitions_gather_evidence_and_console_tails_are_rate_limited() {
    let sandbox_id = pplx_space_common_core::types::SandboxId::new().to_string();
    let state = Arc::new(Mutex::new(HashMap::from([(
        sandbox_id.clone(),
        test_health(),
    )])));
    let db = Arc::new(std::sync::Mutex::new(crate::db::test_conn()));
    let mut transitions = Vec::new();
    for guest in [
        HealthProbeOutcome::Success,
        HealthProbeOutcome::Success,
        HealthProbeOutcome::Timeout,
        HealthProbeOutcome::Timeout,
        HealthProbeOutcome::Timeout,
        HealthProbeOutcome::Timeout,
        HealthProbeOutcome::Success,
        HealthProbeOutcome::Success,
        HealthProbeOutcome::Unavailable,
        HealthProbeOutcome::Unavailable,
    ] {
        transitions.push(apply_guest_outcome(&state, &db, &sandbox_id, guest).await);
    }

    let observed = transitions
        .iter()
        .map(|transition| {
            transition.as_ref().map(|transition| {
                (
                    transition.current,
                    transition.guest_current,
                    transition.evidence_request().is_some(),
                    transition.console_tails,
                )
            })
        })
        .collect::<Vec<_>>();
    use ComposedHealthStatus::{Degraded, Healthy};
    use ConsoleTails::{Included, NotRequested, RateLimited};
    use GuestHealthCategory::Degraded as GuestDegraded;
    use GuestHealthCategory::Healthy as GuestHealthy;
    assert_eq!(
        observed,
        vec![
            None,
            Some((Healthy, GuestHealthy, false, NotRequested)),
            None,
            // Two guest failures degrade the composed status...
            Some((Degraded, GuestHealthy, true, Included)),
            // ...and the third the guest category, a second degrading change.
            Some((Degraded, GuestDegraded, true, NotRequested)),
            None,
            Some((Degraded, GuestHealthy, false, NotRequested)),
            Some((Healthy, GuestHealthy, false, NotRequested)),
            None,
            Some((Degraded, GuestHealthy, true, RateLimited)),
        ]
    );

    let degraded = transitions[3].as_ref().expect("degraded transition");
    let guest = degraded
        .component(HealthComponent::SpacedGuest)
        .expect("guest snapshot");
    assert_eq!(guest.outcome, Some(HealthProbeOutcome::Timeout));
    assert_eq!(guest.consecutive_failures, 2);
    assert!(guest.degraded);
    assert!(
        guest.last_success_age.is_some(),
        "the guest answered before it degraded"
    );
}

#[derive(Clone, Copy)]
enum FakeGuest {
    Acknowledge,
    Reset,
    Silent,
    Garbage,
}

/// Serve one connection on a fresh socket the way Firecracker relays a guest:
/// read the `CONNECT` line, then answer per `guest`.
fn fake_firecracker_vsock(guest: FakeGuest) -> (PathBuf, tokio::task::JoinHandle<()>) {
    let path = PathBuf::from(format!("/tmp/sv-raw-{}.sock", uuid::Uuid::now_v7()));
    let listener = tokio::net::UnixListener::bind(&path).expect("bind fake vsock socket");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut stream = tokio::io::BufReader::new(stream);
        let mut line = String::new();
        stream.read_line(&mut line).await.expect("read CONNECT");
        assert_eq!(line, "CONNECT 5002\n");
        match guest {
            FakeGuest::Acknowledge => stream.write_all(b"OK 1073741824\n").await.unwrap(),
            FakeGuest::Garbage => stream.write_all(b"ERR\n").await.unwrap(),
            FakeGuest::Reset => {}
            FakeGuest::Silent => std::future::pending().await,
        }
    });
    (path, server)
}

#[tokio::test]
async fn raw_vsock_check_separates_answering_resetting_and_silent_guests() {
    let own_pid = i32::try_from(std::process::id()).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let own_uid =
        std::os::unix::fs::MetadataExt::uid(&std::fs::metadata(directory.path()).unwrap());
    for (guest, expected_peer, timeout, outcome, peer) in [
        (
            FakeGuest::Acknowledge,
            Some(ExpectedUdsPeer::new(own_pid, own_uid)),
            Duration::from_secs(5),
            RawVsockOutcome::Accepted,
            PeerCheck::Matched,
        ),
        (
            FakeGuest::Acknowledge,
            Some(ExpectedUdsPeer::new(own_pid + 1, own_uid)),
            Duration::from_secs(5),
            RawVsockOutcome::Accepted,
            PeerCheck::Mismatched,
        ),
        (
            FakeGuest::Reset,
            None,
            Duration::from_secs(5),
            RawVsockOutcome::Refused,
            PeerCheck::Unknown,
        ),
        (
            FakeGuest::Garbage,
            None,
            Duration::from_secs(5),
            RawVsockOutcome::UnexpectedReply,
            PeerCheck::Unknown,
        ),
        (
            FakeGuest::Silent,
            None,
            Duration::from_millis(50),
            RawVsockOutcome::Timeout,
            PeerCheck::Unknown,
        ),
    ] {
        let (path, server) = fake_firecracker_vsock(guest);
        let check = raw_vsock_check(&path, expected_peer, timeout).await;
        assert_eq!((check.outcome, check.peer), (outcome, peer));
        server.abort();
        let _ = std::fs::remove_file(path);
    }

    let missing = PathBuf::from(format!("/tmp/sv-raw-{}.sock", uuid::Uuid::now_v7()));
    let check = raw_vsock_check(&missing, None, Duration::from_secs(5)).await;
    assert_eq!(check.outcome, RawVsockOutcome::HostSocketUnavailable);
}

#[tokio::test]
async fn console_tail_keeps_the_newest_whole_lines_without_state_polls() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("fc.log");
    let poll = "T [vm:fc_api] The API server received a Get request on \"/\".\n\
                T [vm:fc_api] The request was executed successfully. Status code: 200 OK.\n";
    let mut content = String::new();
    for line in 0..20 {
        content.push_str(&format!("event {line:02}\n"));
    }
    content.push_str("T [vm:fc_api] The API server received a Patch request on \"/vm\".\n");
    content.push_str(
        "T [vm:fc_api] The request was executed successfully. Status code: 204 No Content.\n",
    );
    for _ in 0..50 {
        content.push_str(poll);
    }
    content.push_str("last \u{1b}[31mred\u{1b}[0m\tline\n");
    std::fs::write(&path, &content).unwrap();
    let window = TailWindow {
        scan_bytes: 64 * 1024,
        // Exactly the five newest kept lines.
        keep_bytes: 187,
        noise: tail::firecracker_state_poll,
    };
    let reads = HostReads::new(1);

    let tail = reads
        .tail(path.clone(), window)
        .await
        .expect("existing file has a tail");
    assert_eq!(
        tail,
        "event 18\nevent 19\n\
         T [vm:fc_api] The API server received a Patch request on \"/vm\".\n\
         T [vm:fc_api] The request was executed successfully. Status code: 204 No Content.\n\
         last ?[31mred?[0m\tline\n"
    );

    // A scan that starts mid-file drops the partial first line.
    let short = TailWindow {
        scan_bytes: 12,
        keep_bytes: 160,
        noise: tail::no_noise,
    };
    assert_eq!(reads.tail(path, short).await.as_deref(), Some(""));
    assert_eq!(
        reads.tail(directory.path().join("absent"), window).await,
        None
    );
}

fn evidence_request(sandbox_id: &str) -> EvidenceRequest {
    EvidenceRequest {
        sandbox_id: sandbox_id.to_string(),
        transition_id: TransitionId::new(),
        console_tails: ConsoleTails::Included,
        queued_at: Instant::now(),
        merged_transitions: 0,
    }
}

/// Opening a FIFO for reading blocks until a writer opens it, as a read of a
/// wedged filesystem would. Opening it for writing releases a waiting reader
/// and, closed at once, gives it end of file; with no reader waiting it fails.
fn release_stuck_read(fifo: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    std::fs::OpenOptions::new()
        .write(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(fifo)
        .map(drop)
}

/// Releases the stuck read if the test fails first, since the runtime waits
/// for blocking-pool work before the test can finish.
struct ReleaseOnDrop(PathBuf);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        let _ = release_stuck_read(&self.0);
    }
}

// Real time throughout: the stuck read occupies a blocking-pool thread,
// which paused Tokio time does not wait for.
#[tokio::test]
async fn evidence_deadline_keeps_what_was_collected_and_caps_stuck_host_reads() {
    let cgroup = tempfile::tempdir().unwrap();
    std::fs::write(
        cgroup.path().join("cpu.stat"),
        "usage_usec 5000\nthrottled_usec 0\n",
    )
    .unwrap();
    std::fs::write(cgroup.path().join("cpu.max"), "max 100000\n").unwrap();
    let stuck = cgroup.path().join("cpu.pressure");
    nix::unistd::mkfifo(
        &stuck,
        nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
    )
    .unwrap();
    let _release = ReleaseOnDrop(stuck.clone());
    let paths = EvidencePaths {
        cgroup: cgroup.path().to_path_buf(),
        firecracker_log: cgroup.path().join("absent-fc.log"),
        guest_console: cgroup.path().join("absent-console.log"),
    };
    let limits = EvidenceLimits {
        deadline: Duration::from_millis(300),
        raw_vsock_timeout: Duration::from_millis(100),
        sample_window: Duration::from_millis(20),
    };
    // One slot, so the stuck read holds every slot there is.
    let reads = HostReads::new(1);
    let request = evidence_request("sbx_stuck");
    let target_for = |path: &Path| EvidenceTarget {
        vsock_uds_path: Some(path.to_path_buf()),
        expected_peer: None,
    };

    let (socket, server) = fake_firecracker_vsock(FakeGuest::Acknowledge);
    let started = Instant::now();
    let stalled =
        TransitionEvidence::gather(&request, Some(&target_for(&socket)), &paths, limits, &reads)
            .await;
    let elapsed = started.elapsed();
    server.abort();
    let _ = std::fs::remove_file(&socket);
    assert!(
        elapsed >= limits.deadline && elapsed < Duration::from_secs(2),
        "gather returned after {elapsed:?}"
    );
    assert!(stalled.is_partial());
    assert_eq!(stalled.stage, EvidenceStage::ResourceFiles);
    assert_eq!(
        stalled.raw_vsock.as_ref().map(|raw| raw.outcome),
        Some(RawVsockOutcome::Accepted)
    );
    assert_eq!(stalled.resources.cpu_util_pct, Some(0.0));
    assert_eq!(stalled.resources.cpu_max.as_deref(), Some("max 100000"));
    assert_eq!(stalled.resources.cpu_pressure_some_avg10, None);

    // The abandoned read still holds the only slot, so the next report
    // starts no read of its own and stops at its first one.
    let queued_behind = TransitionEvidence::gather(&request, None, &paths, limits, &reads).await;
    assert!(queued_behind.is_partial());
    assert_eq!(queued_behind.stage, EvidenceStage::CpuBefore);
    assert!(queued_behind.raw_vsock.is_none());

    // Once the stuck read returns, its slot does too.
    release_stuck_read(&stuck).expect("the stuck read is waiting for a writer");
    std::fs::remove_file(&stuck).unwrap();
    std::fs::write(&stuck, "some avg10=1.50 avg60=0.20 avg300=0.00 total=1\n").unwrap();
    let limits = EvidenceLimits {
        deadline: Duration::from_secs(5),
        ..limits
    };
    let (socket, server) = fake_firecracker_vsock(FakeGuest::Acknowledge);
    let complete =
        TransitionEvidence::gather(&request, Some(&target_for(&socket)), &paths, limits, &reads)
            .await;
    server.abort();
    let _ = std::fs::remove_file(&socket);
    assert!(!complete.is_partial());
    assert_eq!(complete.stage, EvidenceStage::Done);
    assert_eq!(complete.resources.cpu_pressure_some_avg10, Some(1.5));
}
