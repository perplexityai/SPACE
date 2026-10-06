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
                    transition.needs_evidence(),
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
async fn console_tail_is_bounded_to_the_newest_whole_lines() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("fc.log");
    let mut content = String::new();
    for line in 0..100 {
        content.push_str(&format!("line {line:03}\n"));
    }
    content.push_str("last \u{1b}[31mred\u{1b}[0m\tline\n");
    std::fs::write(&path, &content).unwrap();

    let tail = read_tail(&path, 64)
        .await
        .expect("existing file has a tail");
    assert!(
        tail.len() <= 64,
        "tail of {} bytes exceeds the cap",
        tail.len()
    );
    assert!(tail.starts_with("line "), "tail starts mid-line: {tail:?}");
    assert!(tail.ends_with("last ?[31mred?[0m\tline\n"));

    std::fs::write(&path, "short\n").unwrap();
    assert_eq!(read_tail(&path, 64).await.as_deref(), Some("short\n"));
    assert_eq!(read_tail(&directory.path().join("absent"), 64).await, None);
}
