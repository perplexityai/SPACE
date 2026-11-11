use super::*;
use crate::service::tests::service_tests::LogFieldCapture;
use tracing_subscriber::prelude::*;

const PEER_PAYLOAD: &str = "untrusted-clock-status-content";
#[derive(Clone, Copy, PartialEq)]
enum ClockOutcome {
    Synced,
    Timeout,
    Rejected,
}

struct Peer {
    calls: Arc<Mutex<Vec<&'static str>>>,
    env_fails: bool,
    clock: ClockOutcome,
}

impl ResidentRecoveryPeer for Peer {
    async fn configure(&mut self, env: &BTreeMap<String, String>) -> Result<(), Status> {
        assert_eq!(env.get("SECRET").map(String::as_str), Some("test-only"));
        self.calls.lock().unwrap().push("env");
        if self.env_fails {
            Err(Status::unavailable("env rejected"))
        } else {
            Ok(())
        }
    }
    async fn synchronize_clock(&mut self) -> Result<(), Status> {
        self.calls.lock().unwrap().push("clock");
        match self.clock {
            ClockOutcome::Synced => Ok(()),
            ClockOutcome::Timeout => std::future::pending().await,
            ClockOutcome::Rejected => Err(Status::unavailable(PEER_PAYLOAD)),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn recovered_peer_setup_authenticates_before_secrets_and_replays_every_thaw() {
    for (source, target, authenticated, env_fails, clock, expected, succeeds) in [
        (
            SandboxState::Resuming,
            SandboxState::Started,
            true,
            false,
            ClockOutcome::Synced,
            vec!["env", "clock"],
            true,
        ),
        (
            SandboxState::Resuming,
            SandboxState::Started,
            false,
            false,
            ClockOutcome::Synced,
            vec![],
            false,
        ),
        (
            SandboxState::Resuming,
            SandboxState::Started,
            true,
            true,
            ClockOutcome::Synced,
            vec!["env"],
            false,
        ),
        (
            SandboxState::Resuming,
            SandboxState::Started,
            true,
            false,
            ClockOutcome::Timeout,
            vec!["env", "clock"],
            true,
        ),
        (
            SandboxState::Resuming,
            SandboxState::Paused,
            true,
            false,
            ClockOutcome::Synced,
            vec![],
            true,
        ),
        (
            SandboxState::Checkpointing,
            SandboxState::Started,
            true,
            false,
            ClockOutcome::Synced,
            vec!["clock"],
            true,
        ),
        (
            SandboxState::Checkpointing,
            SandboxState::Paused,
            false,
            false,
            ClockOutcome::Synced,
            vec![],
            false,
        ),
        (
            SandboxState::Resuming,
            SandboxState::Started,
            true,
            false,
            ClockOutcome::Rejected,
            vec!["env", "clock"],
            true,
        ),
    ] {
        let raw_log = LogFieldCapture::new("result");
        let codes = LogFieldCapture::new("status_code");
        let _subscriber = tracing::subscriber::set_default(
            tracing_subscriber::registry()
                .with(raw_log.clone())
                .with(codes.clone()),
        );
        let calls = Arc::new(Mutex::new(Vec::new()));
        let connect = async {
            if !authenticated {
                return Err(Status::permission_denied("replaced endpoint"));
            }
            Ok(Peer {
                calls: calls.clone(),
                env_fails,
                clock,
            })
        };
        let result = prepare_resident_peer(
            "fixture",
            source,
            target,
            &BTreeMap::from([("SECRET".into(), "test-only".into())]),
            connect,
        )
        .await;
        assert_eq!(result.is_ok(), succeeds);
        assert_eq!(*calls.lock().unwrap(), expected);
        assert!(
            raw_log
                .values
                .lock()
                .unwrap()
                .iter()
                .all(|v| !v.contains(PEER_PAYLOAD))
        );
        if clock == ClockOutcome::Rejected {
            assert!(
                codes
                    .values
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|v| v.contains("Unavailable"))
            );
        }
    }
}
