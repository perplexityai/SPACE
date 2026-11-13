use std::collections::HashMap;
use std::sync::Mutex as StdMutex;

use pplx_space_accounts::NodeId;
use pplx_space_api::controllers::substrate::NodeReportedArtifact;
use pplx_space_common_core::types::{LocalArtifactId, SandboxId};
use pplx_space_sandbox_store::restore_pin_repair::RestorePinRepairCandidate;

use tokio_util::sync::CancellationToken;

use super::{NodePin, Reconciler, ReconcilerConfig, RepairSink, TickReport, node_pin_from_report};

/// A fake: each sandbox's node answer and repair verdict, and the cursor
/// every listing was asked from.
#[derive(Default)]
struct FakeSink {
    rows: Vec<RestorePinRepairCandidate>,
    answers: HashMap<SandboxId, Result<NodePin, String>>,
    verdicts: HashMap<SandboxId, bool>,
    asked_after: StdMutex<Vec<Option<SandboxId>>>,
    repaired: StdMutex<Vec<(SandboxId, LocalArtifactId)>>,
    nodes_asked: StdMutex<usize>,
    /// Nodes that take the whole RPC budget to answer.
    slow: Vec<SandboxId>,
}

impl RepairSink for FakeSink {
    async fn candidates(
        &self,
        after: Option<SandboxId>,
        limit: u32,
    ) -> Result<Vec<RestorePinRepairCandidate>, String> {
        self.asked_after.lock().unwrap().push(after);
        // A pinned row is no longer a candidate.
        let repaired = self.repaired.lock().unwrap();
        Ok(self
            .rows
            .iter()
            .copied()
            .filter(|row| after.is_none_or(|after| row.sandbox_id > after))
            .filter(|row| !repaired.iter().any(|(id, _)| *id == row.sandbox_id))
            .take(limit as usize)
            .collect())
    }

    async fn node_pin(&self, candidate: RestorePinRepairCandidate) -> Result<NodePin, String> {
        *self.nodes_asked.lock().unwrap() += 1;
        if self.slow.contains(&candidate.sandbox_id) {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        }
        self.answers[&candidate.sandbox_id].clone()
    }

    async fn repair(
        &self,
        candidate: RestorePinRepairCandidate,
        artifact: LocalArtifactId,
    ) -> Result<bool, String> {
        let verdict = self.verdicts[&candidate.sandbox_id];
        if verdict {
            self.repaired
                .lock()
                .unwrap()
                .push((candidate.sandbox_id, artifact));
        }
        Ok(verdict)
    }
}

/// A tick pins what the node holds and the store accepts, counts the rest
/// unrepairable, retries an unreachable node on a later pass, and pages
/// through the candidates, wrapping to the start after a pass.
#[tokio::test]
async fn a_tick_pins_accepted_artifacts_and_pages_through_the_rest() {
    let node = NodeId::new();
    let mut ids: Vec<SandboxId> = (0..4).map(|_| SandboxId::new()).collect();
    ids.sort();
    let artifact = LocalArtifactId::new();
    let mut sink = FakeSink {
        rows: ids
            .iter()
            .map(|&sandbox_id| RestorePinRepairCandidate {
                sandbox_id,
                node_id: node,
                lifecycle_changed_at: time::OffsetDateTime::UNIX_EPOCH,
            })
            .collect(),
        ..FakeSink::default()
    };
    sink.answers.insert(ids[0], Ok(NodePin::Held(artifact)));
    sink.verdicts.insert(ids[0], true);
    sink.answers
        .insert(ids[1], Ok(NodePin::Held(LocalArtifactId::new())));
    sink.verdicts.insert(ids[1], false);
    sink.answers
        .insert(ids[2], Ok(NodePin::Unusable("the node is gone")));
    sink.answers.insert(ids[3], Err("unreachable".to_owned()));
    let reconciler = Reconciler::new(
        ReconcilerConfig {
            batch: 3,
            ..ReconcilerConfig::default()
        },
        sink,
    );

    assert_eq!(
        reconciler.tick(&CancellationToken::new()).await,
        TickReport {
            pinned: 1,
            unrepairable: 2
        }
    );
    assert_eq!(
        reconciler.tick(&CancellationToken::new()).await,
        TickReport::default()
    );
    reconciler.tick(&CancellationToken::new()).await;
    assert_eq!(
        *reconciler.sink.asked_after.lock().unwrap(),
        [None, Some(ids[2]), Some(ids[3])]
    );
    reconciler.tick(&CancellationToken::new()).await;
    assert_eq!(reconciler.sink.asked_after.lock().unwrap()[3], None);
    assert_eq!(
        *reconciler.sink.repaired.lock().unwrap(),
        [(ids[0], artifact)]
    );
}

/// Only a present, healthy, suspended report with a readable pin names an
/// artifact; a live VM, a degraded report, an unreadable pin, another
/// lifecycle or no pin don't.
#[test]
fn only_a_suspended_nodes_readable_pin_names_the_artifact() {
    let artifact = LocalArtifactId::new();
    let held = NodeReportedArtifact {
        present: true,
        pinned_artifact: Some(artifact),
        reports_suspended: true,
        ..NodeReportedArtifact::default()
    };
    assert_eq!(node_pin_from_report(&held), NodePin::Held(artifact));
    for (case, report) in [
        (
            "absent",
            NodeReportedArtifact {
                present: false,
                ..held.clone()
            },
        ),
        (
            "alive",
            NodeReportedArtifact {
                vm_alive: true,
                ..held.clone()
            },
        ),
        (
            "degraded",
            NodeReportedArtifact {
                state_degraded: true,
                ..held.clone()
            },
        ),
        (
            "unreadable",
            NodeReportedArtifact {
                pin_unreadable: true,
                ..held.clone()
            },
        ),
        (
            "not suspended",
            NodeReportedArtifact {
                reports_suspended: false,
                ..held.clone()
            },
        ),
        (
            "no pin",
            NodeReportedArtifact {
                pinned_artifact: None,
                ..held.clone()
            },
        ),
    ] {
        assert!(
            matches!(node_pin_from_report(&report), NodePin::Unusable(_)),
            "{case}"
        );
    }
}

/// Slow nodes are probed together: a batch of them takes about one RPC
/// budget, not one per sandbox.
#[tokio::test(start_paused = true)]
async fn slow_nodes_are_probed_concurrently() {
    let node = NodeId::new();
    let ids: Vec<SandboxId> = (0..4).map(|_| SandboxId::new()).collect();
    let mut sink = FakeSink {
        rows: ids
            .iter()
            .map(|&sandbox_id| RestorePinRepairCandidate {
                sandbox_id,
                node_id: node,
                lifecycle_changed_at: time::OffsetDateTime::UNIX_EPOCH,
            })
            .collect(),
        slow: ids.clone(),
        ..FakeSink::default()
    };
    for &id in &ids {
        sink.answers
            .insert(id, Ok(NodePin::Unusable("the node is gone")));
    }
    let reconciler = Reconciler::new(ReconcilerConfig::default(), sink);
    let started = tokio::time::Instant::now();
    let report = reconciler.tick(&CancellationToken::new()).await;
    assert_eq!(report.unrepairable, 4);
    assert!(started.elapsed() < std::time::Duration::from_secs(60));
}

/// A cancelled tick asks no node and writes no pin.
#[tokio::test]
async fn a_cancelled_tick_does_nothing() {
    let node = NodeId::new();
    let sandbox_id = SandboxId::new();
    let artifact = LocalArtifactId::new();
    let mut sink = FakeSink {
        rows: vec![RestorePinRepairCandidate {
            sandbox_id,
            node_id: node,
            lifecycle_changed_at: time::OffsetDateTime::UNIX_EPOCH,
        }],
        ..FakeSink::default()
    };
    sink.answers.insert(sandbox_id, Ok(NodePin::Held(artifact)));
    sink.verdicts.insert(sandbox_id, true);
    let reconciler = Reconciler::new(ReconcilerConfig::default(), sink);
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    assert_eq!(reconciler.tick(&shutdown).await, TickReport::default());
    assert!(reconciler.sink.repaired.lock().unwrap().is_empty());
    assert_eq!(*reconciler.sink.nodes_asked.lock().unwrap(), 0);
}

#[test]
fn the_repair_is_off_unless_enabled() {
    let mut env = pplx_space_test_env::TestEnv::default();
    assert!(!ReconcilerConfig::from_environment(&env).enabled);
    for on in ["true", "1", "yes", "on", "enabled"] {
        env.set("SPACE_RESTORE_PIN_REPAIR_ENABLED", on);
        assert!(ReconcilerConfig::from_environment(&env).enabled, "{on}");
    }
    env.set("SPACE_RESTORE_PIN_REPAIR_ENABLED", "off");
    assert!(!ReconcilerConfig::from_environment(&env).enabled);
    env.set("SPACE_RESTORE_PIN_REPAIR_BATCH", "7");
    assert_eq!(ReconcilerConfig::from_environment(&env).batch, 7);
    env.set("SPACE_RESTORE_PIN_REPAIR_BATCH", "1000000");
    assert_eq!(
        ReconcilerConfig::from_environment(&env).batch,
        super::MAX_BATCH
    );
}
