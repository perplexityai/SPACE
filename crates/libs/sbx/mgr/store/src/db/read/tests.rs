use pplx_space_common_core::inventory::{InventoryBudget, MAX_ITEMS, MAX_RAW_BYTES};
use pplx_space_common_core::types::{
    ArtifactKind, AutoLifecycleDeleteReceiptId, AutoLifecyclePolicy, ExecutionGeneration,
    LocalArtifactId, SandboxActivationAttemptId, SandboxCleanupEpisodeId, SandboxId,
    SandboxQuarantineReasonCode, SnapshotName, TransferScopeId,
};
use pplx_space_common_svc::types::AutoLifecyclePolicyFingerprint;
use rusqlite::params;

use super::*;
use crate::auto_lifecycle_delete_receipts::{AutoLifecycleDeleteReceipt, RouteIdentityProvenance};
use crate::db::inventory_read::InventoryReadError;
use crate::db::route_activation_claims::OpenRouteActivationClaim;
use crate::db::test_fixtures::state_write_fixtures::{
    RUNTIME_ACTIVATION_ATTEMPT, begin_cold_start, force_state, insert_row,
    insert_suspended_with_artifact, seed_runtime, seed_teardown_job,
};
use crate::db::test_fixtures::{
    setup_quarantine_recovery_candidate, setup_quarantine_recovery_episode,
};
use crate::db::{
    ClaimedQuarantineRecoveryCandidate, CleanupCompletion, CleanupEpisodeRequestOutcome,
    Connection, PlanRejectionRunEffect, test_conn,
};
use crate::state::SandboxState;

/// How an accessor answered, coarse enough to state independently of its
/// implementation: whether it found something, said yes or no, kept an
/// undecodable record, or failed the read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Answer {
    Some,
    None,
    True,
    False,
    Undecodable,
    Err,
}

use Answer::{Err as E, False as F, None as N, Some as S, True as T, Undecodable as U};

fn opt<T, Er>(result: std::result::Result<Option<T>, Er>) -> Answer {
    match result {
        Ok(Some(_)) => S,
        Ok(None) => N,
        Err(_) => E,
    }
}

fn flag<Er>(result: std::result::Result<bool, Er>) -> Answer {
    match result {
        Ok(true) => T,
        Ok(false) => F,
        Err(_) => E,
    }
}

fn value<T, Er>(result: std::result::Result<T, Er>) -> Answer {
    match result {
        Ok(_) => S,
        Err(_) => E,
    }
}

fn list<T, Er>(result: std::result::Result<Vec<T>, Er>) -> Answer {
    match result {
        Ok(items) if items.is_empty() => N,
        Ok(_) => S,
        Err(_) => E,
    }
}

fn decoded<T>(result: crate::db::Result<Decoded<T>>) -> Answer {
    match result {
        Ok(Decoded::Absent) => N,
        Ok(Decoded::Present(_)) => S,
        Ok(Decoded::Undecodable(_)) => U,
        Err(_) => E,
    }
}

/// The states each accessor is characterized against. `Accepting` gives every
/// accessor something to find or agree with; every other fixture is it with
/// one class of defect.
#[derive(Clone, Copy, Debug)]
enum Fixture {
    Accepting,
    /// Every row's process environment but one is not JSON, and one child
    /// record of each kind does not decode: the cleanup episode's activation
    /// attempt, the teardown job's payload, both restore scopes' ids and both
    /// quarantines' reason codes. The second restore's row stays decodable,
    /// so its reclaim check reaches the scope. (The schema admits the episode's JSON
    /// column, a prepared clone root, only for a pool-clone disposal.)
    CorruptJson,
    /// Every `sandboxes` row is removed by a raw `DELETE` with only the
    /// foreign-key cascades, not the production delete path (which first
    /// runs `delete_sandbox_child_rows`): the cleanup episodes, route claim
    /// and quarantine state cascade, and the tables without a foreign key
    /// keep their rows. A pending delete receipt guards its row against
    /// deletion, so its outbox row is deleted first.
    MissingRow,
    /// The main row has a previous node execution without a current one.
    PreviousWithoutCurrent,
}

const FIXTURES: [Fixture; 4] = [
    Fixture::Accepting,
    Fixture::CorruptJson,
    Fixture::MissingRow,
    Fixture::PreviousWithoutCurrent,
];

/// Six sandboxes, one per shape an accessor needs to agree with.
struct Seeded {
    conn: Connection,
    /// A requested cold-start cleanup over a live runtime, with a running
    /// teardown-suspend job, a retained execution delete receipt, a written
    /// route claim and a pending auto-lifecycle delete receipt.
    main: SandboxId,
    main_ids: Vec<String>,
    cleanup_id: SandboxCleanupEpisodeId,
    claim: OpenRouteActivationClaim,
    /// Suspended, pinning an artifact it owns, with a snapshot-upload job and
    /// a named publication that has not been backed up.
    suspended: String,
    pin: LocalArtifactId,
    /// A remote restore interrupted before it bound anything, with the scope
    /// of its activation attempt.
    restore: String,
    scope: TransferScopeId,
    attempt: SandboxActivationAttemptId,
    /// A legacy-recovery quarantine whose claim is held.
    recovering: String,
    claimed: ClaimedQuarantineRecoveryCandidate,
    /// An sbx-manager quarantine of an orphan whose row went missing.
    orphan: String,
    /// Paused under the main generation, with no cleanup intent or episode,
    /// a pending receipt for an older generation and an expired execution
    /// receipt.
    paused: String,
    paused_id: SandboxId,
    /// A second interrupted restore; only its scope is corrupted.
    restore_scope_only: String,
    scope_only: TransferScopeId,
}

const GENERATION: i64 = 7;

fn generation() -> ExecutionGeneration {
    ExecutionGeneration::new(GENERATION).unwrap()
}

fn seed(fixture: Fixture) -> Seeded {
    let conn = test_conn();
    // Claimed first: the claim fixture expects to be the only candidate.
    let (recovering_id, candidate) = setup_quarantine_recovery_candidate(&conn);
    let claimed = crate::db::claim_quarantine_recovery_candidate(
        &conn,
        &candidate,
        PlanRejectionRunEffect::Leaves,
    )
    .unwrap()
    .unwrap();

    let main_text = insert_row(&conn, SandboxState::Stopped);
    let main: SandboxId = main_text.parse().unwrap();
    let episode = begin_cold_start(&conn, &main_text);
    seed_runtime(&conn, &main_text);
    assert_eq!(
        crate::db::request_cleanup_episode(
            &conn,
            &main_text,
            episode.id,
            SandboxQuarantineReasonCode::ColdActivationInterrupted,
            "seeded",
        )
        .unwrap(),
        CleanupEpisodeRequestOutcome::Requested
    );
    seed_teardown_job(&conn, &main_text);
    conn.execute(
        "UPDATE jobs SET status = 'running' WHERE kind = 'teardown_suspend' AND subject_id = ?1",
        params![main_text],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO sandbox_execution_delete_receipts
             (sandbox_id, execution_generation, deleted_at)
         VALUES (?1, ?2, ?3)",
        params![main_text, GENERATION, crate::db::now_epoch_i64().unwrap()],
    )
    .unwrap();
    let identity = crate::db::get_sandbox(&conn, &main_text)
        .unwrap()
        .unwrap()
        .space_api_route_identity;
    let claim = OpenRouteActivationClaim::for_cluster_fenced_identity(
        &identity,
        RouteIdentityProvenance::ClusterFenced,
    )
    .unwrap();
    crate::db::route_activation_claims::insert_create_claim(&conn, &main_text, &claim).unwrap();
    crate::db::auto_lifecycle_delete_receipts::insert_pending_receipt(
        &conn,
        AutoLifecycleDeleteReceipt {
            receipt_id: AutoLifecycleDeleteReceiptId::generate(),
            sandbox_id: main,
            execution_generation: generation(),
            route_activation_id: RUNTIME_ACTIVATION_ATTEMPT.parse().unwrap(),
            policy_fingerprint: AutoLifecyclePolicyFingerprint::for_policy(
                &AutoLifecyclePolicy::disabled(),
            ),
        },
    )
    .unwrap();

    let (suspended, pin) = insert_suspended_with_artifact(&conn);
    conn.execute(
        "INSERT INTO jobs (job_id, kind, status, payload, subject_id)
         VALUES (?1, 'snapshot_upload', 'completed', '{\"snapshot_id\":\"seeded\"}', ?2)",
        params![uuid::Uuid::now_v7().to_string(), suspended],
    )
    .unwrap();
    crate::db::publish_named_local_artifact(
        &conn,
        &suspended,
        LocalArtifactId::new(),
        ArtifactKind::Disk,
        &SnapshotName::parse("d-manual-20260605T000000Z").unwrap(),
        "named",
    )
    .unwrap();

    let attempt = SandboxActivationAttemptId::new();
    let interrupted_restore = |scope| {
        let id = insert_row(&conn, SandboxState::Stopped);
        force_state(&conn, &id, SandboxState::Restoring);
        assert!(
            crate::db::insert_restore_transfer_scope(
                &conn,
                &id,
                scope,
                Some(generation()),
                Some(attempt),
                "staging",
            )
            .unwrap()
        );
        id
    };
    let (scope, scope_only) = (TransferScopeId::new(), TransferScopeId::new());
    let restore = interrupted_restore(scope);
    let restore_scope_only = interrupted_restore(scope_only);

    let orphan = setup_quarantine_recovery_episode(
        &conn,
        SandboxQuarantineReasonCode::OrphanedSandboxRowMissing,
    )
    .to_string();

    let paused = insert_row(&conn, SandboxState::Paused);
    let paused_id: SandboxId = paused.parse().unwrap();
    conn.execute(
        "UPDATE sandboxes SET space_api_route_execution_generation = ?2 WHERE id = ?1",
        params![paused, GENERATION],
    )
    .unwrap();
    crate::db::auto_lifecycle_delete_receipts::insert_pending_receipt(
        &conn,
        AutoLifecycleDeleteReceipt {
            receipt_id: AutoLifecycleDeleteReceiptId::generate(),
            sandbox_id: paused_id,
            execution_generation: ExecutionGeneration::new(GENERATION - 1).unwrap(),
            route_activation_id: SandboxActivationAttemptId::new(),
            policy_fingerprint: AutoLifecyclePolicyFingerprint::for_policy(
                &AutoLifecyclePolicy::disabled(),
            ),
        },
    )
    .unwrap();
    conn.execute(
        "INSERT INTO sandbox_execution_delete_receipts
             (sandbox_id, execution_generation, deleted_at)
         VALUES (?1, ?2, ?3)",
        params![
            paused,
            GENERATION,
            crate::db::now_epoch_i64().unwrap()
                - crate::db::EXECUTION_DELETE_RECEIPT_RETENTION_SECS
                - 1
        ],
    )
    .unwrap();

    let recovering = recovering_id.to_string();
    let every_row = [
        &main_text,
        &suspended,
        &restore,
        &restore_scope_only,
        &recovering,
        &orphan,
        &paused,
    ];
    match fixture {
        Fixture::Accepting => {}
        Fixture::CorruptJson => {
            for id in every_row
                .into_iter()
                .filter(|id| **id != restore_scope_only)
            {
                corrupt(
                    &conn,
                    "UPDATE sandboxes SET process_env_json = 'malformed' WHERE id = ?1",
                    id,
                );
            }
            corrupt(
                &conn,
                "UPDATE sandbox_cleanup_episodes SET activation_attempt_id = 'malformed' WHERE sandbox_id = ?1",
                &main_text,
            );
            corrupt(
                &conn,
                "UPDATE jobs SET payload = 'malformed' WHERE kind = 'teardown_suspend' AND subject_id = ?1",
                &main_text,
            );
            for id in [&restore, &restore_scope_only] {
                corrupt(
                    &conn,
                    "UPDATE sandbox_restore_transfer_scopes SET transfer_scope_id = 'malformed-' || sandbox_id WHERE sandbox_id = ?1",
                    id,
                );
            }
            for id in [&recovering, &orphan] {
                corrupt(
                    &conn,
                    "UPDATE sandbox_quarantine_state SET reason_code = 'malformed' WHERE sandbox_id = ?1",
                    id,
                );
            }
        }
        Fixture::MissingRow => {
            for id in [&main_text, &paused] {
                corrupt(
                    &conn,
                    "DELETE FROM auto_lifecycle_delete_outbox WHERE sandbox_id = ?1",
                    id,
                );
            }
            for id in every_row {
                corrupt(&conn, "DELETE FROM sandboxes WHERE id = ?1", id);
            }
        }
        Fixture::PreviousWithoutCurrent => corrupt(
            &conn,
            "UPDATE sandboxes
                SET previous_node_execution_id = node_execution_id,
                    node_execution_id = NULL
              WHERE id = ?1",
            &main_text,
        ),
    }
    Seeded {
        conn,
        main,
        main_ids: vec![main_text],
        cleanup_id: episode.id,
        claim,
        suspended,
        pin,
        restore,
        scope,
        attempt,
        recovering,
        claimed,
        orphan,
        paused,
        paused_id,
        restore_scope_only,
        scope_only,
    }
}

/// Apply one defect, which must touch exactly one row.
fn corrupt(conn: &Connection, statement: &str, id: &str) {
    assert_eq!(
        conn.execute(statement, params![id]).unwrap(),
        1,
        "{statement}"
    );
}

type Probe = fn(&Seeded) -> Answer;

struct Case {
    accessor: &'static str,
    view: Probe,
    old: Probe,
    /// One answer per entry of [`FIXTURES`].
    expected: [Answer; 4],
}

fn now() -> i64 {
    crate::db::now_epoch_i64().unwrap()
}

impl Seeded {
    fn main_text(&self) -> &str {
        &self.main_ids[0]
    }

    fn main(&self) -> SandboxView<'_> {
        sandbox_view(&self.conn, self.main_text())
    }

    fn typed(&self, sandbox_id: SandboxId) -> TypedSandboxView<'_> {
        typed_sandbox_view(&self.conn, sandbox_id)
    }

    fn view<'s>(&'s self, id: &'s str) -> SandboxView<'s> {
        sandbox_view(&self.conn, id)
    }
}

type Labels<'a> =
    crate::db::Result<Vec<(&'a String, crate::db::guest_health_scan::StateLabelRead)>>;

/// The old batch reader keyed one id; the view's answer for that id.
fn first_label(result: Labels<'_>) -> Answer {
    match result {
        Ok(labels) => match labels.into_iter().next() {
            Some((_, label)) => opt(label),
            None => N,
        },
        Err(_) => E,
    }
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            accessor: "current_state_label",
            view: |s| opt(s.main().current_state_label()),
            old: |s| opt(crate::db::current_state_label(&s.conn, s.main_text())),
            expected: [S, S, N, S],
        },
        Case {
            accessor: "runtime_execution_generation",
            view: |s| opt(s.main().runtime_execution_generation()),
            old: |s| {
                opt(crate::db::runtime_execution_generation(
                    &s.conn,
                    s.main_text(),
                ))
            },
            expected: [S, S, N, S],
        },
        Case {
            accessor: "node_execution_fence",
            view: |s| opt(s.main().node_execution_fence()),
            old: |s| opt(crate::db::node_execution_fence(&s.conn, s.main_text())),
            expected: [S, S, E, E],
        },
        Case {
            accessor: "cleanup_target",
            view: |s| opt(s.main().cleanup_target()),
            old: |s| opt(crate::db::cleanup_target(&s.conn, s.main_text())),
            expected: [S, S, N, S],
        },
        Case {
            accessor: "cleanup_runtime_label",
            view: |s| opt(s.main().cleanup_runtime_label()),
            old: |s| opt(crate::db::cleanup_runtime_label(&s.conn, s.main_text())),
            expected: [S, S, N, S],
        },
        Case {
            accessor: "cleanup_intent_current",
            view: |s| {
                flag(
                    s.main()
                        .cleanup_intent_current(CleanupCompletion::MarkStopped),
                )
            },
            old: |s| {
                flag(crate::db::cleanup_intent_current(
                    &s.conn,
                    s.main_text(),
                    CleanupCompletion::MarkStopped,
                ))
            },
            expected: [T, T, F, T],
        },
        Case {
            accessor: "activation_route_provenance",
            view: |s| value(s.main().activation_route_provenance()),
            old: |s| {
                value(crate::db::activation_route_provenance(
                    &s.conn,
                    s.main_text(),
                ))
            },
            expected: [S, S, E, S],
        },
        Case {
            accessor: "exists",
            view: |s| flag(s.main().exists()),
            old: |s| flag(crate::db::sandbox_exists(&s.conn, s.main_text())),
            expected: [T, T, F, T],
        },
        Case {
            accessor: "suspended_with_pinned_artifact",
            view: |s| {
                flag(
                    s.view(&s.suspended)
                        .suspended_with_pinned_artifact(&s.pin.to_string()),
                )
            },
            old: |s| {
                flag(crate::db::sandbox_suspended_with_pinned_artifact(
                    &s.conn,
                    &s.suspended,
                    &s.pin.to_string(),
                ))
            },
            expected: [T, T, F, T],
        },
        Case {
            accessor: "state_labels",
            view: |s| first_label(sandbox_views(&s.conn, &s.main_ids).state_labels()),
            old: |s| {
                first_label(crate::db::guest_health_scan::state_labels_for_ids(
                    &s.conn,
                    &s.main_ids,
                ))
            },
            expected: [S, S, N, S],
        },
        Case {
            accessor: "cleanup_episode.present",
            view: |s| flag(s.main().cleanup_episode().present()),
            old: |s| flag(crate::db::has_cleanup_episode(&s.conn, s.main_text())),
            expected: [T, T, F, T],
        },
        Case {
            accessor: "cleanup_episode.strict",
            view: |s| opt(s.main().cleanup_episode().strict()),
            old: |s| opt(crate::db::cleanup_episode(&s.conn, s.main_text())),
            expected: [S, E, N, S],
        },
        Case {
            accessor: "cleanup_episode.decoded",
            view: |s| decoded(s.main().cleanup_episode().decoded()),
            // Presence from the EXISTS probe, the record from the strict read.
            old: |s| match crate::db::has_cleanup_episode(&s.conn, s.main_text()) {
                Ok(false) => N,
                Ok(true) => match crate::db::cleanup_episode(&s.conn, s.main_text()) {
                    Ok(Some(_)) => S,
                    Ok(None) => N,
                    Err(_) => U,
                },
                Err(_) => E,
            },
            expected: [S, U, N, S],
        },
        Case {
            accessor: "cleanup_episode.decoded.is_present",
            view: |s| flag(s.main().cleanup_episode().decoded().map(|d| d.is_present())),
            old: |s| flag(crate::db::has_cleanup_episode(&s.conn, s.main_text())),
            expected: [T, T, F, T],
        },
        Case {
            accessor: "cleanup_episode.decoded.into_strict",
            view: |s| {
                opt(s
                    .main()
                    .cleanup_episode()
                    .decoded()
                    .and_then(Decoded::into_strict))
            },
            old: |s| opt(crate::db::cleanup_episode(&s.conn, s.main_text())),
            expected: [S, E, N, S],
        },
        Case {
            accessor: "cleanup_episode.current",
            view: |s| flag(s.main().cleanup_episode().current(s.cleanup_id)),
            old: |s| {
                flag(crate::db::cleanup_episode_current(
                    &s.conn,
                    s.main_text(),
                    s.cleanup_id,
                ))
            },
            expected: [T, T, F, T],
        },
        Case {
            accessor: "restore_scope.scope",
            view: |s| opt(s.view(&s.restore).restore_scope().scope()),
            old: |s| opt(crate::db::restore_transfer_scope(&s.conn, &s.restore)),
            expected: [S, E, S, S],
        },
        Case {
            accessor: "restore_scope.reclaim_attempt_is_current",
            view: |s| {
                flag(
                    s.view(&s.restore)
                        .restore_scope()
                        .reclaim_attempt_is_current(s.scope, generation(), s.attempt),
                )
            },
            old: |s| {
                flag(crate::db::canonical_restore_reclaim_attempt_is_current(
                    &s.conn,
                    &s.restore,
                    s.scope,
                    generation(),
                    s.attempt,
                ))
            },
            // Strict on the row, which it decodes before the scope.
            expected: [T, E, F, T],
        },
        Case {
            accessor: "restore_scope.reclaim_attempt_is_current (row decodes)",
            view: |s| {
                flag(
                    s.view(&s.restore_scope_only)
                        .restore_scope()
                        .reclaim_attempt_is_current(s.scope_only, generation(), s.attempt),
                )
            },
            old: |s| {
                flag(crate::db::canonical_restore_reclaim_attempt_is_current(
                    &s.conn,
                    &s.restore_scope_only,
                    s.scope_only,
                    generation(),
                    s.attempt,
                ))
            },
            // On CorruptJson only the scope fails to decode.
            expected: [T, E, F, T],
        },
        Case {
            accessor: "delete_receipts.pending",
            view: |s| opt(s.typed(s.main).delete_receipts().pending()),
            old: |s| {
                opt(
                    crate::db::auto_lifecycle_delete_receipts::pending_receipt_for_sandbox(
                        &s.conn, s.main,
                    ),
                )
            },
            expected: [S, S, N, S],
        },
        Case {
            accessor: "delete_receipts.pending_matches_observed_execution",
            view: |s| {
                flag(
                    s.typed(s.main)
                        .delete_receipts()
                        .pending_matches_observed_execution(),
                )
            },
            old: |s| {
                flag(
                    crate::db::auto_lifecycle_delete_receipts::pending_receipt_matches_observed_execution(
                        &s.conn, s.main,
                    ),
                )
            },
            expected: [T, T, T, T],
        },
        Case {
            accessor: "delete_receipts.pending_matches_observed_execution (older receipt)",
            view: |s| {
                flag(
                    s.typed(s.paused_id)
                        .delete_receipts()
                        .pending_matches_observed_execution(),
                )
            },
            old: |s| {
                flag(
                    crate::db::auto_lifecycle_delete_receipts::pending_receipt_matches_observed_execution(
                        &s.conn, s.paused_id,
                    ),
                )
            },
            // The row's generation has moved past the receipt's; without the
            // row there is nothing to disagree with.
            expected: [F, F, T, F],
        },
        Case {
            accessor: "delete_receipts.execution_covers",
            view: |s| {
                flag(
                    s.main()
                        .delete_receipts()
                        .execution_covers(generation(), now()),
                )
            },
            old: |s| {
                flag(crate::db::execution_delete_receipt_covers(
                    &s.conn,
                    s.main_text(),
                    generation(),
                ))
            },
            expected: [T, T, T, T],
        },
        Case {
            accessor: "delete_receipts.execution_covers (expired receipt)",
            view: |s| {
                flag(
                    s.view(&s.paused)
                        .delete_receipts()
                        .execution_covers(generation(), now()),
                )
            },
            old: |s| {
                flag(crate::db::execution_delete_receipt_covers(
                    &s.conn,
                    &s.paused,
                    generation(),
                ))
            },
            expected: [F, F, F, F],
        },
        Case {
            accessor: "teardown.latest",
            view: |s| opt(s.main().teardown().latest()),
            old: |s| {
                opt(crate::db::latest_teardown_suspend_operation(
                    &s.conn,
                    s.main_text(),
                ))
            },
            expected: [S, E, S, S],
        },
        Case {
            accessor: "teardown.status",
            view: |s| opt(s.main().teardown().status()),
            old: |s| opt(crate::db::teardown_suspend_status(&s.conn, s.main_text())),
            expected: [S, E, N, S],
        },
        Case {
            accessor: "teardown.running_for",
            view: |s| flag(s.main().teardown().running_for(generation())),
            old: |s| {
                flag(crate::db::teardown_suspend_operation_running_for(
                    &s.conn,
                    s.main_text(),
                    generation(),
                ))
            },
            expected: [T, E, T, T],
        },
        Case {
            accessor: "teardown.release_evidence",
            view: |s| value(s.main().teardown().release_evidence(generation())),
            old: |s| {
                value(crate::db::teardown_release_evidence(
                    &s.conn,
                    s.main_text(),
                    generation(),
                ))
            },
            expected: [S, E, S, S],
        },
        Case {
            accessor: "teardown.retry_due",
            view: |s| flag(s.main().teardown().retry_due(generation())),
            old: |s| {
                flag(crate::db::teardown_suspend_retry_due(
                    &s.conn,
                    s.main_text(),
                    generation(),
                ))
            },
            expected: [T, E, T, T],
        },
        Case {
            accessor: "teardown.retry_admissible",
            view: |s| flag(s.view(&s.paused).teardown().retry_admissible(generation())),
            old: |s| {
                flag(crate::db::teardown_suspend_retry_admissible(
                    &s.conn,
                    &s.paused,
                    generation(),
                ))
            },
            expected: [T, T, F, T],
        },
        Case {
            accessor: "quarantine.is_orphan_recovery_state",
            view: |s| {
                flag(
                    s.view(&s.orphan)
                        .quarantine()
                        .is_orphan_recovery_state(SandboxState::Quarantined),
                )
            },
            old: |s| {
                flag(crate::db::is_orphan_quarantine_recovery_state(
                    &s.conn,
                    &s.orphan,
                    SandboxState::Quarantined,
                ))
            },
            expected: [T, F, F, T],
        },
        Case {
            accessor: "quarantine.claim_is_current",
            view: |s| {
                flag(
                    s.view(&s.recovering)
                        .quarantine()
                        .claim_is_current(&s.claimed),
                )
            },
            old: |s| {
                flag(crate::db::quarantine_recovery_claim_is_current(
                    &s.conn, &s.claimed,
                ))
            },
            expected: [T, F, F, T],
        },
        Case {
            accessor: "route_claim.matches",
            view: |s| flag(s.main().route_claim().matches(&s.claim)),
            old: |s| {
                flag(crate::db::route_activation_claims::claim_matches(
                    &s.conn,
                    s.main_text(),
                    &s.claim,
                ))
            },
            expected: [T, T, F, T],
        },
        Case {
            accessor: "pinned_artifact.pin_and_updated_at",
            view: |s| opt(s.view(&s.suspended).pinned_artifact().pin_and_updated_at()),
            old: |s| {
                opt(crate::db::eviction_scan::artifact_pin_and_updated_at(
                    &s.conn,
                    &s.suspended,
                ))
            },
            expected: [S, S, N, S],
        },
        Case {
            accessor: "pinned_artifact.owned_evidence",
            view: |s| {
                opt(s
                    .view(&s.suspended)
                    .pinned_artifact()
                    .owned_evidence(&s.pin.to_string()))
            },
            old: |s| {
                opt(crate::db::eviction_scan::owned_artifact_evidence(
                    &s.conn,
                    &s.pin.to_string(),
                    &s.suspended,
                ))
            },
            expected: [S, S, S, S],
        },
        Case {
            accessor: "latest_upload",
            view: |s| opt(s.view(&s.suspended).latest_upload()),
            old: |s| {
                opt(crate::db::eviction_scan::latest_upload_job(
                    &s.conn,
                    &s.suspended,
                ))
            },
            expected: [S, S, S, S],
        },
        Case {
            accessor: "artifacts.named",
            view: |s| list(s.view(&s.suspended).artifacts().named()),
            old: |s| {
                list(crate::db::local_artifact_leases::list_named_publications(
                    &s.conn,
                    &s.suspended,
                ))
            },
            expected: [S, S, S, S],
        },
        Case {
            accessor: "artifacts.others_awaiting_backup",
            view: |s| {
                list(
                    s.view(&s.suspended)
                        .artifacts()
                        .others_awaiting_backup(s.pin),
                )
            },
            old: |s| {
                list(crate::db::other_local_artifacts_awaiting_backup(
                    &s.conn,
                    &s.suspended,
                    s.pin,
                ))
            },
            expected: [S, S, S, S],
        },
    ]
}

/// Each narrow accessor keeps its old reader's missing-row and decode rule.
#[test]
fn every_view_accessor_answers_like_its_old_reader() {
    let cases = cases();
    for (index, fixture) in FIXTURES.into_iter().enumerate() {
        let seeded = seed(fixture);
        for case in &cases {
            let expected = case.expected[index];
            assert_eq!(
                (case.view)(&seeded),
                expected,
                "{} on {fixture:?}",
                case.accessor
            );
            assert_eq!(
                (case.old)(&seeded),
                expected,
                "old reader of {} on {fixture:?}",
                case.accessor
            );
        }
    }
}

/// The smallest byte allowance under which `read` succeeds. Allowances above
/// it succeed too, because a budget only ever subtracts.
fn bytes_needed(read: impl Fn(InventoryBudget) -> bool) -> usize {
    let (mut low, mut high) = (0, MAX_RAW_BYTES);
    assert!(read(InventoryBudget::new(MAX_ITEMS, high)));
    while low < high {
        let middle = low + (high - low) / 2;
        if read(InventoryBudget::new(MAX_ITEMS, middle)) {
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    low
}

/// The smallest item allowance under which `read` succeeds.
fn items_needed(read: impl Fn(InventoryBudget) -> bool) -> usize {
    (0..=MAX_ITEMS)
        .find(|&items| read(InventoryBudget::new(items, MAX_RAW_BYTES)))
        .unwrap()
}

fn unlimited() -> InventoryBudget {
    InventoryBudget::new(MAX_ITEMS, MAX_RAW_BYTES)
}

fn state_page_with(
    conn: &Connection,
    ids: &[String],
    raw: InventoryBudget,
    metadata: InventoryBudget,
) -> bool {
    let mut budgets = StateBudgets { raw, metadata };
    match state_page(conn, ids, &mut budgets) {
        Ok(_) => true,
        Err(StatePageError::Metadata(InventoryReadError::Limit(_)))
        | Err(StatePageError::Row(InventoryReadError::Limit(_))) => false,
        Err(error) => panic!("state page failed outside its budgets: {error}"),
    }
}

/// The State RPC's two budgets as they were charged before `state_page`:
/// the metadata preflight, then each row's whole-row read.
fn old_metadata_preflight(conn: &Connection, ids: &[String], mut budget: InventoryBudget) -> bool {
    match crate::db::inventory_read::check_state_metadata(conn, ids, &mut budget) {
        Ok(()) => true,
        Err(InventoryReadError::Limit(_)) => false,
        Err(error) => panic!("metadata preflight failed outside its budget: {error}"),
    }
}

fn old_row_reads(conn: &Connection, ids: &[String], mut budget: InventoryBudget) -> bool {
    for id in ids {
        match crate::db::inventory_read::get(conn, id, &mut budget) {
            Ok(_) | Err(InventoryReadError::Database(_)) => {}
            Err(InventoryReadError::Limit(_)) => return false,
            Err(error) => panic!("row read failed outside its budget: {error}"),
        }
    }
    true
}

/// Every existing row is charged to both budgets, a row that does not decode
/// included; a missing id is charged to neither.
#[test]
fn state_page_charges_both_budgets_exactly_as_the_state_rpc_did() {
    let conn = test_conn();
    let decodable = insert_row(&conn, SandboxState::Stopped);
    let quarantined = insert_row(&conn, SandboxState::Stopped);
    crate::db::mark_quarantined(
        &conn,
        &quarantined,
        crate::db::SandboxQuarantineSource::SbxManager,
        SandboxQuarantineReasonCode::RuntimeProbeInconsistent,
        "seeded",
        crate::db::SandboxQuarantineNextAction::RequiresOperatorAttention,
    )
    .unwrap();
    let undecodable = insert_row(&conn, SandboxState::Stopped);
    conn.execute(
        "UPDATE sandboxes SET process_env_json = 'malformed' WHERE id = ?1",
        params![undecodable],
    )
    .unwrap();
    let ids = vec![
        decodable,
        SandboxId::new().to_string(),
        quarantined,
        undecodable,
    ];

    let metadata = bytes_needed(|budget| state_page_with(&conn, &ids, unlimited(), budget));
    assert_eq!(
        metadata,
        bytes_needed(|budget| old_metadata_preflight(&conn, &ids, budget))
    );
    let raw = bytes_needed(|budget| state_page_with(&conn, &ids, budget, unlimited()));
    assert_eq!(
        raw,
        bytes_needed(|budget| old_row_reads(&conn, &ids, budget))
    );
    assert!(
        metadata > 0 && raw > metadata,
        "metadata {metadata}, raw {raw}"
    );

    assert_eq!(
        items_needed(|budget| state_page_with(&conn, &ids, unlimited(), budget)),
        3
    );
    assert_eq!(
        items_needed(|budget| state_page_with(&conn, &ids, budget, unlimited())),
        3
    );
    // The metadata preflight is charged first: with both budgets too small,
    // the page fails before any row is read.
    let mut both_short = StateBudgets {
        raw: InventoryBudget::new(MAX_ITEMS, 1),
        metadata: InventoryBudget::new(MAX_ITEMS, 1),
    };
    assert!(matches!(
        state_page(&conn, &ids, &mut both_short),
        Err(StatePageError::Metadata(InventoryReadError::Limit(_)))
    ));
}
