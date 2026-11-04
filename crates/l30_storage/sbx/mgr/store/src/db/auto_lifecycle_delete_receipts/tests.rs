use std::collections::BTreeMap;

use crate::db::{Connection, OptionalExtension};
use pplx_space_common_core::types::{
    AutoLifecycleConditionSet, AutoLifecycleConditionSetId, AutoLifecycleDeleteReceiptId,
    AutoLifecycleIdleAction, AutoLifecyclePolicy, AutoLifecycleStage, AutoLifecycleWakePolicy,
    ExecutionGeneration, NodeManagedBy, NodeRole, PolicyAuthority, RuntimeKind,
    SandboxActivationAttemptId, SandboxId, SandboxOwner, SandboxRuntimeLabel,
    SpaceApiRouteIdentity,
};
use pplx_space_common_svc::types::AutoLifecyclePolicyFingerprint;

use super::*;
use crate::auto_lifecycle_delete_receipts::*;

fn delete_policy() -> AutoLifecyclePolicy {
    AutoLifecyclePolicy {
        wake: AutoLifecycleWakePolicy::default(),
        condition_sets: [(
            AutoLifecycleConditionSetId::AccessIdle,
            AutoLifecycleConditionSet::access_idle(),
        )]
        .into(),
        stages: vec![AutoLifecycleStage {
            condition_set: AutoLifecycleConditionSetId::AccessIdle,
            at: 900,
            action: AutoLifecycleIdleAction::Delete,
        }],
    }
}

fn insert_sandbox(conn: &Connection, sandbox_id: SandboxId) -> AutoLifecycleDeleteExpectation {
    let owner = SandboxOwner::parse("user").unwrap();
    let runtime_label = SandboxRuntimeLabel::from_sandbox_id(sandbox_id);
    let policy = delete_policy();
    crate::db::insert_sandbox(
        conn,
        crate::db::InsertSandboxParams {
            guest_image: Default::default(),
            id: &sandbox_id.to_string(),
            uuid8: &runtime_label,
            state: crate::state::SandboxState::Started,
            template: "seed",
            template_hash: Some("hash"),
            user: &owner,
            runtime: RuntimeKind::Firecracker.as_str(),
            snapshots_enabled: false,
            rootfs_size_mb: None,
            disk_io_limits: Default::default(),
            pool_template: None,
            volume_encryption: false,
            process_env: &BTreeMap::new(),
            auto_lifecycle: &policy,
            network_policy_override: None,
            network_policy_generation: 0,
            network_policy_authority: PolicyAuthority::Cluster,
            auto_lifecycle_authority: PolicyAuthority::Cluster,
        },
    )
    .unwrap();
    AutoLifecycleDeleteExpectation {
        policy_fingerprint: AutoLifecyclePolicyFingerprint::for_policy(&policy),
        authority: PolicyAuthority::Cluster,
    }
}

fn cluster_route(
    generation: i64,
    activation_attempt_id: SandboxActivationAttemptId,
) -> SpaceApiRouteIdentity {
    SpaceApiRouteIdentity {
        execution_generation: Some(ExecutionGeneration::new(generation).unwrap()),
        activation_attempt_id: Some(activation_attempt_id),
        substrate_kind: Some(NodeManagedBy::Kubernetes),
        ..SpaceApiRouteIdentity::default()
    }
}

fn receipt(
    sandbox_id: SandboxId,
    expectation: AutoLifecycleDeleteExpectation,
    generation: i64,
    activation_attempt_id: SandboxActivationAttemptId,
) -> AutoLifecycleDeleteReceipt {
    AutoLifecycleDeleteReceipt {
        receipt_id: AutoLifecycleDeleteReceiptId::generate(),
        sandbox_id,
        execution_generation: ExecutionGeneration::new(generation).unwrap(),
        route_activation_id: activation_attempt_id,
        policy_fingerprint: expectation.policy_fingerprint,
    }
}

#[test]
fn delete_admission_requires_consistent_authority_and_execution_evidence() {
    use AutoLifecycleDeleteBlockReason as Block;
    use AutoLifecycleDeleteOrdinaryReason as Ordinary;

    #[derive(Clone, Copy, Debug)]
    enum Expected {
        Ordinary(Ordinary),
        Receipt,
        Blocked(Block),
    }
    let cases = [
        (
            "local",
            Some(NodeManagedBy::Space),
            PolicyAuthority::Local,
            "UPDATE sandboxes SET space_api_route_substrate_kind = 'k8s' WHERE id = ?3",
            Expected::Ordinary(Ordinary::LocalAuthority),
        ),
        (
            "route-less direct",
            Some(NodeManagedBy::Space),
            PolicyAuthority::Cluster,
            "UPDATE sandboxes SET state = 'stopped' WHERE id = ?3",
            Expected::Ordinary(Ordinary::DirectUnsupported),
        ),
        (
            "explicit direct",
            None,
            PolicyAuthority::Cluster,
            "UPDATE sandboxes SET space_api_route_substrate_kind = 'space' WHERE id = ?3",
            Expected::Ordinary(Ordinary::DirectUnsupported),
        ),
        (
            "current",
            Some(NodeManagedBy::Kubernetes),
            PolicyAuthority::Cluster,
            "UPDATE sandboxes SET space_api_route_execution_generation = 7, space_api_route_activation_attempt_id = ?1, space_api_route_substrate_kind = 'k8s', space_api_route_identity_provenance = 'cluster_fenced' WHERE id = ?3",
            Expected::Receipt,
        ),
        (
            "retained",
            Some(NodeManagedBy::Kubernetes),
            PolicyAuthority::Cluster,
            "UPDATE sandboxes SET last_observed_execution_generation = 7, last_observed_route_activation_id = ?1, last_observed_route_substrate_kind = 'k8s', last_observed_route_identity_provenance = 'cluster_fenced' WHERE id = ?3",
            Expected::Receipt,
        ),
        (
            "suspended subset",
            Some(NodeManagedBy::Kubernetes),
            PolicyAuthority::Cluster,
            "UPDATE sandboxes SET state = 'suspended', space_api_route_execution_generation = 7, last_observed_execution_generation = 7, last_observed_route_activation_id = ?1, last_observed_route_substrate_kind = 'k8s', last_observed_route_identity_provenance = 'cluster_fenced' WHERE id = ?3",
            Expected::Receipt,
        ),
        (
            "missing Kubernetes",
            Some(NodeManagedBy::Kubernetes),
            PolicyAuthority::Cluster,
            "UPDATE sandboxes SET state = 'stopped' WHERE id = ?3",
            Expected::Blocked(Block::MissingRouteIdentity),
        ),
        (
            "missing unknown host",
            None,
            PolicyAuthority::Cluster,
            "UPDATE sandboxes SET state = 'stopped' WHERE id = ?3",
            Expected::Blocked(Block::MissingRouteIdentity),
        ),
        (
            "stale retained",
            Some(NodeManagedBy::Kubernetes),
            PolicyAuthority::Cluster,
            "UPDATE sandboxes SET space_api_route_execution_generation = 8, space_api_route_activation_attempt_id = ?1, space_api_route_substrate_kind = 'k8s', space_api_route_identity_provenance = 'cluster_fenced', last_observed_execution_generation = 7, last_observed_route_activation_id = ?1, last_observed_route_substrate_kind = 'k8s', last_observed_route_identity_provenance = 'cluster_fenced' WHERE id = ?3",
            Expected::Blocked(Block::ConflictingRouteIdentity),
        ),
        (
            "conflicting attempts",
            Some(NodeManagedBy::Kubernetes),
            PolicyAuthority::Cluster,
            "UPDATE sandboxes SET space_api_route_execution_generation = 7, space_api_route_activation_attempt_id = ?1, space_api_route_substrate_kind = 'k8s', space_api_route_identity_provenance = 'cluster_fenced', last_observed_execution_generation = 7, last_observed_route_activation_id = ?2, last_observed_route_substrate_kind = 'k8s', last_observed_route_identity_provenance = 'cluster_fenced' WHERE id = ?3",
            Expected::Blocked(Block::ConflictingRouteIdentity),
        ),
        (
            "host contradicts route",
            Some(NodeManagedBy::Space),
            PolicyAuthority::Cluster,
            "UPDATE sandboxes SET space_api_route_substrate_kind = 'k8s' WHERE id = ?3",
            Expected::Blocked(Block::ConflictingRouteIdentity),
        ),
        (
            "malformed current attempt",
            Some(NodeManagedBy::Kubernetes),
            PolicyAuthority::Cluster,
            "UPDATE sandboxes SET space_api_route_execution_generation = 7, space_api_route_activation_attempt_id = 'malformed', space_api_route_substrate_kind = 'k8s', space_api_route_identity_provenance = 'cluster_fenced' WHERE id = ?3",
            Expected::Blocked(Block::ConflictingRouteIdentity),
        ),
        (
            "malformed retained attempt",
            Some(NodeManagedBy::Kubernetes),
            PolicyAuthority::Cluster,
            "UPDATE sandboxes SET space_api_route_execution_generation = 7, space_api_route_activation_attempt_id = ?1, space_api_route_substrate_kind = 'k8s', space_api_route_identity_provenance = 'cluster_fenced', last_observed_execution_generation = 7, last_observed_route_activation_id = 'malformed', last_observed_route_substrate_kind = 'k8s', last_observed_route_identity_provenance = 'cluster_fenced' WHERE id = ?3",
            Expected::Blocked(Block::ConflictingRouteIdentity),
        ),
        (
            "invalid current generation",
            Some(NodeManagedBy::Kubernetes),
            PolicyAuthority::Cluster,
            "UPDATE sandboxes SET space_api_route_execution_generation = -1, space_api_route_activation_attempt_id = ?1, space_api_route_substrate_kind = 'k8s', space_api_route_identity_provenance = 'cluster_fenced' WHERE id = ?3",
            Expected::Blocked(Block::ConflictingRouteIdentity),
        ),
        (
            "invalid retained generation",
            Some(NodeManagedBy::Kubernetes),
            PolicyAuthority::Cluster,
            "UPDATE sandboxes SET last_observed_execution_generation = 0, last_observed_route_activation_id = ?1, last_observed_route_substrate_kind = 'k8s', last_observed_route_identity_provenance = 'cluster_fenced' WHERE id = ?3",
            Expected::Blocked(Block::ConflictingRouteIdentity),
        ),
        (
            "mixed substrates",
            None,
            PolicyAuthority::Cluster,
            "UPDATE sandboxes SET space_api_route_substrate_kind = 'space', last_observed_route_substrate_kind = 'k8s' WHERE id = ?3",
            Expected::Blocked(Block::ConflictingRouteIdentity),
        ),
        (
            "unknown substrate",
            Some(NodeManagedBy::Space),
            PolicyAuthority::Cluster,
            "UPDATE sandboxes SET last_observed_route_substrate_kind = 'future' WHERE id = ?3",
            Expected::Blocked(Block::UnknownSubstrate),
        ),
        (
            "partial retained",
            Some(NodeManagedBy::Kubernetes),
            PolicyAuthority::Cluster,
            "UPDATE sandboxes SET space_api_route_execution_generation = 7, space_api_route_activation_attempt_id = ?1, space_api_route_substrate_kind = 'k8s', space_api_route_identity_provenance = 'cluster_fenced', last_observed_execution_generation = 7 WHERE id = ?3",
            Expected::Blocked(Block::ConflictingRouteIdentity),
        ),
        (
            "pod contradicts direct",
            Some(NodeManagedBy::Space),
            PolicyAuthority::Cluster,
            "UPDATE sandboxes SET space_api_route_pod_uid = 'pod' WHERE id = ?3",
            Expected::Blocked(Block::ConflictingRouteIdentity),
        ),
        (
            "retained placement contradicts direct",
            Some(NodeManagedBy::Space),
            PolicyAuthority::Cluster,
            "UPDATE sandboxes SET last_observed_route_generation_substrate_kind = 'k8s' WHERE id = ?3",
            Expected::Blocked(Block::ConflictingRouteIdentity),
        ),
    ];
    for (name, host, authority, setup, expected) in cases {
        let conn = crate::db::test_conn();
        let id = SandboxId::new();
        let mut expectation = insert_sandbox(&conn, id);
        expectation.authority = authority;
        let attempt = SandboxActivationAttemptId::new();
        // Seed historical malformed evidence without weakening admission writes.
        conn.pragma_update(None, "ignore_check_constraints", true)
            .unwrap();
        conn.execute(
            setup,
            rusqlite::params![
                attempt.to_string(),
                SandboxActivationAttemptId::new().to_string(),
                id.to_string()
            ],
        )
        .unwrap();
        conn.pragma_update(None, "ignore_check_constraints", false)
            .unwrap();
        conn.execute(
            "UPDATE sandboxes SET auto_lifecycle_authority = ?1 WHERE id = ?2",
            rusqlite::params![authority.to_string(), id.to_string()],
        )
        .unwrap();
        let admitted =
            classify_and_record_admission(&conn, id, Some(NodeRole::Worker), host, expectation)
                .unwrap();
        match (expected, admitted) {
            (Expected::Ordinary(expected), AutoLifecycleDeleteAdmission::Ordinary(actual)) => {
                assert_eq!(actual, expected, "{name}")
            }
            (Expected::Receipt, AutoLifecycleDeleteAdmission::Receipt(evidence)) => {
                assert_eq!(evidence.execution_generation.get(), 7, "{name}");
                assert_eq!(evidence.activation_attempt_id, attempt, "{name}");
            }
            (Expected::Blocked(expected), AutoLifecycleDeleteAdmission::Blocked { reason, .. }) => {
                assert_eq!(reason, expected, "{name}");
                let retry: i64 = conn
                    .query_row(
                        "SELECT next_auto_event_at FROM sandboxes WHERE id = ?1",
                        [id.to_string()],
                        |row| row.get(0),
                    )
                    .unwrap();
                assert!(retry > crate::db::now_epoch_i64().unwrap(), "{name}");
                assert!(crate::db::sandbox_exists(&conn, &id.to_string()).unwrap());
            }
            pair => panic!("unexpected {name}: {pair:?}"),
        }
    }
    for completed in [false, true] {
        let conn = crate::db::test_conn();
        let id = SandboxId::new();
        let expectation = insert_sandbox(&conn, id);
        let pending = receipt(id, expectation, 7, SandboxActivationAttemptId::new());
        insert_pending_receipt(&conn, pending).unwrap();
        if completed {
            complete_pending_receipt(&conn, id, pending.receipt_id).unwrap();
        }
        let admitted = classify_and_record_admission(
            &conn,
            id,
            Some(NodeRole::Worker),
            Some(NodeManagedBy::Space),
            expectation,
        )
        .unwrap();
        if completed {
            assert_eq!(
                admitted,
                AutoLifecycleDeleteAdmission::Ordinary(Ordinary::DirectUnsupported)
            );
        } else {
            assert!(matches!(
                admitted,
                AutoLifecycleDeleteAdmission::Blocked {
                    reason: Block::ConflictingRouteIdentity,
                    ..
                }
            ));
        }
        assert_eq!(
            conn.query_row(
                "SELECT receipt_id, state FROM auto_lifecycle_delete_outbox WHERE sandbox_id = ?1",
                [id.to_string()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .unwrap(),
            (
                pending.receipt_id.to_string(),
                if completed { "completed" } else { "pending" }.into()
            )
        );
    }
}

#[test]
fn strict_admission_requires_complete_current_cluster_fencing() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let expectation = insert_sandbox(&conn, sandbox_id);
    let activation_attempt_id = SandboxActivationAttemptId::new();
    crate::db::update_space_api_route_identity(
        &conn,
        sandbox_id,
        &cluster_route(7, activation_attempt_id),
        RouteIdentityProvenance::ClusterFenced,
    )
    .unwrap();

    assert!(matches!(
        classify_and_record_admission(
            &conn,
            sandbox_id,
            Some(NodeRole::Worker),
            None, expectation,
        )
        .unwrap(),
        AutoLifecycleDeleteAdmission::Receipt(RouteEvidence {
            execution_generation,
            activation_attempt_id: observed_attempt,
            ..
        }) if execution_generation == ExecutionGeneration::new(7).unwrap()
            && observed_attempt == activation_attempt_id
    ));

    conn.execute(
        "UPDATE sandboxes
            SET space_api_route_execution_generation = NULL,
                space_api_route_activation_attempt_id = NULL,
                space_api_route_substrate_kind = NULL,
                space_api_route_identity_provenance = NULL
          WHERE id = ?1",
        [sandbox_id.to_string()],
    )
    .unwrap();
    assert!(matches!(
        classify_and_record_admission(&conn, sandbox_id, Some(NodeRole::Worker), None, expectation,)
            .unwrap(),
        AutoLifecycleDeleteAdmission::Receipt(RouteEvidence {
            execution_generation,
            activation_attempt_id: observed_attempt,
            ..
        }) if execution_generation == ExecutionGeneration::new(7).unwrap()
            && observed_attempt == activation_attempt_id
    ));

    conn.execute(
        "UPDATE sandboxes
            SET space_api_route_execution_generation = 8,
                space_api_route_activation_attempt_id = NULL,
                space_api_route_substrate_kind = NULL,
                space_api_route_identity_provenance = NULL
          WHERE id = ?1",
        [sandbox_id.to_string()],
    )
    .unwrap();
    assert!(matches!(
        classify_and_record_admission(
            &conn,
            sandbox_id,
            Some(NodeRole::Worker),
            None,
            expectation,
        )
        .unwrap(),
        AutoLifecycleDeleteAdmission::Blocked {
            reason: AutoLifecycleDeleteBlockReason::MissingSubstrate,
            changed: true,
        }
    ));
    let (reason, blocked_at, next_auto_event_at, runtime_state_json): (
        Option<String>,
        Option<String>,
        Option<i64>,
        String,
    ) = conn
        .query_row(
            "SELECT auto_lifecycle_delete_block_reason,
                    auto_lifecycle_delete_blocked_at,
                    next_auto_event_at,
                    auto_lifecycle_state_json
               FROM sandboxes WHERE id = ?1",
            [sandbox_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(reason.as_deref(), Some("missing_substrate"));
    assert!(blocked_at.is_some());
    assert!(next_auto_event_at.unwrap() > crate::db::now_epoch_i64().unwrap());
    let runtime_state: pplx_space_common_core::types::AutoLifecycleRuntimeState =
        serde_json::from_str(&runtime_state_json).unwrap();
    assert!(
        runtime_state
            .last_auto_error
            .as_deref()
            .is_some_and(|error| error.contains("missing_substrate"))
    );

    crate::db::update_space_api_route_identity(
        &conn,
        sandbox_id,
        &cluster_route(8, SandboxActivationAttemptId::new()),
        RouteIdentityProvenance::ClusterFenced,
    )
    .unwrap();
    let (reason, blocked_at, runtime_state_json): (Option<String>, Option<String>, String) = conn
        .query_row(
            "SELECT auto_lifecycle_delete_block_reason,
                    auto_lifecycle_delete_blocked_at,
                    auto_lifecycle_state_json
               FROM sandboxes WHERE id = ?1",
            [sandbox_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!((reason, blocked_at), (None, None));
    let runtime_state: pplx_space_common_core::types::AutoLifecycleRuntimeState =
        serde_json::from_str(&runtime_state_json).unwrap();
    assert_eq!(runtime_state.last_auto_error, None);
}

#[test]
fn dormant_suspend_identity_uses_matching_retained_route_evidence() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let expectation = insert_sandbox(&conn, sandbox_id);
    let activation_attempt_id = SandboxActivationAttemptId::new();
    crate::db::update_space_api_route_identity(
        &conn,
        sandbox_id,
        &cluster_route(7, activation_attempt_id),
        RouteIdentityProvenance::ClusterFenced,
    )
    .unwrap();

    // Suspend completion retains generation for central publication but clears
    // the remaining current route fields. The matching complete retained tuple
    // is still the exact route that produced this dormant sandbox.
    conn.execute(
        "UPDATE sandboxes
            SET state = 'suspended',
                space_api_route_activation_attempt_id = NULL,
                space_api_route_substrate_kind = NULL,
                space_api_route_identity_provenance = NULL
          WHERE id = ?1",
        [sandbox_id.to_string()],
    )
    .unwrap();

    assert!(matches!(
        classify_and_record_admission(
            &conn,
            sandbox_id,
            Some(NodeRole::Worker),
            None, expectation,
        )
        .unwrap(),
        AutoLifecycleDeleteAdmission::Receipt(RouteEvidence {
            execution_generation,
            activation_attempt_id: observed_attempt,
            ..
        }) if execution_generation == ExecutionGeneration::new(7).unwrap()
            && observed_attempt == activation_attempt_id
    ));
}

#[test]
fn non_dormant_partial_route_does_not_reuse_retained_evidence() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let expectation = insert_sandbox(&conn, sandbox_id);
    let activation_attempt_id = SandboxActivationAttemptId::new();
    crate::db::update_space_api_route_identity(
        &conn,
        sandbox_id,
        &cluster_route(7, activation_attempt_id),
        RouteIdentityProvenance::ClusterFenced,
    )
    .unwrap();

    conn.execute(
        "UPDATE sandboxes
            SET space_api_route_activation_attempt_id = NULL,
                space_api_route_substrate_kind = NULL,
                space_api_route_identity_provenance = NULL
          WHERE id = ?1",
        [sandbox_id.to_string()],
    )
    .unwrap();

    assert!(matches!(
        classify_and_record_admission(
            &conn,
            sandbox_id,
            Some(NodeRole::Worker),
            None,
            expectation,
        )
        .unwrap(),
        AutoLifecycleDeleteAdmission::Blocked {
            reason: AutoLifecycleDeleteBlockReason::MissingSubstrate,
            changed: true,
        }
    ));
}

#[test]
fn contradictory_current_and_retained_routes_block_deletion() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let expectation = insert_sandbox(&conn, sandbox_id);
    let retained_attempt = SandboxActivationAttemptId::new();
    crate::db::update_space_api_route_identity(
        &conn,
        sandbox_id,
        &cluster_route(8, retained_attempt),
        RouteIdentityProvenance::ClusterFenced,
    )
    .unwrap();

    // Model a stale or corrupt current-route writer without updating the
    // retained tuple. Receipt admission must not choose either side of the
    // contradiction and delete local state that central fencing will retain.
    conn.execute(
        "UPDATE sandboxes
            SET space_api_route_execution_generation = 7,
                space_api_route_activation_attempt_id = ?1,
                space_api_route_substrate_kind = 'k8s',
                space_api_route_identity_provenance = 'cluster_fenced'
          WHERE id = ?2",
        rusqlite::params![
            SandboxActivationAttemptId::new().to_string(),
            sandbox_id.to_string()
        ],
    )
    .unwrap();

    assert!(matches!(
        classify_and_record_admission(
            &conn,
            sandbox_id,
            Some(NodeRole::Worker),
            None, expectation,
        )
        .unwrap(),
        AutoLifecycleDeleteAdmission::Blocked { reason, changed: true }
            if reason.as_str() == "conflicting_route_identity"
    ));
}

#[test]
fn later_current_attempt_blocks_receipt_admission() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let expectation = insert_sandbox(&conn, sandbox_id);
    let retained_attempt = SandboxActivationAttemptId::new();
    crate::db::update_space_api_route_identity(
        &conn,
        sandbox_id,
        &cluster_route(8, retained_attempt),
        RouteIdentityProvenance::ClusterFenced,
    )
    .unwrap();

    let current_attempt = SandboxActivationAttemptId::new_after(retained_attempt).unwrap();
    conn.execute(
        "UPDATE sandboxes
            SET space_api_route_activation_attempt_id = ?1
          WHERE id = ?2",
        rusqlite::params![current_attempt.to_string(), sandbox_id.to_string()],
    )
    .unwrap();

    assert!(matches!(
        classify_and_record_admission(
            &conn,
            sandbox_id,
            Some(NodeRole::Worker),
            None, expectation,
        )
        .unwrap(),
        AutoLifecycleDeleteAdmission::Blocked { reason, changed: true }
            if reason == AutoLifecycleDeleteBlockReason::ConflictingRouteIdentity
    ));
}

#[test]
fn delayed_current_attempt_blocks_receipt_admission() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let expectation = insert_sandbox(&conn, sandbox_id);
    let delayed_attempt = SandboxActivationAttemptId::new();
    let retained_attempt = SandboxActivationAttemptId::new_after(delayed_attempt).unwrap();
    crate::db::update_space_api_route_identity(
        &conn,
        sandbox_id,
        &cluster_route(8, retained_attempt),
        RouteIdentityProvenance::ClusterFenced,
    )
    .unwrap();

    conn.execute(
        "UPDATE sandboxes
            SET space_api_route_activation_attempt_id = ?1
          WHERE id = ?2",
        rusqlite::params![delayed_attempt.to_string(), sandbox_id.to_string()],
    )
    .unwrap();

    assert!(matches!(
        classify_and_record_admission(
            &conn,
            sandbox_id,
            Some(NodeRole::Worker),
            None, expectation,
        )
        .unwrap(),
        AutoLifecycleDeleteAdmission::Blocked { reason, changed: true }
            if reason == AutoLifecycleDeleteBlockReason::ConflictingRouteIdentity
    ));
}

#[test]
fn receipt_eligibility_excludes_non_worker_and_non_kubernetes_populations() {
    let conn = crate::db::test_conn();

    let direct_id = SandboxId::new();
    let direct_expectation = insert_sandbox(&conn, direct_id);
    let direct_identity = SpaceApiRouteIdentity {
        execution_generation: Some(ExecutionGeneration::new(1).unwrap()),
        activation_attempt_id: Some(SandboxActivationAttemptId::new()),
        substrate_kind: Some(NodeManagedBy::Space),
        ..SpaceApiRouteIdentity::default()
    };
    crate::db::update_space_api_route_identity(
        &conn,
        direct_id,
        &direct_identity,
        RouteIdentityProvenance::Unfenced,
    )
    .unwrap();
    conn.execute(
        "UPDATE sandboxes
            SET space_api_route_activation_attempt_id = 'not-an-attempt'
          WHERE id = ?1",
        [direct_id.to_string()],
    )
    .unwrap();
    assert_eq!(
        classify_and_record_admission(
            &conn,
            direct_id,
            Some(NodeRole::Worker),
            None,
            direct_expectation,
        )
        .unwrap(),
        AutoLifecycleDeleteAdmission::Ordinary(
            AutoLifecycleDeleteOrdinaryReason::DirectUnsupported,
        )
    );

    let builder_id = SandboxId::new();
    let builder_expectation = insert_sandbox(&conn, builder_id);
    crate::db::update_space_api_route_identity(
        &conn,
        builder_id,
        &cluster_route(2, SandboxActivationAttemptId::new()),
        RouteIdentityProvenance::ClusterFenced,
    )
    .unwrap();
    conn.execute(
        "UPDATE sandboxes
            SET space_api_route_activation_attempt_id = 'not-an-attempt'
          WHERE id = ?1",
        [builder_id.to_string()],
    )
    .unwrap();
    assert!(matches!(
        classify_and_record_admission(&conn, builder_id, None, None, builder_expectation,).unwrap(),
        AutoLifecycleDeleteAdmission::Blocked {
            reason: AutoLifecycleDeleteBlockReason::NodeRoleUnavailable,
            ..
        }
    ));
    assert!(matches!(
        classify_and_record_admission(
            &conn,
            builder_id,
            Some(NodeRole::Builder),
            None,
            builder_expectation,
        )
        .unwrap(),
        AutoLifecycleDeleteAdmission::Blocked {
            reason: AutoLifecycleDeleteBlockReason::NodeRoleNotWorker,
            ..
        }
    ));
    let blocked_at: String = conn
        .query_row(
            "SELECT auto_lifecycle_delete_blocked_at FROM sandboxes WHERE id = ?1",
            [builder_id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    record_retained_route_evidence(
        &conn,
        builder_id,
        RouteEvidence::from_identity(
            &cluster_route(2, SandboxActivationAttemptId::new()),
            RouteIdentityProvenance::ClusterFenced,
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        conn.query_row(
            "SELECT auto_lifecycle_delete_block_reason,
                    auto_lifecycle_delete_blocked_at
               FROM sandboxes WHERE id = ?1",
            [builder_id.to_string()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .unwrap(),
        ("node_role_not_worker".to_string(), blocked_at),
        "route repair cannot clear a publisher-role block"
    );

    let local_id = SandboxId::new();
    let mut local_expectation = insert_sandbox(&conn, local_id);
    conn.execute(
        "UPDATE sandboxes SET auto_lifecycle_authority = 'local' WHERE id = ?1",
        [local_id.to_string()],
    )
    .unwrap();
    local_expectation.authority = PolicyAuthority::Local;
    assert_eq!(
        classify_and_record_admission(&conn, local_id, None, None, local_expectation,).unwrap(),
        AutoLifecycleDeleteAdmission::Ordinary(AutoLifecycleDeleteOrdinaryReason::LocalAuthority,)
    );

    let provisional_id = SandboxId::new();
    let provisional_expectation = insert_sandbox(&conn, provisional_id);
    crate::db::update_space_api_route_identity(
        &conn,
        provisional_id,
        &cluster_route(3, SandboxActivationAttemptId::new()),
        RouteIdentityProvenance::Unfenced,
    )
    .unwrap();
    assert!(matches!(
        classify_and_record_admission(
            &conn,
            provisional_id,
            Some(NodeRole::Worker),
            None,
            provisional_expectation,
        )
        .unwrap(),
        AutoLifecycleDeleteAdmission::Blocked {
            reason: AutoLifecycleDeleteBlockReason::UnfencedRoute,
            ..
        }
    ));

    let missing_route_id = SandboxId::new();
    let missing_route_expectation = insert_sandbox(&conn, missing_route_id);
    assert!(matches!(
        classify_and_record_admission(
            &conn,
            missing_route_id,
            Some(NodeRole::Worker),
            None,
            missing_route_expectation,
        )
        .unwrap(),
        AutoLifecycleDeleteAdmission::Blocked {
            reason: AutoLifecycleDeleteBlockReason::MissingRouteIdentity,
            ..
        }
    ));
    crate::db::update_space_api_route_identity(
        &conn,
        missing_route_id,
        &cluster_route(4, SandboxActivationAttemptId::new()),
        RouteIdentityProvenance::ClusterFenced,
    )
    .unwrap();
    assert!(matches!(
        classify_and_record_admission(
            &conn,
            missing_route_id,
            Some(NodeRole::Worker),
            None,
            missing_route_expectation,
        )
        .unwrap(),
        AutoLifecycleDeleteAdmission::Receipt(_)
    ));
}

#[test]
fn matching_receipt_completes_atomically_with_row_deletion() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let expectation = insert_sandbox(&conn, sandbox_id);
    let activation_attempt_id = SandboxActivationAttemptId::new();
    let claimed_receipt = receipt(sandbox_id, expectation, 9, activation_attempt_id);
    insert_pending_receipt(&conn, claimed_receipt).unwrap();
    conn.execute(
        "UPDATE sandboxes
            SET state = 'deleting',
                cleanup_completion = 'delete_row',
                cleanup_keep_filesystem = 0
          WHERE id = ?1",
        [sandbox_id.to_string()],
    )
    .unwrap();

    crate::db::complete_row_intent_cleanup_with_receipt(
        &conn,
        &sandbox_id.to_string(),
        crate::db::CleanupCompletion::DeleteRow,
        None,
        Some(claimed_receipt),
    )
    .unwrap();

    assert!(
        crate::db::get_sandbox(&conn, &sandbox_id.to_string())
            .unwrap()
            .is_none()
    );
    let state: String = conn
        .query_row(
            "SELECT state FROM auto_lifecycle_delete_outbox WHERE receipt_id = ?1",
            [claimed_receipt.receipt_id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "completed");

    let recovered_sandbox_id = SandboxId::new();
    let recovered_expectation = insert_sandbox(&conn, recovered_sandbox_id);
    let recovered_attempt = SandboxActivationAttemptId::new();
    let recovered_receipt = receipt(
        recovered_sandbox_id,
        recovered_expectation,
        10,
        recovered_attempt,
    );
    insert_pending_receipt(&conn, recovered_receipt).unwrap();
    conn.execute(
        "UPDATE sandboxes
            SET state = 'deleting',
                cleanup_completion = 'delete_row',
                cleanup_keep_filesystem = 0
          WHERE id = ?1",
        [recovered_sandbox_id.to_string()],
    )
    .unwrap();
    crate::db::complete_row_intent_cleanup_with_receipt(
        &conn,
        &recovered_sandbox_id.to_string(),
        crate::db::CleanupCompletion::DeleteRow,
        None,
        None,
    )
    .unwrap();
    assert!(
        crate::db::get_sandbox(&conn, &recovered_sandbox_id.to_string())
            .unwrap()
            .is_none()
    );
    assert_eq!(
        conn.query_row(
            "SELECT route_activation_id, state
               FROM auto_lifecycle_delete_outbox
              WHERE receipt_id = ?1",
            [recovered_receipt.receipt_id.to_string()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .unwrap(),
        (recovered_attempt.to_string(), "completed".to_string())
    );
}

#[test]
fn pending_receipt_does_not_silently_survive_lost_delete_intent() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let expectation = insert_sandbox(&conn, sandbox_id);
    let pending = receipt(
        sandbox_id,
        expectation,
        11,
        SandboxActivationAttemptId::new(),
    );
    insert_pending_receipt(&conn, pending).unwrap();

    let error = crate::db::complete_row_intent_cleanup_with_receipt(
        &conn,
        &sandbox_id.to_string(),
        crate::db::CleanupCompletion::DeleteRow,
        None,
        Some(pending),
    )
    .unwrap_err();

    assert!(
        crate::db::get_sandbox(&conn, &sandbox_id.to_string())
            .unwrap()
            .is_some()
    );
    assert!(
        pending_receipt_for_sandbox(&conn, sandbox_id)
            .unwrap()
            .is_some()
    );
    assert!(matches!(
        error,
        crate::db::CompleteRowIntentCleanupError::PendingReceiptLostDeleteIntent {
            sandbox_id: observed,
        } if observed == sandbox_id
    ));
}

#[test]
fn pending_receipt_guard_aborts_only_the_delete_statement() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let expectation = insert_sandbox(&conn, sandbox_id);
    insert_pending_receipt(
        &conn,
        receipt(
            sandbox_id,
            expectation,
            3,
            SandboxActivationAttemptId::new(),
        ),
    )
    .unwrap();

    let tx = crate::db::Transaction::immediate(&conn).unwrap();
    tx.execute(
        "UPDATE sandboxes SET failure_reason = 'before guarded delete' WHERE id = ?1",
        [sandbox_id.to_string()],
    )
    .unwrap();
    let error = tx
        .execute(
            "DELETE FROM sandboxes WHERE id = ?1",
            [sandbox_id.to_string()],
        )
        .unwrap_err();
    let error = map_pending_receipt_delete_error(error);
    let rusqlite::Error::ToSqlConversionFailure(error) = error.raw() else {
        panic!("pending-receipt guard must map to a typed database error")
    };
    assert!(error.downcast_ref::<PendingReceiptOwnsDeletion>().is_some());
    tx.commit().unwrap();

    let failure_reason: Option<String> = conn
        .query_row(
            "SELECT failure_reason FROM sandboxes WHERE id = ?1",
            [sandbox_id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(failure_reason.as_deref(), Some("before guarded delete"));
}

#[test]
fn list_and_ack_touch_only_completed_receipts() {
    let conn = crate::db::test_conn();
    let completed_sandbox = SandboxId::new();
    let pending_sandbox = SandboxId::new();
    let completed_expectation = insert_sandbox(&conn, completed_sandbox);
    let pending_expectation = insert_sandbox(&conn, pending_sandbox);
    let completed = receipt(
        completed_sandbox,
        completed_expectation,
        1,
        SandboxActivationAttemptId::new(),
    );
    let pending = receipt(
        pending_sandbox,
        pending_expectation,
        1,
        SandboxActivationAttemptId::new(),
    );
    insert_pending_receipt(&conn, completed).unwrap();
    insert_pending_receipt(&conn, pending).unwrap();
    complete_pending_receipt(&conn, completed_sandbox, completed.receipt_id).unwrap();

    assert_eq!(list_completed_receipts(&conn).unwrap(), vec![completed]);
    assert_eq!(
        acknowledge_completed_receipts(&conn, &[completed.receipt_id, pending.receipt_id]).unwrap(),
        vec![completed.receipt_id]
    );
    assert!(
        pending_receipt_for_sandbox(&conn, pending_sandbox)
            .unwrap()
            .is_some()
    );
    assert!(
        conn.query_row(
            "SELECT receipt_id FROM auto_lifecycle_delete_outbox WHERE sandbox_id = ?1",
            [completed_sandbox.to_string()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .unwrap()
        .is_none()
    );
}

#[test]
fn completed_receipt_listing_is_bounded_without_starving_fresh_evidence() {
    let conn = crate::db::test_conn();
    let receipts = (0..17)
        .map(|generation| {
            let sandbox_id = SandboxId::new();
            let expectation = insert_sandbox(&conn, sandbox_id);
            let receipt = receipt(
                sandbox_id,
                expectation,
                generation + 1,
                SandboxActivationAttemptId::new(),
            );
            insert_pending_receipt(&conn, receipt).unwrap();
            complete_pending_receipt(&conn, sandbox_id, receipt.receipt_id).unwrap();
            receipt
        })
        .collect::<Vec<_>>();

    let first = list_completed_receipts(&conn).unwrap();
    assert_eq!(first.len(), 16);
    let omitted = receipts
        .iter()
        .copied()
        .find(|receipt| !first.contains(receipt))
        .expect("the bounded first batch omits one fresh receipt");

    let second = list_completed_receipts(&conn).unwrap();
    assert_eq!(second.len(), 16);
    assert_eq!(second.first(), Some(&omitted));
}

#[test]
fn pending_receipt_rejects_regressing_and_completed_evidence() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let expectation = insert_sandbox(&conn, sandbox_id);
    let older_attempt = SandboxActivationAttemptId::new();
    let first_attempt = SandboxActivationAttemptId::new_after(older_attempt).unwrap();
    let first = receipt(sandbox_id, expectation, 4, first_attempt);
    assert_eq!(insert_pending_receipt(&conn, first).unwrap(), first);

    let retry = AutoLifecycleDeleteReceipt {
        receipt_id: AutoLifecycleDeleteReceiptId::generate(),
        ..first
    };
    assert_eq!(insert_pending_receipt(&conn, retry).unwrap(), first);

    let conflicting = AutoLifecycleDeleteReceipt {
        route_activation_id: older_attempt,
        ..retry
    };
    insert_pending_receipt(&conn, conflicting).expect_err("conflicting receipt rejected");

    complete_pending_receipt(&conn, sandbox_id, first.receipt_id).unwrap();
    insert_pending_receipt(&conn, retry).expect_err("conflicting receipt rejected");
}

#[test]
fn newer_attempt_conflicts_with_a_pending_receipt() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let expectation = insert_sandbox(&conn, sandbox_id);
    let first_attempt = SandboxActivationAttemptId::new();
    let newer_attempt = SandboxActivationAttemptId::new_after(first_attempt).unwrap();
    let first = receipt(sandbox_id, expectation, 4, first_attempt);
    insert_pending_receipt(&conn, first).unwrap();

    let replacement = AutoLifecycleDeleteReceipt {
        receipt_id: AutoLifecycleDeleteReceiptId::generate(),
        route_activation_id: newer_attempt,
        ..first
    };

    insert_pending_receipt(&conn, replacement).expect_err("conflicting receipt rejected");
    assert_eq!(
        pending_receipt_for_sandbox(&conn, sandbox_id).unwrap(),
        Some(first)
    );
}

#[test]
fn pending_receipt_blocks_same_generation_route_adoption() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let expectation = insert_sandbox(&conn, sandbox_id);
    let admitted_attempt = SandboxActivationAttemptId::new();
    let pending = receipt(sandbox_id, expectation, 4, admitted_attempt);
    insert_pending_receipt(&conn, pending).unwrap();

    let replacement_attempt = SandboxActivationAttemptId::new_after(admitted_attempt).unwrap();
    assert!(
        crate::db::update_space_api_route_identity(
            &conn,
            sandbox_id,
            &cluster_route(4, replacement_attempt),
            RouteIdentityProvenance::ClusterFenced,
        )
        .is_err()
    );
    assert_eq!(
        crate::db::get_sandbox(&conn, &sandbox_id.to_string())
            .unwrap()
            .unwrap()
            .space_api_route_identity
            .activation_attempt_id,
        None
    );
    assert_eq!(
        pending_receipt_for_sandbox(&conn, sandbox_id).unwrap(),
        Some(pending)
    );
}

#[test]
fn current_policy_refreshes_a_pending_receipt_without_hiding_its_age() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let expectation = insert_sandbox(&conn, sandbox_id);
    let activation_attempt_id = SandboxActivationAttemptId::new();
    let first = receipt(sandbox_id, expectation, 4, activation_attempt_id);
    insert_pending_receipt(&conn, first).unwrap();
    conn.execute(
        "UPDATE auto_lifecycle_delete_outbox
            SET created_at = '1'
          WHERE sandbox_id = ?1",
        [sandbox_id.to_string()],
    )
    .unwrap();

    let mut revised_policy = delete_policy();
    revised_policy.stages[0].at += 1;
    let replacement = AutoLifecycleDeleteReceipt {
        receipt_id: AutoLifecycleDeleteReceiptId::generate(),
        policy_fingerprint: AutoLifecyclePolicyFingerprint::for_policy(&revised_policy),
        ..first
    };

    assert_eq!(
        insert_pending_receipt(&conn, replacement).unwrap(),
        replacement,
    );
    assert_eq!(
        conn.query_row(
            "SELECT receipt_id, auto_lifecycle_policy_fingerprint, created_at
               FROM auto_lifecycle_delete_outbox
              WHERE sandbox_id = ?1",
            [sandbox_id.to_string()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .unwrap(),
        (
            replacement.receipt_id.to_string(),
            replacement.policy_fingerprint.to_string(),
            "1".to_string(),
        ),
    );
}

#[test]
fn only_generation_supersession_discards_a_pending_receipt() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let expectation = insert_sandbox(&conn, sandbox_id);
    let pending = receipt(
        sandbox_id,
        expectation,
        4,
        SandboxActivationAttemptId::new(),
    );
    insert_pending_receipt(&conn, pending).unwrap();

    assert!(
        discard_pending_receipt_superseded_by_generation(
            &conn,
            sandbox_id,
            pending.execution_generation,
        )
        .unwrap()
        .is_none()
    );
    assert_eq!(
        pending_receipt_for_sandbox(&conn, sandbox_id).unwrap(),
        Some(pending)
    );

    let next_generation = ExecutionGeneration::new(5).unwrap();
    assert_eq!(
        discard_pending_receipt_superseded_by_generation(&conn, sandbox_id, next_generation,)
            .unwrap(),
        Some(SupersededPendingReceipt {
            receipt_id: pending.receipt_id,
            execution_generation: pending.execution_generation,
        })
    );
    assert!(
        pending_receipt_for_sandbox(&conn, sandbox_id)
            .unwrap()
            .is_none()
    );
}

#[test]
fn pending_receipt_fence_is_distinct_from_a_missing_sandbox() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let expectation = insert_sandbox(&conn, sandbox_id);
    let generation = ExecutionGeneration::new(4).unwrap();
    insert_pending_receipt(
        &conn,
        receipt(
            sandbox_id,
            expectation,
            generation.get(),
            SandboxActivationAttemptId::new(),
        ),
    )
    .unwrap();

    let error = crate::db::update_space_api_route_identity(
        &conn,
        sandbox_id,
        &cluster_route(generation.get(), SandboxActivationAttemptId::new()),
        RouteIdentityProvenance::ClusterFenced,
    )
    .unwrap_err();

    assert!(is_pending_receipt_execution_fence(&error));

    let missing_id = SandboxId::new();
    let missing_error = crate::db::update_space_api_route_identity(
        &conn,
        missing_id,
        &cluster_route(generation.get(), SandboxActivationAttemptId::new()),
        RouteIdentityProvenance::ClusterFenced,
    )
    .unwrap_err();
    assert!(missing_error.is_no_rows());
}

#[test]
fn deleting_row_rejects_runtime_attachment_without_retiring_its_receipt() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let expectation = insert_sandbox(&conn, sandbox_id);
    let pending = receipt(
        sandbox_id,
        expectation,
        4,
        SandboxActivationAttemptId::new(),
    );
    insert_pending_receipt(&conn, pending).unwrap();
    conn.execute(
        "UPDATE sandboxes
            SET state = 'deleting',
                cleanup_completion = 'delete_row',
                cleanup_keep_filesystem = 0
          WHERE id = ?1",
        [sandbox_id.to_string()],
    )
    .unwrap();

    let error = crate::db::update_runtime_fields(
        &conn,
        sandbox_id,
        crate::db::RuntimeFields {
            uid: 10_000,
            slot: 7,
            guest_cid: 37,
            pid: std::process::id(),
            spaced_host_pid: Some(10_001),
            netgtw_pid: 10_002,
            vsock_uds_path: "/space/run/firecracker/sandboxes/test/root/v.sock",
            api_socket_path: "/space/run/firecracker/sandboxes/test/api.sock",
            spaced_endpoint_proof: None,
            space_api_route_identity: SpaceApiRouteIdentity {
                execution_generation: Some(ExecutionGeneration::new(5).unwrap()),
                ..SpaceApiRouteIdentity::default()
            },
            route_identity_provenance: RouteIdentityProvenance::Unfenced,
        },
    )
    .expect_err("runtime attachment must not supersede cleanup ownership");

    assert!(is_lifecycle_execution_fence(&error));
    assert!(!is_pending_receipt_execution_fence(&error));
    assert_eq!(
        pending_receipt_for_sandbox(&conn, sandbox_id).unwrap(),
        Some(pending)
    );
}

#[test]
fn beginning_a_new_execution_retires_an_older_pending_receipt() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let expectation = insert_sandbox(&conn, sandbox_id);
    conn.execute(
        "UPDATE sandboxes SET state = 'created' WHERE id = ?1",
        [sandbox_id.to_string()],
    )
    .unwrap();
    let pending = receipt(
        sandbox_id,
        expectation,
        4,
        SandboxActivationAttemptId::new(),
    );
    insert_pending_receipt(&conn, pending).unwrap();
    let next_generation = ExecutionGeneration::new(5).unwrap();
    let next_attempt = SandboxActivationAttemptId::new();

    let episode = crate::db::begin_activation_with_route_evidence(
        &conn,
        &sandbox_id.to_string(),
        crate::state::SandboxState::Created,
        crate::db::ActivationKind::ColdStart,
        Some(next_generation),
        Some(next_attempt),
        crate::db::ActivationRouteAdmission {
            auto_lifecycle: None,
            operation_activation_attempt_id: None,
            evidence: Some(RouteEvidence {
                execution_generation: next_generation,
                activation_attempt_id: next_attempt,
                substrate: NodeManagedBy::Kubernetes,
                provenance: RouteIdentityProvenance::ClusterFenced,
            }),
            open_claim: None,
            runtime_upgrade: None,
        },
    )
    .unwrap();

    assert!(episode.is_some());
    assert!(
        pending_receipt_for_sandbox(&conn, sandbox_id)
            .unwrap()
            .is_none(),
        "the activation claim must retire stale cleanup evidence atomically"
    );
}

#[test]
fn beginning_an_execution_cannot_supersede_a_current_pending_receipt() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let expectation = insert_sandbox(&conn, sandbox_id);
    conn.execute(
        "UPDATE sandboxes SET state = 'created' WHERE id = ?1",
        [sandbox_id.to_string()],
    )
    .unwrap();
    let generation = ExecutionGeneration::new(5).unwrap();
    let pending = receipt(
        sandbox_id,
        expectation,
        generation.get(),
        SandboxActivationAttemptId::new(),
    );
    insert_pending_receipt(&conn, pending).unwrap();

    let episode = crate::db::begin_activation_with_route_evidence(
        &conn,
        &sandbox_id.to_string(),
        crate::state::SandboxState::Created,
        crate::db::ActivationKind::ColdStart,
        Some(generation),
        Some(SandboxActivationAttemptId::new()),
        crate::db::ActivationRouteAdmission::default(),
    )
    .unwrap();

    assert!(episode.is_none());
    assert_eq!(
        crate::db::get_sandbox(&conn, &sandbox_id.to_string())
            .unwrap()
            .unwrap()
            .state,
        crate::state::SandboxState::Created
    );
    assert_eq!(
        pending_receipt_for_sandbox(&conn, sandbox_id).unwrap(),
        Some(pending)
    );
}

#[test]
fn generation_writers_discard_an_older_pending_receipt_without_complete_route_evidence() {
    let conn = crate::db::test_conn();

    let route_sandbox_id = SandboxId::new();
    let route_expectation = insert_sandbox(&conn, route_sandbox_id);
    let route_pending = receipt(
        route_sandbox_id,
        route_expectation,
        4,
        SandboxActivationAttemptId::new(),
    );
    insert_pending_receipt(&conn, route_pending).unwrap();
    crate::db::update_space_api_route_identity(
        &conn,
        route_sandbox_id,
        &SpaceApiRouteIdentity {
            execution_generation: Some(ExecutionGeneration::new(5).unwrap()),
            ..SpaceApiRouteIdentity::default()
        },
        RouteIdentityProvenance::Unfenced,
    )
    .unwrap();
    assert!(
        pending_receipt_for_sandbox(&conn, route_sandbox_id)
            .unwrap()
            .is_none()
    );

    let sandbox_id = SandboxId::new();
    let expectation = insert_sandbox(&conn, sandbox_id);
    let pending = receipt(
        sandbox_id,
        expectation,
        4,
        SandboxActivationAttemptId::new(),
    );
    insert_pending_receipt(&conn, pending).unwrap();
    crate::db::update_runtime_fields(
        &conn,
        sandbox_id,
        crate::db::RuntimeFields {
            uid: 10_000,
            slot: 7,
            guest_cid: 37,
            pid: std::process::id(),
            spaced_host_pid: Some(10_001),
            netgtw_pid: 10_002,
            vsock_uds_path: "/space/run/firecracker/sandboxes/test/root/v.sock",
            api_socket_path: "/space/run/firecracker/sandboxes/test/api.sock",
            spaced_endpoint_proof: None,
            space_api_route_identity: SpaceApiRouteIdentity {
                execution_generation: Some(ExecutionGeneration::new(5).unwrap()),
                ..SpaceApiRouteIdentity::default()
            },
            route_identity_provenance: RouteIdentityProvenance::Unfenced,
        },
    )
    .unwrap();
    assert!(
        pending_receipt_for_sandbox(&conn, sandbox_id)
            .unwrap()
            .is_none()
    );
}

#[test]
fn completion_reports_a_typed_missing_expected_receipt() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let expectation = insert_sandbox(&conn, sandbox_id);
    let pending = receipt(
        sandbox_id,
        expectation,
        4,
        SandboxActivationAttemptId::new(),
    );
    insert_pending_receipt(&conn, pending).unwrap();
    conn.execute(
        "UPDATE sandboxes
            SET state = 'deleting',
                cleanup_completion = 'delete_row',
                cleanup_keep_filesystem = 0
          WHERE id = ?1",
        [sandbox_id.to_string()],
    )
    .unwrap();
    discard_pending_receipt_superseded_by_generation(
        &conn,
        sandbox_id,
        ExecutionGeneration::new(5).unwrap(),
    )
    .unwrap();

    let error = crate::db::complete_row_intent_cleanup_with_receipt(
        &conn,
        &sandbox_id.to_string(),
        crate::db::CleanupCompletion::DeleteRow,
        None,
        Some(pending),
    )
    .unwrap_err();

    assert!(matches!(
        error,
        crate::db::CompleteRowIntentCleanupError::ExpectedReceiptMissingOrChanged {
            sandbox_id: observed,
        } if observed == sandbox_id
    ));
}

#[test]
fn newer_route_generation_supersedes_receipt_completion() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let expectation = insert_sandbox(&conn, sandbox_id);
    let pending = receipt(
        sandbox_id,
        expectation,
        4,
        SandboxActivationAttemptId::new(),
    );
    insert_pending_receipt(&conn, pending).unwrap();
    conn.execute(
        "UPDATE sandboxes
            SET state = 'deleting',
                cleanup_completion = 'delete_row',
                cleanup_keep_filesystem = 0
          WHERE id = ?1",
        [sandbox_id.to_string()],
    )
    .unwrap();
    record_retained_route_evidence(
        &conn,
        sandbox_id,
        RouteEvidence {
            execution_generation: ExecutionGeneration::new(5).unwrap(),
            activation_attempt_id: SandboxActivationAttemptId::new(),
            substrate: NodeManagedBy::Kubernetes,
            provenance: RouteIdentityProvenance::ClusterFenced,
        },
    )
    .unwrap();
    discard_pending_receipt_superseded_by_generation(
        &conn,
        sandbox_id,
        ExecutionGeneration::new(5).unwrap(),
    )
    .unwrap();

    assert_eq!(
        crate::db::complete_row_intent_cleanup_with_receipt(
            &conn,
            &sandbox_id.to_string(),
            crate::db::CleanupCompletion::DeleteRow,
            None,
            Some(pending),
        )
        .unwrap(),
        crate::db::CompleteRowIntentCleanupDisposition::Superseded,
    );
    assert!(
        crate::db::get_sandbox(&conn, &sandbox_id.to_string())
            .unwrap()
            .is_some(),
        "the superseded cleanup must not delete the replacement execution"
    );
}

#[test]
fn newer_current_execution_generation_supersedes_receipt_completion() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let expectation = insert_sandbox(&conn, sandbox_id);
    let pending = receipt(
        sandbox_id,
        expectation,
        4,
        SandboxActivationAttemptId::new(),
    );
    insert_pending_receipt(&conn, pending).unwrap();
    assert!(
        discard_pending_receipt_superseded_by_generation(
            &conn,
            sandbox_id,
            ExecutionGeneration::new(5).unwrap(),
        )
        .unwrap()
        .is_some()
    );
    crate::db::update_space_api_route_identity(
        &conn,
        sandbox_id,
        &SpaceApiRouteIdentity {
            execution_generation: Some(ExecutionGeneration::new(5).unwrap()),
            ..SpaceApiRouteIdentity::default()
        },
        RouteIdentityProvenance::Unfenced,
    )
    .unwrap();
    conn.execute(
        "UPDATE sandboxes
            SET state = 'started',
                cleanup_completion = NULL,
                cleanup_keep_filesystem = NULL
          WHERE id = ?1",
        [sandbox_id.to_string()],
    )
    .unwrap();

    assert_eq!(
        crate::db::complete_row_intent_cleanup_with_receipt(
            &conn,
            &sandbox_id.to_string(),
            crate::db::CleanupCompletion::DeleteRow,
            None,
            Some(pending),
        )
        .unwrap(),
        crate::db::CompleteRowIntentCleanupDisposition::Superseded,
    );
    assert!(
        crate::db::get_sandbox(&conn, &sandbox_id.to_string())
            .unwrap()
            .is_some(),
        "the superseded cleanup must not delete the replacement execution"
    );
    assert!(
        pending_receipt_for_sandbox(&conn, sandbox_id)
            .unwrap()
            .is_none()
    );
}

#[test]
fn equal_current_execution_generation_does_not_supersede_receipt_completion() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let expectation = insert_sandbox(&conn, sandbox_id);
    let generation = ExecutionGeneration::new(4).unwrap();
    crate::db::update_space_api_route_identity(
        &conn,
        sandbox_id,
        &SpaceApiRouteIdentity {
            execution_generation: Some(generation),
            ..SpaceApiRouteIdentity::default()
        },
        RouteIdentityProvenance::Unfenced,
    )
    .unwrap();
    let pending = receipt(
        sandbox_id,
        expectation,
        generation.get(),
        SandboxActivationAttemptId::new(),
    );
    insert_pending_receipt(&conn, pending).unwrap();
    conn.execute(
        "UPDATE sandboxes
            SET state = 'deleting',
                cleanup_completion = 'delete_row',
                cleanup_keep_filesystem = 0
          WHERE id = ?1",
        [sandbox_id.to_string()],
    )
    .unwrap();
    let mismatched = AutoLifecycleDeleteReceipt {
        receipt_id: AutoLifecycleDeleteReceiptId::generate(),
        ..pending
    };

    let error = crate::db::complete_row_intent_cleanup_with_receipt(
        &conn,
        &sandbox_id.to_string(),
        crate::db::CleanupCompletion::DeleteRow,
        None,
        Some(mismatched),
    )
    .unwrap_err();

    assert!(matches!(
        error,
        crate::db::CompleteRowIntentCleanupError::ExpectedReceiptMissingOrChanged {
            sandbox_id: observed,
        } if observed == sandbox_id
    ));
    assert!(
        crate::db::get_sandbox(&conn, &sandbox_id.to_string())
            .unwrap()
            .is_some()
    );
    assert_eq!(
        pending_receipt_for_sandbox(&conn, sandbox_id).unwrap(),
        Some(pending)
    );
}

#[test]
fn equal_generation_retained_evidence_cannot_be_downgraded() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    insert_sandbox(&conn, sandbox_id);
    let first_attempt = SandboxActivationAttemptId::new();
    let replacement_attempt = SandboxActivationAttemptId::new();
    record_retained_route_evidence(
        &conn,
        sandbox_id,
        RouteEvidence {
            execution_generation: ExecutionGeneration::new(4).unwrap(),
            activation_attempt_id: first_attempt,
            substrate: NodeManagedBy::Kubernetes,
            provenance: RouteIdentityProvenance::ClusterFenced,
        },
    )
    .unwrap();

    assert!(
        !record_retained_route_evidence(
            &conn,
            sandbox_id,
            RouteEvidence {
                execution_generation: ExecutionGeneration::new(4).unwrap(),
                activation_attempt_id: replacement_attempt,
                substrate: NodeManagedBy::Kubernetes,
                provenance: RouteIdentityProvenance::Unfenced,
            },
        )
        .unwrap()
    );
    assert_eq!(
        conn.query_row(
            "SELECT last_observed_route_activation_id,
                    last_observed_route_identity_provenance
               FROM sandboxes WHERE id = ?1",
            [sandbox_id.to_string()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .unwrap(),
        (first_attempt.to_string(), "cluster_fenced".to_string())
    );

    assert!(
        record_retained_route_evidence(
            &conn,
            sandbox_id,
            RouteEvidence {
                execution_generation: ExecutionGeneration::new(4).unwrap(),
                activation_attempt_id: replacement_attempt,
                substrate: NodeManagedBy::Kubernetes,
                provenance: RouteIdentityProvenance::ClusterFenced,
            },
        )
        .unwrap()
    );
    assert_eq!(
        conn.query_row(
            "SELECT last_observed_route_activation_id,
                    last_observed_route_identity_provenance
               FROM sandboxes WHERE id = ?1",
            [sandbox_id.to_string()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .unwrap(),
        (
            replacement_attempt.to_string(),
            "cluster_fenced".to_string()
        )
    );

    assert!(
        !record_retained_route_evidence(
            &conn,
            sandbox_id,
            RouteEvidence {
                execution_generation: ExecutionGeneration::new(4).unwrap(),
                activation_attempt_id: first_attempt,
                substrate: NodeManagedBy::Kubernetes,
                provenance: RouteIdentityProvenance::ClusterFenced,
            },
        )
        .unwrap(),
        "delayed same-generation evidence must not replace the newer attempt"
    );
    assert_eq!(
        conn.query_row(
            "SELECT last_observed_route_activation_id
               FROM sandboxes WHERE id = ?1",
            [sandbox_id.to_string()],
            |row| row.get::<_, String>(0),
        )
        .unwrap(),
        replacement_attempt.to_string()
    );
}

#[test]
fn receipt_conflict_uses_bounded_retry_without_claiming_a_durable_block() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    insert_sandbox(&conn, sandbox_id);

    let storage_id = sandbox_id.to_string();
    assert!(record_receipt_conflict_retry(&conn, &storage_id).unwrap());
    assert!(!record_receipt_conflict_retry(&conn, &storage_id).unwrap());

    let (block_reason, retry_at, runtime_state_json): (Option<String>, i64, String) = conn
        .query_row(
            "SELECT auto_lifecycle_delete_block_reason,
                    next_auto_event_at,
                    auto_lifecycle_state_json
               FROM sandboxes
              WHERE id = ?1",
            [sandbox_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    let runtime_state: pplx_space_common_core::types::AutoLifecycleRuntimeState =
        serde_json::from_str(&runtime_state_json).unwrap();
    assert!(block_reason.is_none());
    assert!(retry_at > crate::db::now_epoch_i64().unwrap());
    assert_eq!(
        runtime_state.next_auto_event_at,
        Some(u64::try_from(retry_at).unwrap())
    );
    assert!(
        runtime_state
            .last_auto_error
            .as_deref()
            .is_some_and(|error| error.contains("conflicting receipt evidence"))
    );
}

#[test]
fn policy_replacement_retires_obsolete_delete_block() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let expectation = insert_sandbox(&conn, sandbox_id);

    assert!(matches!(
        classify_and_record_admission(
            &conn,
            sandbox_id,
            Some(NodeRole::Worker),
            None,
            expectation,
        )
        .unwrap(),
        AutoLifecycleDeleteAdmission::Blocked {
            reason: AutoLifecycleDeleteBlockReason::MissingRouteIdentity,
            ..
        }
    ));

    crate::db::update_auto_lifecycle(
        &conn,
        &sandbox_id.to_string(),
        &AutoLifecyclePolicy::disabled(),
        PolicyAuthority::Cluster,
    )
    .unwrap();

    let (reason, blocked_at): (Option<String>, Option<String>) = conn
        .query_row(
            "SELECT auto_lifecycle_delete_block_reason,
                    auto_lifecycle_delete_blocked_at
               FROM sandboxes
              WHERE id = ?1",
            [sandbox_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((reason, blocked_at), (None, None));
}

#[test]
fn unknown_substrate_is_durably_blocked() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let expectation = insert_sandbox(&conn, sandbox_id);
    conn.execute(
        "UPDATE sandboxes
            SET space_api_route_execution_generation = 7,
                space_api_route_activation_attempt_id = ?1,
                space_api_route_substrate_kind = 'future_substrate',
                space_api_route_identity_provenance = 'cluster_fenced'
          WHERE id = ?2",
        rusqlite::params![
            SandboxActivationAttemptId::new().to_string(),
            sandbox_id.to_string()
        ],
    )
    .unwrap();

    let admission =
        classify_and_record_admission(&conn, sandbox_id, Some(NodeRole::Worker), None, expectation)
            .expect("unknown substrate must fail closed as a durable block");
    let AutoLifecycleDeleteAdmission::Blocked { reason, changed } = admission else {
        panic!("unknown substrate must not admit deletion: {admission:?}");
    };
    assert_eq!(reason.as_str(), "unknown_substrate");
    assert!(changed);
}
