use pplx_space_accounts::OrgId;
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_public_api::{OperationKind, OperationState, OperationTargetType, PublicSandboxId};
use pplx_space_sandbox_model::{SandboxId, SandboxLifecycle};
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use uuid::Uuid;

use super::{SEED_ORG, SEED_USER};
use crate::operations;

#[tokio::test(flavor = "current_thread")]
async fn due_for_gc_retains_active_force_restart_stop_receipts_and_collects_released_ones()
-> TestResult {
    let db = pg_test_db().await?;
    let org_id = Uuid::parse_str(SEED_ORG)?;
    let user_id = Uuid::parse_str(SEED_USER)?;
    let other_org = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, slug, name) VALUES ($1, 'other-gc-org', 'Other GC Org')"
    ))
    .bind(other_org)
    .execute(&db.pool)
    .await?;

    let stopped_at = time::OffsetDateTime::from_unix_timestamp(1_760_000_000)?;
    let campaign_created_at = stopped_at - time::Duration::seconds(5);
    let during_stop = stopped_at - time::Duration::seconds(2);
    let before = stopped_at - time::Duration::seconds(10);
    let after = stopped_at + time::Duration::seconds(5);
    let expired_gc = time::OffsetDateTime::from_unix_timestamp(1_750_000_000)?;
    let from_env = format!("sha256:{}", "0".repeat(64));
    let to_env = format!("sha256:{}", "1".repeat(64));
    let worker_node = Uuid::now_v7();
    sqlx::query(sql!("INSERT INTO nodes (id, role) VALUES ($1, 'worker')"))
        .bind(worker_node)
        .execute(&db.pool)
        .await?;

    #[derive(Clone, Copy)]
    enum ObservedBinding {
        Unpublished,
        Unbound,
        Source,
        Target,
    }

    struct Case {
        name: &'static str,
        sandbox_lifecycle: SandboxLifecycle,
        observed: ObservedBinding,
        campaign_kind: &'static str,
        campaign_phase: &'static str,
        item: Option<(&'static str, i64)>,
        op_org: Option<Uuid>,
        op_kind: OperationKind,
        op_target_type: Option<OperationTargetType>,
        malformed_target: Option<&'static str>,
        op_state: OperationState,
        requested_at: time::OffsetDateTime,
        completed_at: Option<time::OffsetDateTime>,
        retained: bool,
    }

    let cases = [
        Case {
            name: "paused force-restart pending item retains succeeded stop at fence",
            sandbox_lifecycle: SandboxLifecycle::Stopped,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "paused",
            item: Some(("pending", 0)),
            op_org: None,
            op_kind: OperationKind::StopSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: before,
            completed_at: Some(stopped_at),
            retained: true,
        },
        Case {
            name: "paused force-restart failed item retains succeeded kill after fence",
            sandbox_lifecycle: SandboxLifecycle::Stopped,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "paused",
            item: Some(("failed", 5)),
            op_org: None,
            op_kind: OperationKind::KillSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            retained: true,
        },
        Case {
            name: "aborted campaign still draining in_flight item retains succeeded stop",
            sandbox_lifecycle: SandboxLifecycle::Stopping,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "aborted",
            item: Some(("in_flight", 1)),
            op_org: None,
            op_kind: OperationKind::StopSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: after,
            completed_at: None,
            retained: true,
        },
        Case {
            name: "canary force-restart pending item retains succeeded stop",
            sandbox_lifecycle: SandboxLifecycle::Stopped,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "canary",
            item: Some(("pending", 0)),
            op_org: None,
            op_kind: OperationKind::StopSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            retained: true,
        },
        Case {
            name: "pre-scan sandbox without item row in canary campaign retains stop receipt",
            sandbox_lifecycle: SandboxLifecycle::Started,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "canary",
            item: None,
            op_org: None,
            op_kind: OperationKind::StopSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: before,
            completed_at: Some(before),
            retained: true,
        },
        Case {
            name: "pre-scan started sandbox bound to source environment in paused campaign retains activation receipt",
            sandbox_lifecycle: SandboxLifecycle::Started,
            observed: ObservedBinding::Source,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "paused",
            item: None,
            op_org: None,
            op_kind: OperationKind::StartSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            retained: true,
        },
        Case {
            name: "pre-scan started sandbox unbound in observed states retains activation receipt",
            sandbox_lifecycle: SandboxLifecycle::Started,
            observed: ObservedBinding::Unbound,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "paused",
            item: None,
            op_org: None,
            op_kind: OperationKind::StartSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            retained: true,
        },
        Case {
            name: "pre-scan started sandbox bound to target environment in paused campaign is collected",
            sandbox_lifecycle: SandboxLifecycle::Started,
            observed: ObservedBinding::Target,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "paused",
            item: None,
            op_org: None,
            op_kind: OperationKind::StartSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            retained: false,
        },
        Case {
            name: "reopenable skipped item bound to target environment is collected",
            sandbox_lifecycle: SandboxLifecycle::Started,
            observed: ObservedBinding::Target,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "rolling",
            item: Some(("skipped", 1)),
            op_org: None,
            op_kind: OperationKind::StartSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            retained: false,
        },
        Case {
            name: "pending item bound to target environment stays retained",
            sandbox_lifecycle: SandboxLifecycle::Started,
            observed: ObservedBinding::Target,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "rolling",
            item: Some(("pending", 0)),
            op_org: None,
            op_kind: OperationKind::StartSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            retained: true,
        },
        Case {
            name: "pending item retains pre-campaign activation receipt for fence horizon",
            sandbox_lifecycle: SandboxLifecycle::Stopped,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "paused",
            item: Some(("pending", 0)),
            op_org: None,
            op_kind: OperationKind::WakeSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: before,
            completed_at: Some(before),
            retained: true,
        },
        Case {
            name: "pre-scan creating sandbox without item row in canary campaign retains stop receipt",
            sandbox_lifecycle: SandboxLifecycle::Creating,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "canary",
            item: None,
            op_org: None,
            op_kind: OperationKind::StopSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: before,
            completed_at: Some(before),
            retained: true,
        },
        Case {
            name: "pre-scan checkpointing sandbox without item row in rolling campaign retains activation receipt",
            sandbox_lifecycle: SandboxLifecycle::Checkpointing,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "rolling",
            item: None,
            op_org: None,
            op_kind: OperationKind::WakeSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            retained: true,
        },
        Case {
            name: "pre-scan sandbox without item row in paused campaign retains activation receipt",
            sandbox_lifecycle: SandboxLifecycle::Started,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "paused",
            item: None,
            op_org: None,
            op_kind: OperationKind::WakeSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            retained: true,
        },
        Case {
            name: "pre-scan sandbox without item row in aborted campaign releases stop receipt",
            sandbox_lifecycle: SandboxLifecycle::Stopped,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "aborted",
            item: None,
            op_org: None,
            op_kind: OperationKind::StopSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            retained: false,
        },
        Case {
            name: "stop completed during in-flight campaign stop before stopped_at settles is retained",
            sandbox_lifecycle: SandboxLifecycle::Stopped,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "paused",
            item: Some(("pending", 0)),
            op_org: None,
            op_kind: OperationKind::StopSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: during_stop,
            completed_at: Some(during_stop),
            retained: true,
        },
        Case {
            name: "latest pre-campaign stop on stopped sandbox is retained for initiated_at fence",
            sandbox_lifecycle: SandboxLifecycle::Stopped,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "rolling",
            item: Some(("pending", 0)),
            op_org: None,
            op_kind: OperationKind::StopSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: before,
            completed_at: Some(before),
            retained: true,
        },
        Case {
            name: "stale started sandbox retains pre-campaign stop for initiated_at fence",
            sandbox_lifecycle: SandboxLifecycle::Started,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "rolling",
            item: Some(("pending", 0)),
            op_org: None,
            op_kind: OperationKind::StopSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: before,
            completed_at: Some(before),
            retained: true,
        },
        Case {
            name: "transient restoring sandbox retains post-campaign stop across drain re-placement",
            sandbox_lifecycle: SandboxLifecycle::Restoring,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "rolling",
            item: Some(("pending", 0)),
            op_org: None,
            op_kind: OperationKind::StopSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: during_stop,
            completed_at: Some(during_stop),
            retained: true,
        },
        Case {
            name: "pre-campaign start operation on completed item is collected",
            sandbox_lifecycle: SandboxLifecycle::Stopped,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "rolling",
            item: Some(("done", 1)),
            op_org: None,
            op_kind: OperationKind::StartSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: before,
            completed_at: Some(before),
            retained: false,
        },
        Case {
            name: "reopenable skipped item on active sandbox retains stop receipt",
            sandbox_lifecycle: SandboxLifecycle::Started,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "rolling",
            item: Some(("skipped", 1)),
            op_org: None,
            op_kind: OperationKind::StopSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            retained: true,
        },
        Case {
            name: "skipped item on stopped sandbox releases stop receipt immediately",
            sandbox_lifecycle: SandboxLifecycle::Stopped,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "rolling",
            item: Some(("skipped", 1)),
            op_org: None,
            op_kind: OperationKind::StopSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            retained: false,
        },
        Case {
            name: "pre-scan stopped sandbox without item row releases stop receipt",
            sandbox_lifecycle: SandboxLifecycle::Stopped,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "rolling",
            item: None,
            op_org: None,
            op_kind: OperationKind::StopSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            retained: false,
        },
        Case {
            name: "exhausted skipped item releases stop receipt to gc",
            sandbox_lifecycle: SandboxLifecycle::Started,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "rolling",
            item: Some(("skipped", 5)),
            op_org: None,
            op_kind: OperationKind::StopSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            retained: false,
        },
        Case {
            name: "retired skipped item releases stop receipt to gc",
            sandbox_lifecycle: SandboxLifecycle::Started,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "rolling",
            item: Some(("skipped", i64::from(i32::MAX))),
            op_org: None,
            op_kind: OperationKind::StopSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            retained: false,
        },
        Case {
            name: "done item releases stop receipt to gc while campaign continues rolling",
            sandbox_lifecycle: SandboxLifecycle::Started,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "rolling",
            item: Some(("done", 1)),
            op_org: None,
            op_kind: OperationKind::StopSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            retained: false,
        },
        Case {
            name: "aborted campaign with pending item releases stop receipt to gc",
            sandbox_lifecycle: SandboxLifecycle::Stopped,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "aborted",
            item: Some(("pending", 0)),
            op_org: None,
            op_kind: OperationKind::StopSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            retained: false,
        },
        Case {
            name: "completed campaign never retains stop receipt",
            sandbox_lifecycle: SandboxLifecycle::Stopped,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "completed",
            item: Some(("in_flight", 1)),
            op_org: None,
            op_kind: OperationKind::StopSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            retained: false,
        },
        Case {
            name: "stopped-sandbox campaign kind does not retain stop receipt",
            sandbox_lifecycle: SandboxLifecycle::Stopped,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandboxes",
            campaign_phase: "rolling",
            item: Some(("pending", 0)),
            op_org: None,
            op_kind: OperationKind::StopSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            retained: false,
        },
        Case {
            name: "failed stop operation is collected",
            sandbox_lifecycle: SandboxLifecycle::Stopped,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "rolling",
            item: Some(("pending", 0)),
            op_org: None,
            op_kind: OperationKind::StopSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Failed,
            requested_at: after,
            completed_at: Some(after),
            retained: false,
        },
        Case {
            name: "post-campaign start on running sandbox is retained across pause before drain",
            sandbox_lifecycle: SandboxLifecycle::Started,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "paused",
            item: Some(("pending", 0)),
            op_org: None,
            op_kind: OperationKind::StartSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            retained: true,
        },
        Case {
            name: "post-campaign start on unpinned stopped sandbox is retained before drain",
            sandbox_lifecycle: SandboxLifecycle::Stopped,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "rolling",
            item: Some(("pending", 0)),
            op_org: None,
            op_kind: OperationKind::StartSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            retained: true,
        },
        Case {
            name: "post-campaign rollback on running sandbox is retained",
            sandbox_lifecycle: SandboxLifecycle::Started,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "rolling",
            item: Some(("pending", 0)),
            op_org: None,
            op_kind: OperationKind::RollbackSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            retained: true,
        },
        Case {
            name: "checkpoint operation is collected",
            sandbox_lifecycle: SandboxLifecycle::Stopped,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "rolling",
            item: Some(("pending", 0)),
            op_org: None,
            op_kind: OperationKind::CheckpointSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            retained: false,
        },
        Case {
            name: "cross-org stop operation is collected",
            sandbox_lifecycle: SandboxLifecycle::Stopped,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "rolling",
            item: Some(("pending", 0)),
            op_org: Some(other_org),
            op_kind: OperationKind::StopSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            retained: false,
        },
        Case {
            name: "malformed target_id is collected without uuid cast error",
            sandbox_lifecycle: SandboxLifecycle::Stopped,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "rolling",
            item: Some(("pending", 0)),
            op_org: None,
            op_kind: OperationKind::StopSandbox,
            op_target_type: Some(OperationTargetType::Sandbox),
            malformed_target: Some("sbx_not-a-uuid"),
            op_state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            retained: false,
        },
        Case {
            name: "null target_type is collected without null-negation drop",
            sandbox_lifecycle: SandboxLifecycle::Stopped,
            observed: ObservedBinding::Unpublished,
            campaign_kind: "sandbox_force_restarts",
            campaign_phase: "rolling",
            item: Some(("pending", 0)),
            op_org: None,
            op_kind: OperationKind::StopSandbox,
            op_target_type: None,
            malformed_target: None,
            op_state: OperationState::Succeeded,
            requested_at: after,
            completed_at: Some(after),
            retained: false,
        },
    ];

    for case in &cases {
        let mut tx = db.pool.begin().await?;
        let sandbox = Uuid::now_v7();
        sqlx::query(sql!(
            "INSERT INTO sandboxes (
                 id, user_id, org_id, lifecycle, lifecycle_changed_at, network_policy_generation
             )
             VALUES ($1, $2, $3, $4, $5, 1)"
        ))
        .bind(sandbox)
        .bind(user_id)
        .bind(org_id)
        .bind(case.sandbox_lifecycle.as_str())
        .bind(stopped_at)
        .execute(&mut *tx)
        .await?;

        let observed_metadata = match case.observed {
            ObservedBinding::Unpublished => None,
            ObservedBinding::Unbound => Some(serde_json::json!({})),
            ObservedBinding::Source => Some(serde_json::json!({ "environment": from_env })),
            ObservedBinding::Target => Some(serde_json::json!({ "environment": to_env })),
        };
        if let Some(execution_metadata) = observed_metadata {
            sqlx::query(sql!(
                "INSERT INTO sandbox_observed_states (
                     sandbox_id, node_id, runtime, runtime_version, execution_metadata
                 )
                 VALUES ($1, $2, 'firecracker', '1.9.0', $3)"
            ))
            .bind(sandbox)
            .bind(worker_node)
            .bind(execution_metadata)
            .execute(&mut *tx)
            .await?;
        }

        let campaign = sqlx::query_scalar::<_, Uuid>(sql!(
            "INSERT INTO runtime_upgrade_campaigns (
                 from_environment_ref, to_environment_ref, kind, phase,
                 max_in_flight_per_node, max_in_flight_per_cluster, failure_budget,
                 include_running, reason, created_at, finished_at
             )
             VALUES ($1, $2, $3, $4, 1, 4, 2, $5, 'gc test', $6,
                     CASE WHEN $4 IN ('completed', 'aborted') THEN NOW() END)
             RETURNING id"
        ))
        .bind(&from_env)
        .bind(&to_env)
        .bind(case.campaign_kind)
        .bind(case.campaign_phase)
        .bind(case.campaign_kind == "sandboxes")
        .bind(campaign_created_at)
        .fetch_one(&mut *tx)
        .await?;

        if let Some((item_phase, attempts)) = case.item {
            let in_flight = item_phase == "in_flight";
            sqlx::query(sql!(
                "INSERT INTO runtime_upgrade_items (
                     campaign_id, subject_kind, subject_id, phase, attempts,
                     lease_token, lease_expires_at
                 )
                 VALUES ($1, 'sandbox', $2, $3, $4, $5,
                         CASE WHEN $6 THEN NOW() + INTERVAL '5 minutes' END)"
            ))
            .bind(campaign)
            .bind(sandbox)
            .bind(item_phase)
            .bind(attempts)
            .bind(in_flight.then(Uuid::now_v7))
            .bind(in_flight)
            .execute(&mut *tx)
            .await?;
        }

        let op_id = Uuid::now_v7();
        let default_target = PublicSandboxId::from(SandboxId::from_uuid(sandbox)).to_string();
        let target_id = match (case.op_target_type.as_ref(), case.malformed_target) {
            (None, _) => None,
            (Some(_), Some(bad)) => Some(bad),
            (Some(_), None) => Some(default_target.as_str()),
        };
        sqlx::query(sql!(
            "INSERT INTO public_operations (
                 operation_id, org_id, actor_user_id, kind, target_type, target_id,
                 state, requested_at, completed_at, gc_after
             )
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
        ))
        .bind(op_id)
        .bind(case.op_org.unwrap_or(org_id))
        .bind(case.op_org.is_none().then_some(user_id))
        .bind(case.op_kind.as_str())
        .bind(
            case.op_target_type
                .as_ref()
                .map(OperationTargetType::as_str),
        )
        .bind(target_id)
        .bind(case.op_state.as_str())
        .bind(case.requested_at)
        .bind(case.completed_at)
        .bind(expired_gc)
        .execute(&mut *tx)
        .await?;

        let due = operations::due_for_gc(&mut *tx, 200).await?;
        if case.retained {
            assert!(
                !due.contains(&op_id),
                "case `{}` must be retained by due_for_gc",
                case.name
            );
            let deleted = operations::delete_by_ids_with_idempotency(&mut *tx, &[op_id]).await?;
            assert!(
                deleted.operations.is_empty(),
                "case `{}` must also be retained by delete_by_ids_with_idempotency",
                case.name
            );
        } else {
            assert!(
                due.contains(&op_id),
                "case `{}` must be returned by due_for_gc",
                case.name
            );
            let deleted = operations::delete_by_ids_with_idempotency(&mut *tx, &[op_id]).await?;
            assert_eq!(
                deleted.operations,
                vec![op_id],
                "case `{}` must be deleted by delete_by_ids_with_idempotency",
                case.name
            );
        }
        tx.rollback().await?;
    }

    {
        let mut tx = db.pool.begin().await?;
        let aborted_campaign = sqlx::query_scalar::<_, Uuid>(sql!(
            "INSERT INTO runtime_upgrade_campaigns (
                 from_environment_ref, to_environment_ref, kind, phase,
                 max_in_flight_per_node, max_in_flight_per_cluster, failure_budget,
                 include_running, reason, created_at, finished_at
             )
             VALUES ($1, $2, 'sandbox_force_restarts', 'aborted', 1, 4, 2, false, 'gc lease test', $3, NOW())
             RETURNING id"
        ))
        .bind(&from_env)
        .bind(&to_env)
        .bind(campaign_created_at)
        .fetch_one(&mut *tx)
        .await?;

        let rolling_campaign = sqlx::query_scalar::<_, Uuid>(sql!(
            "INSERT INTO runtime_upgrade_campaigns (
                 from_environment_ref, to_environment_ref, kind, phase,
                 max_in_flight_per_node, max_in_flight_per_cluster, failure_budget,
                 include_running, reason, created_at
             )
             VALUES ($1, $2, 'sandbox_force_restarts', 'rolling', 1, 4, 2, false, 'gc redrive test', $3)
             RETURNING id"
        ))
        .bind(&to_env)
        .bind(&from_env)
        .bind(campaign_created_at)
        .fetch_one(&mut *tx)
        .await?;

        let mut lease_ops = Vec::new();
        for (campaign_id, lease_lapsed) in [
            (aborted_campaign, false),
            (aborted_campaign, true),
            (rolling_campaign, true),
        ] {
            let sandbox = Uuid::now_v7();
            sqlx::query(sql!(
                "INSERT INTO sandboxes (
                     id, user_id, org_id, lifecycle, lifecycle_changed_at, network_policy_generation
                 )
                 VALUES ($1, $2, $3, 'stopped', $4, 1)"
            ))
            .bind(sandbox)
            .bind(user_id)
            .bind(org_id)
            .bind(stopped_at)
            .execute(&mut *tx)
            .await?;

            sqlx::query(sql!(
                "INSERT INTO runtime_upgrade_items (
                     campaign_id, subject_kind, subject_id, phase, attempts,
                     lease_token, lease_expires_at
                 )
                 VALUES (
                     $1, 'sandbox', $2, 'in_flight', 1, $3,
                     CASE
                         WHEN $4 THEN NOW() - INTERVAL '5 minutes'
                         ELSE NOW() + INTERVAL '5 minutes'
                     END
                 )"
            ))
            .bind(campaign_id)
            .bind(sandbox)
            .bind(Uuid::now_v7())
            .bind(lease_lapsed)
            .execute(&mut *tx)
            .await?;

            let op_id = Uuid::now_v7();
            let target_id = PublicSandboxId::from(SandboxId::from_uuid(sandbox)).to_string();
            sqlx::query(sql!(
                "INSERT INTO public_operations (
                     operation_id, org_id, actor_user_id, kind, target_type, target_id,
                     state, requested_at, completed_at, gc_after
                 )
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
            ))
            .bind(op_id)
            .bind(org_id)
            .bind(user_id)
            .bind(OperationKind::StopSandbox.as_str())
            .bind(OperationTargetType::Sandbox.as_str())
            .bind(target_id)
            .bind(OperationState::Succeeded.as_str())
            .bind(after)
            .bind(after)
            .bind(expired_gc)
            .execute(&mut *tx)
            .await?;
            lease_ops.push(op_id);
        }

        let due = operations::due_for_gc(&mut *tx, 200).await?;
        assert!(
            !due.contains(&lease_ops[0]),
            "aborted campaign item with an unexpired lease must retain its receipt while draining"
        );
        assert!(
            due.contains(&lease_ops[1]),
            "aborted campaign item whose lease lapsed must release its receipt even before release_lapsed_in_finished runs"
        );
        assert!(
            !due.contains(&lease_ops[2]),
            "rolling campaign item whose lease lapsed must keep its receipt until redrive_expired re-queues it"
        );
        let deleted = operations::delete_by_ids_with_idempotency(&mut *tx, &lease_ops).await?;
        assert_eq!(
            deleted,
            operations::DeletedOperations {
                operations: vec![lease_ops[1]],
                idempotency_records: Vec::new(),
            },
            "delete_by_ids_with_idempotency must delete only the lapsed aborted item's receipt"
        );
        let remaining = sqlx::query_scalar::<_, Uuid>(sql!(
            "SELECT operation_id
             FROM public_operations
             WHERE operation_id = ANY($1)
             ORDER BY array_position($1, operation_id)"
        ))
        .bind(&lease_ops)
        .fetch_all(&mut *tx)
        .await?;
        assert_eq!(remaining, vec![lease_ops[0], lease_ops[2]]);
        tx.rollback().await?;
    }

    let mut expected_retained = Vec::new();
    let mut expected_due = Vec::new();

    // Share one active `sandbox_force_restarts` campaign across these pool-committed
    // fixtures (matching `create_campaign`'s one-unfinished-campaign-per-source
    // invariant) so an item-specific state such as `tied_stops_sbx`'s reopenable
    // `skipped` row cannot be masked by another unfinished campaign's `i.campaign_id
    // IS NULL` branch.
    let campaign = sqlx::query_scalar::<_, Uuid>(sql!(
        "INSERT INTO runtime_upgrade_campaigns (
             from_environment_ref, to_environment_ref, kind, phase,
             max_in_flight_per_node, max_in_flight_per_cluster, failure_budget,
             include_running, reason, created_at
         )
         VALUES ($1, $2, 'sandbox_force_restarts', 'rolling', 1, 4, 2, false, 'gc test', $3)
         RETURNING id"
    ))
    .bind(&from_env)
    .bind(&to_env)
    .bind(campaign_created_at)
    .fetch_one(&db.pool)
    .await?;

    for (name, op_kind, artifact_kind, artifact_origin, retained) in [
        (
            "drain-pinned stopped sandbox retains start receipt",
            OperationKind::StartSandbox,
            "disk",
            "stop",
            true,
        ),
        (
            "drain-pinned stopped sandbox retains wake receipt",
            OperationKind::WakeSandbox,
            "disk",
            "stop",
            true,
        ),
        (
            "drain-pinned stopped sandbox retains restore receipt",
            OperationKind::RestoreSandbox,
            "disk",
            "stop",
            true,
        ),
        (
            "drain-pinned stopped sandbox retains delete receipt",
            OperationKind::DeleteSandbox,
            "disk",
            "stop",
            true,
        ),
        (
            "suspend-pinned sandbox still retains post-campaign start receipt",
            OperationKind::StartSandbox,
            "disk",
            "suspend",
            true,
        ),
    ] {
        let sandbox = Uuid::now_v7();
        sqlx::query(sql!(
            "INSERT INTO sandboxes (
                 id, user_id, org_id, lifecycle, lifecycle_changed_at,
                 current_local_artifact_id, current_local_artifact_kind,
                 current_local_artifact_origin, network_policy_generation
             )
             VALUES ($1, $2, $3, 'stopped', $4, $5, $6, $7, 1)"
        ))
        .bind(sandbox)
        .bind(user_id)
        .bind(org_id)
        .bind(stopped_at)
        .bind(Uuid::now_v7())
        .bind(artifact_kind)
        .bind(artifact_origin)
        .execute(&db.pool)
        .await?;

        sqlx::query(sql!(
            "INSERT INTO runtime_upgrade_items (
                 campaign_id, subject_kind, subject_id, phase, attempts
             )
             VALUES ($1, 'sandbox', $2, 'pending', 0)"
        ))
        .bind(campaign)
        .bind(sandbox)
        .execute(&db.pool)
        .await?;

        let op_id = Uuid::now_v7();
        let target = PublicSandboxId::from(SandboxId::from_uuid(sandbox)).to_string();
        sqlx::query(sql!(
            "INSERT INTO public_operations (
                 operation_id, org_id, actor_user_id, kind, target_type, target_id,
                 state, requested_at, completed_at, gc_after
             )
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, $9)"
        ))
        .bind(op_id)
        .bind(org_id)
        .bind(user_id)
        .bind(op_kind.as_str())
        .bind(OperationTargetType::Sandbox.as_str())
        .bind(target)
        .bind(OperationState::Succeeded.as_str())
        .bind(after)
        .bind(expired_gc)
        .execute(&db.pool)
        .await?;

        if retained {
            expected_retained.push((name, op_id));
        } else {
            expected_due.push((name, op_id));
        }
    }

    // On unscanned or reopenable-`skipped` active sandboxes (which cannot yet hold
    // a campaign-recorded stop fence), an older pre-campaign stop superseded by a
    // pre-campaign start is collected along with that pre-campaign start, while
    // among tied pre-campaign stops without a newer fenced operation only the
    // highest `operation_id` stays. By contrast, once an item is `pending`,
    // `in_flight`, or `failed` on a retaining campaign, `dispatcher.stop` may have
    // captured `initiated_at = LEAST(NOW(), pre_stop_lifecycle_changed_at)` before
    // `c.created_at` and then advanced `sandboxes.lifecycle_changed_at` past
    // `c.created_at`, so pre-campaign `stop_kinds` receipts must stay retained even
    // when followed by a pre-campaign idempotent activation (`wake_sandbox`).
    let prior_episode_sbx = Uuid::now_v7();
    let tied_stops_sbx = Uuid::now_v7();
    for (sbx, item_state) in [
        (prior_episode_sbx, None),
        (tied_stops_sbx, Some(("skipped", 1_i64))),
    ] {
        sqlx::query(sql!(
            "INSERT INTO sandboxes (
                 id, user_id, org_id, lifecycle, lifecycle_changed_at, network_policy_generation
             )
             VALUES ($1, $2, $3, 'started', $4, 1)"
        ))
        .bind(sbx)
        .bind(user_id)
        .bind(org_id)
        .bind(stopped_at)
        .execute(&db.pool)
        .await?;
        if let Some((phase, attempts)) = item_state {
            sqlx::query(sql!(
                "INSERT INTO runtime_upgrade_items (
                     campaign_id, subject_kind, subject_id, phase, attempts
                 )
                 VALUES ($1, 'sandbox', $2, $3, $4)"
            ))
            .bind(campaign)
            .bind(sbx)
            .bind(phase)
            .bind(attempts)
            .execute(&db.pool)
            .await?;
        }
    }
    let pre_campaign_rollback_sbx = Uuid::now_v7();
    let fenced_pending_sbx = Uuid::now_v7();
    for (sbx, changed_at) in [
        (pre_campaign_rollback_sbx, stopped_at),
        (fenced_pending_sbx, after),
    ] {
        sqlx::query(sql!(
            "INSERT INTO sandboxes (
                 id, user_id, org_id, lifecycle, lifecycle_changed_at, network_policy_generation
             )
             VALUES ($1, $2, $3, 'stopped', $4, 1)"
        ))
        .bind(sbx)
        .bind(user_id)
        .bind(org_id)
        .bind(changed_at)
        .execute(&db.pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO runtime_upgrade_items (
                 campaign_id, subject_kind, subject_id, phase, attempts
             )
             VALUES ($1, 'sandbox', $2, 'pending', 0)"
        ))
        .bind(campaign)
        .bind(sbx)
        .execute(&db.pool)
        .await?;
    }
    let old_stop_id = Uuid::now_v7();
    let boot_id = Uuid::now_v7();
    for (id, kind, ts, gc_at) in [
        (
            old_stop_id,
            OperationKind::StopSandbox,
            before - time::Duration::seconds(20),
            expired_gc - time::Duration::minutes(5),
        ),
        (
            boot_id,
            OperationKind::StartSandbox,
            before - time::Duration::seconds(10),
            expired_gc - time::Duration::minutes(10),
        ),
    ] {
        let target = PublicSandboxId::from(SandboxId::from_uuid(prior_episode_sbx)).to_string();
        sqlx::query(sql!(
            "INSERT INTO public_operations (
                 operation_id, org_id, actor_user_id, kind, target_type, target_id,
                 state, requested_at, completed_at, gc_after
             )
             VALUES ($1, $2, $3, $4, 'sandbox', $5, 'succeeded', $6, $6, $7)"
        ))
        .bind(id)
        .bind(org_id)
        .bind(user_id)
        .bind(kind.as_str())
        .bind(target)
        .bind(ts)
        .bind(gc_at)
        .execute(&db.pool)
        .await?;
    }
    expected_due.push((
        "prior-episode pre-campaign stop superseded by pre-campaign start on unscanned sandbox is collected first",
        old_stop_id,
    ));
    expected_retained.push((
        "prior-episode pre-campaign start is retained while older stop receipt remains uncollected",
        boot_id,
    ));

    let fenced_pre_campaign_stop_id = Uuid::now_v7();
    let fenced_pre_campaign_wake_id = Uuid::now_v7();
    for (id, kind, ts) in [
        (
            fenced_pre_campaign_stop_id,
            OperationKind::StopSandbox,
            before - time::Duration::seconds(20),
        ),
        (
            fenced_pre_campaign_wake_id,
            OperationKind::WakeSandbox,
            before - time::Duration::seconds(10),
        ),
    ] {
        let target = PublicSandboxId::from(SandboxId::from_uuid(fenced_pending_sbx)).to_string();
        sqlx::query(sql!(
            "INSERT INTO public_operations (
                 operation_id, org_id, actor_user_id, kind, target_type, target_id,
                 state, requested_at, completed_at, gc_after
             )
             VALUES ($1, $2, $3, $4, 'sandbox', $5, 'succeeded', $6, $6, $7)"
        ))
        .bind(id)
        .bind(org_id)
        .bind(user_id)
        .bind(kind.as_str())
        .bind(target)
        .bind(ts)
        .bind(expired_gc)
        .execute(&db.pool)
        .await?;
    }
    expected_retained.push((
        "pending item whose campaign stop advanced lifecycle_changed_at past campaign_created_at retains pre-campaign stop receipt despite pre-campaign wake",
        fenced_pre_campaign_stop_id,
    ));
    expected_retained.push((
        "pending item retains pre-campaign wake receipt alongside pre-campaign stop receipt so external_stop_since sees the clearing activation",
        fenced_pre_campaign_wake_id,
    ));

    let tied_low = Uuid::from_u128(1);
    let tied_high = Uuid::from_u128(2);
    for id in [tied_low, tied_high] {
        let target = PublicSandboxId::from(SandboxId::from_uuid(tied_stops_sbx)).to_string();
        sqlx::query(sql!(
            "INSERT INTO public_operations (
                 operation_id, org_id, actor_user_id, kind, target_type, target_id,
                 state, requested_at, completed_at, gc_after
             )
             VALUES ($1, $2, $3, 'stop_sandbox', 'sandbox', $4, 'succeeded', $5, $5, $6)"
        ))
        .bind(id)
        .bind(org_id)
        .bind(user_id)
        .bind(target)
        .bind(before)
        .bind(expired_gc)
        .execute(&db.pool)
        .await?;
    }
    expected_due.push((
        "older tied pre-campaign stop operation_id is collected",
        tied_low,
    ));
    expected_retained.push((
        "newest tied pre-campaign stop operation_id is retained",
        tied_high,
    ));

    let pre_campaign_rollback_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO public_operations (
             operation_id, org_id, actor_user_id, kind, target_type, target_id,
             state, requested_at, completed_at, gc_after
         )
         VALUES ($1, $2, $3, 'rollback_sandbox', 'sandbox', $4, 'succeeded', $5, $5, $6)"
    ))
    .bind(pre_campaign_rollback_id)
    .bind(org_id)
    .bind(user_id)
    .bind(PublicSandboxId::from(SandboxId::from_uuid(pre_campaign_rollback_sbx)).to_string())
    .bind(before)
    .bind(expired_gc)
    .execute(&db.pool)
    .await?;
    expected_retained.push((
        "pre-campaign rollback_sandbox on stopped sandbox is retained via stop_kinds",
        pre_campaign_rollback_id,
    ));

    let due = operations::due_for_gc(&db.pool, 200).await?;
    for (name, op_id) in &expected_retained {
        assert!(
            !due.contains(op_id),
            "case `{name}` must be retained by due_for_gc"
        );
    }
    for (name, op_id) in &expected_due {
        assert!(
            due.contains(op_id),
            "case `{name}` must be returned by due_for_gc"
        );
    }
    assert_eq!(
        operations::due_for_gc(&db.pool, 1).await?,
        vec![old_stop_id],
        "bounded batch must collect old_stop_id before boot_id despite boot_id having an earlier gc_after"
    );
    assert!(
        operations::delete_by_ids_with_idempotency(&db.pool, &[boot_id])
            .await?
            .operations
            .is_empty(),
        "delete_by_ids_with_idempotency must retain boot_id while old_stop_id still exists"
    );
    assert_eq!(
        operations::delete_by_ids_with_idempotency(&db.pool, &[old_stop_id])
            .await?
            .operations,
        vec![old_stop_id]
    );
    assert_eq!(
        operations::due_for_gc(&db.pool, 1).await?,
        vec![boot_id],
        "once old_stop_id is collected, boot_id becomes collectable immediately under active campaigns"
    );

    sqlx::query(sql!(
        "UPDATE runtime_upgrade_campaigns
         SET phase = 'completed', finished_at = NOW()"
    ))
    .execute(&db.pool)
    .await?;
    let due_after_completion = operations::due_for_gc(&db.pool, 200).await?;
    for (name, op_id) in &expected_retained {
        assert!(
            due_after_completion.contains(op_id),
            "case `{name}` must be released once campaigns complete"
        );
    }

    // If a `sandbox_force_restarts` campaign commits under `READ COMMITTED`
    // between `due_for_gc` and `delete_by_ids_with_idempotency`, retention must
    // preserve both the raced campaign's operation receipt and its dependent
    // `public_idempotency_records` row while atomically deleting non-retained
    // operations even when their idempotency record has `response_json IS NULL`.
    let retained_idem_id = Uuid::now_v7();
    let doomed_idem_id = Uuid::now_v7();
    for (idem_id, key_name, op_id) in [
        (
            retained_idem_id,
            "idem-retained-rollback",
            pre_campaign_rollback_id,
        ),
        (doomed_idem_id, "idem-doomed-start", boot_id),
    ] {
        crate::idempotency::insert_for_operation(
            &db.pool,
            idem_id,
            "native",
            crate::idempotency::IdempotencyKeyRef {
                org_id: OrgId::from_uuid(org_id),
                method: "POST",
                path_template: "/v1/sandboxes/{id}/rollback",
                idempotency_key: key_name,
            },
            &"1".repeat(64),
            op_id,
            24,
        )
        .await?;
    }
    sqlx::query(sql!(
        "INSERT INTO runtime_upgrade_campaigns (
             from_environment_ref, to_environment_ref, kind, phase,
             max_in_flight_per_node, max_in_flight_per_cluster, failure_budget,
             include_running, reason, created_at
         )
         VALUES ($1, $2, 'sandbox_force_restarts', 'rolling', 1, 4, 2, false, 'raced create', $3)"
    ))
    .bind(&from_env)
    .bind(&to_env)
    .bind(campaign_created_at)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1"
    ))
    .bind(pre_campaign_rollback_sbx)
    .execute(&db.pool)
    .await?;
    let deleted =
        operations::delete_by_ids_with_idempotency(&db.pool, &due_after_completion).await?;
    assert!(
        !deleted.operations.contains(&pre_campaign_rollback_id),
        "delete_by_ids_with_idempotency must recheck retention and skip receipts covered by a raced campaign"
    );
    assert!(
        !deleted.idempotency_records.contains(&retained_idem_id),
        "idempotency record of a raced retained operation must not be deleted"
    );
    assert!(
        deleted.operations.contains(&boot_id)
            && deleted.idempotency_records.contains(&doomed_idem_id),
        "non-retained operation and its response_json IS NULL idempotency record must be deleted together"
    );
    let retained_idem_exists: bool = sqlx::query_scalar(sql!(
        "SELECT EXISTS(SELECT 1 FROM public_idempotency_records WHERE id = $1 AND operation_id = $2)"
    ))
    .bind(retained_idem_id)
    .bind(pre_campaign_rollback_id)
    .fetch_one(&db.pool)
    .await?;
    assert!(
        retained_idem_exists,
        "retained operation's idempotency record must stay intact"
    );

    // `share_lock_campaign_retention` allows concurrent shared GC readers while
    // blocking an exclusive `sandbox_force_restarts` campaign-creation advisory
    // lock until the GC transaction commits.
    let mut tx_gc = db.pool.begin().await?;
    let gc_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
        .fetch_one(&mut *tx_gc)
        .await?;
    operations::share_lock_campaign_retention(&mut *tx_gc).await?;

    let mut tx_second_gc = db.pool.begin().await?;
    sqlx::query(sql!("SET LOCAL lock_timeout = '1s'"))
        .execute(&mut *tx_second_gc)
        .await?;
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        operations::share_lock_campaign_retention(&mut *tx_second_gc),
    )
    .await
    .map_err(|_| "concurrent share_lock_campaign_retention timed out")??;
    tx_second_gc.rollback().await?;

    let mut conn_create = db.pool.acquire().await?;
    let contender_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
        .fetch_one(&mut *conn_create)
        .await?;
    let contender_from: pplx_space_execution_spec::EnvironmentReference =
        format!("sha256:{}", "33".repeat(32)).parse()?;
    let contender_to: pplx_space_execution_spec::EnvironmentReference =
        format!("sha256:{}", "44".repeat(32)).parse()?;
    let create_task = tokio::spawn(async move {
        pplx_space_runtime_upgrade_campaign_store::create_campaign(
            &mut *conn_create,
            &pplx_space_runtime_upgrade_campaign_store::NewCampaign {
                from: contender_from,
                to: contender_to,
                kind: pplx_space_runtime_upgrade_campaign_store::CampaignKind::SandboxForceRestarts,
                canary: false,
                max_in_flight_per_node: 1,
                max_in_flight_per_cluster: 4,
                failure_budget: 2,
                include_running: false,
                reason: "serialized create".to_owned(),
                created_by: None,
            },
        )
        .await
    });

    let wait_deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let waiting: bool = sqlx::query_scalar(sql!(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_locks
                 WHERE pid = $1
                   AND locktype = 'advisory'
                   AND NOT granted
                   AND $2 = ANY(pg_blocking_pids($1))
             )"
        ))
        .bind(contender_pid)
        .bind(gc_pid)
        .fetch_one(&mut *tx_gc)
        .await?;
        if waiting {
            break;
        }
        assert!(
            std::time::Instant::now() < wait_deadline,
            "exclusive campaign-creation lock must block on share_lock_campaign_retention"
        );
        tokio::task::yield_now().await;
    }
    assert!(
        !create_task.is_finished(),
        "campaign creation must remain blocked until GC transaction commits"
    );
    let _ = operations::due_for_gc(&mut *tx_gc, 200).await?;
    tx_gc.commit().await?;
    create_task.await??;
    Ok(())
}
