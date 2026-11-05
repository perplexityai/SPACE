use pplx_space_common_core::types::{NodeId, OrgId, SandboxId, UserId};

use super::*;
use crate::controllers::substrate::{
    AdmittedPoolClaim, AdmittedPoolClaimLookup, PooledClaimRefusal, PooledExecutionClaimAttempt,
};
use crate::federated_identity::{
    DEFAULT_FEDERATED_GRANT_AUDIENCE, FEDERATED_OWNER_GRANT_PURPOSE, FederatedOwner,
    FederatedServiceGrant,
};
use pplx_space_common_core::types::PodUid;

#[cfg(feature = "kubernetes")]
mod kubernetes;

type InsertedPooledSandboxRow = (
    Option<String>,
    UserId,
    OrgId,
    String,
    String,
    String,
    Option<NodeId>,
    Option<NodeId>,
    Option<String>,
    Option<String>,
    Option<i32>,
    Option<i32>,
    Option<i32>,
    i64,
);

type InsertedFederatedPooledSandboxRow = (
    UserId,
    OrgId,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    String,
    String,
    Option<NodeId>,
    Option<NodeId>,
);
use crate::models::{SandboxMetadata, SandboxMetadataValue};
use pplx_space_common_core::types::{UpstreamIssuer, UpstreamOrgId, UpstreamSubjectId};
use pplx_space_common_svc::types::ReconcileFacet;
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, insert_ready_worker_node, pg_test_db,
};
use pplx_space_placement::test_support::stored_lifecycle_matrix;
use sqlx::{PgPool, Postgres, Transaction};
use std::{
    collections::VecDeque,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use pplx_space_test_env::TestResult;

async fn insert_actor(pool: &PgPool) -> TestResult<User> {
    let org_id = OrgId::new();
    let user_id = UserId::new();

    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug)
         VALUES ($1, 'Pooled Sandbox Insert Test', $2)"
    ))
    .bind(org_id)
    .bind(format!("pooled-sandbox-insert-{org_id}"))
    .execute(pool)
    .await?;

    Ok(sqlx::query_as::<_, User>(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, $2, 'unused', 'Pooled Sandbox Insert Test', $3, 'member')
         RETURNING id, email, password_hash, display_name, org_id, role, owner_source,
                   created_at, updated_at"
    ))
    .bind(user_id)
    .bind(format!("pooled-sandbox-insert-{user_id}@example.com"))
    .bind(org_id)
    .fetch_one(pool)
    .await?)
}

async fn insert_node(pool: &PgPool, node_id: NodeId) -> TestResult {
    insert_ready_worker_node(pool, node_id).await
}

/// Create finalization moves a row to `started` on its node only out of the
/// lifecycle its create owns: a plain create out of `creating`, whether the
/// row is still unplaced or a receipt already placed it there, and also out
/// of a `started` row a receipt already converged onto the same node; a
/// leased restore-shaped create only out of the `restoring` row its lease
/// token names. Every other stored lifecycle is refused and left as found.
/// `A`: finalized; `.`: refused.
#[tokio::test(flavor = "current_thread")]
async fn create_finalization_finalizes_only_the_lifecycle_its_create_owns() -> TestResult {
    const EXPECTED: &[&str] = &[
        //              unplaced  on_node  leased
        "created            .        .        .",
        "creating           A        A        .",
        "starting           .        .        .",
        "started            .        A        .",
        "restoring          .        .        A",
        "pausing            .        .        .",
        "paused             .        .        .",
        "resuming           .        .        .",
        "suspending         .        .        .",
        "suspended          .        .        .",
        "stopping           .        .        .",
        "stopped            .        .        .",
        "deleting           .        .        .",
        "checkpointing      .        .        .",
        "quarantined        .        .        .",
        "failed             .        .        .",
        "deleted            .        .        .",
        "cleanup_required   .        .        .",
        "bogus              .        .        .",
        "_Suspended_        .        .        .",
    ];
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    insert_node(&db.pool, node_id).await?;

    let mut mismatches = Vec::new();
    for (stored, cells) in stored_lifecycle_matrix(["unplaced", "on_node", "leased"], EXPECTED) {
        for (shape, finalizes) in cells {
            let finalizes = finalizes.applies();
            let sandbox_id = SandboxId::new();
            let restore_lease_token = (shape == "leased").then(OperationId::new);
            let placed_on = (shape == "on_node").then_some(node_id);
            sqlx::query(sql!(
                "INSERT INTO sandboxes (
                     id, user_id, org_id, managed_by, lifecycle, placement, active_node_id,
                     network_policy_generation, restore_lease_token, restore_lease_expires_at
                 )
                 VALUES (
                     $1, $2, $3, 'k8s', $4,
                     CASE WHEN $5::uuid IS NULL THEN 'unplaced' ELSE 'on_node' END, $5, 1,
                     $6, CASE WHEN $6::uuid IS NULL THEN NULL ELSE NOW() + INTERVAL '5 minutes' END
                 )"
            ))
            .bind(sandbox_id)
            .bind(SEED_USER_ID)
            .bind(SEED_ORG_ID)
            .bind(stored)
            .bind(placed_on)
            .bind(restore_lease_token.map(|token| token.as_uuid()))
            .execute(&db.pool)
            .await?;

            let finalized = finalize_created_sandbox(
                &db.pool,
                sandbox_id,
                "started",
                "on_node",
                Some(node_id),
                false,
                restore_lease_token,
            )
            .await?;

            let lifecycle: String =
                sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
                    .bind(sandbox_id)
                    .fetch_one(&db.pool)
                    .await?;
            let expected_lifecycle = if finalizes { "started" } else { stored };
            if finalized != finalizes || lifecycle != expected_lifecycle {
                mismatches.push(format!(
                    "{shape} from {stored}: finalized {finalized}, row now {lifecycle}"
                ));
            }
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn create_finalization_accepts_receipt_converged_started_state() -> TestResult {
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    insert_node(&db.pool, node_id).await?;
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement, active_node_id,
             network_policy_generation
         )
         VALUES ($1, $2, $3, 'k8s', 'started', 'on_node', $4, 1)"
    ))
    .bind(sandbox_id)
    .bind(actor.id)
    .bind(actor.org_id)
    .bind(node_id)
    .execute(&db.pool)
    .await?;

    let finalized = finalize_created_sandbox(
        &db.pool,
        sandbox_id,
        "started",
        "on_node",
        Some(node_id),
        false,
        None,
    )
    .await?;

    assert!(
        finalized,
        "a matching activation-completion receipt must not lose create ownership"
    );
    let command_ready_pending: bool = sqlx::query_scalar(sql!(
        "SELECT command_ready_pending FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert!(
        !command_ready_pending,
        "idempotent finalization must not restore a cleared readiness marker"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn create_finalization_accepts_operation_leased_restoring_state() -> TestResult {
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let restore_lease_token = OperationId::new();
    insert_node(&db.pool, node_id).await?;
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement, active_node_id,
             network_policy_generation, restore_lease_token, restore_lease_expires_at,
             preserve_local_artifact_on_restore_failure
         )
         VALUES (
             $1, $2, $3, 'k8s', 'restoring', 'unplaced', NULL, 1, $4,
             NOW() + INTERVAL '5 minutes', TRUE
         )"
    ))
    .bind(sandbox_id)
    .bind(actor.id)
    .bind(actor.org_id)
    .bind(restore_lease_token.as_uuid())
    .execute(&db.pool)
    .await?;

    let wrong_owner_finalized = finalize_created_sandbox(
        &db.pool,
        sandbox_id,
        "started",
        "on_node",
        Some(node_id),
        true,
        Some(OperationId::new()),
    )
    .await?;
    assert!(
        !wrong_owner_finalized,
        "a different operation lease must not finalize the restoring row"
    );

    let finalized = finalize_created_sandbox(
        &db.pool,
        sandbox_id,
        "started",
        "on_node",
        Some(node_id),
        false,
        Some(restore_lease_token),
    )
    .await?;

    assert!(
        finalized,
        "the fork operation lease must finalize its restoring row"
    );
    let (
        remaining_restore_lease,
        preserve_local_artifact,
        command_ready_pending,
        placement_changed_at,
    ): (Option<Uuid>, bool, bool, Option<time::OffsetDateTime>) = sqlx::query_as(sql!(
        "SELECT restore_lease_token, preserve_local_artifact_on_restore_failure,
                    command_ready_pending, placement_changed_at
             FROM sandboxes
             WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert!(
        remaining_restore_lease.is_none(),
        "successful finalization must release the restore ownership lease"
    );
    assert!(
        !preserve_local_artifact,
        "fork-only artifact retention must end with the initial restore"
    );
    assert!(
        !command_ready_pending,
        "restore completion must not opt into create-only command readiness recovery"
    );
    assert!(
        placement_changed_at.is_none(),
        "restoring-row finalization must preserve the snapshot freshness boundary"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn restoring_convergence_clears_fork_only_artifact_retention() -> TestResult {
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let sandbox_id = SandboxId::new();
    let restore_lease_token = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             network_policy_generation, restore_lease_token, restore_lease_expires_at,
             preserve_local_artifact_on_restore_failure
         ) VALUES (
             $1, $2, $3, 'k8s', 'restoring', 'unplaced', 1, $4,
             NOW() + INTERVAL '5 minutes', TRUE
         )"
    ))
    .bind(sandbox_id)
    .bind(actor.id)
    .bind(actor.org_id)
    .bind(restore_lease_token)
    .execute(&db.pool)
    .await?;

    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;

    let (remaining_restore_lease, preserve_local_artifact): (Option<Uuid>, bool) =
        sqlx::query_as(sql!(
            "SELECT restore_lease_token, preserve_local_artifact_on_restore_failure
             FROM sandboxes
             WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&db.pool)
        .await?;
    assert!(
        remaining_restore_lease.is_none(),
        "leaving restoring must clear the restore lease"
    );
    assert!(
        !preserve_local_artifact,
        "leaving restoring must clear fork-only artifact retention"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn restore_finalization_requires_the_receipt_converged_node_binding() -> TestResult {
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    insert_node(&db.pool, node_id).await?;
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
                 id, user_id, org_id, managed_by, lifecycle, placement, active_node_id,
                 network_policy_generation
             )
             VALUES ($1, $2, $3, 'k8s', 'started', 'on_node', $4, 1)"
    ))
    .bind(sandbox_id)
    .bind(actor.id)
    .bind(actor.org_id)
    .bind(node_id)
    .execute(&db.pool)
    .await?;

    let other_node_id = NodeId::new();
    insert_node(&db.pool, other_node_id).await?;
    let mismatched = finalize_created_sandbox(
        &db.pool,
        sandbox_id,
        "started",
        "on_node",
        Some(other_node_id),
        false,
        Some(OperationId::new()),
    )
    .await?;
    assert!(!mismatched, "a different node receipt must lose the CAS");
    let placement_changed_at: Option<time::OffsetDateTime> = sqlx::query_scalar(sql!(
        "SELECT placement_changed_at FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert!(
        placement_changed_at.is_none(),
        "a losing receipt must not stamp placement freshness"
    );

    let finalized = finalize_created_sandbox(
        &db.pool,
        sandbox_id,
        "started",
        "on_node",
        Some(node_id),
        false,
        Some(OperationId::new()),
    )
    .await?;
    assert!(finalized, "the converged node receipt is idempotent");

    let (active_node_id, placement_changed_at): (Option<NodeId>, Option<time::OffsetDateTime>) =
        sqlx::query_as(sql!(
            "SELECT active_node_id, placement_changed_at
             FROM sandboxes
             WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(active_node_id, Some(node_id));
    assert!(
        placement_changed_at.is_some(),
        "the winning restore receipt must stamp placement freshness"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn create_finalization_records_operation_leased_restore_failure() -> TestResult {
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let sandbox_id = SandboxId::new();
    let restore_operation_id = OperationId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             network_policy_generation, restore_lease_token, restore_lease_expires_at
         ) VALUES (
             $1, $2, $3, 'k8s', 'restoring', 'unplaced', 1, $4,
             NOW() + INTERVAL '5 minutes'
         )"
    ))
    .bind(sandbox_id)
    .bind(actor.id)
    .bind(actor.org_id)
    .bind(restore_operation_id.as_uuid())
    .execute(&db.pool)
    .await?;

    let finalized = finalize_created_sandbox(
        &db.pool,
        sandbox_id,
        "failed",
        "unplaced",
        None,
        false,
        Some(restore_operation_id),
    )
    .await?;
    assert!(
        finalized,
        "the lease owner must record terminal restore failure"
    );
    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(lifecycle, "failed");
    Ok(())
}
async fn insert_template_release(
    pool: &PgPool,
    template_name: &str,
    content_hash: &str,
) -> TestResult<Uuid> {
    sqlx::query(sql!(
        "INSERT INTO templates (id, name, description, org_id)
         VALUES ($1, $2, NULL, NULL)
         ON CONFLICT (name) DO NOTHING"
    ))
    .bind(Uuid::now_v7())
    .bind(template_name)
    .execute(pool)
    .await?;

    let release_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO template_releases (id, template_name, type, hash)
         VALUES ($1, $2, 'fc', $3)"
    ))
    .bind(release_id)
    .bind(template_name)
    .bind(content_hash)
    .execute(pool)
    .await?;
    Ok(release_id)
}

async fn insert_template_release_tag(
    pool: &PgPool,
    release_id: Uuid,
    template_name: &str,
    tag: &str,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO template_release_tags (template_release_id, template_name, tag)
         VALUES ($1, $2, $3)"
    ))
    .bind(release_id)
    .bind(template_name)
    .bind(tag)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_org_specific_template_release(
    pool: &PgPool,
    template_name: &str,
    content_hash: &str,
    owner_org_id: OrgId,
) -> TestResult<Uuid> {
    sqlx::query(
        sql!("INSERT INTO templates (id, name, description, org_id, visibility, network_policy_generation)
         VALUES ($1, $2, NULL, $3, 'org_specific', 1)"),
    )
    .bind(Uuid::now_v7())
    .bind(template_name)
    .bind(owner_org_id)
    .execute(pool)
    .await?;

    let release_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO template_releases (id, template_name, type, hash, org_id)
         VALUES ($1, $2, 'fc', $3, $4)"
    ))
    .bind(release_id)
    .bind(template_name)
    .bind(content_hash)
    .bind(owner_org_id)
    .execute(pool)
    .await?;
    insert_template_release_tag(pool, release_id, template_name, "latest").await?;
    Ok(release_id)
}

async fn mapped_federated_owner(
    pool: &PgPool,
    owner: &FederatedOwner,
) -> TestResult<SandboxCreateOwner> {
    Ok(crate::federated_identity::materialize_owner(pool, owner)
        .await?
        .into())
}

/// Grant expiries are set by the credential provider and stored verbatim, and the table
/// rejects one that is already past, so tests pick a live absolute instant rather than
/// relying on a write-path default.
fn test_service_grant() -> TestResult<FederatedServiceGrant> {
    Ok(FederatedServiceGrant::new(
        b"opaque-grant".to_vec(),
        time::OffsetDateTime::now_utc() + time::Duration::days(8),
    )?)
}

fn test_federated_owner(upstream_org_id: &str) -> TestResult<FederatedOwner> {
    Ok(FederatedOwner {
        issuer: UpstreamIssuer::parse("example-issuer")?,
        org_id: UpstreamOrgId::parse(upstream_org_id)?,
        subject_id: UpstreamSubjectId::parse("123")?,
    })
}

// Direct is only a placeholder: no test using this depends on substrate behavior.
fn direct_sandbox_controller(pool: &PgPool) -> SandboxController {
    SandboxController::new(crate::controllers::substrate::Substrate::from_direct(
        crate::controllers::substrate::direct::DirectSubstrate::new(pool.clone()),
    ))
}

async fn install_failing_federated_grant_triggers(pool: &PgPool) -> TestResult {
    sqlx::query(sql!(
        "CREATE OR REPLACE FUNCTION fail_sandbox_federated_service_grant_insert()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
             RAISE EXCEPTION 'forced sandbox_federated_service_grants failure';
         END
         $$"
    ))
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "CREATE TRIGGER fail_sandbox_federated_service_grant_insert
         BEFORE INSERT ON sandbox_federated_service_grants
         FOR EACH ROW
         EXECUTE FUNCTION fail_sandbox_federated_service_grant_insert()"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

fn pooled_create_request(
    name: &str,
    network_policy: Option<NetworkPolicyV1>,
) -> CreateSandboxRequest {
    CreateSandboxRequest {
        name: Some(name.to_string()),
        template: None,
        runtime: Some("fc".to_string()),
        boot_mode: Some("pool".to_string()),
        env: Default::default(),
        metadata: Default::default(),
        vcpu: Some(4),
        memory_mib: Some(2048),
        disk_mib: None,
        file_changes: None,
        auto_lifecycle: None,
        wait_until_ready: false,
        network_policy,
        node_pool: None,
    }
}

fn pooled_create_input<'a>(
    pool: &'a PgPool,
    req: &'a CreateSandboxRequest,
    requested_template: &'a TemplateReference,
    auto_lifecycle: &'a AutoLifecyclePolicy,
    template_network_policy: &'a NetworkPolicyV1,
    create_owner: &'a SandboxCreateOwner,
) -> PooledCreateInput<'a> {
    PooledCreateInput {
        federated_grant_audience: DEFAULT_FEDERATED_GRANT_AUDIENCE.parse().unwrap(),
        pool,
        req,
        env: &req.env,
        metric_identity: SandboxApiMetricIdentity::native("create"),
        planned_sandbox_id: SandboxId::new(),
        sandbox_id_pinned: false,
        create_owner,
        auto_lifecycle,
        backend: "fc",
        requested_template,
        template: "seed",
        content_hash: "sha256:pooled",
        template_release_id: Uuid::now_v7(),
        template_network_policy,
        template_network_policy_generation: 1,
        network_policy_override: req.network_policy.as_ref(),
        egress_profiles: Default::default(),
        boot_mode: CreateBootMode::Pool,
        federated_owner: None,
        federated_service_grant: None,
        volume_encryption: false,
    }
}

fn pooled_claim_with_pod(
    sandbox_id: SandboxId,
    node_id: NodeId,
    pod_name: &str,
) -> PooledExecutionClaim {
    PooledExecutionClaim {
        claim_id: Uuid::now_v7(),
        pool_slot_id: Some(Uuid::now_v7()),
        sandbox_id,
        content_hash: "sha256:pooled".to_string(),
        pod_name: pod_name.to_string(),
        pod_uid: Some(PodUid::parse("pod-uid").unwrap()),
        execution_generation: 1,
        pod_ip: "10.90.0.11".to_string(),
        vcpu: Some(4),
        memory_mib: Some(2048),
        disk_mib: None,
        disk_io_limits: Default::default(),
        active_node_id: Some(node_id),
        route_identity: None,
    }
}

fn pooled_claim(sandbox_id: SandboxId, node_id: NodeId) -> PooledExecutionClaim {
    pooled_claim_with_pod(sandbox_id, node_id, "space-sbx-pooled-g1")
}

fn sandbox_network_policy_element(sandbox_id: SandboxId) -> String {
    NetworkPolicyTarget::sandbox(sandbox_id).as_element_string()
}

async fn pooled_slot_and_sandbox_state(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> TestResult<(String, String)> {
    Ok(sqlx::query_as(sql!(
        "SELECT ps.state, s.lifecycle
         FROM kubernetes_pool_slots ps
         JOIN sandboxes s ON s.id = ps.reserved_sandbox_id
         WHERE ps.reserved_sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

struct RetryingPoolSubstrate {
    pool: PgPool,
    claims: Mutex<VecDeque<PooledExecutionClaim>>,
    prepare_results: Mutex<VecDeque<bool>>,
    prepared_policies: Mutex<Vec<AutoLifecyclePolicy>>,
    policy_after_prepare_failure: Option<AutoLifecyclePolicy>,
    pool_config_id: Uuid,
    template_release_id: Uuid,
    claim_calls: AtomicUsize,
    prepare_calls: AtomicUsize,
    /// Reported alongside a miss, as the Kubernetes substrate does when a
    /// sandbox's node-group boundary admits no pooled pod.
    miss_refusal: Option<PooledClaimRefusal>,
    /// Answers for `lookup_admitted_pooled_execution`, in call order.
    admitted_lookups: Mutex<VecDeque<AdmittedPoolClaimLookup>>,
    admitted_lookup_calls: AtomicUsize,
    rollback_calls: AtomicUsize,
    prepare_wait_until_ready: Mutex<Vec<bool>>,
}

impl RetryingPoolSubstrate {
    fn new(
        pool: PgPool,
        pool_config_id: Uuid,
        template_release_id: Uuid,
        claims: Vec<PooledExecutionClaim>,
        prepare_results: Vec<bool>,
    ) -> Self {
        Self {
            pool,
            claims: Mutex::new(claims.into()),
            prepare_results: Mutex::new(prepare_results.into()),
            prepared_policies: Mutex::new(Vec::new()),
            policy_after_prepare_failure: None,
            pool_config_id,
            template_release_id,
            claim_calls: AtomicUsize::new(0),
            prepare_calls: AtomicUsize::new(0),
            miss_refusal: None,
            admitted_lookups: Mutex::new(VecDeque::new()),
            admitted_lookup_calls: AtomicUsize::new(0),
            rollback_calls: AtomicUsize::new(0),
            prepare_wait_until_ready: Mutex::new(Vec::new()),
        }
    }

    fn with_admitted_lookup(self, lookup: AdmittedPoolClaimLookup) -> Self {
        self.admitted_lookups.lock().unwrap().push_back(lookup);
        self
    }

    fn admitted_lookup_calls(&self) -> usize {
        self.admitted_lookup_calls.load(Ordering::SeqCst)
    }

    fn rollback_calls(&self) -> usize {
        self.rollback_calls.load(Ordering::SeqCst)
    }

    fn prepare_wait_until_ready(&self) -> Vec<bool> {
        self.prepare_wait_until_ready.lock().unwrap().clone()
    }

    fn refusing(mut self, message: &str) -> Self {
        self.miss_refusal = Some(PooledClaimRefusal::PlacementIncompatible {
            message: message.to_string(),
        });
        self
    }

    fn unresolvable(mut self, message: &str) -> Self {
        self.miss_refusal = Some(PooledClaimRefusal::PlacementUnresolvable {
            message: message.to_string(),
        });
        self
    }

    fn claim_calls(&self) -> usize {
        self.claim_calls.load(Ordering::SeqCst)
    }

    fn prepare_calls(&self) -> usize {
        self.prepare_calls.load(Ordering::SeqCst)
    }
}

impl SubstrateAdapter for RetryingPoolSubstrate {
    fn managed_by(&self) -> &'static str {
        "k8s"
    }

    fn capabilities(&self) -> SubstrateCapabilities {
        SubstrateCapabilities::KUBERNETES
    }

    async fn create_execution(
        &self,
        _request: CreateExecutionRequest<'_>,
    ) -> Result<crate::controllers::substrate::CreateExecutionOutcome, SubstrateError> {
        panic!("unused")
    }

    async fn delete_execution(&self, _sandbox_id: SandboxId) -> Result<(), SubstrateError> {
        panic!("unused")
    }

    async fn retire_execution(
        &self,
        _request: crate::controllers::substrate::RetireExecutionRequest,
    ) -> Result<crate::controllers::substrate::RetireExecutionOutcome, SubstrateError> {
        panic!("unused")
    }

    async fn start_execution(
        &self,
        _metric_identity: SandboxApiMetricIdentity,
        _sandbox_id: SandboxId,
        _options: ActivationOptions,
        _charge: crate::controllers::substrate::RecoveryCharge,
    ) -> Result<crate::controllers::substrate::ExecutionActivation, SubstrateError> {
        panic!("unused")
    }

    async fn restore_execution(
        &self,
        _metric_identity: SandboxApiMetricIdentity,
        _sandbox_id: SandboxId,
        _options: ActivationOptions,
        _charge: crate::controllers::substrate::RecoveryCharge,
    ) -> Result<crate::controllers::substrate::ActivationCompletion, SubstrateError> {
        panic!("unused")
    }

    async fn suspend_execution(
        &self,
        _sandbox_id: SandboxId,
    ) -> Result<Option<crate::controllers::substrate::PublishedLocalArtifact>, SubstrateError> {
        panic!("unused")
    }

    async fn wake_execution(
        &self,
        _metric_identity: SandboxApiMetricIdentity,
        _sandbox: &Sandbox,
        _timeout: Duration,
        _wake_activity: Option<crate::controllers::substrate::ExecutionActivityKind>,
        _charge: crate::controllers::substrate::RecoveryCharge,
    ) -> Result<crate::controllers::substrate::ExecutionActivation, SubstrateError> {
        panic!("unused")
    }

    async fn runtime_client(
        &self,
        _sandbox_id: SandboxId,
        _charge: crate::controllers::substrate::RecoveryCharge,
    ) -> Result<RuntimeClient, ControllerError> {
        panic!("unused")
    }

    async fn reachable_runtime_client(
        &self,
        _sandbox_id: SandboxId,
    ) -> Result<Option<RuntimeClient>, ControllerError> {
        panic!("unused")
    }

    async fn probe_runtime_client(
        &self,
        _sandbox_id: SandboxId,
    ) -> Result<Option<RuntimeClient>, ControllerError> {
        panic!("unused")
    }

    async fn access_runtime_client(
        &self,
        _metric_identity: SandboxApiMetricIdentity,
        _sandbox: &Sandbox,
        _activity: ExecutionActivityKind,
        _timeout: Duration,
        _charge: crate::controllers::substrate::RecoveryCharge,
    ) -> Result<RuntimeClient, ControllerError> {
        panic!("unused")
    }

    async fn builder_client(
        &self,
    ) -> Result<crate::controllers::substrate::BuilderClient, ControllerError> {
        panic!("unused")
    }

    async fn builder_client_for_node(
        &self,
        _node_id: NodeId,
    ) -> Result<crate::controllers::substrate::BuilderClient, ControllerError> {
        panic!("unused")
    }

    fn pool_eligible_for_request(
        &self,
        _boot_mode: CreateBootMode,
        _backend: &str,
        _wait_until_ready: bool,
        _disk_mib: Option<u32>,
        _file_changes_enabled: bool,
        _implicit_wake_enabled: bool,
    ) -> bool {
        true
    }

    fn pool_supports_request_env(&self) -> bool {
        true
    }

    async fn claim_pooled_execution(
        &self,
        _request: &PooledExecutionClaimRequest<'_>,
    ) -> Result<PooledExecutionClaimAttempt, ControllerError> {
        self.claim_calls.fetch_add(1, Ordering::SeqCst);
        Ok(match self.claims.lock().unwrap().pop_front() {
            Some(claim) => PooledExecutionClaimAttempt::claimed(
                claim,
                PooledExecutionPoolShape::new(4, 2048, Default::default()),
            ),
            None => {
                let pool_shape = Some(PooledExecutionPoolShape::new(4, 2048, Default::default()));
                match self.miss_refusal.clone() {
                    Some(refusal) => PooledExecutionClaimAttempt::refused(pool_shape, refusal),
                    None => PooledExecutionClaimAttempt::missed(pool_shape),
                }
            }
        })
    }

    async fn lookup_admitted_pooled_execution(
        &self,
        _request: &PooledExecutionClaimRequest<'_>,
        _admitted: AdmittedPoolClaim,
        _sandbox_id: SandboxId,
    ) -> Result<AdmittedPoolClaimLookup, ControllerError> {
        self.admitted_lookup_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self
            .admitted_lookups
            .lock()
            .unwrap()
            .pop_front()
            .expect("missing test admitted lookup"))
    }

    async fn rollback_pooled_execution_claim(
        &self,
        _claim: &PooledExecutionClaim,
    ) -> Result<(), ControllerError> {
        self.rollback_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn persist_pooled_execution_claim(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        request: &PooledExecutionClaimRequest<'_>,
        claim: &PooledExecutionClaim,
    ) -> Result<(), ControllerError> {
        sqlx::query(sql!(
            "INSERT INTO kubernetes_pool_slots (
                 config_id, template_release_id, reserved_sandbox_id, template_name,
                 content_hash, vcpu, memory_mib, pod_name, pod_namespace, pod_uid,
                 pod_ip, k8s_node_name, state, claim_id, ready_at, claimed_at
             )
             VALUES (
                 $1, $2, $3, $4, $5, $6, $7, $8, 'space', $9,
                 $10::inet, 'worker-1', 'claimed', $11, NOW(), NOW()
             )"
        ))
        .bind(self.pool_config_id)
        .bind(self.template_release_id)
        .bind(claim.sandbox_id)
        .bind(request.template)
        .bind(&claim.content_hash)
        .bind(claim.vcpu.unwrap_or(4) as i32)
        .bind(claim.memory_mib.unwrap_or(2048) as i32)
        .bind(&claim.pod_name)
        .bind(claim.pod_uid.as_ref().map(PodUid::as_str))
        .bind(&claim.pod_ip)
        .bind(claim.claim_id)
        .execute(&mut **tx)
        .await
        .map_err(|err| {
            ControllerError::internal(format!("failed to persist test pool slot: {err}"))
        })?;

        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions (
                 sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip,
                 k8s_node_name, state, active
             )
             VALUES ($1, 1, $2, 'space', $3, $4::inet, 'worker-1', 'running', TRUE)"
        ))
        .bind(claim.sandbox_id)
        .bind(&claim.pod_name)
        .bind(claim.pod_uid.as_ref().map(PodUid::as_str))
        .bind(&claim.pod_ip)
        .execute(&mut **tx)
        .await
        .map_err(|err| {
            ControllerError::internal(format!("failed to persist test pool execution: {err}"))
        })?;

        Ok(())
    }

    async fn cleanup_failed_pooled_prepare(
        &self,
        claim: &PooledExecutionClaim,
    ) -> Result<PooledPrepareFailureCleanup, ControllerError> {
        let mut tx = self.pool.begin().await.map_err(|err| {
            ControllerError::internal(format!(
                "failed to start test cleanup-pending pool slot transaction: {err}"
            ))
        })?;
        let marked = pplx_space_cluster_pool::WarmPoolStore::mark_claimed_slot_cleanup_pending(
            &mut tx,
            claim.claim_id,
            claim.sandbox_id,
        )
        .await
        .map_err(|err| {
            ControllerError::internal(format!(
                "failed to mark test claimed pool slot cleanup_pending: {err}"
            ))
        })?;
        if !marked {
            return Err(ControllerError::internal(
                "test claimed pool slot missing during cleanup-pending mark",
            ));
        }
        tx.commit().await.map_err(|err| {
            ControllerError::internal(format!(
                "failed to commit test cleanup-pending pool slot transaction: {err}"
            ))
        })?;
        Ok(PooledPrepareFailureCleanup::DeferredToReconciler)
    }

    async fn prepare_pooled_execution(
        &self,
        _metric_identity: SandboxApiMetricIdentity,
        _request: &PooledExecutionClaimRequest<'_>,
        claim: &PooledExecutionClaim,
        policy: &AutoLifecyclePolicy,
        wait_until_ready: bool,
        _timeout: Duration,
    ) -> Result<(), ControllerError> {
        self.prepare_calls.fetch_add(1, Ordering::SeqCst);
        self.prepare_wait_until_ready
            .lock()
            .unwrap()
            .push(wait_until_ready);
        self.prepared_policies.lock().unwrap().push(policy.clone());
        let succeeds = self
            .prepare_results
            .lock()
            .unwrap()
            .pop_front()
            .expect("missing test prepare result");
        if succeeds {
            Ok(())
        } else {
            if let Some(policy) = &self.policy_after_prepare_failure {
                pplx_space_sandbox_store::set_auto_lifecycle_policy(
                    &self.pool,
                    claim.sandbox_id,
                    policy,
                )
                .await
                .map_err(|error| ControllerError::internal(error.to_string()))?;
            }
            Err(ControllerError::service_unavailable(
                "pooled sandbox metadata update failed: status: Unknown, message: \"transport error\"",
            ))
        }
    }

    async fn resolve_node_api_endpoint(
        &self,
        _target: &NodeApiTarget<'_>,
    ) -> Result<String, SubstrateError> {
        panic!("unused")
    }

    async fn list_inventory_nodes(
        &self,
        _role_filter: Option<&str>,
    ) -> Result<Option<Vec<NodeInventoryEntry>>, SubstrateError> {
        panic!("unused")
    }
}

/// The real Kubernetes substrate with only its node RPC faked: admitted-claim
/// lookup, persistence, rollback, and failure cleanup all run against the real
/// pool store, while `prepare_pooled_execution` (the gRPC calls into the slot's
/// pod) records its arguments and succeeds.
#[cfg(feature = "kubernetes")]
struct NodeRpcStubbedKubernetes {
    inner: crate::controllers::substrate::kubernetes::KubernetesSubstrate,
    prepared: Mutex<Vec<(SandboxId, bool)>>,
}

#[cfg(feature = "kubernetes")]
impl SubstrateAdapter for NodeRpcStubbedKubernetes {
    fn managed_by(&self) -> &'static str {
        self.inner.managed_by()
    }

    fn capabilities(&self) -> SubstrateCapabilities {
        self.inner.capabilities()
    }

    async fn create_execution(
        &self,
        _request: CreateExecutionRequest<'_>,
    ) -> Result<crate::controllers::substrate::CreateExecutionOutcome, SubstrateError> {
        panic!("unused")
    }

    async fn delete_execution(&self, _sandbox_id: SandboxId) -> Result<(), SubstrateError> {
        panic!("unused")
    }

    async fn retire_execution(
        &self,
        _request: crate::controllers::substrate::RetireExecutionRequest,
    ) -> Result<crate::controllers::substrate::RetireExecutionOutcome, SubstrateError> {
        panic!("unused")
    }

    async fn start_execution(
        &self,
        _metric_identity: SandboxApiMetricIdentity,
        _sandbox_id: SandboxId,
        _options: ActivationOptions,
        _charge: crate::controllers::substrate::RecoveryCharge,
    ) -> Result<crate::controllers::substrate::ExecutionActivation, SubstrateError> {
        panic!("unused")
    }

    async fn restore_execution(
        &self,
        _metric_identity: SandboxApiMetricIdentity,
        _sandbox_id: SandboxId,
        _options: ActivationOptions,
        _charge: crate::controllers::substrate::RecoveryCharge,
    ) -> Result<crate::controllers::substrate::ActivationCompletion, SubstrateError> {
        panic!("unused")
    }

    async fn suspend_execution(
        &self,
        _sandbox_id: SandboxId,
    ) -> Result<Option<crate::controllers::substrate::PublishedLocalArtifact>, SubstrateError> {
        panic!("unused")
    }

    async fn wake_execution(
        &self,
        _metric_identity: SandboxApiMetricIdentity,
        _sandbox: &Sandbox,
        _timeout: Duration,
        _wake_activity: Option<crate::controllers::substrate::ExecutionActivityKind>,
        _charge: crate::controllers::substrate::RecoveryCharge,
    ) -> Result<crate::controllers::substrate::ExecutionActivation, SubstrateError> {
        panic!("unused")
    }

    async fn runtime_client(
        &self,
        _sandbox_id: SandboxId,
        _charge: crate::controllers::substrate::RecoveryCharge,
    ) -> Result<RuntimeClient, ControllerError> {
        panic!("unused")
    }

    async fn reachable_runtime_client(
        &self,
        _sandbox_id: SandboxId,
    ) -> Result<Option<RuntimeClient>, ControllerError> {
        panic!("unused")
    }

    async fn probe_runtime_client(
        &self,
        _sandbox_id: SandboxId,
    ) -> Result<Option<RuntimeClient>, ControllerError> {
        panic!("unused")
    }

    async fn access_runtime_client(
        &self,
        _metric_identity: SandboxApiMetricIdentity,
        _sandbox: &Sandbox,
        _activity: ExecutionActivityKind,
        _timeout: Duration,
        _charge: crate::controllers::substrate::RecoveryCharge,
    ) -> Result<RuntimeClient, ControllerError> {
        panic!("unused")
    }

    async fn builder_client(
        &self,
    ) -> Result<crate::controllers::substrate::BuilderClient, ControllerError> {
        panic!("unused")
    }

    async fn builder_client_for_node(
        &self,
        _node_id: NodeId,
    ) -> Result<crate::controllers::substrate::BuilderClient, ControllerError> {
        panic!("unused")
    }

    async fn resolve_node_api_endpoint(
        &self,
        _target: &NodeApiTarget<'_>,
    ) -> Result<String, SubstrateError> {
        panic!("unused")
    }

    async fn list_inventory_nodes(
        &self,
        _role_filter: Option<&str>,
    ) -> Result<Option<Vec<NodeInventoryEntry>>, SubstrateError> {
        panic!("unused")
    }

    fn pool_eligible_for_request(
        &self,
        boot_mode: CreateBootMode,
        backend: &str,
        wait_until_ready: bool,
        disk_mib: Option<u32>,
        file_changes_enabled: bool,
        implicit_wake_enabled: bool,
    ) -> bool {
        self.inner.pool_eligible_for_request(
            boot_mode,
            backend,
            wait_until_ready,
            disk_mib,
            file_changes_enabled,
            implicit_wake_enabled,
        )
    }

    fn pool_supports_request_env(&self) -> bool {
        self.inner.pool_supports_request_env()
    }

    async fn claim_pooled_execution(
        &self,
        _request: &PooledExecutionClaimRequest<'_>,
    ) -> Result<PooledExecutionClaimAttempt, ControllerError> {
        panic!("an admitted create must never claim a different slot")
    }

    async fn lookup_admitted_pooled_execution(
        &self,
        request: &PooledExecutionClaimRequest<'_>,
        admitted: AdmittedPoolClaim,
        sandbox_id: SandboxId,
    ) -> Result<AdmittedPoolClaimLookup, ControllerError> {
        self.inner
            .lookup_admitted_pooled_execution(request, admitted, sandbox_id)
            .await
    }

    async fn rollback_pooled_execution_claim(
        &self,
        claim: &PooledExecutionClaim,
    ) -> Result<(), ControllerError> {
        self.inner.rollback_pooled_execution_claim(claim).await
    }

    async fn persist_pooled_execution_claim(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        request: &PooledExecutionClaimRequest<'_>,
        claim: &PooledExecutionClaim,
    ) -> Result<(), ControllerError> {
        self.inner
            .persist_pooled_execution_claim(tx, request, claim)
            .await
    }

    async fn cleanup_failed_pooled_prepare(
        &self,
        claim: &PooledExecutionClaim,
    ) -> Result<PooledPrepareFailureCleanup, ControllerError> {
        self.inner.cleanup_failed_pooled_prepare(claim).await
    }

    async fn prepare_pooled_execution(
        &self,
        _metric_identity: SandboxApiMetricIdentity,
        _request: &PooledExecutionClaimRequest<'_>,
        claim: &PooledExecutionClaim,
        _policy: &AutoLifecyclePolicy,
        wait_until_ready: bool,
        _timeout: Duration,
    ) -> Result<(), ControllerError> {
        self.prepared
            .lock()
            .unwrap()
            .push((claim.sandbox_id, wait_until_ready));
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_insert_pooled_sandbox_writes_api_row() -> TestResult {
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    let mut env = std::collections::BTreeMap::new();
    env.insert("HELLO".to_string(), "world".to_string());
    let mut metadata = SandboxMetadata::default();
    metadata.insert("warm".to_string(), SandboxMetadataValue::Bool(true));
    let network_policy_override = NetworkPolicyV1 {
        tls_interception: pplx_space_net_common::TlsInterception::On,
        ..Default::default()
    };
    let mut req = pooled_create_request("pooled-sandbox", Some(network_policy_override));
    req.env = env;
    req.metadata = metadata;
    req.disk_mib = Some(512);
    let template_network_policy = NetworkPolicyV1::deny_all();
    let requested_template = TemplateReference::latest(TemplateName::parse("seed").unwrap());
    let create_owner = SandboxCreateOwner::local(&actor);
    let input = pooled_create_input(
        &db.pool,
        &req,
        &requested_template,
        &auto_lifecycle,
        &template_network_policy,
        &create_owner,
    );
    let mut claim = pooled_claim(sandbox_id, node_id);
    claim.disk_mib = Some(32768);
    claim.disk_io_limits =
        pplx_space_common_core::types::DiskIoLimits::try_new(5_000, 200).unwrap();
    let mut tx = db.pool.begin().await?;

    let inserted_id =
        insert_pooled_sandbox(&mut tx, &input, &create_owner, &claim, "space").await?;
    tx.commit().await?;

    assert_eq!(inserted_id, sandbox_id);
    let (
        name,
        user_id,
        org_id,
        managed_by,
        lifecycle,
        placement,
        active_node_id,
        target_node_id,
        template_id,
        content_hash,
        vcpu,
        memory_mib,
        disk_mib,
        network_policy_generation,
    ): InsertedPooledSandboxRow = sqlx::query_as(sql!(
        "SELECT name, user_id, org_id, managed_by, lifecycle, placement,
                active_node_id, target_node_id, template_id, content_hash,
                vcpu, memory_mib, disk_mib, network_policy_generation
         FROM sandboxes
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(name.as_deref(), Some("pooled-sandbox"));
    assert_eq!(user_id, actor.id);
    assert_eq!(org_id, actor.org_id);
    assert_eq!(managed_by, "space");
    assert_eq!(lifecycle, "paused");
    assert_eq!(placement, "on_node");
    assert_eq!(active_node_id, Some(node_id));
    assert_eq!(target_node_id, Some(node_id));
    assert_eq!(template_id.as_deref(), Some("seed"));
    assert_eq!(content_hash.as_deref(), Some("sha256:pooled"));
    assert_eq!(vcpu, Some(4));
    assert_eq!(memory_mib, Some(2048));
    assert_eq!(disk_mib, Some(32768));
    assert_eq!(network_policy_generation, 1);

    let disk_io = sqlx::query_as::<_, (i64, i64)>(sql!(
        "SELECT disk_io_max_operations_per_sec,
                disk_io_max_bandwidth_mib_per_sec
         FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(disk_io, (5_000, 200));

    let (env, metadata, network_policy): (
        serde_json::Value,
        serde_json::Value,
        Option<serde_json::Value>,
    ) = sqlx::query_as(sql!(
        "SELECT env, metadata, network_policy_override_json
         FROM sandboxes
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(env, serde_json::json!({"HELLO": "world"}));
    assert_eq!(metadata, serde_json::json!({"warm": true}));
    let network_policy: NetworkPolicyV1 =
        serde_json::from_value(network_policy.expect("network policy override"))?;
    assert_eq!(
        network_policy.tls_interception,
        pplx_space_net_common::TlsInterception::On
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_insert_pooled_sandbox_can_write_federated_owner_row() -> TestResult {
    let db = pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    let req = pooled_create_request("federated-pooled-sandbox", None);
    let template_network_policy = NetworkPolicyV1::deny_all();
    let requested_template = TemplateReference::latest(TemplateName::parse("seed").unwrap());
    let upstream_org_id = "example-org-not-a-space-uuid".to_string();
    let federated_owner = FederatedOwner {
        issuer: UpstreamIssuer::parse("example-issuer")?,
        org_id: UpstreamOrgId::parse(upstream_org_id.clone())?,
        subject_id: UpstreamSubjectId::parse("123")?,
    };
    let create_owner = mapped_federated_owner(&db.pool, &federated_owner).await?;
    let mut tx = db.pool.begin().await?;
    let mut input = pooled_create_input(
        &db.pool,
        &req,
        &requested_template,
        &auto_lifecycle,
        &template_network_policy,
        &create_owner,
    );
    input.federated_owner = Some(&federated_owner);
    let claim = pooled_claim(sandbox_id, node_id);

    let inserted_id =
        insert_pooled_sandbox(&mut tx, &input, &create_owner, &claim, "space").await?;
    tx.commit().await?;

    assert_eq!(inserted_id, sandbox_id);
    let (
        user_id,
        org_id,
        owner_source,
        upstream_issuer,
        stored_upstream_org_id,
        upstream_subject_id,
        lifecycle,
        placement,
        active_node_id,
        target_node_id,
    ): InsertedFederatedPooledSandboxRow = sqlx::query_as(sql!(
        "SELECT s.user_id, s.org_id, u.owner_source, u.upstream_issuer, o.upstream_org_id,
                u.upstream_subject_id, s.lifecycle, s.placement, s.active_node_id, s.target_node_id
         FROM sandboxes s
         JOIN users u ON u.id = s.user_id
         JOIN organizations o ON o.id = s.org_id
         WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;

    assert_eq!(user_id, create_owner.user_id);
    assert_eq!(org_id, create_owner.org_id);
    assert_eq!(owner_source, "federated_identity");
    assert_eq!(upstream_issuer.as_deref(), Some("example-issuer"));
    assert_eq!(
        stored_upstream_org_id.as_deref(),
        Some(upstream_org_id.as_str())
    );
    assert_eq!(upstream_subject_id.as_deref(), Some("123"));
    assert_eq!(lifecycle, "paused");
    assert_eq!(placement, "on_node");
    assert_eq!(active_node_id, Some(node_id));
    assert_eq!(target_node_id, Some(node_id));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_pooled_federated_sandbox_persists_service_grant() -> TestResult {
    let db = pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    let req = pooled_create_request("federated-pooled-sandbox", None);
    let template_network_policy = NetworkPolicyV1::deny_all();
    let requested_template = TemplateReference::latest(TemplateName::parse("seed").unwrap());
    let upstream_org_id = "example-org-not-a-space-uuid";
    let federated_owner = FederatedOwner {
        issuer: UpstreamIssuer::parse("example-issuer")?,
        org_id: UpstreamOrgId::parse(upstream_org_id)?,
        subject_id: UpstreamSubjectId::parse("123")?,
    };
    let federated_service_grant = test_service_grant()?;
    let create_owner = mapped_federated_owner(&db.pool, &federated_owner).await?;
    let mut tx = db.pool.begin().await?;
    let mut input = pooled_create_input(
        &db.pool,
        &req,
        &requested_template,
        &auto_lifecycle,
        &template_network_policy,
        &create_owner,
    );
    input.federated_grant_audience = "test-provider".parse()?;
    input.federated_owner = Some(&federated_owner);
    input.federated_service_grant = Some(&federated_service_grant);
    let claim = pooled_claim(sandbox_id, node_id);

    insert_pooled_sandbox(&mut tx, &input, &create_owner, &claim, "space").await?;
    upsert_pooled_federated_service_grant(&mut tx, sandbox_id, &input).await?;
    tx.commit().await?;

    let (grant_audience, grant_purpose, grant_blob): (String, String, Vec<u8>) =
        sqlx::query_as(sql!(
            "SELECT grant_audience, grant_purpose, grant_blob
         FROM sandbox_federated_service_grants
         WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&db.pool)
        .await?;

    assert_eq!(grant_audience, "test-provider");
    assert_eq!(grant_purpose, FEDERATED_OWNER_GRANT_PURPOSE);
    assert_eq!(grant_blob, b"opaque-grant");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_cold_federated_create_is_atomic_with_grant_upsert() -> TestResult {
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let release_id = insert_template_release(&db.pool, "seed", "sha256:federated-cold").await?;
    insert_template_release_tag(&db.pool, release_id, "seed", "latest").await?;
    install_failing_federated_grant_triggers(&db.pool).await?;
    let controller = direct_sandbox_controller(&db.pool);
    let mut req = pooled_create_request("federated-cold-sandbox", None);
    req.boot_mode = Some("cold".to_string());
    req.template = Some("seed:latest".parse()?);
    let federated_owner = test_federated_owner("8a3f64c2-1f2b-4c4e-9a90-1f0d4f6f5a01")?;
    let federated_service_grant = test_service_grant()?;

    let result = controller
        .create(
            &db.pool,
            &actor,
            &req,
            SandboxApiMetricIdentity::native("create"),
            FederatedSandboxCreateContext::new(
                Some(&federated_owner),
                Some(&federated_service_grant),
            ),
        )
        .await;

    result.unwrap_err();
    let (sandbox_count,): (i64,) = sqlx::query_as(sql!(
        "SELECT COUNT(*)
         FROM sandboxes s
         JOIN users u ON u.id = s.user_id
         JOIN organizations o ON o.id = s.org_id
         WHERE u.owner_source = 'federated_identity'
           AND o.owner_source = 'federated_identity'
           AND u.upstream_issuer = $1
           AND o.upstream_issuer = $1
           AND o.upstream_org_id = $2
           AND u.upstream_subject_id = $3"
    ))
    .bind(federated_owner.issuer.as_str())
    .bind(federated_owner.org_id.as_str())
    .bind(federated_owner.subject_id.as_str())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(sandbox_count, 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_federated_create_denies_api_key_org_specific_template() -> TestResult {
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let federated_owner = test_federated_owner("federated-template-denied-org")?;
    let create_owner = mapped_federated_owner(&db.pool, &federated_owner).await?;
    assert_ne!(create_owner.org_id, actor.org_id);
    insert_org_specific_template_release(
        &db.pool,
        "api-key-secret",
        "sha256:federated-denied",
        actor.org_id,
    )
    .await?;
    let controller = direct_sandbox_controller(&db.pool);
    let mut req = pooled_create_request("federated-denied-template", None);
    req.boot_mode = Some("cold".to_string());
    req.template = Some("api-key-secret:latest".parse()?);
    let federated_service_grant = test_service_grant()?;

    let result = controller
        .create(
            &db.pool,
            &actor,
            &req,
            SandboxApiMetricIdentity::native("create"),
            FederatedSandboxCreateContext::new(
                Some(&federated_owner),
                Some(&federated_service_grant),
            ),
        )
        .await;

    let error = result.unwrap_err();
    assert_eq!(error.status(), StatusCode::NOT_FOUND);
    let sandbox_exists: bool = sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1
             FROM sandboxes
             WHERE template_id = 'api-key-secret'
         )"
    ))
    .fetch_one(&db.pool)
    .await?;
    assert!(!sandbox_exists);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_federated_create_allows_mapped_org_specific_template() -> TestResult {
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let federated_owner = test_federated_owner("federated-template-allowed-org")?;
    let create_owner = mapped_federated_owner(&db.pool, &federated_owner).await?;
    assert_ne!(create_owner.org_id, actor.org_id);
    insert_org_specific_template_release(
        &db.pool,
        "federated-secret",
        "sha256:federated-allowed",
        create_owner.org_id,
    )
    .await?;
    install_failing_federated_grant_triggers(&db.pool).await?;
    let controller = direct_sandbox_controller(&db.pool);
    let mut req = pooled_create_request("federated-allowed-template", None);
    req.boot_mode = Some("cold".to_string());
    req.template = Some("federated-secret:latest".parse()?);
    let federated_service_grant = test_service_grant()?;

    let result = controller
        .create(
            &db.pool,
            &actor,
            &req,
            SandboxApiMetricIdentity::native("create"),
            FederatedSandboxCreateContext::new(
                Some(&federated_owner),
                Some(&federated_service_grant),
            ),
        )
        .await;

    let error = result.unwrap_err();
    assert_eq!(error.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let sandbox_exists: bool = sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1
             FROM sandboxes
             WHERE template_id = 'federated-secret'
         )"
    ))
    .fetch_one(&db.pool)
    .await?;
    assert!(!sandbox_exists);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_pool_configured_resources_uses_enabled_release_shape() -> TestResult {
    let db = pg_test_db().await?;
    let release_id =
        insert_template_release(&db.pool, "app-runtime", "sha256:pool-default").await?;

    sqlx::query(sql!(
        "INSERT INTO kubernetes_pool_configs (
             id, template_release_id, vcpu, memory_mib, disk_mib, target_ready_slots,
             refill_per_tick, max_warming_slots, namespace, enabled,
             disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec
         )
         VALUES
             ($1, $4, 16, 32768, 65536, 200, 1, 1, 'space', false, $5, $6),
             ($2, $4, 4, 2048, 4096, 25, 1, 1, 'space', true, $5, $6),
             ($3, $4, 2, 8192, 32768, 200, 1, 1, 'space', true, $5, $6)"
    ))
    .bind(Uuid::now_v7())
    .bind(Uuid::now_v7())
    .bind(Uuid::now_v7())
    .bind(release_id)
    .bind(6_000_i64)
    .bind(150_i64)
    .execute(&db.pool)
    .await?;

    let resources =
        resolve_kubernetes_pool_configured_resources(&db.pool, "app-runtime", release_id)
            .await?
            .expect("enabled pool config should provide resources");

    assert_eq!(resources.resources.vcpu, Some(2));
    assert_eq!(resources.resources.memory_mib, Some(8192));
    assert_eq!(resources.resources.disk_mib, Some(32768));
    assert_eq!(
        resources.disk_io_limits,
        pplx_space_common_core::types::DiskIoLimits::try_new(6_000, 150)?
    );
    sqlx::query(sql!(
        "UPDATE kubernetes_pool_configs SET disk_mib = 131073
         WHERE template_release_id = $1 AND vcpu = 2"
    ))
    .bind(release_id)
    .execute(&db.pool)
    .await?;
    let Err(error) =
        resolve_kubernetes_pool_configured_resources(&db.pool, "app-runtime", release_id).await
    else {
        panic!("unsupported inherited configuration must not become a client size error");
    };
    assert_eq!(error.status(), StatusCode::CONFLICT);
    Ok(())
}

async fn insert_inventory_shape(
    pool: &PgPool,
    role: &str,
    content_hash: &str,
    shape: (i32, i32),
    observed_secs_ago: i32,
) -> TestResult {
    let node_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.10'::INET, $2, 'space', 'ready')"
    ))
    .bind(node_id)
    .bind(role)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_template_inventory (
             node_id, template_name, content_hash, runtime, vcpu, memory_mib, observed_at
         )
         VALUES ($1, 'app-runtime', $2, 'fc', $3, $4, now() - make_interval(secs => $5))"
    ))
    .bind(node_id)
    .bind(content_hash)
    .bind(shape.0)
    .bind(shape.1)
    .bind(f64::from(observed_secs_ago))
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_template_build_shape_prefers_the_builder_inventory() -> TestResult {
    use crate::template_inventory_store::{TemplateBuildShape, release_build_shape};
    let db = pg_test_db().await?;
    insert_template_release(&db.pool, "app-runtime", "sha256:shape").await?;
    insert_inventory_shape(&db.pool, "worker", "sha256:shape", (2, 2048), 0).await?;
    insert_inventory_shape(&db.pool, "builder", "sha256:shape", (4, 4096), 60).await?;
    insert_inventory_shape(&db.pool, "builder", "sha256:other", (8, 8192), 0).await?;
    insert_inventory_shape(&db.pool, "builder", "sha256:partial", (4, 0), 0).await?;

    let template = TemplateName::parse("app-runtime").unwrap();
    let load = |content_hash| {
        release_build_shape(&db.pool, &template, content_hash, RuntimeKind::Firecracker)
    };
    assert_eq!(
        load("sha256:shape").await?,
        Some(TemplateBuildShape {
            vcpu: pplx_space_common_core::types::VcpuCount::try_from_u32(4).unwrap(),
            memory: pplx_space_common_core::types::MemorySize::mib(4096).unwrap(),
        })
    );
    assert_eq!(load("sha256:partial").await?, None);
    assert_eq!(load("sha256:absent").await?, None);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_kubernetes_create_fills_unset_resources_from_the_template_shape() -> TestResult {
    let db = pg_test_db().await?;
    let release_id = insert_template_release(&db.pool, "app-runtime", "sha256:shape").await?;
    insert_inventory_shape(&db.pool, "builder", "sha256:shape", (4, 4096), 0).await?;
    let template = TemplateName::parse("app-runtime").unwrap();
    let k8s = SubstrateCapabilities::KUBERNETES;
    let space = SubstrateCapabilities::DIRECT;
    let resolve_with = |substrate, boot_mode, requested, restoring| {
        resolve_cold_create_resources(
            &db.pool,
            ColdCreateShapeInput {
                capabilities: substrate,
                runtime: RuntimeKind::Firecracker,
                boot_mode,
                template: &template,
                content_hash: "sha256:shape",
                template_release_id: release_id,
                requested,
                restoring,
            },
        )
    };
    let resolve =
        |substrate, boot_mode, requested| resolve_with(substrate, boot_mode, requested, false);
    let unset = || SandboxResources {
        vcpu: None,
        memory_mib: None,
        disk_mib: None,
    };
    let shape =
        |resources: &SandboxResources| (resources.vcpu, resources.memory_mib, resources.disk_mib);

    let (resources, _, source) = resolve(k8s, CreateBootMode::Fallthrough, unset()).await?;
    assert_eq!(source, Some(CreateShapeSource::Build));
    assert_eq!(
        shape(&resources),
        (Some(4), Some(4096), None),
        "an unshaped Kubernetes create must not boot at the shim's fixed defaults"
    );

    let (resources, _, source) = resolve(
        k8s,
        CreateBootMode::Cold,
        SandboxResources {
            vcpu: None,
            memory_mib: Some(2048),
            disk_mib: Some(8192),
        },
    )
    .await?;
    assert_eq!(
        shape(&resources),
        (Some(4), Some(2048), Some(8192)),
        "requested values win; only unset ones come from the template"
    );
    assert_eq!(
        source,
        Some(CreateShapeSource::Build),
        "a partial request reports the fill"
    );

    let (resources, _, source) = resolve(space, CreateBootMode::Fallthrough, unset()).await?;
    assert_eq!(source, None);
    assert_eq!(
        shape(&resources),
        (None, None, None),
        "direct creates keep the node's snapshot or metadata shape"
    );

    let (resources, _, _) = resolve_with(k8s, CreateBootMode::Cold, unset(), true).await?;
    assert_eq!(
        shape(&resources),
        (None, None, None),
        "a fork keeps its source snapshot's shape"
    );

    sqlx::query(sql!(
        "INSERT INTO kubernetes_pool_configs (
             id, template_release_id, vcpu, memory_mib, disk_mib, target_ready_slots,
             refill_per_tick, max_warming_slots, namespace, enabled
         )
         VALUES ($1, $2, 2, 8192, 32768, 1, 1, 1, 'space', true)"
    ))
    .bind(Uuid::now_v7())
    .bind(release_id)
    .execute(&db.pool)
    .await?;
    let (resources, _, source) = resolve(k8s, CreateBootMode::Fallthrough, unset()).await?;
    assert_eq!(
        shape(&resources),
        (Some(2), Some(8192), Some(32768)),
        "a configured pool shape still wins for a fallthrough create"
    );
    assert_eq!(source, Some(CreateShapeSource::PoolConfig));

    // A release with no build shape falls back to the shim default.
    let (resources, _, source) = resolve_cold_create_resources(
        &db.pool,
        ColdCreateShapeInput {
            capabilities: k8s,
            runtime: RuntimeKind::Firecracker,
            boot_mode: CreateBootMode::Cold,
            template: &template,
            content_hash: "sha256:unshaped",
            template_release_id: release_id,
            requested: unset(),
            restoring: false,
        },
    )
    .await?;
    assert_eq!(shape(&resources), (None, None, None));
    assert_eq!(source, Some(CreateShapeSource::Default));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_pool_miss_can_retain_rolling_policy_shape_before_materialization() -> TestResult {
    let db = pg_test_db().await?;
    let release_id = insert_template_release(&db.pool, "app-runtime", "sha256:new-release").await?;

    sqlx::query(sql!(
        "INSERT INTO warm_pool_policies (
             template_name, runtime, vcpu, memory_mib, disk_mib, min_ready_slots,
             max_ready_slots, refill_per_tick, max_warming_slots, retain_releases,
             disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec
         )
         VALUES ('app-runtime', 'fc', 4, 8192, 32768, 1, 4, 1, 2, 1, 2000, 200)"
    ))
    .execute(&db.pool)
    .await?;

    let assignment =
        resolve_kubernetes_pool_configured_resources(&db.pool, "app-runtime", release_id)
            .await?
            .expect("enabled policy should retain its shape before config materialization");

    assert_eq!(assignment.resources.vcpu, Some(4));
    assert_eq!(assignment.resources.memory_mib, Some(8192));
    assert_eq!(assignment.resources.disk_mib, Some(32768));
    assert_eq!(assignment.source, CreateShapeSource::Policy);
    assert_eq!(
        assignment.disk_io_limits,
        DiskIoLimits::try_new(2_000, 200).unwrap()
    );
    sqlx::query(sql!(
        "UPDATE warm_pool_policies SET disk_mib = 131073 WHERE template_name = 'app-runtime'"
    ))
    .execute(&db.pool)
    .await?;
    let Err(error) =
        resolve_kubernetes_pool_configured_resources(&db.pool, "app-runtime", release_id).await
    else {
        panic!("unsupported retained policy must require operator repair");
    };
    assert_eq!(error.status(), StatusCode::CONFLICT);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_pool_miss_retains_policy_shape_without_legacy_opt_in() -> TestResult {
    let db = pg_test_db().await?;
    let release_id = insert_template_release(&db.pool, "app-runtime", "sha256:new-release").await?;

    // The deprecated per-policy flag no longer gates miss sizing.
    sqlx::query(sql!(
        "INSERT INTO warm_pool_policies (
             template_name, runtime, vcpu, memory_mib, min_ready_slots,
             max_ready_slots, refill_per_tick, max_warming_slots, retain_releases,
             retain_shape_on_miss, disk_io_max_operations_per_sec,
             disk_io_max_bandwidth_mib_per_sec
         )
         VALUES ('app-runtime', 'fc', 4, 8192, 1, 4, 1, 2, 1, FALSE, 2000, 200)"
    ))
    .execute(&db.pool)
    .await?;

    let assignment =
        resolve_kubernetes_pool_configured_resources(&db.pool, "app-runtime", release_id)
            .await?
            .expect("an enabled policy supplies the cold-miss shape without the legacy flag");
    assert_eq!(assignment.resources.vcpu, Some(4));
    assert_eq!(assignment.resources.memory_mib, Some(8192));
    assert_eq!(
        assignment.disk_io_limits,
        DiskIoLimits::try_new(2_000, 200).unwrap()
    );

    sqlx::query(sql!(
        "UPDATE warm_pool_policies SET enabled = FALSE WHERE template_name = 'app-runtime'"
    ))
    .execute(&db.pool)
    .await?;
    assert!(
        resolve_kubernetes_pool_configured_resources(&db.pool, "app-runtime", release_id)
            .await?
            .is_none(),
        "a disabled policy does not supply a cold-miss shape"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_pool_miss_retains_shape_for_release_outside_warming_window() -> TestResult {
    let db = pg_test_db().await?;
    let old_release_id =
        insert_template_release(&db.pool, "app-runtime", "sha256:old-release").await?;
    let new_release_id =
        insert_template_release(&db.pool, "app-runtime", "sha256:new-release").await?;
    sqlx::query(sql!(
        "UPDATE template_releases
         SET created_at = CASE WHEN id = $1 THEN NOW() - INTERVAL '1 day' ELSE NOW() END
         WHERE id = ANY($2::UUID[])"
    ))
    .bind(old_release_id)
    .bind([old_release_id, new_release_id])
    .execute(&db.pool)
    .await?;

    sqlx::query(sql!(
        "INSERT INTO warm_pool_policies (
             template_name, runtime, vcpu, memory_mib, min_ready_slots,
             max_ready_slots, refill_per_tick, max_warming_slots, retain_releases,
             disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec
         )
         VALUES ('app-runtime', 'fc', 4, 8192, 1, 4, 1, 2, 1, 2000, 200)"
    ))
    .execute(&db.pool)
    .await?;

    let old_assignment =
        resolve_kubernetes_pool_configured_resources(&db.pool, "app-runtime", old_release_id)
            .await?
            .expect("cold misses retain policy shape outside the warming window");
    assert_eq!(old_assignment.resources.vcpu, Some(4));
    assert_eq!(old_assignment.resources.memory_mib, Some(8192));
    assert_eq!(
        old_assignment.disk_io_limits,
        DiskIoLimits::try_new(2_000, 200).unwrap()
    );
    assert!(
        resolve_kubernetes_pool_configured_resources(&db.pool, "app-runtime", new_release_id)
            .await?
            .is_some()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_pool_miss_uses_same_policy_priority_as_zero_demand_materialization() -> TestResult
{
    let db = pg_test_db().await?;
    let release_id = insert_template_release(&db.pool, "app-runtime", "sha256:new-release").await?;

    sqlx::query(sql!(
        "INSERT INTO warm_pool_policies (
             template_name, runtime, vcpu, memory_mib, min_ready_slots,
             max_ready_slots, refill_per_tick, max_warming_slots, retain_releases,
             disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec
         )
         VALUES
             ('app-runtime', 'fc', 4, 8192, 1, 100, 1, 2, 1, 2000, 200),
             ('app-runtime', 'fc', 8, 16384, 5, 10, 1, 2, 1, 3000, 250)"
    ))
    .execute(&db.pool)
    .await?;

    let assignment =
        resolve_kubernetes_pool_configured_resources(&db.pool, "app-runtime", release_id)
            .await?
            .expect("an enabled policy should provide the cold-miss assignment");

    assert_eq!(assignment.resources.vcpu, Some(8));
    assert_eq!(assignment.resources.memory_mib, Some(16384));
    assert_eq!(
        assignment.disk_io_limits,
        DiskIoLimits::try_new(3_000, 250).unwrap()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_failed_pooled_create_finalizes_after_claim_cleanup() -> TestResult {
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    let req = pooled_create_request("pooled-sandbox-cleanup", None);
    let template_network_policy = NetworkPolicyV1::deny_all();
    let requested_template = TemplateReference::latest(TemplateName::parse("seed").unwrap());
    let create_owner = SandboxCreateOwner::local(&actor);
    let input = pooled_create_input(
        &db.pool,
        &req,
        &requested_template,
        &auto_lifecycle,
        &template_network_policy,
        &create_owner,
    );
    let claim = pooled_claim(sandbox_id, node_id);
    let mut tx = db.pool.begin().await?;
    insert_pooled_sandbox(&mut tx, &input, &create_owner, &claim, "space").await?;
    tx.commit().await?;
    let substrate = crate::controllers::substrate::direct::DirectSubstrate::new(db.pool.clone());

    SandboxController::rollback_failed_pooled_create_with_substrate(&substrate, &db.pool, &claim)
        .await?;

    let sandbox_exists: bool = sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1
             FROM sandboxes
             WHERE id = $1
         )"
    ))
    .bind(claim.sandbox_id)
    .fetch_one(&db.pool)
    .await?;

    assert!(!sandbox_exists);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_pooled_create_retries_prepare_in_place_before_reclaiming() -> TestResult {
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let node_id = NodeId::new();
    let release_id = insert_template_release(&db.pool, "seed", "sha256:pooled").await?;
    let pool_config_id = Uuid::now_v7();
    let sandbox_id = SandboxId::new();
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    let mut req = pooled_create_request("pooled-sandbox-in-place-retry", None);
    req.boot_mode = None;
    let template_network_policy = NetworkPolicyV1::deny_all();
    let requested_template = TemplateReference::latest(TemplateName::parse("seed").unwrap());
    let create_owner = SandboxCreateOwner::local(&actor);
    let mut input = pooled_create_input(
        &db.pool,
        &req,
        &requested_template,
        &auto_lifecycle,
        &template_network_policy,
        &create_owner,
    );
    input.template_release_id = release_id;
    input.boot_mode = CreateBootMode::Fallthrough;

    insert_node(&db.pool, node_id).await?;
    sqlx::query(sql!(
        "INSERT INTO kubernetes_pool_configs (
             id, template_release_id, vcpu, memory_mib, target_ready_slots,
             refill_per_tick, max_warming_slots, namespace, enabled
         )
         VALUES ($1, $2, 4, 2048, 3, 1, 1, 'space', true)"
    ))
    .bind(pool_config_id)
    .bind(release_id)
    .execute(&db.pool)
    .await?;

    let mut substrate = RetryingPoolSubstrate::new(
        db.pool.clone(),
        pool_config_id,
        release_id,
        vec![pooled_claim_with_pod(
            sandbox_id,
            node_id,
            "space-sbx-pooled-g1",
        )],
        vec![false, true],
    );

    let updated_policy = crate::e2b::auto_lifecycle_policy(120, true, false);
    substrate.policy_after_prepare_failure = Some(updated_policy.clone());

    let response = SandboxController::try_create_from_pool_with_substrate(
        &substrate,
        input,
        Arc::new(AtomicU64::new(0)),
    )
    .await?
    .response
    .expect("same warm-pool claim should succeed after prepare retry");

    assert_eq!(response.id, sandbox_id);
    assert_eq!(response.lifecycle.as_deref(), Some("paused"));
    assert_eq!(substrate.claim_calls(), 1);
    assert_eq!(substrate.prepare_calls(), 2);
    assert_eq!(
        *substrate.prepared_policies.lock().unwrap(),
        vec![auto_lifecycle, updated_policy],
        "retry must observe the timeout policy accepted after its first attempt"
    );

    let (slot_state, lifecycle) = pooled_slot_and_sandbox_state(&db.pool, sandbox_id).await?;
    assert_eq!(slot_state, "claimed");
    assert_eq!(lifecycle, "paused");

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_terminal_pooled_prepare_failure_transfers_claim_to_cleanup() -> TestResult {
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let node_id = NodeId::new();
    let release_id = insert_template_release(&db.pool, "seed", "sha256:pooled").await?;
    let pool_config_id = Uuid::now_v7();
    let sandbox_id = SandboxId::new();
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    let req = pooled_create_request("pooled-sandbox-terminal-prepare-failure", None);
    let template_network_policy = NetworkPolicyV1::deny_all();
    let requested_template = TemplateReference::latest(TemplateName::parse("seed").unwrap());
    let create_owner = SandboxCreateOwner::local(&actor);
    let mut input = pooled_create_input(
        &db.pool,
        &req,
        &requested_template,
        &auto_lifecycle,
        &template_network_policy,
        &create_owner,
    );
    input.template_release_id = release_id;

    insert_node(&db.pool, node_id).await?;
    sqlx::query(sql!(
        "INSERT INTO kubernetes_pool_configs (
             id, template_release_id, vcpu, memory_mib, target_ready_slots,
             refill_per_tick, max_warming_slots, namespace, enabled
         )
         VALUES ($1, $2, 4, 2048, 3, 1, 1, 'space', true)"
    ))
    .bind(pool_config_id)
    .bind(release_id)
    .execute(&db.pool)
    .await?;

    let substrate = RetryingPoolSubstrate::new(
        db.pool.clone(),
        pool_config_id,
        release_id,
        vec![pooled_claim_with_pod(
            sandbox_id,
            node_id,
            "space-sbx-pooled-terminal-failure",
        )],
        vec![false, false],
    );

    let error = match SandboxController::try_create_from_pool_with_substrate(
        &substrate,
        input,
        Arc::new(AtomicU64::new(0)),
    )
    .await
    {
        Ok(_) => panic!("terminal prepare failure must propagate after transferring cleanup"),
        Err(error) => error,
    };

    assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(substrate.claim_calls(), 1);
    assert_eq!(substrate.prepare_calls(), 2);
    let (slot_state, lifecycle) = pooled_slot_and_sandbox_state(&db.pool, sandbox_id).await?;
    assert_eq!(slot_state, "cleanup_pending");
    assert_eq!(lifecycle, "paused");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_byok_org_bypasses_warm_pool_and_cold_creates() -> TestResult {
    // A BYOK org must cold-create with an encrypted rootfs: a running warm
    // pool VM is already on a plaintext rootfs and dm-crypt is set up
    // pre-boot, so it cannot be encrypted at adoption. Even with a claimable,
    // prepare-ready pool slot, the create must bypass the pool entirely and
    // fall through to the cold path (Ok(None)), never touching the pool.
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let node_id = NodeId::new();
    let release_id = insert_template_release(&db.pool, "seed", "sha256:pooled").await?;
    let pool_config_id = Uuid::now_v7();
    let sandbox_id = SandboxId::new();
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    let mut req = pooled_create_request("byok-cold-start", None);
    req.boot_mode = None;
    let template_network_policy = NetworkPolicyV1::deny_all();
    let requested_template = TemplateReference::latest(TemplateName::parse("seed").unwrap());
    let create_owner = SandboxCreateOwner::local(&actor);
    let mut input = pooled_create_input(
        &db.pool,
        &req,
        &requested_template,
        &auto_lifecycle,
        &template_network_policy,
        &create_owner,
    );
    input.template_release_id = release_id;
    input.boot_mode = CreateBootMode::Fallthrough;
    // The create is stamped encrypted — the only difference from a create
    // that would otherwise claim the pool slot set up below.
    input.volume_encryption = true;

    insert_node(&db.pool, node_id).await?;
    sqlx::query(sql!(
        "INSERT INTO kubernetes_pool_configs (
             id, template_release_id, vcpu, memory_mib, target_ready_slots,
             refill_per_tick, max_warming_slots, namespace, enabled
         )
         VALUES ($1, $2, 4, 2048, 3, 1, 1, 'space', true)"
    ))
    .bind(pool_config_id)
    .bind(release_id)
    .execute(&db.pool)
    .await?;

    // A ready claim with a successful prepare is available, so a non-BYOK
    // create here would adopt the pool slot. The BYOK org must not.
    let substrate = RetryingPoolSubstrate::new(
        db.pool.clone(),
        pool_config_id,
        release_id,
        vec![pooled_claim_with_pod(
            sandbox_id,
            node_id,
            "space-sbx-pooled-g1",
        )],
        vec![true],
    );

    let response = SandboxController::try_create_from_pool_with_substrate(
        &substrate,
        input,
        Arc::new(AtomicU64::new(0)),
    )
    .await?;

    assert!(
        response.response.is_none(),
        "BYOK org must cold-create, not adopt a plaintext warm-pool sandbox"
    );
    assert_eq!(
        substrate.claim_calls(),
        0,
        "BYOK org must not touch the warm pool"
    );

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_pinned_sandbox_id_bypasses_warm_pool_and_cold_creates() -> TestResult {
    // A create whose sandbox ID was already handed to the caller must produce
    // exactly that ID. Warm-pool adoption returns the slot's pre-reserved ID
    // instead, so even with a claimable, prepare-ready slot the create must
    // fall through to the cold path without touching the pool.
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let node_id = NodeId::new();
    let release_id = insert_template_release(&db.pool, "seed", "sha256:pooled").await?;
    let pool_config_id = Uuid::now_v7();
    let slot_sandbox_id = SandboxId::new();
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    let mut req = pooled_create_request("pinned-id-cold-start", None);
    req.boot_mode = None;
    req.vcpu = None;
    req.memory_mib = None;
    req.wait_until_ready = true;
    let template_network_policy = NetworkPolicyV1::deny_all();
    let requested_template = TemplateReference::latest(TemplateName::parse("seed").unwrap());
    let create_owner = SandboxCreateOwner::local(&actor);
    let mut input = pooled_create_input(
        &db.pool,
        &req,
        &requested_template,
        &auto_lifecycle,
        &template_network_policy,
        &create_owner,
    );
    input.template_release_id = release_id;
    input.boot_mode = CreateBootMode::Fallthrough;
    input.sandbox_id_pinned = true;
    assert_ne!(input.planned_sandbox_id, slot_sandbox_id);

    insert_node(&db.pool, node_id).await?;
    sqlx::query(sql!(
        "INSERT INTO kubernetes_pool_configs (
             id, template_release_id, vcpu, memory_mib, target_ready_slots,
             refill_per_tick, max_warming_slots, namespace, enabled
         )
         VALUES ($1, $2, 4, 2048, 3, 1, 1, 'space', true)"
    ))
    .bind(pool_config_id)
    .bind(release_id)
    .execute(&db.pool)
    .await?;

    let substrate = RetryingPoolSubstrate::new(
        db.pool.clone(),
        pool_config_id,
        release_id,
        vec![pooled_claim_with_pod(
            slot_sandbox_id,
            node_id,
            "space-sbx-pooled-g1",
        )],
        vec![true],
    );

    let response = SandboxController::try_create_from_pool_with_substrate(
        &substrate,
        input,
        Arc::new(AtomicU64::new(0)),
    )
    .await?;

    assert!(
        response.response.is_none(),
        "a pinned sandbox ID must cold-create, not adopt a slot with another ID"
    );
    assert_eq!(
        substrate.claim_calls(),
        0,
        "a pinned sandbox ID must not touch the warm pool"
    );

    Ok(())
}

/// Fixture for creates admitted against a warm-pool slot: the create's
/// public ID is the slot's reserved ID, and the request is otherwise an
/// ordinary pool-eligible native create (which does not wait for readiness).
struct AdmittedCreateFixture {
    db: pplx_space_db_test_fixtures::TestDb,
    actor: User,
    node_id: NodeId,
    release_id: Uuid,
    pool_config_id: Uuid,
    sandbox_id: SandboxId,
    req: CreateSandboxRequest,
    requested_template: TemplateReference,
    auto_lifecycle: AutoLifecyclePolicy,
    template_network_policy: NetworkPolicyV1,
}

impl AdmittedCreateFixture {
    async fn new() -> TestResult<Self> {
        let db = pg_test_db().await?;
        let actor = insert_actor(&db.pool).await?;
        let node_id = NodeId::new();
        let release_id = insert_template_release(&db.pool, "seed", "sha256:pooled").await?;
        let pool_config_id = Uuid::now_v7();
        insert_node(&db.pool, node_id).await?;
        sqlx::query(sql!(
            "INSERT INTO kubernetes_pool_configs (
                 id, template_release_id, vcpu, memory_mib, target_ready_slots,
                 refill_per_tick, max_warming_slots, namespace, enabled
             )
             VALUES ($1, $2, 4, 2048, 3, 1, 1, 'space', true)"
        ))
        .bind(pool_config_id)
        .bind(release_id)
        .execute(&db.pool)
        .await?;
        let mut req = pooled_create_request("admitted-create", None);
        req.boot_mode = None;
        req.wait_until_ready = false;
        Ok(Self {
            db,
            actor,
            node_id,
            release_id,
            pool_config_id,
            sandbox_id: SandboxId::new(),
            req,
            requested_template: TemplateReference::latest(TemplateName::parse("seed").unwrap()),
            auto_lifecycle: AutoLifecyclePolicy::disabled(),
            template_network_policy: NetworkPolicyV1::deny_all(),
        })
    }

    fn admitted(&self) -> AdmittedPoolClaim {
        AdmittedPoolClaim {
            claim_id: Uuid::now_v7(),
            pool_slot_id: Uuid::now_v7(),
        }
    }

    fn claim(&self, admitted: AdmittedPoolClaim) -> PooledExecutionClaim {
        let mut claim = pooled_claim(self.sandbox_id, self.node_id);
        claim.claim_id = admitted.claim_id;
        claim.pool_slot_id = Some(admitted.pool_slot_id);
        claim
    }

    fn substrate(&self) -> RetryingPoolSubstrate {
        RetryingPoolSubstrate::new(
            self.db.pool.clone(),
            self.pool_config_id,
            self.release_id,
            Vec::new(),
            vec![true],
        )
    }

    async fn run(
        &self,
        substrate: &RetryingPoolSubstrate,
        admitted: AdmittedPoolClaim,
        volume_encryption: bool,
    ) -> Result<PoolCreateResult, ControllerError> {
        let create_owner = SandboxCreateOwner::local(&self.actor);
        let mut input = pooled_create_input(
            &self.db.pool,
            &self.req,
            &self.requested_template,
            &self.auto_lifecycle,
            &self.template_network_policy,
            &create_owner,
        );
        input.template_release_id = self.release_id;
        input.boot_mode = CreateBootMode::Fallthrough;
        input.planned_sandbox_id = self.sandbox_id;
        input.sandbox_id_pinned = true;
        input.volume_encryption = volume_encryption;
        SandboxController::try_create_from_admitted_pool_claim_with_substrate(
            substrate,
            input,
            admitted,
            true,
            Arc::new(AtomicU64::new(0)),
        )
        .await
    }

    async fn sandbox_rows(&self) -> TestResult<i64> {
        Ok(
            sqlx::query_scalar(sql!("SELECT COUNT(*) FROM sandboxes WHERE id = $1"))
                .bind(self.sandbox_id)
                .fetch_one(&self.db.pool)
                .await?,
        )
    }
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_admitted_pool_claim_adopts_the_claimed_slot_under_its_id() -> TestResult {
    let fixture = AdmittedCreateFixture::new().await?;
    let admitted = fixture.admitted();
    let substrate = fixture
        .substrate()
        .with_admitted_lookup(AdmittedPoolClaimLookup::Adoptable {
            claim: Box::new(fixture.claim(admitted)),
            pool_shape: PooledExecutionPoolShape::new(4, 2048, Default::default()),
        });

    let result = fixture.run(&substrate, admitted, false).await?;

    let response = result
        .response
        .expect("an adoptable admitted slot is adopted");
    assert_eq!(response.id, fixture.sandbox_id);
    assert_eq!(substrate.admitted_lookup_calls(), 1);
    assert_eq!(
        substrate.claim_calls(),
        0,
        "an admitted create must never claim a different slot"
    );
    assert_eq!(
        substrate.prepare_wait_until_ready(),
        vec![true],
        "adoption waits for readiness even when the request does not"
    );
    let (lifecycle, active_node_id): (String, Option<NodeId>) = sqlx::query_as(sql!(
        "SELECT lifecycle, active_node_id FROM sandboxes WHERE id = $1"
    ))
    .bind(fixture.sandbox_id)
    .fetch_one(&fixture.db.pool)
    .await?;
    assert_eq!(lifecycle, "started");
    assert_eq!(active_node_id, Some(fixture.node_id));
    let executions: Vec<(i64, bool)> = sqlx::query_as(sql!(
        "SELECT generation, active FROM sandbox_k8s_executions WHERE sandbox_id = $1"
    ))
    .bind(fixture.sandbox_id)
    .fetch_all(&fixture.db.pool)
    .await?;
    assert_eq!(executions, vec![(1, true)]);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_admitted_pool_claim_released_slot_builds_cold_without_pool_calls() -> TestResult {
    let fixture = AdmittedCreateFixture::new().await?;
    let admitted = fixture.admitted();
    let substrate = fixture
        .substrate()
        .with_admitted_lookup(AdmittedPoolClaimLookup::Released);

    let result = fixture.run(&substrate, admitted, false).await?;

    assert!(result.response.is_none(), "a released slot builds cold");
    assert_eq!(substrate.claim_calls(), 0);
    assert_eq!(substrate.prepare_calls(), 0);
    assert_eq!(substrate.rollback_calls(), 0);
    assert_eq!(fixture.sandbox_rows().await?, 0);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_admitted_pool_claim_waits_while_a_slot_holds_the_id() -> TestResult {
    let fixture = AdmittedCreateFixture::new().await?;
    let admitted = fixture.admitted();
    let substrate = fixture
        .substrate()
        .with_admitted_lookup(AdmittedPoolClaimLookup::ReleasePending);

    let error = match fixture.run(&substrate, admitted, false).await {
        Ok(_) => panic!("a held slot must not resolve the create"),
        Err(error) => error,
    };

    assert_eq!(
        error.retry_after_cause(),
        Some(ControllerRetryAfterCause::PoolSlotRelease)
    );
    assert!(error.retry_after_header().is_some());
    assert_eq!(substrate.claim_calls(), 0);
    assert_eq!(substrate.prepare_calls(), 0);
    assert_eq!(fixture.sandbox_rows().await?, 0);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_admitted_pool_claim_releases_a_slot_it_may_not_adopt() -> TestResult {
    // An encrypted create can never adopt a plaintext warm member. The slot
    // is given back to the pool for collection rather than adopted, and the
    // create waits for it before building cold.
    let fixture = AdmittedCreateFixture::new().await?;
    let admitted = fixture.admitted();
    let substrate = fixture
        .substrate()
        .with_admitted_lookup(AdmittedPoolClaimLookup::Adoptable {
            claim: Box::new(fixture.claim(admitted)),
            pool_shape: PooledExecutionPoolShape::new(4, 2048, Default::default()),
        });

    let error = match fixture.run(&substrate, admitted, true).await {
        Ok(_) => panic!("an encrypted create must not adopt"),
        Err(error) => error,
    };

    assert_eq!(
        error.retry_after_cause(),
        Some(ControllerRetryAfterCause::PoolSlotRelease)
    );
    assert_eq!(substrate.rollback_calls(), 1);
    assert_eq!(substrate.prepare_calls(), 0);
    assert_eq!(fixture.sandbox_rows().await?, 0);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_admitted_pool_claim_prepare_failure_never_reclaims() -> TestResult {
    // A failed adoption must not fall back to another slot (the ID is fixed)
    // or report success. It parks the slot for the pool's cleanup and asks to
    // be retried; the retry builds cold once that cleanup has run.
    let fixture = AdmittedCreateFixture::new().await?;
    let admitted = fixture.admitted();
    let mut substrate =
        fixture
            .substrate()
            .with_admitted_lookup(AdmittedPoolClaimLookup::Adoptable {
                claim: Box::new(fixture.claim(admitted)),
                pool_shape: PooledExecutionPoolShape::new(4, 2048, Default::default()),
            });
    substrate.prepare_results =
        Mutex::new(vec![false; MAX_POOLED_PREPARE_IN_PLACE_ATTEMPTS].into());

    let error = match fixture.run(&substrate, admitted, false).await {
        Ok(_) => panic!("a failed adoption must not succeed"),
        Err(error) => error,
    };

    assert_eq!(
        error.retry_after_cause(),
        Some(ControllerRetryAfterCause::PoolSlotRelease)
    );
    assert_eq!(substrate.claim_calls(), 0, "no other slot is claimed");
    assert_eq!(substrate.admitted_lookup_calls(), 1);
    assert_eq!(
        pooled_slot_and_sandbox_state(&fixture.db.pool, fixture.sandbox_id)
            .await?
            .0,
        "cleanup_pending"
    );

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_pooled_create_reclaims_after_prepare_retry_budget_exhaustion() -> TestResult {
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let node_id = NodeId::new();
    let release_id = insert_template_release(&db.pool, "seed", "sha256:pooled").await?;
    let pool_config_id = Uuid::now_v7();
    let first_sandbox_id = SandboxId::new();
    let second_sandbox_id = SandboxId::new();
    let third_sandbox_id = SandboxId::new();
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    let mut req = pooled_create_request("pooled-sandbox-retry", None);
    req.boot_mode = None;
    let template_network_policy = NetworkPolicyV1::deny_all();
    let requested_template = TemplateReference::latest(TemplateName::parse("seed").unwrap());
    let create_owner = SandboxCreateOwner::local(&actor);
    let mut input = pooled_create_input(
        &db.pool,
        &req,
        &requested_template,
        &auto_lifecycle,
        &template_network_policy,
        &create_owner,
    );
    input.template_release_id = release_id;
    input.boot_mode = CreateBootMode::Fallthrough;

    insert_node(&db.pool, node_id).await?;
    sqlx::query(sql!(
        "INSERT INTO kubernetes_pool_configs (
             id, template_release_id, vcpu, memory_mib, target_ready_slots,
             refill_per_tick, max_warming_slots, namespace, enabled
         )
         VALUES ($1, $2, 4, 2048, 3, 1, 1, 'space', true)"
    ))
    .bind(pool_config_id)
    .bind(release_id)
    .execute(&db.pool)
    .await?;

    let substrate = RetryingPoolSubstrate::new(
        db.pool.clone(),
        pool_config_id,
        release_id,
        vec![
            pooled_claim_with_pod(first_sandbox_id, node_id, "space-sbx-pooled-g1"),
            pooled_claim_with_pod(second_sandbox_id, node_id, "space-sbx-pooled-g2"),
            pooled_claim_with_pod(third_sandbox_id, node_id, "space-sbx-pooled-g3"),
        ],
        vec![false, false, false, false, true],
    );

    let response = SandboxController::try_create_from_pool_with_substrate(
        &substrate,
        input,
        Arc::new(AtomicU64::new(0)),
    )
    .await?
    .response
    .expect("third warm-pool claim should succeed");

    assert_eq!(response.id, third_sandbox_id);
    assert_eq!(response.lifecycle.as_deref(), Some("paused"));
    assert_eq!(substrate.claim_calls(), 3);
    assert_eq!(substrate.prepare_calls(), 5);

    let (first_slot_state, first_lifecycle) =
        pooled_slot_and_sandbox_state(&db.pool, first_sandbox_id).await?;
    let (second_slot_state, second_lifecycle) =
        pooled_slot_and_sandbox_state(&db.pool, second_sandbox_id).await?;
    let (third_slot_state, third_lifecycle) =
        pooled_slot_and_sandbox_state(&db.pool, third_sandbox_id).await?;

    assert_eq!(
        first_slot_state,
        pplx_space_cluster_pool::PoolSlotState::CleanupPending.as_str()
    );
    assert_eq!(first_lifecycle, "paused");
    assert_eq!(
        second_slot_state,
        pplx_space_cluster_pool::PoolSlotState::CleanupPending.as_str()
    );
    assert_eq!(second_lifecycle, "paused");
    assert_eq!(third_slot_state, "claimed");
    assert_eq!(third_lifecycle, "paused");

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_federated_restore_inserts_restore_ownership() -> TestResult {
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let release_id = insert_template_release(&db.pool, "seed", "sha256:federated-restore").await?;
    insert_template_release_tag(&db.pool, release_id, "seed", "latest").await?;
    sqlx::query(sql!(
        "CREATE OR REPLACE FUNCTION verify_federated_restore_insert()
             RETURNS trigger
             LANGUAGE plpgsql
             AS $$
             BEGIN
                 IF NEW.lifecycle = 'restoring'
                    AND NEW.restore_lease_token IS NOT NULL
                    AND NEW.restore_lease_expires_at IS NOT NULL
                    AND NEW.preserve_local_artifact_on_restore_failure
                    AND NEW.egress_profiles = ARRAY['sensor']::text[]
                    AND NEW.network_policy_override_json->'egress'->'allow' @> '[{\"domain\":\"sensor.example.test\"}]'::jsonb THEN
                     RAISE EXCEPTION 'observed correct federated restore ownership';
                 END IF;
                 RAISE EXCEPTION 'missing federated restore ownership';
             END
             $$"
    ))
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "CREATE TRIGGER verify_federated_restore_insert
             BEFORE INSERT ON sandboxes
             FOR EACH ROW
             EXECUTE FUNCTION verify_federated_restore_insert()"
    ))
    .execute(&db.pool)
    .await?;

    let profiles = crate::egress_profiles::EgressProfiles::from_config(
        r#"{"sensor":{"allow":[{"domain":"sensor.example.test"}]}}"#,
    )?;
    let selection = profiles.select(Some("+sensor"), true)?;
    // The source and its configuration may be gone; restore retains the saved policy.
    let controller = direct_sandbox_controller(&db.pool);
    let mut req = pooled_create_request("federated-restore-sandbox", None);
    req.boot_mode = Some("cold".to_string());
    req.network_policy =
        Some(profiles.compose(&pplx_space_net_common::NetworkPolicyV1::deny_all()));
    req.template = Some("seed:latest".parse()?);
    let owner = test_federated_owner("8a3f64c2-1f2b-4c4e-9a90-1f0d4f6f5a02")?;
    let grant = test_service_grant()?;
    let operation_id = pplx_space_public_api::OperationId::new();
    let source_sandbox_id = SandboxId::new();
    let error = controller
        .restore_fork_disk_as_new(
            &db.pool,
            &actor,
            &req,
            SandboxApiMetricIdentity::native("fork"),
            RestoreForkDiskAsNewContext {
                volume_encryption: false,
                sandbox_id: SandboxId::new(),
                federated: FederatedSandboxCreateContext::new(Some(&owner), Some(&grant))
                    .with_egress_profiles(Some(&selection)),
                operation_id,
                restore_lease_seconds: 300,
                snapshot: crate::controllers::substrate::SnapshotRef {
                    snapshot_id: "snapshot-1",
                    storage_uri: Some("s3://bucket/snapshot-1"),
                    source_sandbox_id,
                },
            },
        )
        .await
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("observed correct federated restore ownership"),
        "federated restore insert did not preserve restore ownership: {error}"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_pooled_sandbox_network_policy_override_enqueues_reconcile() -> TestResult {
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    insert_node(&db.pool, node_id).await?;

    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    let req = pooled_create_request("pooled-sandbox-policy", Some(NetworkPolicyV1::default()));
    let template_network_policy = NetworkPolicyV1::deny_all();
    let requested_template = TemplateReference::latest(TemplateName::parse("seed").unwrap());
    let create_owner = SandboxCreateOwner::local(&actor);
    let input = pooled_create_input(
        &db.pool,
        &req,
        &requested_template,
        &auto_lifecycle,
        &template_network_policy,
        &create_owner,
    );
    let claim = pooled_claim(sandbox_id, node_id);
    let mut tx = db.pool.begin().await?;

    insert_pooled_sandbox(&mut tx, &input, &create_owner, &claim, "space").await?;
    enqueue_pooled_network_policy_reconcile_if_needed(
        &mut tx,
        input.req.network_policy.as_ref(),
        sandbox_id,
        claim.active_node_id,
    )
    .await?;
    tx.commit().await?;

    let element = sandbox_network_policy_element(sandbox_id);
    let (generation,): (i64,) = sqlx::query_as(sql!(
        "SELECT generation
         FROM node_reconcile_queue
         WHERE node_id = $1
           AND facet = $2
           AND element = $3"
    ))
    .bind(node_id)
    .bind(ReconcileFacet::NetworkPolicy.as_str())
    .bind(&element)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(generation, 0);

    let (reason,): (String,) = sqlx::query_as(sql!(
        "SELECT reason
         FROM node_reconcile_triggers
         WHERE node_id = $1"
    ))
    .bind(node_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        reason,
        ReconcileReason::SandboxNetworkPolicyUpdated.as_str()
    );
    Ok(())
}

/// A strict `boot_mode=pool` create answers **409**, not 503, when the miss is
/// a standing placement refusal.
///
/// The distinction is the whole reason `PooledExecutionClaimAttempt` carries a
/// refusal. Every other boot mode falls through to a cold create that resolves
/// the same boundary and produces the caller's error itself; `boot_mode=pool`
/// has nowhere to fall through to, so without this the client is told "no
/// warm-pool sandbox is available" — a 503, i.e. retry — for a condition no
/// retry can clear, and a well-behaved client loops forever.
///
/// Deleting the branch, or inverting its boot-mode test, sends the create back
/// to deferring — which `create` renders as that same 503 — and nothing else on
/// the branch notices. Verified by removing the branch: this test fails, with
/// the create returning `Ok` where it must return a conflict.
#[tokio::test(flavor = "current_thread")]
async fn postgres_strict_pool_placement_refusal_is_a_conflict_not_unavailable() -> TestResult {
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let release_id = insert_template_release(&db.pool, "seed", "sha256:pooled").await?;
    let pool_config_id = Uuid::now_v7();
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    let mut req = pooled_create_request("strict-pool-refused", None);
    req.boot_mode = Some("pool".to_string());
    let template_network_policy = NetworkPolicyV1::deny_all();
    let requested_template = TemplateReference::latest(TemplateName::parse("seed").unwrap());
    let create_owner = SandboxCreateOwner::local(&actor);

    sqlx::query(sql!(
        "INSERT INTO kubernetes_pool_configs (
             id, template_release_id, vcpu, memory_mib, target_ready_slots,
             refill_per_tick, max_warming_slots, namespace, enabled
         )
         VALUES ($1, $2, 4, 2048, 3, 1, 1, 'space', true)"
    ))
    .bind(pool_config_id)
    .bind(release_id)
    .execute(&db.pool)
    .await?;

    let refused_input = {
        let mut input = pooled_create_input(
            &db.pool,
            &req,
            &requested_template,
            &auto_lifecycle,
            &template_network_policy,
            &create_owner,
        );
        input.template_release_id = release_id;
        input
    };
    // No claims to hand out, so the claim misses — and reports why it will keep
    // missing.
    let refused =
        RetryingPoolSubstrate::new(db.pool.clone(), pool_config_id, release_id, vec![], vec![])
            .refusing(
                "sandbox template 'seed' may run only on node groups dedicated to its \
                 organization",
            );

    let error = match SandboxController::try_create_from_pool_with_substrate(
        &refused,
        refused_input,
        Arc::new(AtomicU64::new(0)),
    )
    .await
    {
        Ok(_) => panic!("a strict-pool create with no adoptable slot must not succeed"),
        Err(error) => error,
    };
    assert_eq!(
        error.status(),
        StatusCode::CONFLICT,
        "a refusal no retry can clear must not be reported as a temporary unavailability: \
         {error:?}"
    );
    assert!(
        format!("{error:?}").contains("dedicated to its organization"),
        "the substrate's reason has to reach the caller rather than being replaced by a \
         generic pool-miss message: {error:?}"
    );

    // The control: the same substrate and the same boot mode, minus the
    // refusal. This function does not raise the 503 itself — it hands back a
    // cold fall-through and `create` turns that into "no warm-pool sandbox is
    // available" — so the control is that an unrefused miss still *defers*
    // rather than deciding. That is exactly what the refusal changes, and it
    // shows the 409 above came from the refusal and not from the boot mode.
    let plain_input = {
        let mut input = pooled_create_input(
            &db.pool,
            &req,
            &requested_template,
            &auto_lifecycle,
            &template_network_policy,
            &create_owner,
        );
        input.template_release_id = release_id;
        input
    };
    let plain =
        RetryingPoolSubstrate::new(db.pool.clone(), pool_config_id, release_id, vec![], vec![]);
    let plain = SandboxController::try_create_from_pool_with_substrate(
        &plain,
        plain_input,
        Arc::new(AtomicU64::new(0)),
    )
    .await?;
    assert!(
        plain.response.is_none(),
        "an unrefused miss must not answer the caller; it falls through for `create` to \
         turn into the retryable 503"
    );

    // The other half of the guard, and the one with the wider blast radius: a
    // **fall-through** create must keep falling through even when the claim
    // refuses. Deleting the boot-mode conjunct would make every refused
    // fall-through create answer 409 instead of building cold — and a stale
    // pool config on an org-specific template is the documented post-stage-4
    // steady state, so that is not a corner.
    let fallthrough_input = {
        let mut input = pooled_create_input(
            &db.pool,
            &req,
            &requested_template,
            &auto_lifecycle,
            &template_network_policy,
            &create_owner,
        );
        input.template_release_id = release_id;
        input.boot_mode = CreateBootMode::Fallthrough;
        input
    };
    let fallthrough_refused =
        RetryingPoolSubstrate::new(db.pool.clone(), pool_config_id, release_id, vec![], vec![])
            .refusing(
                "sandbox template 'seed' may run only on node groups dedicated to its \
                 organization",
            );
    let fell_through = SandboxController::try_create_from_pool_with_substrate(
        &fallthrough_refused,
        fallthrough_input,
        Arc::new(AtomicU64::new(0)),
    )
    .await?;
    assert!(
        fell_through.response.is_none(),
        "a refusal must not answer the caller for a boot mode that still has a cold path; \
         the cold create resolves the same boundary and owns the error"
    );

    // And the class comes from the refusal, not from the call site. A boundary
    // that could not be *computed* — a reaped `templates` row — is a
    // control-plane fault the cold path answers 500, so the strict-pool answer
    // is 500 too. Rendering every refusal as a conflict would report it as a
    // client error and take the only signal of a missing row out of 5xx
    // alerting; rendering it 503 would tell the client to retry what only an
    // operator can fix.
    let unresolvable_input = {
        let mut input = pooled_create_input(
            &db.pool,
            &req,
            &requested_template,
            &auto_lifecycle,
            &template_network_policy,
            &create_owner,
        );
        input.template_release_id = release_id;
        input
    };
    let unresolvable =
        RetryingPoolSubstrate::new(db.pool.clone(), pool_config_id, release_id, vec![], vec![])
            .unresolvable("template 'seed' has no templates row");
    let unresolvable_error = match SandboxController::try_create_from_pool_with_substrate(
        &unresolvable,
        unresolvable_input,
        Arc::new(AtomicU64::new(0)),
    )
    .await
    {
        Ok(_) => panic!("an unresolvable boundary must not succeed"),
        Err(error) => error,
    };
    assert_eq!(
        unresolvable_error.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "a boundary that cannot be computed is a control-plane fault, not a client \
         conflict and not a retry instruction: {unresolvable_error:?}"
    );
    Ok(())
}

/// End to end across both halves: `POST /v1/sandboxes` claims a ready slot at
/// admission, and the create executor's admitted-claim path adopts exactly
/// that slot, so the sandbox runs under the ID the 202 returned. The real
/// Kubernetes substrate and pool store carry the claim through; only the node
/// RPC into the slot's pod is faked.
#[cfg(feature = "kubernetes")]
#[rstest::rstest]
#[case(
    None,
    None,
    "21600s",
    pplx_space_public_api::SandboxIdleAction::Suspend
)]
#[case(
    Some("600s"),
    None,
    "600s",
    pplx_space_public_api::SandboxIdleAction::Suspend
)]
#[case(
    Some("1.5m"),
    Some(pplx_space_public_api::SandboxIdleAction::Suspend),
    "90s",
    pplx_space_public_api::SandboxIdleAction::Suspend
)]
#[case(
    Some("600s"),
    Some(pplx_space_public_api::SandboxIdleAction::Delete),
    "600s",
    pplx_space_public_api::SandboxIdleAction::Delete
)]
#[tokio::test(flavor = "current_thread")]
async fn admission_claim_executes_and_runs_under_the_claimed_id(
    #[case] idle_timeout: Option<&str>,
    #[case] idle_action: Option<pplx_space_public_api::SandboxIdleAction>,
    #[case] expected_timeout: &str,
    #[case] expected_action: pplx_space_public_api::SandboxIdleAction,
) -> TestResult {
    use crate::routes::public::sandboxes::admission_test_fixture::{
        AdmissionPoolFixture, NAMESPACE, TEMPLATE,
    };
    use k8s_openapi::api::core::v1::{Pod, PodSpec, PodStatus};

    let fixture = AdmissionPoolFixture::new(1).await?;
    let slot = &fixture.slots[0];
    let node_id = NodeId::new();
    insert_node(&fixture.db.pool, node_id).await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
    ))
    .bind(node_id)
    .bind(&slot.node_name)
    .execute(&fixture.db.pool)
    .await?;
    let pod = Pod {
        metadata: kube::api::ObjectMeta {
            name: Some(slot.pod_name.clone()),
            namespace: Some(NAMESPACE.to_string()),
            uid: Some(slot.pod_uid.clone()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            node_name: Some(slot.node_name.clone()),
            ..Default::default()
        }),
        status: Some(PodStatus {
            phase: Some("Running".to_string()),
            pod_ip: Some(slot.pod_ip.clone()),
            ..Default::default()
        }),
    };
    let kubernetes = || {
        crate::controllers::substrate::kubernetes::tests::substrate_with_inventory_caches_and_pool(
            vec![pod.clone()],
            Vec::new(),
            fixture.db.pool.clone(),
        )
    };

    // Admission: the route claims the slot and returns its reserved ID.
    let admission_controller = AdmissionPoolFixture::controller_with(kubernetes());
    let mut request = fixture.request();
    request.idle_timeout = idle_timeout.map(|value| value.parse().unwrap());
    request.idle_action = idle_action;
    let response = fixture
        .create(Some(&admission_controller), "admission-e2e", request)
        .await
        .expect("create is recorded");
    assert_eq!(
        response.sandbox_id,
        pplx_space_public_api::PublicSandboxId::from(slot.reserved_sandbox_id)
    );
    let payload = fixture.payload(response.operation_id).await?;
    let admitted: AdmittedPoolClaim = payload
        .pool_claim
        .expect("the queued create carries the claim")
        .into();

    // Execution: the executor's input for this payload, through the
    // admitted-claim path from the previous change.
    let actor: User = sqlx::query_as(sql!(
        "SELECT id, email, password_hash, display_name, org_id, role, owner_source,
                created_at, updated_at
         FROM users WHERE id = $1"
    ))
    .bind(fixture.user_id)
    .fetch_one(&fixture.db.pool)
    .await?;
    let create_owner = SandboxCreateOwner::local(&actor);
    let req = crate::public_sandbox_operations::controller_create_request(
        payload.request.clone(),
        payload.node_pool.clone(),
    );
    let requested_template = req
        .template
        .clone()
        .expect("native creates name a template");
    let auto_lifecycle = req
        .auto_lifecycle
        .clone()
        .expect("native creates carry the default policy");
    let template_network_policy = NetworkPolicyV1::deny_all();
    let content_hash = fixture.content_hash.to_string();
    let mut input = pooled_create_input(
        &fixture.db.pool,
        &req,
        &requested_template,
        &auto_lifecycle,
        &template_network_policy,
        &create_owner,
    );
    input.template = TEMPLATE;
    input.content_hash = &content_hash;
    input.template_release_id = fixture.release_id;
    input.boot_mode = CreateBootMode::Fallthrough;
    input.planned_sandbox_id = SandboxId::from_uuid(payload.sandbox_id.as_uuid());
    input.sandbox_id_pinned = true;
    let substrate = NodeRpcStubbedKubernetes {
        inner: kubernetes(),
        prepared: Mutex::new(Vec::new()),
    };
    let result = SandboxController::try_create_from_admitted_pool_claim_with_substrate(
        &substrate,
        input,
        admitted,
        true,
        Arc::new(AtomicU64::new(0)),
    )
    .await?;

    let sandbox = result
        .response
        .expect("the admitted slot is adopted, not rebuilt cold");
    assert_eq!(sandbox.id, slot.reserved_sandbox_id);
    assert_eq!(
        *substrate.prepared.lock().unwrap(),
        vec![(slot.reserved_sandbox_id, true)]
    );
    let (lifecycle, active_node_id): (String, Option<NodeId>) = sqlx::query_as(sql!(
        "SELECT lifecycle, active_node_id FROM sandboxes WHERE id = $1"
    ))
    .bind(slot.reserved_sandbox_id)
    .fetch_one(&fixture.db.pool)
    .await?;
    assert_eq!(lifecycle, "started");
    assert_eq!(active_node_id, Some(node_id));
    let public = crate::routes::public::sandboxes::read_public_sandbox(
        &fixture.db.pool,
        fixture.org_id,
        pplx_space_public_api::PublicSandboxId::from(slot.reserved_sandbox_id),
    )
    .await
    .expect("created sandbox is readable");
    assert_eq!(public.idle_timeout.as_deref(), Some(expected_timeout));
    assert_eq!(public.idle_action, Some(expected_action));
    let (state, claim_id): (String, Option<Uuid>) = sqlx::query_as(sql!(
        "SELECT state, claim_id FROM kubernetes_pool_slots WHERE id = $1"
    ))
    .bind(slot.id)
    .fetch_one(&fixture.db.pool)
    .await?;
    assert_eq!(
        (state.as_str(), claim_id),
        ("bound", Some(admitted.claim_id)),
        "the admission claim is the one that bound"
    );
    Ok(())
}
