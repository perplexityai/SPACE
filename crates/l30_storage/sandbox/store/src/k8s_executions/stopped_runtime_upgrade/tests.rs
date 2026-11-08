//! Each stay-stopped runtime upgrade statement's losing case, where its guard
//! matches no row and nothing changes, beside the case it wins; and what each
//! read returns for an execution that records an upgrade and one that
//! doesn't.

use pplx_space_accounts::NodeId;
use pplx_space_db_test_fixtures::{insert_ready_worker_node, try_pg_test_db};
use pplx_space_sandbox_model::{SandboxActivationAttemptId, SandboxId};
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::*;
use crate::k8s_executions::ownership_test_fixture::insert_sandbox;
use crate::k8s_executions::retire::abandon_unmaterialized_create_claim;

const LEASE_TTL_SECS: i64 = 300;
const TARGET: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const AUTHORIZATION: &str = "runtime-environment-test:e1";

fn attempt() -> SandboxActivationAttemptId {
    SandboxActivationAttemptId::from_uuid(Uuid::now_v7())
}

/// An execution's columns these statements read or write, as a test seeds
/// and reads them back.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
struct Row {
    active: bool,
    state: String,
    pod_uid: Option<String>,
    pod_ip: Option<String>,
    k8s_node_name: Option<String>,
    activation_attempt_id: Option<Uuid>,
    activation_attempt_target_state: Option<String>,
    /// Whether the attempt's lease still runs by the database clock.
    lease_live: Option<bool>,
    runtime_upgrade_operation_id: Option<Uuid>,
    runtime_upgrade_target_environment_ref: Option<String>,
    runtime_upgrade_authorization_id: Option<String>,
}

impl Row {
    /// An active, unmaterialized `creating` claim `attempt` holds toward
    /// `stopped` with a live lease, recording `operation` if given.
    fn claim(attempt: SandboxActivationAttemptId, operation: Option<Uuid>) -> Self {
        Self {
            active: true,
            state: "creating".to_owned(),
            pod_uid: None,
            pod_ip: None,
            k8s_node_name: None,
            activation_attempt_id: Some(attempt.as_uuid()),
            activation_attempt_target_state: Some("stopped".to_owned()),
            lease_live: Some(true),
            runtime_upgrade_operation_id: operation,
            runtime_upgrade_target_environment_ref: operation.map(|_| TARGET.to_owned()),
            runtime_upgrade_authorization_id: operation.map(|_| AUTHORIZATION.to_owned()),
        }
    }

    fn materialized(mut self) -> Self {
        self.state = "running".to_owned();
        self.pod_uid = Some("pod-uid-1".to_owned());
        self.pod_ip = Some("10.0.0.5/32".to_owned());
        self
    }

    fn expired(mut self) -> Self {
        self.lease_live = Some(false);
        self
    }

    fn targeting(mut self, target_state: &str) -> Self {
        self.activation_attempt_target_state = Some(target_state.to_owned());
        self
    }

    fn on_node(mut self, node_name: &str) -> Self {
        self.k8s_node_name = Some(node_name.to_owned());
        self
    }

    fn retired(mut self) -> Self {
        self.active = false;
        self
    }
}

async fn insert_row(
    pool: &PgPool,
    sandbox_id: SandboxId,
    generation: i64,
    row: &Row,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, state, active, pod_uid, pod_ip,
             k8s_node_name, activation_attempt_id, activation_attempt_target_state,
             activation_attempt_expires_at, runtime_upgrade_operation_id,
             runtime_upgrade_target_environment_ref, runtime_upgrade_authorization_id,
             terminated_at)
         VALUES ($1, $2, $3, 'space-sandboxes', $4, $5, $6, $7::INET, $8, $9, $10,
                 CASE WHEN $11 THEN NOW() + INTERVAL '1 hour'
                      WHEN NOT $11 THEN NOW() - INTERVAL '1 hour' END,
                 $12, $13, $14,
                 CASE WHEN $5 THEN NULL ELSE NOW() END)"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(format!("sbx-{}-{generation}", sandbox_id.as_uuid()))
    .bind(&row.state)
    .bind(row.active)
    .bind(&row.pod_uid)
    .bind(&row.pod_ip)
    .bind(&row.k8s_node_name)
    .bind(row.activation_attempt_id)
    .bind(&row.activation_attempt_target_state)
    .bind(row.lease_live)
    .bind(row.runtime_upgrade_operation_id)
    .bind(&row.runtime_upgrade_target_environment_ref)
    .bind(&row.runtime_upgrade_authorization_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn read_row(pool: &PgPool, sandbox_id: SandboxId, generation: i64) -> TestResult<Row> {
    Ok(sqlx::query_as::<_, Row>(sql!(
        "SELECT active, state, pod_uid, pod_ip::TEXT AS pod_ip, k8s_node_name,
                activation_attempt_id, activation_attempt_target_state,
                activation_attempt_expires_at > NOW() AS lease_live,
                runtime_upgrade_operation_id, runtime_upgrade_target_environment_ref,
                runtime_upgrade_authorization_id
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .fetch_one(pool)
    .await?)
}

/// A sandbox in `lifecycle` with one execution, `row`, at generation 1.
async fn seed(pool: &PgPool, lifecycle: &str, row: &Row) -> TestResult<SandboxId> {
    let sandbox_id = insert_sandbox(pool, lifecycle).await?;
    insert_row(pool, sandbox_id, 1, row).await?;
    Ok(sandbox_id)
}

/// Place `sandbox_id` on a registered node named `node_name`, with `target`
/// as its chosen target node (`None` for none).
async fn place(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_name: &str,
    target: Option<NodeId>,
) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    insert_ready_worker_node(pool, node_id).await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
    ))
    .bind(node_id)
    .bind(node_name)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = $2, target_node_id = $3 WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(target)
    .execute(pool)
    .await?;
    Ok(node_id)
}

#[tokio::test(flavor = "current_thread")]
async fn a_stopped_claim_is_read_only_while_active_and_targeting_stopped() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let owner = attempt();
    let operation = Uuid::now_v7();

    // Another target, a retired execution, and an execution no attempt
    // holds read as no claim.
    let mut unheld = Row::claim(owner, None);
    unheld.activation_attempt_id = None;
    for row in [
        Row::claim(owner, Some(operation)).targeting("started"),
        Row::claim(owner, Some(operation)).retired(),
        unheld,
    ] {
        let sandbox_id = seed(pool, "stopped", &row).await?;
        assert_eq!(read_stopped_claim(pool, sandbox_id, "stopped").await?, None);
    }

    // A drain's claim records nothing; the upgrade's records its operation.
    for recorded in [None, Some(operation)] {
        let sandbox_id = seed(
            pool,
            "stopped",
            &Row::claim(owner, recorded).on_node("node-a"),
        )
        .await?;
        assert_eq!(
            read_stopped_claim(pool, sandbox_id, "stopped").await?,
            Some(StoppedClaim {
                generation: 1,
                activation_attempt_id: owner,
                pod_name: format!("sbx-{}-1", sandbox_id.as_uuid()),
                k8s_node_name: Some("node-a".to_owned()),
                lease_live: true,
                runtime_upgrade_operation_id: recorded,
                runtime_upgrade_target_environment_ref: recorded.map(|_| TARGET.to_owned()),
                runtime_upgrade_authorization_id: recorded.map(|_| AUTHORIZATION.to_owned()),
            })
        );
    }

    // An expired lease reads as not live.
    let sandbox_id = seed(
        pool,
        "stopped",
        &Row::claim(owner, Some(operation)).expired(),
    )
    .await?;
    let claim = read_stopped_claim(pool, sandbox_id, "stopped").await?;
    assert_eq!(claim.map(|claim| claim.lease_live), Some(false));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn recording_an_upgrade_loses_to_a_moved_execution_or_another_operations_record() -> TestResult
{
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let owner = attempt();
    let operation = Uuid::now_v7();
    let record = UpgradeRecord {
        operation_id: operation,
        target_environment_ref: TARGET,
        authorization_id: AUTHORIZATION,
    };

    // Another attempt holds it, it's retired, or it records another
    // operation, whose backup may still be owed.
    for row in [
        Row::claim(attempt(), None),
        Row::claim(owner, None).retired(),
        Row::claim(owner, Some(Uuid::now_v7())),
    ] {
        let sandbox_id = seed(pool, "stopped", &row).await?;
        let mut tx = pool.begin().await?;
        assert!(!record_upgrade(&mut *tx, sandbox_id, 1, owner, record).await?);
        tx.commit().await?;
        assert_eq!(read_row(pool, sandbox_id, 1).await?, row);
    }
    // Another generation.
    let row = Row::claim(owner, None);
    let sandbox_id = seed(pool, "stopped", &row).await?;
    let mut tx = pool.begin().await?;
    assert!(!record_upgrade(&mut *tx, sandbox_id, 2, owner, record).await?);
    tx.commit().await?;
    assert_eq!(read_row(pool, sandbox_id, 1).await?, row);

    // The claim's own record, and recording the same operation again.
    let mut tx = pool.begin().await?;
    assert!(record_upgrade(&mut *tx, sandbox_id, 1, owner, record).await?);
    assert!(record_upgrade(&mut *tx, sandbox_id, 1, owner, record).await?);
    tx.commit().await?;
    assert_eq!(
        read_row(pool, sandbox_id, 1).await?,
        Row::claim(owner, Some(operation))
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn clearing_a_record_touches_only_the_active_generation() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let owner = attempt();
    let recorded = Row::claim(owner, Some(Uuid::now_v7()));

    let sandbox_id = seed(pool, "stopped", &recorded.clone().retired()).await?;
    let mut tx = pool.begin().await?;
    assert_eq!(clear_recorded_upgrade(&mut *tx, sandbox_id, 1).await?, 0);
    tx.commit().await?;
    assert_eq!(
        read_row(pool, sandbox_id, 1).await?,
        recorded.clone().retired()
    );

    let sandbox_id = seed(pool, "stopped", &recorded).await?;
    let mut tx = pool.begin().await?;
    assert_eq!(clear_recorded_upgrade(&mut *tx, sandbox_id, 2).await?, 0);
    assert_eq!(clear_recorded_upgrade(&mut *tx, sandbox_id, 1).await?, 1);
    tx.commit().await?;
    assert_eq!(
        read_row(pool, sandbox_id, 1).await?,
        Row::claim(owner, None)
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn expiring_an_attempt_keeps_it_only_with_a_record() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let owner = attempt();
    let operation = Uuid::now_v7();

    // Another attempt holds it: nothing changes.
    let row = Row::claim(owner, Some(operation));
    let sandbox_id = seed(pool, "stopped", &row).await?;
    assert_eq!(
        expire_attempt_keeping_record(pool, sandbox_id, 1, attempt()).await?,
        0
    );
    assert_eq!(read_row(pool, sandbox_id, 1).await?, row);

    // With a record, the attempt and its target stay, with the lease ended.
    assert_eq!(
        expire_attempt_keeping_record(pool, sandbox_id, 1, owner).await?,
        1
    );
    assert_eq!(read_row(pool, sandbox_id, 1).await?, row.expired());

    // Without one, the attempt is given up.
    let sandbox_id = seed(pool, "stopped", &Row::claim(owner, None)).await?;
    assert_eq!(
        expire_attempt_keeping_record(pool, sandbox_id, 1, owner).await?,
        1
    );
    let mut given_up = Row::claim(owner, None).expired();
    given_up.activation_attempt_id = None;
    given_up.activation_attempt_target_state = None;
    assert_eq!(read_row(pool, sandbox_id, 1).await?, given_up);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn reserving_an_expired_upgrade_loses_to_a_live_lease_or_a_moved_claim() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let expired = attempt();
    let reserved = attempt();
    let operation = Uuid::now_v7();

    // A live lease, another attempt, another target, or no record.
    for (row, holder) in [
        (Row::claim(expired, Some(operation)), expired),
        (Row::claim(expired, Some(operation)).expired(), attempt()),
        (
            Row::claim(expired, Some(operation))
                .expired()
                .targeting("started"),
            expired,
        ),
        (Row::claim(expired, None).expired(), expired),
    ] {
        let sandbox_id = seed(pool, "stopped", &row).await?;
        assert!(
            !reserve_expired_upgrade(
                pool,
                sandbox_id,
                1,
                holder,
                reserved,
                "stopped",
                LEASE_TTL_SECS,
            )
            .await?
        );
        assert_eq!(read_row(pool, sandbox_id, 1).await?, row);
    }

    let row = Row::claim(expired, Some(operation)).expired();
    let sandbox_id = seed(pool, "stopped", &row).await?;
    assert!(
        !reserve_expired_upgrade(
            pool,
            sandbox_id,
            2,
            expired,
            reserved,
            "stopped",
            LEASE_TTL_SECS
        )
        .await?
    );
    assert!(
        reserve_expired_upgrade(
            pool,
            sandbox_id,
            1,
            expired,
            reserved,
            "stopped",
            LEASE_TTL_SECS
        )
        .await?
    );
    assert_eq!(
        read_row(pool, sandbox_id, 1).await?,
        Row::claim(reserved, Some(operation))
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_unmaterialized_claim_with_a_record_keeps_its_execution_with_its_lease_ended()
-> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let owner = attempt();
    let operation = Uuid::now_v7();

    // The lease end's losing cases: no record, a materialized execution,
    // another attempt, another generation.
    for (row, holder, generation) in [
        (Row::claim(owner, None), owner, 1),
        (Row::claim(owner, Some(operation)).materialized(), owner, 1),
        (Row::claim(owner, Some(operation)), attempt(), 1),
        (Row::claim(owner, Some(operation)), owner, 2),
    ] {
        let sandbox_id = seed(pool, "stopped", &row).await?;
        let mut tx = pool.begin().await?;
        assert_eq!(
            end_recorded_create_claim_lease(&mut *tx, sandbox_id, generation, Some(holder)).await?,
            0
        );
        tx.commit().await?;
        assert_eq!(read_row(pool, sandbox_id, 1).await?, row);
    }

    // A claim with a record: the retire matches nothing, the lease ends and
    // the execution stays with its record.
    let row = Row::claim(owner, Some(operation));
    let sandbox_id = seed(pool, "stopped", &row).await?;
    let mut tx = pool.begin().await?;
    assert_eq!(
        end_recorded_create_claim_lease(&mut *tx, sandbox_id, 1, Some(owner)).await?,
        1
    );
    assert!(!abandon_unmaterialized_create_claim(&mut *tx, sandbox_id, 1, Some(owner)).await?);
    tx.commit().await?;
    assert_eq!(read_row(pool, sandbox_id, 1).await?, row.expired());

    // A claim without one is the retire's.
    let sandbox_id = seed(pool, "stopped", &Row::claim(owner, None)).await?;
    let mut tx = pool.begin().await?;
    assert!(abandon_unmaterialized_create_claim(&mut *tx, sandbox_id, 1, Some(owner)).await?);
    tx.commit().await?;
    let retired = read_row(pool, sandbox_id, 1).await?;
    assert!(!retired.active);
    assert_eq!(retired.state, "replaced");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_stale_pods_execution_is_kept_only_when_it_records_an_upgrade() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let owner = attempt();
    let operation = Uuid::now_v7();

    // No record, or a retired execution, or another generation.
    for (row, generation) in [
        (Row::claim(owner, None).materialized(), 1),
        (
            Row::claim(owner, Some(operation)).materialized().retired(),
            1,
        ),
        (Row::claim(owner, Some(operation)).materialized(), 2),
    ] {
        let sandbox_id = seed(pool, "stopped", &row).await?;
        assert!(!keep_recorded_upgrade_after_stale_pod(pool, sandbox_id, generation).await?);
        assert_eq!(read_row(pool, sandbox_id, 1).await?, row);
    }

    let sandbox_id = seed(
        pool,
        "stopped",
        &Row::claim(owner, Some(operation)).materialized(),
    )
    .await?;
    assert!(keep_recorded_upgrade_after_stale_pod(pool, sandbox_id, 1).await?);
    assert_eq!(
        read_row(pool, sandbox_id, 1).await?,
        Row::claim(owner, Some(operation)).expired()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_leftover_upgrade_names_its_execution_or_its_disk_pins_node() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let owner = attempt();
    let operation = Uuid::now_v7();

    // No record, or a claim toward another target.
    for row in [
        Row::claim(owner, None),
        Row::claim(owner, Some(operation)).targeting("started"),
    ] {
        let sandbox_id = seed(pool, "stopped", &row).await?;
        assert_eq!(
            read_leftover_upgrade(pool, sandbox_id, "stopped").await?,
            None
        );
    }

    let leftover =
        |sandbox_id: SandboxId, node_name: Option<&str>, lease_live: bool| LeftoverUpgrade {
            pod_name: format!("sbx-{}-1", sandbox_id.as_uuid()),
            pod_uid: None,
            operation_id: operation,
            lease_live,
            node_name: node_name.map(str::to_owned),
            generation: 1,
            activation_attempt_id: owner,
        };

    // The execution's own node wins over the placement.
    let sandbox_id = seed(
        pool,
        "stopped",
        &Row::claim(owner, Some(operation)).on_node("execution-node"),
    )
    .await?;
    place(pool, sandbox_id, "placement-node-a", None).await?;
    assert_eq!(
        read_leftover_upgrade(pool, sandbox_id, "stopped").await?,
        Some(leftover(sandbox_id, Some("execution-node"), true))
    );

    // With none, the node the placement pins the disk to.
    let sandbox_id = seed(
        pool,
        "stopped",
        &Row::claim(owner, Some(operation)).expired(),
    )
    .await?;
    place(pool, sandbox_id, "placement-node-b", None).await?;
    assert_eq!(
        read_leftover_upgrade(pool, sandbox_id, "stopped").await?,
        Some(leftover(sandbox_id, Some("placement-node-b"), false))
    );

    // A chosen target elsewhere pins nothing yet.
    let sandbox_id = seed(pool, "stopped", &Row::claim(owner, Some(operation))).await?;
    let elsewhere = NodeId::new();
    insert_ready_worker_node(pool, elsewhere).await?;
    place(pool, sandbox_id, "placement-node-c", Some(elsewhere)).await?;
    assert_eq!(
        read_leftover_upgrade(pool, sandbox_id, "stopped").await?,
        Some(leftover(sandbox_id, None, true))
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn the_lifecycle_and_evacuation_marker_reads_match_only_their_value() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox_id = insert_sandbox(pool, "stopped").await?;
    let mut tx = pool.begin().await?;
    assert!(sandbox_in_lifecycle(&mut *tx, sandbox_id, "stopped").await?);
    assert!(!sandbox_in_lifecycle(&mut *tx, sandbox_id, "started").await?);
    assert!(!sandbox_in_lifecycle(&mut *tx, SandboxId::new(), "stopped").await?);
    assert!(!evacuation_marked(&mut *tx, sandbox_id, "stopped").await?);
    tx.commit().await?;

    sqlx::query(sql!(
        "UPDATE sandboxes SET evacuation_restore_target = 'stopped' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    let mut tx = pool.begin().await?;
    assert!(evacuation_marked(&mut *tx, sandbox_id, "stopped").await?);
    assert!(!evacuation_marked(&mut *tx, sandbox_id, "started").await?);
    tx.commit().await?;
    Ok(())
}
