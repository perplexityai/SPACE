//! Rows and lock probes the tests of the execution-ownership modules share,
//! and the orphan plan's ([`crate::orphan::plan`]).
//!
//! Each probe runs in a second transaction with `NOWAIT` and reports whether
//! it found the row locked (`lock_not_available`, SQLSTATE `55P03`). Which
//! probes a lock refuses pins its mode: `FOR UPDATE` refuses even
//! `FOR KEY SHARE`; `FOR SHARE` refuses `FOR NO KEY UPDATE` but admits
//! another `FOR SHARE`.

use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

/// A Kubernetes-managed sandbox row in `lifecycle`.
pub(crate) async fn insert_sandbox(pool: &PgPool, lifecycle: &str) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, template_id,
             network_policy_generation)
         VALUES ($1, $2, $3, 'k8s', $4, 'tmpl-store-test', 1)"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(lifecycle)
    .execute(pool)
    .await?;
    Ok(id)
}

/// One execution row as a test seeds it.
#[derive(Debug, Clone)]
pub(crate) struct SeededExecution {
    pub(crate) id: Uuid,
    pub(crate) pod_name: String,
}

/// An execution of `sandbox_id` at `generation`, with a pod IP and
/// `pod_uid` as given.
pub(crate) async fn insert_execution(
    pool: &PgPool,
    sandbox_id: SandboxId,
    generation: i64,
    state: &str,
    active: bool,
    pod_uid: Option<&str>,
) -> TestResult<SeededExecution> {
    let id = Uuid::now_v7();
    let pod_name = format!("sbx-test-{id}");
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (id, sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip,
             state, active, terminated_at)
         VALUES ($1, $2, $3, $4, 'space-sandboxes', $5, '10.0.0.5'::INET,
                 $6, $7, CASE WHEN $7 THEN NULL ELSE NOW() - INTERVAL '1 day' END)"
    ))
    .bind(id)
    .bind(sandbox_id)
    .bind(generation)
    .bind(&pod_name)
    .bind(pod_uid)
    .bind(state)
    .bind(active)
    .execute(pool)
    .await?;
    Ok(SeededExecution { id, pod_name })
}

/// The execution row's live tuple's inserting transaction id: it changes
/// exactly when a statement wrote the row. A schema variant can make
/// `sandbox_k8s_executions` a view, which has no `xmin`, so this reads the
/// table behind it.
pub(crate) async fn execution_version(pool: &PgPool, id: Uuid) -> TestResult<String> {
    Ok(sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(format!(
        "SELECT xmin::TEXT FROM {} WHERE id = $1",
        pplx_space_db_test_fixtures::physical_table("sandbox_k8s_executions")?
    ))))
    .bind(id)
    .fetch_one(pool)
    .await?)
}

/// The execution row's `active` and `state`, as stored.
pub(crate) async fn execution_state(pool: &PgPool, id: Uuid) -> TestResult<(bool, String)> {
    Ok(sqlx::query_as(sql!(
        "SELECT active, state FROM sandbox_k8s_executions WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?)
}

/// Whether another transaction's `FOR KEY SHARE NOWAIT` on the execution row
/// fails: only a `FOR UPDATE` lock refuses it.
pub(crate) async fn execution_key_share_blocked(pool: &PgPool, id: Uuid) -> TestResult<bool> {
    let mut probe = pool.begin().await?;
    let result = sqlx::query(sql!(
        "SELECT 1 FROM sandbox_k8s_executions WHERE id = $1 FOR KEY SHARE NOWAIT"
    ))
    .bind(id)
    .fetch_optional(&mut *probe)
    .await;
    probe.rollback().await?;
    lock_not_available(result)
}

/// Whether another transaction's `FOR NO KEY UPDATE NOWAIT` on the execution
/// row fails: every mode stronger than `FOR KEY SHARE` refuses it.
pub(crate) async fn execution_no_key_update_blocked(pool: &PgPool, id: Uuid) -> TestResult<bool> {
    let mut probe = pool.begin().await?;
    let result = sqlx::query(sql!(
        "SELECT 1 FROM sandbox_k8s_executions WHERE id = $1 FOR NO KEY UPDATE NOWAIT"
    ))
    .bind(id)
    .fetch_optional(&mut *probe)
    .await;
    probe.rollback().await?;
    lock_not_available(result)
}

/// Whether another transaction's `FOR SHARE NOWAIT` on the execution row
/// fails: `FOR NO KEY UPDATE` and `FOR UPDATE` refuse it, `FOR SHARE` does
/// not.
pub(crate) async fn execution_share_blocked(pool: &PgPool, id: Uuid) -> TestResult<bool> {
    let mut probe = pool.begin().await?;
    let result = sqlx::query(sql!(
        "SELECT 1 FROM sandbox_k8s_executions WHERE id = $1 FOR SHARE NOWAIT"
    ))
    .bind(id)
    .fetch_optional(&mut *probe)
    .await;
    probe.rollback().await?;
    lock_not_available(result)
}

/// Whether another transaction's `FOR KEY SHARE NOWAIT` on the sandbox row
/// fails: only a `FOR UPDATE` lock refuses it.
pub(crate) async fn sandbox_key_share_blocked(pool: &PgPool, id: SandboxId) -> TestResult<bool> {
    let mut probe = pool.begin().await?;
    let result = sqlx::query(sql!(
        "SELECT 1 FROM sandboxes WHERE id = $1 FOR KEY SHARE NOWAIT"
    ))
    .bind(id)
    .fetch_optional(&mut *probe)
    .await;
    probe.rollback().await?;
    lock_not_available(result)
}

/// Hold the execution row `FOR UPDATE` in a transaction of its own, for a
/// `NOWAIT` read to meet.
pub(crate) async fn hold_execution(
    pool: &PgPool,
    id: Uuid,
) -> TestResult<sqlx::Transaction<'static, sqlx::Postgres>> {
    let mut holder = pool.begin().await?;
    sqlx::query(sql!(
        "SELECT 1 FROM sandbox_k8s_executions WHERE id = $1 FOR UPDATE"
    ))
    .bind(id)
    .fetch_one(&mut *holder)
    .await?;
    Ok(holder)
}

/// Whether `error` is `lock_not_available` (`55P03`).
pub(crate) fn is_lock_not_available(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|error| error.code())
        .is_some_and(|code| code == "55P03")
}

fn lock_not_available(
    result: Result<Option<sqlx::postgres::PgRow>, sqlx::Error>,
) -> TestResult<bool> {
    match result {
        Ok(_) => Ok(false),
        Err(error) if is_lock_not_available(&error) => Ok(true),
        Err(error) => Err(error.into()),
    }
}
