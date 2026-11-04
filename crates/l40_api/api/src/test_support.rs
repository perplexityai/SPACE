use pplx_space_common_svc::bearer_token::{
    SPACE_INTERNAL_CONTROL_TOKEN_ENV, SPACE_INTERNAL_TOKEN_ENV,
    SPACE_NODE_API_INTERNAL_TOKEN_PATH_ENV, SPACE_NODE_API_TOKEN_PATH_ENV,
};
use sqlx::PgPool;

use crate::cluster_admission::ClusterSandboxAdmissionConfig;
use crate::controllers::substrate::{Substrate, direct::DirectSubstrate};

#[cfg(feature = "kubernetes")]
pub(crate) mod kubernetes;

const SIM_WORKER_TOKEN: &str = "sim-worker-test-token";
const SIM_WORKER_CONTROL_TOKEN: &str = "sim-worker-test-control-token";

/// Enter before awaiting fixtures: authenticated clients need process credentials.
pub(crate) fn enter_sim_worker_process() -> bool {
    enter_sim_worker_process_with_token_paths(None, None)
}

/// [`enter_sim_worker_process`], but the node-api token paths name files this
/// test owns instead of being unset. Unset, the token reader falls back to the
/// host's fixed `/var/run/space-auth/*.token` paths and reads them when they
/// exist; these files carry the same tokens the fallback variables do.
///
/// The parent creates the files and keeps them until its child exits; the
/// child's own copy of the directory is unused.
pub(crate) fn enter_sim_worker_process_with_owned_token_files() -> bool {
    let tokens = tempfile::tempdir().expect("create test token directory");
    let node_api = tokens.path().join("node-api.token");
    let node_api_internal = tokens.path().join("node-api-internal.token");
    std::fs::write(&node_api, SIM_WORKER_TOKEN).expect("write node-api token");
    std::fs::write(&node_api_internal, SIM_WORKER_CONTROL_TOKEN)
        .expect("write node-api internal token");
    enter_sim_worker_process_with_token_paths(
        Some(node_api.as_os_str()),
        Some(node_api_internal.as_os_str()),
    )
}

fn enter_sim_worker_process_with_token_paths(
    node_api: Option<&std::ffi::OsStr>,
    node_api_internal: Option<&std::ffi::OsStr>,
) -> bool {
    pplx_space_test_env::enter_test_process(&[
        (
            "SPACE_INTERNAL_RPC_INSECURE_DEV",
            Some(std::ffi::OsStr::new("true")),
        ),
        (
            SPACE_INTERNAL_TOKEN_ENV,
            Some(std::ffi::OsStr::new(SIM_WORKER_TOKEN)),
        ),
        (
            SPACE_INTERNAL_CONTROL_TOKEN_ENV,
            Some(std::ffi::OsStr::new(SIM_WORKER_CONTROL_TOKEN)),
        ),
        (SPACE_NODE_API_TOKEN_PATH_ENV, node_api),
        (SPACE_NODE_API_INTERNAL_TOKEN_PATH_ENV, node_api_internal),
    ])
}

/// A Direct substrate over the test database; it needs no apiserver.
pub(crate) fn direct_substrate(pool: &PgPool) -> Substrate {
    Substrate::from_direct(DirectSubstrate::new(pool.clone()))
}

/// An enabled cluster-admission config with no Redis URL, so the admission
/// it configures has no token bucket.
pub(crate) fn enabled_config() -> ClusterSandboxAdmissionConfig {
    ClusterSandboxAdmissionConfig {
        enabled: true,
        refill_rps: 6.0,
        burst: 60.0,
        redis_url: None,
    }
}

/// Lock `public_operations` in `lock`, a connection inside a transaction, and
/// start `request`, returning once the request's first read of that table
/// waits on the lock. Its earlier reads have run; it finishes after `lock`'s
/// transaction ends, so what that transaction commits is what it reads next.
pub(crate) async fn start_a_request_blocked_on_operations<T: Send + 'static>(
    pool: &PgPool,
    lock: &mut sqlx::PgConnection,
    request: impl std::future::Future<Output = T> + Send + 'static,
) -> pplx_space_db_test_fixtures::TestDbResult<tokio::task::JoinHandle<T>> {
    use pplx_space_common_core::sql;

    sqlx::query(sql!(
        "LOCK TABLE public_operations IN ACCESS EXCLUSIVE MODE"
    ))
    .execute(&mut *lock)
    .await?;
    let request = tokio::spawn(request);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let waiting: bool = sqlx::query_scalar(sql!(
            "SELECT EXISTS (
                 SELECT 1 FROM pg_stat_activity
                  WHERE datname = current_database()
                    AND wait_event_type = 'Lock'
                    AND wait_event = 'relation'
             )"
        ))
        .fetch_one(pool)
        .await?;
        if waiting {
            return Ok(request);
        }
        if request.is_finished() || std::time::Instant::now() >= deadline {
            return Err("the request never read public_operations".into());
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

/// Insert `sandbox_id`'s row for `create`, a native create of it, and mark
/// `create` `succeeded`, as an execution that builds the sandbox does.
pub(crate) async fn record_the_row_and_succeed(
    connection: &mut sqlx::PgConnection,
    create: pplx_space_public_api::OperationId,
    sandbox_id: pplx_space_public_api::PublicSandboxId,
) -> pplx_space_db_test_fixtures::TestDbResult<()> {
    use pplx_space_common_core::sql;

    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, lifecycle, network_policy_override_json,
             network_policy_generation)
         SELECT $2, actor_user_id, org_id, 'started', $3, 1
         FROM public_operations WHERE operation_id = $1"
    ))
    .bind(create.as_uuid())
    .bind(sandbox_id.as_uuid())
    .bind(sqlx::types::Json(
        pplx_space_net_common::NetworkPolicyV1::default(),
    ))
    .execute(&mut *connection)
    .await?;
    sqlx::query(sql!(
        "UPDATE public_operations
         SET state = 'succeeded', claim_token = '', completed_at = NOW(), updated_at = NOW()
         WHERE operation_id = $1"
    ))
    .bind(create.as_uuid())
    .execute(&mut *connection)
    .await?;
    Ok(())
}

/// Run `request` until its first read of `public_operations` waits, then
/// insert `sandbox_id`'s row and mark `create` `succeeded` in one
/// transaction, standing for an execution whose row commit and completion
/// both land while the request waits. Returns what `request` returned; its
/// earlier reads of the sandbox row missed it, and its read of `create` sees
/// it finished.
pub(crate) async fn finish_create_while_a_request_reads_operations<T: Send + 'static>(
    pool: &PgPool,
    create: pplx_space_public_api::OperationId,
    sandbox_id: pplx_space_public_api::PublicSandboxId,
    request: impl std::future::Future<Output = T> + Send + 'static,
) -> pplx_space_db_test_fixtures::TestDbResult<T> {
    use sqlx::{Connection, PgConnection};

    // Outside the pool, which the request and the wait draw on.
    let mut connection = PgConnection::connect_with(&pool.connect_options()).await?;
    let mut lock = connection.begin().await?;
    let request = start_a_request_blocked_on_operations(pool, &mut lock, request).await?;
    record_the_row_and_succeed(&mut lock, create, sandbox_id).await?;
    lock.commit().await?;
    connection.close().await?;
    Ok(request.await?)
}
