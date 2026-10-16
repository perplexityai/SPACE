use pplx_space_common_svc::bearer_token::{
    SPACE_INTERNAL_CONTROL_TOKEN_ENV, SPACE_INTERNAL_TOKEN_ENV,
    SPACE_NODE_API_INTERNAL_TOKEN_PATH_ENV, SPACE_NODE_API_TOKEN_PATH_ENV,
};
use sqlx::PgPool;

use crate::cluster_admission::ClusterSandboxAdmissionConfig;
use crate::controllers::substrate::{Substrate, direct::DirectSubstrate};

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
    Substrate::Direct(Box::new(DirectSubstrate::new(pool.clone())))
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

/// A single-stage `CommandIdle + Suspend` policy, as a template default would set.
/// Command submission resets its idle clock; access keepalives don't.
pub(crate) fn command_idle_suspend_policy(
    timeout_secs: u64,
) -> pplx_space_common_core::types::AutoLifecyclePolicy {
    use pplx_space_common_core::types::{
        AutoLifecycleConditionSet, AutoLifecycleConditionSetId, AutoLifecycleIdleAction,
        AutoLifecyclePolicy, AutoLifecycleStage, AutoLifecycleWakePolicy, AutoLifecycleWakeSource,
    };

    AutoLifecyclePolicy {
        wake: AutoLifecycleWakePolicy {
            on: vec![AutoLifecycleWakeSource::Command],
        },
        condition_sets: std::collections::BTreeMap::from([(
            AutoLifecycleConditionSetId::CommandIdle,
            AutoLifecycleConditionSet::command_idle(),
        )]),
        stages: vec![AutoLifecycleStage {
            condition_set: AutoLifecycleConditionSetId::CommandIdle,
            at: timeout_secs,
            action: AutoLifecycleIdleAction::Suspend,
        }],
    }
    .normalized()
}
