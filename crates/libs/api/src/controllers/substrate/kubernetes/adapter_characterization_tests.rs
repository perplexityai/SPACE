//! Characterization tests for the Kubernetes substrate adapter: restore
//! hydration and generation-bound suspend, warm-pool adoption, and the
//! refusal and failure arms of execution create and teardown.
//!
//! Node-api calls go to the recording double in
//! [`super::scripted_test_doubles`], and so do Kubernetes calls except in
//! `suspend_execution_resumes_retirement_of_a_committed_suspend`, which uses
//! the non-recording `tests::deleted_pod_kubernetes_client`. Every database
//! write lands in a per-test Postgres database. A pod no test intends to dial
//! carries a TEST-NET address that nothing answers on.

mod create_and_teardown;
mod hydration_and_suspend;
mod pooled_adoption;

use std::collections::BTreeMap;

use pplx_space_common_core::kubernetes::{
    SANDBOX_EXECUTION_GENERATION_ANNOTATION, SANDBOX_ID_ANNOTATION,
};
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{
    AutoLifecyclePolicy, Email, OrgId, SandboxId, TemplateName, UserId,
};
use pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity;
use pplx_space_net_common::NetworkPolicyV1;
use sqlx::PgPool;

use super::scripted_test_doubles::{BoxError, WORKER_NODE};
use super::tests::insert_k8s_execution;
use super::*;
use crate::controllers::substrate::{ActorRef, CreateBootMode, SandboxResources, TemplateRef};

type TestResult = Result<(), BoxError>;

/// A documentation-range (TEST-NET-1) address for pods no test intends to
/// dial, so a regression that reaches for a runtime fails to connect instead
/// of reaching whatever listens on this host's loopback.
const UNDIALED_POD_IP: &str = "192.0.2.1";

/// The columns these tests vary on an active `sandbox_k8s_executions` row.
struct ExecutionSeed<'a> {
    generation: i64,
    pod_name: &'a str,
    /// `None` is stored as the empty UID the store reads as absent.
    pod_uid: Option<&'a str>,
    pod_ip: Option<&'a str>,
    k8s_node_name: Option<&'a str>,
    state: &'a str,
}

impl<'a> ExecutionSeed<'a> {
    fn running(pod_name: &'a str, pod_uid: &'a str) -> Self {
        Self {
            generation: 1,
            pod_name,
            pod_uid: Some(pod_uid),
            pod_ip: Some(UNDIALED_POD_IP),
            k8s_node_name: Some(WORKER_NODE),
            state: "running",
        }
    }
}

async fn insert_execution(
    pool: &PgPool,
    sandbox_id: SandboxId,
    seed: ExecutionSeed<'_>,
) -> TestResult {
    insert_k8s_execution(
        pool,
        sandbox_id,
        seed.generation,
        seed.pod_name,
        seed.pod_uid.unwrap_or_default(),
        seed.pod_ip,
        true,
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET k8s_node_name = $3, state = $4
          WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(sandbox_id)
    .bind(seed.generation)
    .bind(seed.k8s_node_name)
    .bind(seed.state)
    .execute(pool)
    .await?;
    Ok(())
}

async fn sandbox_lifecycle(pool: &PgPool, sandbox_id: SandboxId) -> Result<String, BoxError> {
    Ok(
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(pool)
            .await?,
    )
}

/// `(state, active, pod_uid, pod_ip)` of one execution generation, with an
/// empty UID read as absent the way the store reads it.
async fn execution_row(
    pool: &PgPool,
    sandbox_id: SandboxId,
    generation: i64,
) -> Result<(String, bool, Option<String>, Option<String>), BoxError> {
    Ok(sqlx::query_as(sql!(
        "SELECT state, active, NULLIF(BTRIM(pod_uid), ''), host(pod_ip)
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .fetch_one(pool)
    .await?)
}

async fn active_execution_count(pool: &PgPool, sandbox_id: SandboxId) -> Result<i64, BoxError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// A sandbox pod stamped with the identity annotations the create path writes.
fn sandbox_pod(
    pod_name: &str,
    pod_uid: &str,
    sandbox_id: SandboxId,
    generation: i64,
    phase: &str,
    pod_ip: Option<&str>,
) -> Pod {
    use k8s_openapi::api::core::v1::{PodSpec, PodStatus};
    Pod {
        metadata: kube::api::ObjectMeta {
            name: Some(pod_name.to_string()),
            namespace: Some("space".to_string()),
            uid: Some(pod_uid.to_string()),
            annotations: Some(BTreeMap::from([
                (SANDBOX_ID_ANNOTATION.to_string(), sandbox_id.to_string()),
                (
                    SANDBOX_EXECUTION_GENERATION_ANNOTATION.to_string(),
                    generation.to_string(),
                ),
            ])),
            ..Default::default()
        },
        spec: Some(PodSpec {
            node_name: Some(WORKER_NODE.to_string()),
            ..Default::default()
        }),
        status: Some(PodStatus {
            phase: Some(phase.to_string()),
            pod_ip: pod_ip.map(str::to_string),
            ..Default::default()
        }),
    }
}

/// The caller-owned inputs a create or a warm-pool claim borrows.
struct CreateInputs {
    actor: ActorRef,
    template: TemplateName,
    resources: SandboxResources,
    env: BTreeMap<String, String>,
    auto_lifecycle: AutoLifecyclePolicy,
    template_policy: NetworkPolicyV1,
}

impl CreateInputs {
    fn new() -> Self {
        Self {
            actor: ActorRef::new(
                UserId::from_uuid(Uuid::from_u128(0x10)),
                OrgId::from_uuid(Uuid::from_u128(0x1)),
                Email::parse("user@example.com".to_string()).expect("static email parses"),
            ),
            template: TemplateName::parse("seed").expect("static template name parses"),
            resources: SandboxResources {
                vcpu: None,
                memory_mib: None,
                disk_mib: None,
            },
            env: BTreeMap::new(),
            auto_lifecycle: AutoLifecyclePolicy::default(),
            template_policy: NetworkPolicyV1::default(),
        }
    }

    fn template_ref(&self) -> TemplateRef<'_> {
        TemplateRef {
            template: &self.template,
            content_hash: Some("sha256:seed"),
            boot_mode: CreateBootMode::Fallthrough,
            runtime: "fc",
        }
    }

    fn create_request<'a>(
        &'a self,
        sandbox_id: SandboxId,
        template_ref: &'a TemplateRef<'a>,
    ) -> CreateExecutionRequest<'a> {
        CreateExecutionRequest {
            metric_identity: SandboxApiMetricIdentity::native("create"),
            sandbox_id,
            actor: &self.actor,
            template_ref,
            resources: &self.resources,
            disk_io_limits: Default::default(),
            env: &self.env,
            auto_lifecycle: &self.auto_lifecycle,
            network_policy_override: None,
            network_policy_generation: 1,
            template_network_policy: &self.template_policy,
            template_network_policy_generation: 1,
            restore_from: None,
            wait_until_ready: false,
            volume_encryption: false,
            node_pool: None,
        }
    }
}
