//! Synthetic Postgres fixture for admission-time warm-pool claims: one org,
//! one template release, one enabled default-shape pool config, and a number
//! of ready slots, driven through the real Kubernetes substrate's pool store.

use pplx_space_cluster_pool::{NewPoolSlot, WarmPoolStore};
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{
    ContentHash, Digest, IdempotencyKey, MemorySize, OrgId, SandboxId, TemplateName, UserId,
    VcpuCount,
};
use pplx_space_db_test_fixtures::{TestDb, TestDbResult, pg_test_db};
use pplx_space_public_api::{CreateSandboxRequest, OperationId, SandboxOperationResponse};
use sqlx::types::Json as SqlJson;
use uuid::Uuid;

use crate::controllers::{
    sandbox::SandboxController,
    substrate::{Substrate, kubernetes::KubernetesSubstrate},
};
use crate::public_sandbox_operations::CreateSandboxOperationPayload;
use crate::routes::public::error::PublicApiError;

/// The fixture's template. Unique, so no seeded release can shadow it.
pub(crate) const TEMPLATE: &str = "admission-pool";
pub(crate) const NAMESPACE: &str = "space";
pub(crate) const VCPU: u32 = 2;
pub(crate) const MEMORY_MIB: u32 = 1024;

/// A ready slot the fixture inserted.
pub(crate) struct FixtureSlot {
    pub(crate) id: Uuid,
    pub(crate) reserved_sandbox_id: SandboxId,
    pub(crate) pod_name: String,
    pub(crate) pod_uid: String,
    pub(crate) pod_ip: String,
    pub(crate) node_name: String,
}

pub(crate) struct AdmissionPoolFixture {
    pub(crate) db: TestDb,
    pub(crate) org_id: OrgId,
    pub(crate) user_id: UserId,
    pub(crate) release_id: Uuid,
    pub(crate) content_hash: ContentHash,
    pub(crate) slots: Vec<FixtureSlot>,
}

impl AdmissionPoolFixture {
    pub(crate) async fn new(ready_slots: usize) -> TestDbResult<Self> {
        let db = pg_test_db().await?;
        let org_id = OrgId::new();
        let user_id = UserId::new();
        sqlx::query(sql!(
            "INSERT INTO organizations (id, name, slug) VALUES ($1, 'Admission Pool Test', $2)"
        ))
        .bind(org_id)
        .bind(format!("admission-pool-{org_id}"))
        .execute(&db.pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
             VALUES ($1, $2, 'x', 'Admission Pool', $3, 'member')"
        ))
        .bind(user_id)
        .bind(format!("admission-pool-{user_id}@example.com"))
        .bind(org_id)
        .execute(&db.pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO templates (id, name, org_id, visibility)
             VALUES ($1, $2, NULL, 'generic')"
        ))
        .bind(Uuid::now_v7())
        .bind(TEMPLATE)
        .execute(&db.pool)
        .await?;
        let release_id = Uuid::now_v7();
        let content_hash = ContentHash::new(Digest::sha256([0x5a; 32]));
        sqlx::query(sql!(
            "INSERT INTO template_releases (id, template_name, type, hash)
             VALUES ($1, $2, 'fc', $3)"
        ))
        .bind(release_id)
        .bind(TEMPLATE)
        .bind(content_hash.to_string())
        .execute(&db.pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO template_release_tags (template_release_id, template_name, tag)
             VALUES ($1, $2, 'latest')"
        ))
        .bind(release_id)
        .bind(TEMPLATE)
        .execute(&db.pool)
        .await?;
        let config_id = Uuid::now_v7();
        sqlx::query(sql!(
            "INSERT INTO kubernetes_pool_configs (
                 id, template_release_id, vcpu, memory_mib, target_ready_slots,
                 refill_per_tick, max_warming_slots, namespace, enabled
             ) VALUES ($1, $2, $3, $4, 4, 1, 1, $5, TRUE)"
        ))
        .bind(config_id)
        .bind(release_id)
        .bind(VCPU as i32)
        .bind(MEMORY_MIB as i32)
        .bind(NAMESPACE)
        .execute(&db.pool)
        .await?;

        let store = WarmPoolStore::new(db.pool.clone());
        let template_name = TemplateName::parse(TEMPLATE)?;
        let mut slots = Vec::with_capacity(ready_slots);
        for index in 0..ready_slots {
            let reserved_sandbox_id = SandboxId::new();
            let pod_name = format!("space-sbx-admission-{index}-g1");
            let pod_uid = format!("admission-pod-uid-{index}");
            let pod_ip = format!("10.90.0.{}", index + 10);
            let node_name = "admission-worker".to_string();
            let slot = store
                .insert_warming_slot(NewPoolSlot {
                    config_id,
                    template_release_id: release_id,
                    reserved_sandbox_id,
                    template_name: &template_name,
                    content_hash,
                    vcpu: VcpuCount::try_from_u32(VCPU)?,
                    memory: MemorySize::mib(u64::from(MEMORY_MIB))?,
                    pod_name: &pod_name,
                    pod_namespace: NAMESPACE,
                })
                .await?;
            store
                .mark_slot_ready(
                    slot.id,
                    &pod_uid,
                    &pod_ip,
                    &node_name,
                    &pplx_space_common_core::types::K8sNodeUid::parse("original-worker-uid")?,
                )
                .await?;
            slots.push(FixtureSlot {
                id: slot.id,
                reserved_sandbox_id,
                pod_name,
                pod_uid,
                pod_ip,
                node_name,
            });
        }

        Ok(Self {
            db,
            org_id,
            user_id,
            release_id,
            content_hash,
            slots,
        })
    }

    /// A controller over the real Kubernetes substrate and this database.
    pub(crate) fn controller(&self) -> SandboxController {
        Self::controller_with(
            crate::controllers::substrate::kubernetes::tests::substrate_with_store(
                self.db.pool.clone(),
            ),
        )
    }

    pub(crate) fn controller_with(substrate: KubernetesSubstrate) -> SandboxController {
        SandboxController::new(Substrate::Kubernetes(Box::new(substrate)))
    }

    /// A default-shape native create of the fixture template.
    pub(crate) fn request(&self) -> CreateSandboxRequest {
        CreateSandboxRequest {
            template: format!("{TEMPLATE}:latest")
                .parse()
                .expect("fixture template reference parses"),
            runtime: None,
            name: None,
            env: Default::default(),
            metadata: Default::default(),
            resources: None,
            network_policy: None,
            auto_lifecycle: None,
            idle_timeout: None,
            idle_action: None,
        }
    }

    /// `POST /v1/sandboxes` as the route records it, with admission-time
    /// claiming on (`Some`) or off (`None`).
    pub(crate) async fn create(
        &self,
        pool_claims: Option<&SandboxController>,
        idempotency_key: &str,
        request: CreateSandboxRequest,
    ) -> Result<SandboxOperationResponse, PublicApiError> {
        super::create_native_sandbox_operation(
            &self.db.pool,
            self.org_id,
            self.user_id,
            &IdempotencyKey::parse(idempotency_key).expect("fixture idempotency key parses"),
            request,
            None,
            pool_claims,
        )
        .await
    }

    /// `(state, claim_id)` of a fixture slot.
    pub(crate) async fn slot_state(&self, slot_id: Uuid) -> TestDbResult<(String, Option<Uuid>)> {
        Ok(sqlx::query_as(sql!(
            "SELECT state, claim_id FROM kubernetes_pool_slots WHERE id = $1"
        ))
        .bind(slot_id)
        .fetch_one(&self.db.pool)
        .await?)
    }

    pub(crate) async fn slot_count(&self, state: &str) -> TestDbResult<i64> {
        Ok(sqlx::query_scalar(sql!(
            "SELECT COUNT(*) FROM kubernetes_pool_slots
             WHERE template_release_id = $1 AND state = $2"
        ))
        .bind(self.release_id)
        .bind(state)
        .fetch_one(&self.db.pool)
        .await?)
    }

    pub(crate) async fn payload(
        &self,
        operation_id: OperationId,
    ) -> TestDbResult<CreateSandboxOperationPayload> {
        let SqlJson(payload): SqlJson<serde_json::Value> = sqlx::query_scalar(sql!(
            "SELECT payload_json FROM public_operations WHERE operation_id = $1"
        ))
        .bind(operation_id.as_uuid())
        .fetch_one(&self.db.pool)
        .await?;
        Ok(serde_json::from_value(payload)?)
    }

    pub(crate) async fn target_id(&self, operation_id: OperationId) -> TestDbResult<String> {
        Ok(sqlx::query_scalar(sql!(
            "SELECT target_id FROM public_operations WHERE operation_id = $1"
        ))
        .bind(operation_id.as_uuid())
        .fetch_one(&self.db.pool)
        .await?)
    }

    pub(crate) async fn create_operations(&self) -> TestDbResult<i64> {
        Ok(sqlx::query_scalar(sql!(
            "SELECT COUNT(*) FROM public_operations WHERE org_id = $1"
        ))
        .bind(self.org_id)
        .fetch_one(&self.db.pool)
        .await?)
    }

    pub(crate) async fn enable_org_encryption(&self) -> TestDbResult<()> {
        sqlx::query(sql!(
            "UPDATE organizations SET byok_enabled = TRUE WHERE id = $1"
        ))
        .bind(self.org_id)
        .execute(&self.db.pool)
        .await?;
        Ok(())
    }
}
