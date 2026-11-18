//! Production [`ExecutionStore`] backed by Postgres.

use std::collections::HashMap;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{SandboxId, SubstrateKind};
use sqlx::PgPool;
use uuid::Uuid;

use super::backend::ExecutionStore;
use super::classify::ExecutionRow;

#[derive(sqlx::FromRow)]
struct Row {
    pod_name: String,
    sandbox_id: Uuid,
    generation: i64,
    active: bool,
    state: String,
}

pub struct ExecutionDbStore {
    pool: PgPool,
    namespace: String,
}

impl ExecutionDbStore {
    pub fn new(pool: PgPool, namespace: String) -> Self {
        Self { pool, namespace }
    }
}

impl ExecutionStore for ExecutionDbStore {
    /// One indexed lookup on the unique `pod_name`, scoped to the sandbox
    /// namespace and to Kubernetes-managed sandboxes.
    async fn rows_for_pods(
        &self,
        pod_names: &[String],
    ) -> Result<HashMap<String, ExecutionRow>, sqlx::Error> {
        if pod_names.is_empty() {
            return Ok(HashMap::new());
        }
        let rows: Vec<Row> = sqlx::query_as(sql!(
            "SELECT e.pod_name, e.sandbox_id, e.generation, e.active, e.state
               FROM sandbox_k8s_executions e
               JOIN sandboxes s ON s.id = e.sandbox_id
              WHERE e.pod_name = ANY($1)
                AND e.pod_namespace = $2
                AND s.managed_by = $3"
        ))
        .bind(pod_names)
        .bind(&self.namespace)
        .bind(SubstrateKind::Kubernetes)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    row.pod_name,
                    ExecutionRow {
                        sandbox_id: SandboxId::from_uuid(row.sandbox_id),
                        generation: row.generation,
                        active: row.active,
                        state: row.state,
                    },
                )
            })
            .collect())
    }
}
