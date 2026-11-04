//! `GetExecutionCurrency`: whether an execution generation is still a
//! sandbox's active execution, asked by the node its pod was placed on.

use std::str::FromStr;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{ExecutionGeneration, SandboxId};
use tonic::{Request, Response, Status};

use crate::ClusterApiService;
use crate::proto::{ExecutionCurrency, GetExecutionCurrencyRequest, GetExecutionCurrencyResponse};

impl ClusterApiService {
    pub(crate) async fn get_execution_currency(
        &self,
        request: Request<GetExecutionCurrencyRequest>,
    ) -> Result<Response<GetExecutionCurrencyResponse>, Status> {
        let node_ids = self.caller_node_ids(&request).await?;
        let req = request.into_inner();
        let sandbox_id = SandboxId::from_str(req.sandbox_id.trim())
            .map_err(|e| Status::invalid_argument(format!("invalid sandbox_id: {e}")))?;
        let generation = ExecutionGeneration::new(req.execution_generation)
            .map_err(|e| Status::invalid_argument(format!("invalid execution_generation: {e}")))?;
        // Scoped to the execution's own node, the way the node-bound reads are
        // scoped to sandboxes placed on the caller: only the node that may run
        // the runtime learns whether it may.
        let active = sqlx::query_scalar::<_, bool>(sql!(
            "SELECT e.active
               FROM sandbox_k8s_executions e
               JOIN node_k8s nk ON nk.k8s_node_name = e.k8s_node_name
              WHERE e.sandbox_id = $1
                AND e.generation = $2
                AND nk.node_id = ANY($3)"
        ))
        .bind(sandbox_id)
        .bind(generation.get())
        .bind(&node_ids)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Status::internal(format!("execution currency lookup: {e}")))?;
        let currency = match active {
            Some(true) => ExecutionCurrency::Current,
            Some(false) => ExecutionCurrency::Retired,
            None => ExecutionCurrency::Unspecified,
        };
        Ok(Response::new(GetExecutionCurrencyResponse {
            currency: currency.into(),
        }))
    }
}
