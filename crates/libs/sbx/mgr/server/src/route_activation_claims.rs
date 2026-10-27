use crate::db::Connection;

use crate::db::route_activation_claims::OpenRouteActivationClaim;

#[derive(Debug, thiserror::Error)]
pub(crate) enum OpenRouteActivationAdmissionError {
    #[error(transparent)]
    Database(#[from] crate::db::Error),
    #[error(
        "runtime activation identity generation is not newer than retained generation {retained}"
    )]
    StaleOrConflicting { retained: u64 },
}

/// How long a retried activation waits for the control plane's answer about
/// its execution before it starts without one.
const EXECUTION_CURRENCY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Whether the control plane says it retired `generation` of `sandbox_id`.
/// Only that answer counts: a control plane that can't be reached, predates
/// the question or doesn't know the execution leaves the activation to start
/// as it would without asking, and the backup fence still refuses the
/// capture of a runtime it superseded.
pub(crate) async fn retired_on_cluster(
    sandbox_id: &str,
    generation: pplx_space_common_core::types::ExecutionGeneration,
) -> bool {
    use pplx_space_node_api_client::proto::{ExecutionCurrency, GetExecutionCurrencyRequest};
    let ask = async {
        let mut cluster = pplx_space_node_api_client::cluster::connect_from_env()
            .await
            .map_err(|error| error.to_string())?;
        cluster
            .get_execution_currency(GetExecutionCurrencyRequest {
                sandbox_id: sandbox_id.to_string(),
                execution_generation: generation.get(),
            })
            .await
            .map(|reply| reply.into_inner().currency())
            .map_err(|status| format!("{:?}: {}", status.code(), status.message()))
    };
    match tokio::time::timeout(EXECUTION_CURRENCY_TIMEOUT, ask).await {
        Ok(Ok(currency)) => currency == ExecutionCurrency::Retired,
        Ok(Err(error)) => {
            tracing::warn!(sandbox_id, %generation, %error,
                "could not ask whether a retried activation's execution is current; starting it");
            false
        }
        Err(_) => {
            tracing::warn!(sandbox_id, %generation,
                "asking whether a retried activation's execution is current timed out; \
                 starting it");
            false
        }
    }
}

/// Highest live or retained generation observed on this node.
///
/// Cluster-fenced claims reject a different attempt at the live generation;
/// the legacy and Direct fallback keeps its narrower retained-history fence.
fn observed_generation_floor(provenance: crate::db::ActivationRouteProvenance) -> Option<u64> {
    [
        provenance.current_route_generation.map(|value| value.get()),
        provenance
            .current_execution_generation
            .and_then(|value| u64::try_from(value.get()).ok()),
        provenance
            .retained_route_generation
            .map(|value| value.get()),
        provenance
            .retained_execution_generation
            .and_then(|value| u64::try_from(value.get()).ok()),
    ]
    .into_iter()
    .flatten()
    .max()
}

pub(crate) fn validate_cluster_fenced_activation(
    conn: &Connection,
    sandbox_id: &str,
    claim: &OpenRouteActivationClaim,
) -> Result<(), OpenRouteActivationAdmissionError> {
    let provenance =
        crate::db::read::sandbox_view(conn, sandbox_id).activation_route_provenance()?;
    let retained = observed_generation_floor(provenance);
    let requested = claim.requested_generation();
    if crate::db::read::sandbox_view(conn, sandbox_id)
        .route_claim()
        .matches(claim)?
    {
        return Ok(());
    }
    if let Some(retained) = retained
        && requested <= retained
    {
        return Err(OpenRouteActivationAdmissionError::StaleOrConflicting { retained });
    }

    Ok(())
}
