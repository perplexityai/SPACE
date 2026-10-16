use crate::db::Connection;

use crate::db::route_activation_claims::{OpenRouteActivationClaim, claim_matches};

#[derive(Debug, thiserror::Error)]
pub(crate) enum OpenRouteActivationAdmissionError {
    #[error(transparent)]
    Database(#[from] crate::db::Error),
    #[error(
        "runtime activation identity generation is not newer than retained generation {retained}"
    )]
    StaleOrConflicting { retained: u64 },
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
    let provenance = crate::db::activation_route_provenance(conn, sandbox_id)?;
    let retained = observed_generation_floor(provenance);
    let requested = claim.requested_generation();
    if claim_matches(conn, sandbox_id, claim)? {
        return Ok(());
    }
    if let Some(retained) = retained
        && requested <= retained
    {
        return Err(OpenRouteActivationAdmissionError::StaleOrConflicting { retained });
    }

    Ok(())
}
