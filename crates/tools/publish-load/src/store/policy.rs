use anyhow::{Context, Result, ensure};
use pplx_space_common_core::types::{NodeId, PolicyAuthority, SandboxId};
use pplx_space_sql_locator::sql;
use pplx_space_state_reconciliation::{FACET_NETWORK_POLICY, NetworkPolicyTarget};
use sqlx::PgPool;

/// Seed a synthetic failed policy application without changing any existing
/// policy observation. Inserts take normal row/index locks; duplicate conditions
/// are errors because the scenario expects the prior one to have been cleared.
pub async fn seed_policy_failure(
    pool: &PgPool,
    node_id: NodeId,
    target: NetworkPolicyTarget,
) -> Result<()> {
    let element = target.as_element_string();
    sqlx::query(sql!(
        "INSERT INTO node_facet_observed_status (node_id, facet, element, observed_at)
         VALUES ($1, $2, $3, NOW())
         ON CONFLICT (node_id, facet, element) DO NOTHING"
    ))
    .bind(node_id)
    .bind(FACET_NETWORK_POLICY)
    .bind(&element)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_facet_conditions
             (node_id, facet, element, condition_type, condition_status, last_transition_at)
         VALUES ($1, $2, $3, $4, $5, NOW())"
    ))
    .bind(node_id)
    .bind(FACET_NETWORK_POLICY)
    .bind(&element)
    .bind("ApplyFailed")
    .bind("True")
    .execute(pool)
    .await?;
    Ok(())
}

/// Check a generated policy observation and its condition without explicit row
/// locks. Missing observations or mismatched outcomes fail the synthetic run.
pub async fn verify_policy_observation(
    pool: &PgPool,
    node_id: NodeId,
    sandbox_id: SandboxId,
    generation: i64,
    failure_present: bool,
) -> Result<()> {
    let element = NetworkPolicyTarget::sandbox(sandbox_id).as_element_string();
    let matches = sqlx::query_scalar::<_, bool>(sql!(
        "SELECT COALESCE(nfos.observed_generation = $4, FALSE)
                    AND nfos.observed_payload IS NOT NULL
                    AND COALESCE(nfos.applied_authority = $5, FALSE)
                    AND COALESCE(sos.network_policy_authority = $5, FALSE)
                    AND EXISTS (
                        SELECT 1 FROM node_facet_conditions nfc
                        WHERE nfc.node_id = nfos.node_id AND nfc.facet = nfos.facet
                          AND nfc.element = nfos.element AND nfc.condition_type = $6
                    ) = $7
         FROM node_facet_observed_status nfos
         JOIN sandbox_observed_states sos ON sos.sandbox_id = $8 AND sos.node_id = $1
         WHERE nfos.node_id = $1 AND nfos.facet = $2 AND nfos.element = $3"
    ))
    .bind(node_id)
    .bind(FACET_NETWORK_POLICY)
    .bind(&element)
    .bind(generation)
    .bind(PolicyAuthority::Cluster.as_str())
    .bind("ApplyFailed")
    .bind(failure_present)
    .bind(sandbox_id)
    .fetch_optional(pool)
    .await?
    .context("network-policy observation missing")?;
    ensure!(matches, "network-policy observation or condition differed");
    Ok(())
}

/// Check the fixture template's policy without explicit locks. A generation
/// report must clear its failure, while an omitted generation must preserve it.
pub async fn verify_template_policy_observation(
    pool: &PgPool,
    node_id: NodeId,
    generation: i64,
    failure_present: bool,
) -> Result<()> {
    let target = NetworkPolicyTarget::template(
        pplx_space_db_test_fixtures::publish_load::TEMPLATE_NAME.parse()?,
    );
    let matches = sqlx::query_scalar::<_, bool>(sql!(
        "SELECT COALESCE(nfos.observed_generation = $4, FALSE)
                    AND nfos.observed_payload IS NOT NULL
                    AND COALESCE(nfos.applied_authority = $5, FALSE)
                    AND EXISTS (
                        SELECT 1 FROM node_facet_conditions nfc
                        WHERE nfc.node_id = nfos.node_id AND nfc.facet = nfos.facet
                          AND nfc.element = nfos.element AND nfc.condition_type = $6
                    ) = $7
         FROM node_facet_observed_status nfos
         WHERE nfos.node_id = $1 AND nfos.facet = $2 AND nfos.element = $3"
    ))
    .bind(node_id)
    .bind(FACET_NETWORK_POLICY)
    .bind(target.as_element_string())
    .bind(generation)
    .bind(PolicyAuthority::Cluster.as_str())
    .bind("ApplyFailed")
    .bind(failure_present)
    .fetch_optional(pool)
    .await?
    .context("template network-policy observation missing")?;
    ensure!(
        matches,
        "template network-policy observation or condition differed"
    );
    Ok(())
}
