//! Reconcile-state cleanup: deletes the queue refs, observed status and
//! condition rows for the given facet/element pairs inside the caller's
//! transaction. Those rows are keyed by facet and element, not foreign-keyed
//! to what they describe. Callers use it when the element goes away (a
//! sandbox's hard delete in
//! `pplx_space_sandbox_store::delete::delete_with_dependents` or a create
//! rollback, a template delete) and when no node owns it any more (a sandbox
//! recovered to suspended, quarantined, or recovered from quarantine).

use pplx_space_common_core::{sql, types::SandboxId};

use pplx_space_state_reconciliation::{NetworkPolicyTarget, ReconcileElementRef};

pub fn sandbox_reconcile_cleanup_refs(sandbox_id: SandboxId) -> Vec<ReconcileElementRef> {
    vec![
        ReconcileElementRef::network_policy(NetworkPolicyTarget::sandbox(sandbox_id)),
        ReconcileElementRef::sandbox_desired_state(sandbox_id),
        ReconcileElementRef::sandbox_inventory(sandbox_id),
    ]
}

/// Delete every node's queue ref, observed status, and condition rows for the
/// given facet/element pairs.
pub async fn delete_reconcile_state_for_elements(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    refs: &[ReconcileElementRef],
) -> Result<(), sqlx::Error> {
    let facets = refs
        .iter()
        .map(|reference| reference.facet_str().to_string())
        .collect::<Vec<_>>();
    let elements = refs
        .iter()
        .map(ReconcileElementRef::element_string)
        .collect::<Vec<_>>();
    if facets.is_empty() {
        return Ok(());
    }
    sqlx::query(sql!(
        "WITH refs AS (
             SELECT DISTINCT facet, element
             FROM UNNEST($1::TEXT[], $2::TEXT[]) AS refs(facet, element)
         )
         DELETE FROM node_reconcile_queue nrq
         USING refs
         WHERE nrq.facet = refs.facet
           AND nrq.element = refs.element"
    ))
    .bind(&facets)
    .bind(&elements)
    .execute(&mut **tx)
    .await?;
    sqlx::query(sql!(
        "WITH refs AS (
             SELECT DISTINCT facet, element
             FROM UNNEST($1::TEXT[], $2::TEXT[]) AS refs(facet, element)
         )
         DELETE FROM node_facet_observed_status nfos
         USING refs
         WHERE nfos.facet = refs.facet
           AND nfos.element = refs.element"
    ))
    .bind(&facets)
    .bind(&elements)
    .execute(&mut **tx)
    .await?;
    sqlx::query(sql!(
        "WITH refs AS (
             SELECT DISTINCT facet, element
             FROM UNNEST($1::TEXT[], $2::TEXT[]) AS refs(facet, element)
         )
         DELETE FROM node_facet_conditions nfc
         USING refs
         WHERE nfc.facet = refs.facet
           AND nfc.element = refs.element"
    ))
    .bind(&facets)
    .bind(&elements)
    .execute(&mut **tx)
    .await?;
    Ok(())
}
