//! The node-observation cache: what each node last reported about the
//! sandboxes and facet elements it holds. This store owns
//! `sandbox_observed_states` (the last state a node published for each
//! sandbox), `node_facet_observed_status` (what a node last reported applying
//! for a facet/element pair) and `node_facet_conditions` (the conditions it
//! reported for one). They are one store because they describe the same
//! observations and are written and swept together: a publication writes a
//! sandbox's observed row and its facet rows, and the cache's deletes (the
//! census prune, the observed-projection GC, a sandbox's teardown) remove
//! them together, some in one statement. None of it is the durable record of
//! a sandbox; that is `sandboxes` and its snapshots.
//! `sandbox_snapshot_inventory`, the snapshots a node last published for each
//! sandbox, stays `pplx-space-sandbox-store`'s. Its rows reference
//! `sandbox_observed_states` with `ON DELETE CASCADE`, so every delete of an
//! observed row here also deletes that sandbox's inventory rows.
//!
//! Reconcile-state cleanup: deletes the queue refs, observed status and
//! condition rows for the given facet/element pairs inside the caller's
//! transaction. Those rows are keyed by facet and element, not foreign-keyed
//! to what they describe. Callers use it when the element goes away (a
//! sandbox's hard delete in
//! `pplx_space_sandbox_store::delete::delete_with_dependents` or a create
//! rollback, a template delete) and when no node owns it any more (a sandbox
//! recovered to suspended, quarantined, or recovered from quarantine).
//!
//! The same rows' other verbs live alongside: [`observed_states`] for
//! `sandbox_observed_states`, [`observed_status`] for
//! `node_facet_observed_status`, [`conditions`] for
//! `node_facet_conditions`, and [`direct`] for the statements on these
//! tables that join a Direct-only table: the `node_facet_observed_status`
//! writes that resolve a sandbox's node through the Direct route table, a
//! node's sandbox state publication and the api's observed-state reads.
//! [`observed_status`]'s orphaned-facet sweep also
//! deletes the swept rows' `node_facet_conditions` rows, and
//! [`observed_gc`]'s sweeps delete cold `sandbox_observed_states` rows with
//! their sandboxes' facet rows, each in one statement. Some verbs in
//! [`observed_states`], [`observed_status`], [`conditions`] and
//! [`observed_gc`] read, without locking and in the same statement, tables
//! other stores own: `sandboxes`, `sandbox_k8s_executions`, `nodes`,
//! `node_k8s` and `node_template_inventory`. [`direct`]'s verbs read
//! `sandboxes`, `nodes`, `node_k8s`, `node_template_inventory`, the template
//! release tables and the Direct-only tables the same way.

use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;

use pplx_space_state_reconciliation::{NetworkPolicyTarget, ReconcileElementRef};

pub mod conditions;
pub mod direct;
pub mod observed_gc;
pub mod observed_states;
pub mod observed_status;

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
