//! Shared SQL fragments for recording sandbox quarantine entries.
//!
//! These are the column lists and the conflict assignments for the
//! control-plane Postgres `sandbox_quarantine_state` and
//! `sandbox_quarantine_history` tables, which the cluster controller's
//! orphaned-sandbox sweep and the internal API's published-state writer splice
//! into their statements. Other writers of those tables still write their SQL
//! inline, and sbx-mgr's SQLite tables of the same names belong to
//! `pplx-space-sbx-mgr-store`, not here. The fragments sit in their own crate,
//! outside `pplx-space-common-svc`, until store functions for these tables
//! replace them.

pub fn quarantine_state_insert_columns() -> &'static str {
    "sandbox_id,
     quarantine_id,
     entered_at,
     updated_at,
     entered_from_lifecycle,
     source,
     reason_code,
     reason_message,
     evidence,
     next_action,
     last_observed_node_id"
}

pub fn quarantine_state_conflict_assignments() -> &'static str {
    "quarantine_id = EXCLUDED.quarantine_id,
     entered_at = EXCLUDED.entered_at,
     updated_at = EXCLUDED.updated_at,
     entered_from_lifecycle = EXCLUDED.entered_from_lifecycle,
     source = EXCLUDED.source,
     reason_code = EXCLUDED.reason_code,
     reason_message = EXCLUDED.reason_message,
     evidence = EXCLUDED.evidence,
     next_action = EXCLUDED.next_action,
     next_action_after = NULL,
     next_action_owner = NULL,
     attempt_count = 0,
     claim_token = NULL,
     claim_expires_at = NULL,
     last_error = NULL,
     last_observed_node_id = EXCLUDED.last_observed_node_id"
}

pub fn quarantine_history_insert_columns() -> &'static str {
    "id,
     quarantine_id,
     sandbox_id,
     occurred_at,
     source,
     action,
     reason_code,
     message,
     next_action,
     evidence"
}
