//! What each caller-facing surface reports for one sandbox, across every stored
//! lifecycle and every status a node can publish for it.
//!
//! Each row inserts a sandbox row and, unless the observation is absent, its
//! `sandbox_observed_states` record, then reads it the way each surface does:
//! the admin response and the ECW get, connect, timeout and refresh paths
//! through `tracked_response`, the ECW list through `cached_sandbox_response`,
//! and the native `GET /v1/sandboxes/{id}` through `read_public_sandbox`. The
//! rows start from a fetched row, so they also show stored lifecycles that a
//! handler's scoped fetch would refuse first.

use std::fmt::Write as _;

use axum::response::IntoResponse;
use pplx_space_common_core::types::{NodeId, OrgId, SandboxId};
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, pg_test_db};
use pplx_space_placement::{StoredSandboxLifecycle, test_support::NON_CANONICAL_ROWS};
use pplx_space_public_api::SandboxConditionType;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::{
    LiveKeepaliveTouchOutcome, build_detail_sandbox, build_listed_sandbox, connect_touch_recovery,
    should_ensure_usable_pod_on_connect, should_wake_on_connect, terminal_sandbox_error,
    translate_sandbox_state,
};
use crate::{
    controllers::sandbox::fetch_sandbox, e2b::schema::SandboxStateE2b, routes::AppState,
    routes::public::sandboxes::read_public_sandbox,
};
use pplx_space_common_svc::ecw_credential::EcwCredentialKeyring;

/// A padded terminal value, which the CHECK-less `sandboxes.lifecycle` column
/// can hold alongside `NON_CANONICAL_ROWS`.
const PADDED_TERMINAL: &str = " Stopped ";

/// Every status a node publishes: the sandbox-manager lifecycle labels.
const NODE_STATUSES: &[&str] = &[
    "unspecified",
    "creating",
    "created",
    "starting",
    "started",
    "stopping",
    "stopped",
    "deleting",
    "pausing",
    "paused",
    "resuming",
    "checkpointing",
    "suspending",
    "suspended",
    "restoring",
    "cleanup_required",
    "cleanup_failed",
    "quarantined",
    "failed",
];

/// Statuses no node publishes today: padded or mixed case, unknown, empty and
/// whitespace-only.
const ODD_STATUSES: &[&str] = &[
    " Paused ",
    "SUSPENDED",
    "Stopped",
    "FAILED",
    "lost",
    "",
    "   ",
];

#[derive(Clone, Copy)]
enum Observed {
    /// No `sandbox_observed_states` record.
    Absent,
    /// A record whose `status` is NULL.
    NullStatus,
    Status(&'static str),
}

impl Observed {
    fn label(self) -> String {
        match self {
            Self::Absent => "absent".to_string(),
            Self::NullStatus => "null".to_string(),
            Self::Status(status) => format!("{status:?}"),
        }
    }
}

/// Every value the stored vocabulary names, then values outside it.
fn stored_lifecycles() -> Vec<String> {
    StoredSandboxLifecycle::all_as_str()
        .chain(NON_CANONICAL_ROWS.iter().map(|(_, stored)| *stored))
        .chain([PADDED_TERMINAL])
        .map(str::to_string)
        .collect()
}

fn observations() -> Vec<Observed> {
    [Observed::Absent, Observed::NullStatus]
        .into_iter()
        .chain(NODE_STATUSES.iter().copied().map(Observed::Status))
        .chain(ODD_STATUSES.iter().copied().map(Observed::Status))
        .collect()
}

fn ecw_state(state: SandboxStateE2b) -> String {
    serde_json::to_value(state)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_else(|| format!("{state:?}"))
}

fn optional(value: Option<&str>) -> String {
    value.map_or_else(|| "none".to_string(), |value| format!("{value:?}"))
}

async fn insert_node(pool: &PgPool, node_id: NodeId) -> TestResult {
    sqlx::query(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '127.0.0.1', 'worker', 'space', 'registered')",
    )
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_row(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
    stored: &str,
    observed: Observed,
) -> TestResult {
    sqlx::query(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             active_node_id, template_id, content_hash, network_policy_generation
         )
         VALUES ($1, $2, $3, 'space', $4, 'on_node', $5, 'seed', 'hash-seed', 1)",
    )
    .bind(sandbox_id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(stored)
    .bind(node_id)
    .execute(pool)
    .await?;
    let status = match observed {
        Observed::Absent => return Ok(()),
        Observed::NullStatus => None,
        Observed::Status(status) => Some(status),
    };
    sqlx::query(
        "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status, template)
         VALUES ($1, $2, $3, 'seed')",
    )
    .bind(sandbox_id)
    .bind(node_id)
    .bind(status)
    .execute(pool)
    .await?;
    Ok(())
}

async fn render_row(
    state: &AppState,
    keyring: &EcwCredentialKeyring,
    sandbox_id: SandboxId,
) -> TestResult<String> {
    let pool = &state.pool;
    let row = fetch_sandbox(pool, sandbox_id)
        .await?
        .ok_or("inserted sandbox is missing")?;
    let tracked = state
        .sandbox_controller
        .tracked_response(pool, row.clone())
        .await?;
    let listed = state.sandbox_controller.cached_sandbox_response(&row);

    let admin = format!(
        "{}/{}",
        optional(tracked.lifecycle.as_deref()),
        optional(tracked.status.as_deref())
    );
    let ecw_list = match build_listed_sandbox(state, &row, &listed).await? {
        Some(item) => ecw_state(item.state),
        None => "hidden".to_string(),
    };
    let ecw_get = match build_detail_sandbox(keyring, &row, &tracked, None) {
        Ok(detail) => ecw_state(detail.state),
        Err(error) => error.status.as_u16().to_string(),
    };
    // Connect, timeout and refresh (ECW) and the native timeout share this gate.
    let ecw_gate = if translate_sandbox_state(&tracked).is_none() {
        terminal_sandbox_error(&tracked).status.as_u16().to_string()
    } else {
        "pass".to_string()
    };
    let wake = should_wake_on_connect(&row, &tracked);
    let pod = should_ensure_usable_pod_on_connect(&tracked);
    let not_started =
        connect_touch_recovery(LiveKeepaliveTouchOutcome::NotStarted, &row, &tracked, 1);
    let unreachable = connect_touch_recovery(
        LiveKeepaliveTouchOutcome::RuntimeUnreachable,
        &row,
        &tracked,
        1,
    );
    let unreachable_spent = connect_touch_recovery(
        LiveKeepaliveTouchOutcome::RuntimeUnreachable,
        &row,
        &tracked,
        0,
    );
    let native =
        match read_public_sandbox(pool, OrgId::from_uuid(SEED_ORG_ID), sandbox_id.into()).await {
            Ok(sandbox) => {
                let ready = sandbox
                    .conditions
                    .iter()
                    .find(|condition| condition.condition_type == SandboxConditionType::Ready)
                    .map_or_else(
                        || "none".to_string(),
                        |condition| condition.status.to_string(),
                    );
                format!("{} ready={ready}", sandbox.state)
            }
            Err(error) => error.into_response().status().as_u16().to_string(),
        };

    Ok(format!(
        "admin={admin:<40} ecw list={ecw_list:<7} get={ecw_get:<7} gate={ecw_gate:<4} \
         connect wake={wake:<5} pod={pod:<5} not_started={not_started:?} \
         unreachable={unreachable:?} spent={unreachable_spent:?} native={native}"
    ))
}

/// Pins today's outputs so a change to how a surface reads the stored
/// lifecycle and the observed status shows up here as a table diff.
#[tokio::test(flavor = "current_thread")]
async fn each_surface_renders_stored_lifecycle_and_observed_status() -> TestResult {
    Box::pin(async {
        let db = pg_test_db().await?;
        let state = AppState::from_pool(db.pool.clone()).await?;
        let keyring = EcwCredentialKeyring::from_current_key_for_test(b"state-render-table-key");
        let node_id = NodeId::new();
        insert_node(&db.pool, node_id).await?;

        let mut table = String::new();
        for stored in stored_lifecycles() {
            for observed in observations() {
                let sandbox_id = SandboxId::new();
                insert_row(&db.pool, sandbox_id, node_id, &stored, observed).await?;
                let rendered = render_row(&state, &keyring, sandbox_id).await?;
                writeln!(
                    table,
                    "stored={stored:<16} observed={:<20} {rendered}",
                    observed.label()
                )?;
            }
        }
        insta::assert_snapshot!(table);
        Ok(())
    })
    .await
}
