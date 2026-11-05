//! What each caller-facing surface reports for one sandbox, across every stored
//! lifecycle and every status a node can publish for it.
//!
//! Each row inserts a sandbox row and, unless the observation is absent, its
//! `sandbox_observed_states` record, published by the row's route node or by
//! another node, then reads it the way each surface does:
//! the admin response and the ECW get, connect, timeout and refresh paths
//! through `tracked_response`, the ECW list through `cached_sandbox_response`,
//! and the native `GET /v1/sandboxes/{id}` through `read_public_sandbox`. The
//! rows start from a fetched row, so they also show stored lifecycles that a
//! handler's scoped fetch would refuse first.

use std::fmt::Write as _;
use std::sync::Arc;

use axum::response::IntoResponse;
use futures_util::StreamExt as _;
use pplx_space_common_core::types::{NodeId, OrgId, SandboxId};
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, pg_test_db_with_max_connections};
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

/// Statuses a node other than the row's route node still reports, such as
/// the node a sandbox left or was lost on.
const OTHER_NODE_STATUSES: &[&str] = &["started", "paused", "failed"];

#[derive(Clone, Copy)]
enum Observed {
    /// No `sandbox_observed_states` record.
    Absent,
    /// A record whose `status` is NULL.
    NullStatus,
    Status(&'static str),
    /// A record published by a node other than the row's route node.
    OtherNode(&'static str),
}

impl Observed {
    fn label(self) -> String {
        match self {
            Self::Absent => "absent".to_string(),
            Self::NullStatus => "null".to_string(),
            Self::Status(status) => format!("{status:?}"),
            Self::OtherNode(status) => format!("other {status:?}"),
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
        .chain(OTHER_NODE_STATUSES.iter().copied().map(Observed::OtherNode))
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
    [node_id, other_node_id]: [NodeId; 2],
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
    let (observed_node_id, status) = match observed {
        Observed::Absent => return Ok(()),
        Observed::NullStatus => (node_id, None),
        Observed::Status(status) => (node_id, Some(status)),
        Observed::OtherNode(status) => (other_node_id, Some(status)),
    };
    sqlx::query(
        "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status, template)
         VALUES ($1, $2, $3, 'seed')",
    )
    .bind(sandbox_id)
    .bind(observed_node_id)
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

/// The tables [`each_surface_renders_stored_lifecycle_and_observed_status`]
/// deals its rows into: one per `shard` value it runs. Each holds about 25
/// rows, under a second on a loaded CI runner.
const SHARDS: usize = 24;

/// Pins today's outputs so a change to how a surface reads the stored
/// lifecycle and the observed status shows up here as a table diff. The
/// (stored lifecycle, observation) rows are dealt into [`SHARDS`] tables by
/// position, each its own test and snapshot, so every row the lists
/// generate, a new one included, lands in exactly one of them.
#[rstest::rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_surface_renders_stored_lifecycle_and_observed_status(
    #[values(
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23
    )]
    shard: usize,
) -> TestResult {
    // Each row is its own sandbox, so rows render concurrently, each on its
    // own connection; `buffered` keeps the table in row order. Four at a time:
    // nextest runs up to one shard per core against the run's shared Postgres
    // server, and more connections per shard slowed every shard down.
    const CONCURRENT_ROWS: usize = 4;
    Box::pin(async {
        let db = pg_test_db_with_max_connections(CONCURRENT_ROWS as u32 + 2).await?;
        let state = AppState::from_pool(db.pool.clone()).await?;
        let keyring = Arc::new(EcwCredentialKeyring::from_current_key_for_test(
            b"state-render-table-key",
        ));
        let nodes = [NodeId::new(), NodeId::new()];
        for node_id in nodes {
            insert_node(&db.pool, node_id).await?;
        }

        let rows = stored_lifecycles()
            .into_iter()
            .flat_map(|stored| {
                observations()
                    .into_iter()
                    .map(move |observed| (stored.clone(), observed))
            })
            .skip(shard)
            .step_by(SHARDS);
        let rendered: Vec<_> = futures_util::stream::iter(rows)
            .map(|(stored, observed)| {
                let state = state.clone();
                let keyring = keyring.clone();
                tokio::spawn(async move {
                    let sandbox_id = SandboxId::new();
                    insert_row(&state.pool, sandbox_id, nodes, &stored, observed).await?;
                    let rendered = render_row(&state, &keyring, sandbox_id).await?;
                    TestResult::Ok(format!(
                        "stored={stored:<16} observed={:<20} {rendered}",
                        observed.label()
                    ))
                })
            })
            .buffered(CONCURRENT_ROWS)
            .collect()
            .await;
        let mut table = String::new();
        for row in rendered {
            writeln!(table, "{}", row??)?;
        }
        insta::assert_snapshot!(
            format!("each_surface_renders_stored_lifecycle_and_observed_status_{shard}"),
            table
        );
        Ok(())
    })
    .await
}
