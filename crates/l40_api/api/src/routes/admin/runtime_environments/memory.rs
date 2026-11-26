//! `PUT /runtime-environments/{environment_ref}/memory`: retire or unretire an
//! environment's checkpoint memory, gated on restore-time memory-drop coverage.

use axum::{
    Extension, Json,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use pplx_space_execution_spec::{ArtifactComponentKind, EnvironmentReference};
use pplx_space_runtime_environment_policy_store::{
    self as policy_store, SetEnvironmentMemoryError,
};
use pplx_space_sandbox_store::memory_drop::coverage::{
    COVERS_ENCRYPTED_ROWS, COVERS_NODE_HELD_ROWS, MemoryDropCoverageGroup, coverage_groups,
};
use serde::{Deserialize, Serialize};

use super::{
    AuditContext, QUEUE_RECONCILE_FAILED, already_stale, enqueue_policy_reconcile,
    expected_generation, policy_change, policy_read_error, precondition_holds,
};
use crate::controllers::substrate::runtime_upgrade::{
    CatalogEnvironment, RecordedExecution, attribute,
};
use crate::middleware::auth::AuthUser;
use crate::routes::AppState;
use crate::routes::admin::support::{
    ApiError, api_error, db_error, parse_environment_ref, parse_json_body, read_db_error,
    require_managing_org_admin,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MemoryAction {
    Retire,
    Unretire,
}

impl MemoryAction {
    const fn memory_retired(self) -> bool {
        matches!(self, Self::Retire)
    }

    const fn audit_requested_state(self) -> &'static str {
        match self {
            Self::Retire => "memory_retired",
            Self::Unretire => "memory_active",
        }
    }
}

fn parse_memory_action(value: &str) -> Result<MemoryAction, ApiError> {
    match value {
        "retire" => Ok(MemoryAction::Retire),
        "unretire" => Ok(MemoryAction::Unretire),
        _ => Err(api_error(
            StatusCode::BAD_REQUEST,
            format!(
                "unknown runtime environment memory action {value:?}; \
                 expected one of: retire, unretire"
            ),
        )),
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SetMemoryRequest {
    action: String,
    reason: String,
    /// Retire even though sandboxes or in-use template releases whose
    /// environment can't be determined exist. Never waives sandboxes or in-use
    /// template releases attributed to the retiring environment. Only meaningful
    /// with `action: retire`.
    #[serde(default)]
    strand: bool,
}

#[derive(Debug, Serialize)]
struct SetMemoryResponse {
    generation: u64,
    environment_ref: EnvironmentReference,
    memory_retired: bool,
}

/// Counts of runnable non-already-dropped sandboxes that restore-time memory
/// drop cannot convert if `environment` retires its memory now.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub(super) struct UncoveredMemoryDropCounts {
    /// Attributed to the retiring environment on a non-Kubernetes substrate.
    pub(super) not_kubernetes: u64,
    /// Attributed to the retiring environment with volume encryption enabled.
    pub(super) encrypted: u64,
    /// Attributed to the retiring environment while live, `stopped` on an
    /// un-upgraded disk, holding an active `restoring` lease, or whose last
    /// node is still registered in `node_k8s`.
    pub(super) node_held: u64,
    /// Attributed to the retiring environment without a revertable, ready,
    /// unorphaned per-attempt `full` `suspend` pin that is the newest full
    /// snapshot.
    pub(super) unpinned_or_not_ready: u64,
    /// Sandboxes whose environment the control plane cannot attribute from
    /// recorded metadata and catalog identity.
    pub(super) unattributed: u64,
}

impl UncoveredMemoryDropCounts {
    pub(super) const fn attributed_total(&self) -> u64 {
        self.not_kubernetes + self.encrypted + self.node_held + self.unpinned_or_not_ready
    }

    pub(super) const fn total(&self) -> u64 {
        self.attributed_total() + self.unattributed
    }

    pub(super) const fn blocks_retirement(&self, strand: bool) -> bool {
        if strand {
            self.attributed_total() > 0
        } else {
            self.total() > 0
        }
    }
}

fn parse_catalog(
    rows: Vec<pplx_space_runtime_environment_catalog_store::CatalogEnvironmentRow>,
) -> Result<Vec<CatalogEnvironment>, Box<Response>> {
    let parse_ref = |value: &str| -> Result<EnvironmentReference, Box<Response>> {
        value.parse().map_err(|error| {
            tracing::error!(%error, reference = value, "invalid catalog reference");
            Box::new(
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("runtime_environments holds an invalid reference {value:?}: {error}"),
                )
                .into_response(),
            )
        })
    };
    rows.into_iter()
        .map(|row| {
            Ok(CatalogEnvironment {
                reference: parse_ref(&row.environment_ref)?,
                runtime: row.runtime,
                runtime_version: row.runtime_version,
                kernel_version: row.kernel_version,
                state_format_name: row.state_format_name,
                state_format_version: row.state_format_version,
                upgrade_edges: row
                    .upgrade_edges
                    .iter()
                    .map(|edge| parse_ref(edge))
                    .collect::<Result<_, _>>()?,
                memory_edges: row
                    .memory_edges
                    .iter()
                    .map(|edge| parse_ref(edge))
                    .collect::<Result<_, _>>()?,
            })
        })
        .collect()
}

/// Extract `(RecordedExecution, malformed)` from a coverage group's binding
/// metadata and legacy columns.
fn recorded_from_group(group: &MemoryDropCoverageGroup) -> (RecordedExecution, bool) {
    let mut malformed = false;
    let mut environment = None;
    let mut kernel_version = None;
    if let Some(metadata) = &group.execution_metadata {
        if let Some(object) = metadata.as_object() {
            match object.get("environment") {
                None | Some(serde_json::Value::Null) => {}
                Some(serde_json::Value::String(value)) => {
                    if value.parse::<EnvironmentReference>().is_err() {
                        malformed = true;
                    } else {
                        environment = Some(value.clone());
                    }
                }
                Some(_) => {
                    malformed = true;
                }
            }
            kernel_version = object
                .get("components")
                .and_then(serde_json::Value::as_array)
                .and_then(|components| {
                    components.iter().find(|component| {
                        component.get("kind").and_then(serde_json::Value::as_str)
                            == Some(ArtifactComponentKind::Kernel.as_str())
                    })
                })
                .and_then(|kernel| kernel.get("version"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
        } else {
            malformed = true;
        }
    }
    (
        RecordedExecution {
            environment,
            runtime: group.runtime.clone(),
            runtime_version: group.runtime_version.clone(),
            kernel_version,
            state_format_name: group.state_format_name.clone(),
            state_format_version: group.state_format_version.clone(),
        },
        malformed,
    )
}

pub(super) fn classify_coverage(
    environment: EnvironmentReference,
    legacy_unbound_allowed: Option<bool>,
    catalog: &[CatalogEnvironment],
    groups: &[MemoryDropCoverageGroup],
) -> UncoveredMemoryDropCounts {
    let mut uncovered = UncoveredMemoryDropCounts::default();
    for group in groups {
        if group.sandboxes == 0 {
            continue;
        }
        let attributed = if group.recorded {
            let (recorded, malformed) = recorded_from_group(group);
            if malformed {
                None
            } else {
                attribute(&recorded, catalog, legacy_unbound_allowed)
            }
        } else {
            None
        };
        match attributed {
            None => {
                uncovered.unattributed += group.sandboxes;
            }
            Some(other) if other != environment => {}
            Some(_) => {
                if !group.kubernetes {
                    uncovered.not_kubernetes += group.sandboxes;
                } else if group.encrypted && !COVERS_ENCRYPTED_ROWS {
                    uncovered.encrypted += group.sandboxes;
                } else if !group.needs_droppable_pin
                    || (!group.unplaced_on_gone_node && !COVERS_NODE_HELD_ROWS)
                {
                    uncovered.node_held += group.sandboxes;
                } else if !group.droppable_pin {
                    uncovered.unpinned_or_not_ready += group.sandboxes;
                }
            }
        }
    }
    uncovered
}

enum RetirementBlock {
    NoEdgeToDefault { default: EnvironmentReference },
    TemplateReleasesInUse { bound: u64, unattributed: u64 },
    Uncovered(UncoveredMemoryDropCounts),
}

fn has_edge_to_default(
    catalog: &[CatalogEnvironment],
    from: EnvironmentReference,
    default: EnvironmentReference,
) -> bool {
    catalog
        .iter()
        .any(|entry| entry.reference == from && entry.upgrade_edges.contains(&default))
}

/// First `deprecated` environment with `memory_retired = true` that lacks a
/// certified `upgrade_edges` entry to `default`.
pub(super) async fn deprecated_retired_missing_edge_to_default(
    conn: &mut sqlx::PgConnection,
    default: EnvironmentReference,
) -> Result<Option<EnvironmentReference>, Box<Response>> {
    let retired = policy_store::deprecated_memory_retired_environments(&mut *conn)
        .await
        .map_err(|error| Box::new(policy_read_error(error)))?;
    if retired.is_empty() {
        return Ok(None);
    }
    let catalog_rows =
        pplx_space_runtime_environment_catalog_store::unretired_environments(&mut *conn)
            .await
            .map_err(|err| Box::new(read_db_error(err)))?;
    let catalog = parse_catalog(catalog_rows)?;
    Ok(retired
        .into_iter()
        .find(|&from| !has_edge_to_default(&catalog, from, default)))
}

async fn check_retirement_coverage_with_policy(
    conn: &mut sqlx::PgConnection,
    policy: &policy_store::RuntimeEnvironmentPolicy,
    environment: EnvironmentReference,
    strand: bool,
    boots_template_twins: bool,
) -> Result<Option<RetirementBlock>, Box<Response>> {
    let catalog_rows =
        pplx_space_runtime_environment_catalog_store::unretired_environments(&mut *conn)
            .await
            .map_err(|err| Box::new(read_db_error(err)))?;
    let catalog = parse_catalog(catalog_rows)?;
    if let Some(default) = policy.default_environment
        && !has_edge_to_default(&catalog, environment, default)
    {
        return Ok(Some(RetirementBlock::NoEdgeToDefault { default }));
    }
    let release_rows = super::store::template_release_population(&mut *conn, boots_template_twins)
        .await
        .map_err(|err| Box::new(read_db_error(err)))?;
    let mut bound_releases_in_use = 0u64;
    let mut unattributed_releases_in_use = 0u64;
    for row in release_rows {
        if !row.in_use {
            continue;
        }
        let releases = super::count(row.template_releases);
        match super::binding(row.environment_ref.as_deref(), true, row.malformed) {
            super::Binding::Bound(bound) if bound == environment => {
                bound_releases_in_use += releases;
            }
            super::Binding::Bound(_) => {}
            super::Binding::Unbound | super::Binding::Unrecorded | super::Binding::Invalid => {
                unattributed_releases_in_use += releases;
            }
        }
    }
    if bound_releases_in_use > 0 {
        return Ok(Some(RetirementBlock::TemplateReleasesInUse {
            bound: bound_releases_in_use,
            unattributed: unattributed_releases_in_use,
        }));
    }
    let groups = coverage_groups(&mut *conn)
        .await
        .map_err(|err| Box::new(read_db_error(err)))?;
    let uncovered = classify_coverage(
        environment,
        policy.legacy_unbound_allowed,
        &catalog,
        &groups,
    );
    if uncovered.blocks_retirement(strand) {
        return Ok(Some(RetirementBlock::Uncovered(uncovered)));
    }
    if !strand && unattributed_releases_in_use > 0 {
        return Ok(Some(RetirementBlock::TemplateReleasesInUse {
            bound: 0,
            unattributed: unattributed_releases_in_use,
        }));
    }
    Ok(None)
}

async fn check_retirement_coverage(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    environment: EnvironmentReference,
    strand: bool,
    boots_template_twins: bool,
) -> Result<Option<RetirementBlock>, Box<Response>> {
    let policy = policy_store::get(&mut **tx)
        .await
        .map_err(|error| Box::new(policy_read_error(error)))?;
    check_retirement_coverage_with_policy(tx, &policy, environment, strand, boots_template_twins)
        .await
}

/// Pre-transaction coverage scan when the policy already allows retiring
/// `environment` (`default_environment.is_some()` and `state == Deprecated`),
/// so a rejected retirement doesn't hold `FOR UPDATE` on the singleton policy
/// row while scanning every sandbox. When the policy would refuse the write
/// itself, returns `None` so the store refusal wins.
async fn precheck_retirement_coverage(
    pool: &sqlx::PgPool,
    environment: EnvironmentReference,
    strand: bool,
    boots_template_twins: bool,
) -> Option<RetirementBlock> {
    use pplx_space_runtime_environment_policy_store::RuntimeEnvironmentState;

    let mut conn = pool.acquire().await.ok()?;
    if !policy_store::memory_retirement_schema_migrated(&mut *conn)
        .await
        .ok()?
    {
        return None;
    }
    let policy = policy_store::get(&mut *conn).await.ok()?;
    if policy.default_environment.is_none()
        || policy.environments.get(&environment) != Some(&RuntimeEnvironmentState::Deprecated)
    {
        return None;
    }
    check_retirement_coverage_with_policy(
        &mut conn,
        &policy,
        environment,
        strand,
        boots_template_twins,
    )
    .await
    .ok()
    .flatten()
}

fn refuse_retirement_block(
    audit: &AuditContext<'_>,
    environment: EnvironmentReference,
    block: RetirementBlock,
) -> Response {
    match block {
        RetirementBlock::NoEdgeToDefault { default } => audit.refuse(
            StatusCode::CONFLICT,
            format!(
                "environment {environment} has no certified upgrade edge to the cluster \
                 default {default} in runtime_environments, so retiring its memory would strand \
                 restores"
            ),
        ),
        RetirementBlock::TemplateReleasesInUse {
            bound,
            unattributed,
        } => {
            let message = if bound > 0 {
                format!(
                    "environment {environment} still has {bound} template release(s) in use \
                     (tagged or pinned by an enabled pool config) whose new sandboxes would \
                     boot on {environment}; move tags to a release on the default environment \
                     or disable the pool configs first"
                )
            } else {
                format!(
                    "environment {environment} has {unattributed} unbound or unreadable \
                     template release(s) in use whose new sandboxes may boot on \
                     {environment}; inspect GET /runtime-environments/population or pass \
                     strand: true to retire anyway"
                )
            };
            audit.record(None, Some(&message));
            (
                StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "error": message,
                    "template_releases_in_use": {
                        "bound": bound,
                        "unattributed": unattributed,
                    },
                })),
            )
                .into_response()
        }
        RetirementBlock::Uncovered(uncovered) => refuse_uncovered(audit, environment, uncovered),
    }
}

fn refuse_uncovered(
    audit: &AuditContext<'_>,
    environment: EnvironmentReference,
    uncovered: UncoveredMemoryDropCounts,
) -> Response {
    let message = if uncovered.attributed_total() == 0 {
        format!(
            "environment {environment} has {} unattributed runnable sandbox(es) whose \
             environment couldn't be determined; inspect GET /runtime-environments/population \
             or pass strand: true to retire anyway",
            uncovered.unattributed
        )
    } else {
        format!(
            "environment {environment} still has {} runnable sandbox(es) that \
             restore-time memory drop cannot convert; drain or drop them first",
            uncovered.total()
        )
    };
    audit.record(None, Some(&message));
    (
        StatusCode::CONFLICT,
        Json(serde_json::json!({
            "error": message,
            "uncovered": uncovered,
        })),
    )
        .into_response()
}

/// `PUT /runtime-environments/{environment_ref}/memory`: retire or unretire an
/// environment's checkpoint memory.
pub(in crate::routes::admin) async fn set_runtime_environment_memory(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(environment_ref): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let expected = match expected_generation(&headers) {
        Ok(expected) => expected,
        Err(err) => return err.into_response(),
    };
    let environment = match parse_environment_ref(&environment_ref) {
        Ok(environment) => environment,
        Err(err) => return err.into_response(),
    };
    let request: SetMemoryRequest = match parse_json_body(&body, "runtime environment memory") {
        Ok(request) => request,
        Err(err) => return err.into_response(),
    };
    let action = match parse_memory_action(&request.action) {
        Ok(action) => action,
        Err(err) => return err.into_response(),
    };
    if request.strand && action != MemoryAction::Retire {
        return api_error(
            StatusCode::BAD_REQUEST,
            "strand applies only to action 'retire'",
        )
        .into_response();
    }
    let change = match policy_change(&actor, request.reason) {
        Ok(change) => change,
        Err(err) => return err.into_response(),
    };
    let audit = AuditContext {
        actor: &actor,
        action: "runtime-environment-set-memory",
        environment,
        requested_state: action.audit_requested_state(),
        change: &change,
        strand: request.strand,
    };

    if action == MemoryAction::Retire
        && let Err(err) = state
            .sandbox_controller
            .check_environment_memory_retirement()
    {
        let (status, message) = match err {
            crate::controllers::substrate::SubstrateError::Unsupported { message } => {
                (StatusCode::NOT_IMPLEMENTED, message)
            }
            other => (StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        };
        return audit.refuse(status, message);
    }

    if action == MemoryAction::Retire && !state.memory_drop_enabled {
        return audit.refuse(
            StatusCode::SERVICE_UNAVAILABLE,
            "the memory drop is disabled".into(),
        );
    }

    let boots_template_twins = state.sandbox_controller.boots_template_twins();
    if action == MemoryAction::Retire
        && !already_stale(&state.pool, expected).await
        && let Some(block) = precheck_retirement_coverage(
            &state.pool,
            environment,
            request.strand,
            boots_template_twins,
        )
        .await
    {
        return refuse_retirement_block(&audit, environment, block);
    }

    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(err) => return audit.fail("the write failed", db_error(err).into_response()),
    };

    let memory_retired = action.memory_retired();
    let generation = match policy_store::set_environment_memory_retired(
        &mut tx,
        environment,
        memory_retired,
        &change,
    )
    .await
    {
        Ok(generation) => generation,
        Err(SetEnvironmentMemoryError::RetireRequiresDefault(environment)) => {
            return audit.refuse(
                StatusCode::CONFLICT,
                format!(
                    "retiring environment {environment}'s memory requires a cluster default; \
                     set one with PUT /runtime-environments/default first"
                ),
            );
        }
        Err(SetEnvironmentMemoryError::BlockedCannotUnretire(environment)) => {
            return audit.refuse(
                StatusCode::CONFLICT,
                format!(
                    "environment {environment} is blocked and its checkpoint memory can no \
                     longer be unretired"
                ),
            );
        }
        Err(SetEnvironmentMemoryError::RequiresDeprecated {
            environment,
            current,
            memory_retired,
        }) => {
            let verb = if memory_retired { "retire" } else { "unretire" };
            let state_desc = current.map_or("unset", |state| state.as_str());
            return audit.refuse(
                StatusCode::CONFLICT,
                format!(
                    "environment {environment} is {state_desc}; only a deprecated environment \
                     can {verb} its checkpoint memory"
                ),
            );
        }
        Err(SetEnvironmentMemoryError::UnknownState { environment, .. }) => {
            return audit.refuse(
                StatusCode::CONFLICT,
                format!(
                    "environment {environment} has a stored state this server doesn't \
                     recognize; set its state with PUT /runtime-environments/{environment}/state \
                     first"
                ),
            );
        }
        Err(error @ SetEnvironmentMemoryError::SchemaNotMigrated) => {
            return audit.refuse(StatusCode::SERVICE_UNAVAILABLE, error.to_string());
        }
        Err(error @ SetEnvironmentMemoryError::PolicyRowMissing) => {
            tracing::error!(%error, "set runtime environment memory");
            return audit.refuse(StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
        }
        Err(SetEnvironmentMemoryError::Database(err)) => {
            return audit.fail("the write failed", db_error(err).into_response());
        }
    };

    if let Some(expected) = expected.filter(|_| !precondition_holds(expected, generation)) {
        drop(tx);
        return audit.refuse_stale(expected, generation);
    }

    if action == MemoryAction::Retire {
        match check_retirement_coverage(&mut tx, environment, request.strand, boots_template_twins)
            .await
        {
            Ok(None) => {}
            Ok(Some(block)) => {
                drop(tx);
                return refuse_retirement_block(&audit, environment, block);
            }
            Err(response) => {
                drop(tx);
                return audit.fail(
                    "couldn't check restore-time memory-drop coverage",
                    *response,
                );
            }
        }
    }

    if let Err(err) = enqueue_policy_reconcile(&mut tx).await {
        return audit.fail(QUEUE_RECONCILE_FAILED, db_error(err).into_response());
    }
    if let Err(err) = tx.commit().await {
        return audit.fail(
            "the commit failed; the write may have landed",
            db_error(err).into_response(),
        );
    }
    state
        .sandbox_controller
        .invalidate_runtime_policy_cache()
        .await;
    audit.record(Some(generation), None);
    Json(SetMemoryResponse {
        generation,
        environment_ref: environment,
        memory_retired,
    })
    .into_response()
}
