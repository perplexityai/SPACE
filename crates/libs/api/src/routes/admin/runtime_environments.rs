//! `/runtime-environments` operator surface: the cluster's execution
//! environment policy, what each node reports enforcing, and which
//! environments sandboxes and template releases are bound to.
//!
//! Every write goes through `pplx_space_runtime_environment_policy_store`,
//! which locks the policy row, enforces the rules that span writes (the
//! default stays `allowed`, blocking needs a default) and increments the
//! generation in the same transaction. This module adds two checks. A write
//! that names a generation in `If-Match` applies only at that generation. And
//! one from observed state, which the store can't make: a write that stops an
//! environment executing needs `strand: true` while work is bound to it, or
//! while a sandbox's environment can't be told. Blocking stops the blocked
//! environment; the first default stops every environment with no state.
//!
//! Not yet checked: that every node can run a new default (per-node usable
//! environments have a column, but nothing writes it yet), that a template
//! campaign finished before a block (campaigns don't exist yet), and which
//! nodes can't enforce a block (node status reports each node's applied
//! generation; refusing on it is a later change).

use std::collections::{BTreeMap, BTreeSet};

use axum::{
    Extension, Json,
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use pplx_space_common_core::types::admin::ListAdminNodesFilters;
use pplx_space_common_core::types::{
    EnvironmentReference, ExecutionPolicyObservation, NodeId, NodeRole, UserId,
};
use pplx_space_runtime_environment_policy_store::{
    self as policy_store, GetPolicyError, PolicyChange, PolicyChangeReason, RecordedChange,
    RuntimeEnvironmentPolicy, RuntimeEnvironmentPolicyRecord, RuntimeEnvironmentState,
    SetDefaultError, SetEnvironmentStateError,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use super::audit::{self, AdminRuntimeEnvironmentAuditEntry};
use super::node_store::{AdminNodeRow, AdminNodeStore};
use super::runtime_environment_store as store;
use super::support::{
    api_error, db_error, format_time, parse_if_match_generation, parse_json_body, read_db_error,
    require_managing_org_admin,
};
use crate::middleware::auth::AuthUser;
use crate::models::User;
use crate::routes::AppState;

/// A refusal from a parse helper, in the admin error shape.
type ApiError = (StatusCode, Json<serde_json::Value>);

/// Rows per `GET /runtime-environments/nodes` page, matching
/// `GET /nodes/{node_id}/sandboxes`.
const NODE_PAGE_SIZE: usize = 100;

#[derive(Debug, Serialize)]
struct RecordedChangeResponse {
    updated_at: String,
    updated_by_user_id: Option<UserId>,
    reason: String,
}

impl From<&RecordedChange> for RecordedChangeResponse {
    fn from(change: &RecordedChange) -> Self {
        Self {
            updated_at: format_time(change.updated_at),
            updated_by_user_id: change.updated_by,
            reason: change.reason.clone(),
        }
    }
}

#[derive(Debug, Serialize)]
struct EnvironmentStateResponse {
    environment_ref: EnvironmentReference,
    state: &'static str,
    executes: bool,
    is_default: bool,
    last_change: Option<RecordedChangeResponse>,
}

/// `GET /runtime-environments`.
#[derive(Debug, Serialize)]
struct PolicyResponse {
    generation: u64,
    /// `null` until a default is first set: nodes keep their configured one.
    default_environment_ref: Option<EnvironmentReference>,
    /// `null` until first set: nodes keep their configured permission.
    legacy_unbound_allowed: Option<bool>,
    /// `null` at the bootstrap generation.
    last_change: Option<RecordedChangeResponse>,
    environments: Vec<EnvironmentStateResponse>,
}

fn policy_response(record: &RuntimeEnvironmentPolicyRecord) -> PolicyResponse {
    let policy = &record.policy;
    PolicyResponse {
        generation: policy.generation,
        default_environment_ref: policy.default_environment,
        legacy_unbound_allowed: policy.legacy_unbound_allowed,
        last_change: record
            .last_change
            .as_ref()
            .map(RecordedChangeResponse::from),
        environments: policy
            .environments
            .iter()
            .map(|(environment, state)| EnvironmentStateResponse {
                environment_ref: *environment,
                state: state.as_str(),
                executes: state.executes(),
                is_default: policy.default_environment == Some(*environment),
                last_change: record
                    .environment_changes
                    .get(environment)
                    .map(RecordedChangeResponse::from),
            })
            .collect(),
    }
}

fn policy_read_error(error: GetPolicyError) -> Response {
    match error {
        GetPolicyError::Database(err) => read_db_error(err),
        // A stored value this build can't read. The message names it; an
        // unknown state is repaired with `PUT .../{ref}/state`, the rest need
        // a cluster administrator.
        error @ (GetPolicyError::PolicyRowMissing
        | GetPolicyError::InvalidGeneration(_)
        | GetPolicyError::InvalidEnvironmentReference { .. }
        | GetPolicyError::UnknownState { .. }) => {
            tracing::error!(%error, "read runtime environment policy");
            api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("runtime environment policy can't be read: {error}"),
            )
            .into_response()
        }
    }
}

/// `GET /runtime-environments`: the whole policy, with who last changed each
/// row and why.
pub(super) async fn get_runtime_environments(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
) -> Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    match policy_store::get_record(&state.pool).await {
        Ok(record) => Json(policy_response(&record)).into_response(),
        Err(error) => policy_read_error(error),
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SetDefaultRequest {
    environment_ref: String,
    reason: String,
    /// Set the first default even though work may depend on an environment
    /// it stops: see [`AtRisk::of_first_default`].
    #[serde(default)]
    strand: bool,
}

#[derive(Debug, Serialize)]
struct SetDefaultResponse {
    generation: u64,
    default_environment_ref: EnvironmentReference,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SetStateRequest {
    state: String,
    reason: String,
    /// Block even though work is still bound to the environment. Only
    /// meaningful with `state: blocked`.
    #[serde(default)]
    strand: bool,
}

#[derive(Debug, Serialize)]
struct SetStateResponse {
    generation: u64,
    environment_ref: EnvironmentReference,
    state: &'static str,
}

fn parse_environment_ref(value: &str) -> Result<EnvironmentReference, ApiError> {
    value.parse().map_err(|err| {
        api_error(
            StatusCode::BAD_REQUEST,
            format!("invalid environment reference {value:?}: {err}"),
        )
    })
}

fn parse_state(value: &str) -> Result<RuntimeEnvironmentState, ApiError> {
    value.parse().map_err(|_| {
        let accepted = RuntimeEnvironmentState::ALL
            .map(RuntimeEnvironmentState::as_str)
            .join(", ");
        api_error(
            StatusCode::BAD_REQUEST,
            format!("unknown runtime environment state {value:?}; expected one of: {accepted}"),
        )
    })
}

fn policy_change(actor: &User, reason: String) -> Result<PolicyChange, ApiError> {
    let reason = PolicyChangeReason::new(reason)
        .map_err(|err| api_error(StatusCode::BAD_REQUEST, err.to_string()))?;
    Ok(PolicyChange {
        actor: Some(actor.id),
        reason,
    })
}

/// The generation an optional `If-Match` header names. Absent, the write is
/// unconditional.
fn expected_generation(headers: &HeaderMap) -> Result<Option<u64>, ApiError> {
    parse_if_match_generation(headers)
        .map_err(|message| api_error(StatusCode::BAD_REQUEST, message))
}

/// Whether a write that raised the policy to `generation` applied at the one
/// its `If-Match` named. The store write holds the policy row lock and adds
/// exactly one, so this compares under the lock with no second read.
fn precondition_holds(expected: Option<u64>, generation: u64) -> bool {
    expected.is_none_or(|expected| expected.checked_add(1) == Some(generation))
}

/// Whether the policy has already moved past the generation a conditional
/// write names, read before the write. Every write adds one, so such a write
/// can't apply, and its strand check's scan is skipped: the handler then
/// answers 412 under the lock, unless the store refuses the write first.
/// (Only a schema rollback and reapply restarts the generation, which no
/// live request spans.) A failed read decides nothing, leaving the check to
/// run.
async fn already_stale(pool: &sqlx::PgPool, expected: Option<u64>) -> bool {
    let Some(expected) = expected else {
        return false;
    };
    policy_store::get_generation(pool)
        .await
        .is_ok_and(|generation| generation > expected)
}

/// What a write's audit entry needs besides its outcome.
struct AuditContext<'a> {
    actor: &'a User,
    action: &'static str,
    environment: EnvironmentReference,
    requested_state: &'static str,
    change: &'a PolicyChange,
    /// The caller waived the strand check.
    strand: bool,
}

impl AuditContext<'_> {
    fn record(&self, generation: Option<u64>, error: Option<&str>) {
        audit::record_runtime_environment(AdminRuntimeEnvironmentAuditEntry {
            actor: self.actor,
            action: self.action,
            environment_ref: &self.environment.to_string(),
            requested_state: self.requested_state,
            reason: self.change.reason.as_str(),
            strand: self.strand,
            generation,
            success: error.is_none(),
            error,
        });
    }

    /// Audit a write that failed rather than being refused, and give its
    /// response.
    fn fail(&self, what: &str, response: Response) -> Response {
        self.record(None, Some(what));
        response
    }

    /// Audit and answer a write refused because it could strand `at_risk`.
    fn refuse_strand(&self, at_risk: AtRisk) -> Response {
        let message = "this write would stop environments that work may still depend on; pass \
             strand: true to make it anyway. Sandboxes whose environment this server can't \
             tell are counted too: legacy work with no binding executes as the environment it \
             matches, and a sandbox with no observation from its node may be bound to one that stops";
        self.record(None, Some(message));
        (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": message, "at_risk": at_risk })),
        )
            .into_response()
    }

    /// Audit and answer a write whose `If-Match` named a generation other than
    /// the policy's, usually one it has moved past. `generation` is the one
    /// this write raised it to.
    fn refuse_stale(&self, expected: u64, generation: u64) -> Response {
        self.refuse(
            StatusCode::PRECONDITION_FAILED,
            format!(
                "the policy is at generation {}, not {expected} as If-Match expects; \
                 read it again and retry",
                generation - 1
            ),
        )
    }

    /// Audit a refusal and answer it.
    fn refuse(&self, status: StatusCode, message: String) -> Response {
        self.record(None, Some(&message));
        api_error(status, message).into_response()
    }
}

/// The strand check's verdict on a block, read before the write.
enum BlockCheck {
    /// The environment already didn't execute at `generation`: it was
    /// blocked, or had no state under a cluster default. The block stops
    /// nothing new, unless another write lands before this one.
    AlreadyStopped { generation: u64 },
    /// What the block would stop, if anything.
    /// Boxed: an `AtRisk` is large next to the other variant.
    Checked(Option<Box<AtRisk>>),
}

/// What blocking `environment` would stop. A block of an environment that
/// already doesn't execute, say to update the reason, needs no
/// acknowledgement. The error is the response to give, boxed because a
/// `Response` is large.
async fn block_check(
    pool: &sqlx::PgPool,
    environment: EnvironmentReference,
) -> Result<BlockCheck, Box<Response>> {
    let mut conn = pool
        .acquire()
        .await
        .map_err(|err| Box::new(read_db_error(err)))?;
    match policy_store::get(&mut *conn).await {
        Ok(policy) => {
            let executes = match policy.environments.get(&environment) {
                Some(state) => state.executes(),
                None => policy.default_environment.is_none(),
            };
            if !executes {
                return Ok(BlockCheck::AlreadyStopped {
                    generation: policy.generation,
                });
            }
        }
        Err(GetPolicyError::Database(err)) => return Err(Box::new(read_db_error(err))),
        // A stored value this build can't read, possibly this environment's
        // own, which this write repairs. Whether it executes can't be told,
        // so check as for any block.
        Err(error) => {
            tracing::warn!(%error, "runtime environment policy unreadable; checking the block in full");
        }
    }
    let population = read_population(&mut conn)
        .await
        .map_err(|err| Box::new(read_db_error(err)))?;
    Ok(BlockCheck::Checked(
        Some(Box::new(AtRisk::of_block(&population, environment)))
            .filter(|at_risk| !at_risk.is_empty()),
    ))
}

/// What a default write would stop, when it's the cluster's first: `None`
/// when there's a default already or nothing is at risk. The error is the
/// response to give, boxed because a `Response` is large.
async fn first_default_at_risk(
    pool: &sqlx::PgPool,
    default: EnvironmentReference,
) -> Result<Option<AtRisk>, Box<Response>> {
    let mut conn = pool
        .acquire()
        .await
        .map_err(|err| Box::new(read_db_error(err)))?;
    // Only the default: a state row this build can't read mustn't refuse a
    // later default, which stops nothing.
    let existing = policy_store::get_default(&mut *conn)
        .await
        .map_err(|error| Box::new(policy_read_error(error)))?;
    if existing.is_some() {
        return Ok(None);
    }
    // The first default narrows by every state row, so it needs them all:
    // one this build can't read fails the write closed.
    let policy = policy_store::get(&mut *conn)
        .await
        .map_err(|error| Box::new(policy_read_error(error)))?;
    let population = read_population(&mut conn)
        .await
        .map_err(|err| Box::new(read_db_error(err)))?;
    Ok(
        Some(AtRisk::of_first_default(&population, &policy, default))
            .filter(|at_risk| !at_risk.is_empty()),
    )
}

/// `PUT /runtime-environments/default`: make an environment the cluster
/// default. It's recorded as `allowed` if it isn't already, and the old
/// default stays `allowed`.
pub(super) async fn set_default_runtime_environment(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
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
    let request: SetDefaultRequest = match parse_json_body(&body, "runtime environment default") {
        Ok(request) => request,
        Err(err) => return err.into_response(),
    };
    let environment = match parse_environment_ref(&request.environment_ref) {
        Ok(environment) => environment,
        Err(err) => return err.into_response(),
    };
    let change = match policy_change(&actor, request.reason) {
        Ok(change) => change,
        Err(err) => return err.into_response(),
    };
    let audit = AuditContext {
        actor: &actor,
        action: "runtime-environment-set-default",
        environment,
        requested_state: RuntimeEnvironmentState::Allowed.as_str(),
        change: &change,
        strand: request.strand,
    };

    // Read before the write's transaction, as the state write does. A default
    // set concurrently makes this one a later default, which stops nothing, so
    // a stale read only asks for an acknowledgement that wasn't needed. A
    // failed read is answered only after the store's own refusals, so a write
    // the store refuses gets the store's message.
    let at_risk = if request.strand || already_stale(&state.pool, expected).await {
        Ok(None)
    } else {
        first_default_at_risk(&state.pool, environment).await
    };

    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(err) => return audit.fail("the write failed", db_error(err).into_response()),
    };
    let generation = match policy_store::set_default(&mut tx, environment, &change).await {
        Ok(generation) => generation,
        Err(SetDefaultError::Blocked(environment)) => {
            return audit.refuse(
                StatusCode::CONFLICT,
                format!(
                    "environment {environment} is blocked and can't become the default; \
                     allow it first"
                ),
            );
        }
        Err(SetDefaultError::UnknownState { environment, .. }) => {
            return audit.refuse(
                StatusCode::CONFLICT,
                format!(
                    "environment {environment} has a stored state this server doesn't \
                     recognize; set its state with PUT /runtime-environments/{environment}/state \
                     first"
                ),
            );
        }
        Err(error @ SetDefaultError::PolicyRowMissing) => {
            tracing::error!(%error, "set runtime environment default");
            return audit.refuse(StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
        }
        Err(SetDefaultError::Database(err)) => {
            return audit.fail("the write failed", db_error(err).into_response());
        }
    };
    // After the store's own refusals, which describe the policy as it is
    // now, and before the strand check, which a stale caller would answer
    // for a policy it hasn't seen. Dropping the transaction rolls the write
    // back.
    if let Some(expected) = expected.filter(|_| !precondition_holds(expected, generation)) {
        drop(tx);
        return audit.refuse_stale(expected, generation);
    }
    // After the store's own refusals, as for a block.
    match at_risk {
        Ok(None) => {}
        Ok(Some(at_risk)) => {
            drop(tx);
            return audit.refuse_strand(at_risk);
        }
        Err(response) => {
            drop(tx);
            return audit.fail("couldn't check what the write would stop", *response);
        }
    }
    if let Err(err) = tx.commit().await {
        return audit.fail(
            "the commit failed; the write may have landed",
            db_error(err).into_response(),
        );
    }
    audit.record(Some(generation), None);
    Json(SetDefaultResponse {
        generation,
        default_environment_ref: environment,
    })
    .into_response()
}

/// `PUT /runtime-environments/{environment_ref}/state`: record `allowed`,
/// `deprecated` or `blocked` for an environment.
pub(super) async fn set_runtime_environment_state(
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
    let request: SetStateRequest = match parse_json_body(&body, "runtime environment state") {
        Ok(request) => request,
        Err(err) => return err.into_response(),
    };
    let requested = match parse_state(&request.state) {
        Ok(requested) => requested,
        Err(err) => return err.into_response(),
    };
    if request.strand && requested != RuntimeEnvironmentState::Blocked {
        return api_error(
            StatusCode::BAD_REQUEST,
            "strand applies only to state 'blocked'",
        )
        .into_response();
    }
    let change = match policy_change(&actor, request.reason) {
        Ok(change) => change,
        Err(err) => return err.into_response(),
    };
    let audit = AuditContext {
        actor: &actor,
        action: "runtime-environment-set-state",
        environment,
        requested_state: requested.as_str(),
        change: &change,
        strand: request.strand,
    };

    // Read before the write's transaction, so a scan of every sandbox doesn't
    // hold the policy row lock and stall other policy writers. Sandboxes keep
    // changing either way: this is an acknowledgement, not a guarantee that
    // nothing is bound once the block lands. A failed read is answered only
    // after the store's own refusals.
    let check = if requested == RuntimeEnvironmentState::Blocked
        && !request.strand
        && !already_stale(&state.pool, expected).await
    {
        block_check(&state.pool, environment).await
    } else {
        Ok(BlockCheck::Checked(None))
    };

    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(err) => return audit.fail("the write failed", db_error(err).into_response()),
    };
    let generation =
        match policy_store::set_environment_state(&mut tx, environment, requested, &change).await {
            Ok(generation) => generation,
            Err(SetEnvironmentStateError::DefaultMustStayAllowed {
                environment,
                requested,
            }) => {
                return audit.refuse(
                    StatusCode::CONFLICT,
                    format!(
                        "environment {environment} is the cluster default and must stay \
                         allowed, not {requested}; make another environment the default first"
                    ),
                );
            }
            Err(SetEnvironmentStateError::BlockRequiresDefault(environment)) => {
                return audit.refuse(
                    StatusCode::CONFLICT,
                    format!(
                        "blocking environment {environment} requires a cluster default; set \
                         one with PUT /runtime-environments/default first"
                    ),
                );
            }
            Err(error @ SetEnvironmentStateError::PolicyRowMissing) => {
                tracing::error!(%error, "set runtime environment state");
                return audit.refuse(StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
            }
            Err(SetEnvironmentStateError::Database(err)) => {
                return audit.fail("the write failed", db_error(err).into_response());
            }
        };

    // As for the default: after the store's refusals, before the strand check.
    if let Some(expected) = expected.filter(|_| !precondition_holds(expected, generation)) {
        drop(tx);
        return audit.refuse_stale(expected, generation);
    }
    // Refused only after the store's own refusals, which the write above has
    // already returned, so an operator isn't asked to acknowledge stranding
    // work for a block that would be refused anyway. Dropping the transaction
    // rolls the write back.
    let at_risk = match check {
        Ok(BlockCheck::Checked(at_risk)) => at_risk.map(|at_risk| *at_risk),
        // This write raised the generation by exactly one, so nothing changed
        // the policy since it read the environment as stopped.
        Ok(BlockCheck::AlreadyStopped {
            generation: stopped_at,
        }) if generation == stopped_at + 1 => None,
        // Another write landed in between, possibly letting the environment
        // execute again. Check now, under the policy row lock this write holds; a
        // rare path, so the scan's time under the lock is acceptable.
        // The other write may have changed only some other environment, so
        // this can ask for an acknowledgement a retry wouldn't need.
        Ok(BlockCheck::AlreadyStopped { .. }) => match read_population(&mut tx).await {
            Ok(population) => Some(AtRisk::of_block(&population, environment))
                .filter(|at_risk| !at_risk.is_empty()),
            Err(err) => {
                return audit.fail(
                    "couldn't check what the write would stop",
                    db_error(err).into_response(),
                );
            }
        },
        Err(response) => {
            drop(tx);
            return audit.fail("couldn't check what the write would stop", *response);
        }
    };
    if let Some(at_risk) = at_risk {
        drop(tx);
        return audit.refuse_strand(at_risk);
    }
    if let Err(err) = tx.commit().await {
        return audit.fail(
            "the commit failed; the write may have landed",
            db_error(err).into_response(),
        );
    }
    audit.record(Some(generation), None);
    Json(SetStateResponse {
        generation,
        environment_ref: environment,
        state: requested.as_str(),
    })
    .into_response()
}

/// How a node's published execution policy compares with the cluster
/// policy. This compares content, not generations: a node that conforms
/// enforces what the current policy asks for, whichever write it applied, and
/// a node on its startup policy, which publishes no generation, is judged the
/// same way.
#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "conformance", rename_all = "snake_case")]
enum NodePolicyConformance {
    /// The node hasn't published a readable policy, or reports it couldn't
    /// read the cluster policy it persisted (and so refuses everything).
    Unknown,
    Conforms,
    Diverges {
        /// The node resolves unbound work to a different default.
        default_differs: bool,
        /// The node executes environments the cluster policy doesn't, for
        /// example one that was blocked.
        executes_outside_policy: Vec<EnvironmentReference>,
        /// The node allows unbound legacy work the cluster revoked.
        allows_revoked_legacy_unbound: bool,
    },
}

/// A node narrows to the cluster's executing set, never widens past what its
/// build certifies, so its allowlist may be a strict subset. Until a default
/// is set the cluster asks for nothing, and every published policy conforms
/// on the default and allowlist. The rules nodes apply are in
/// `docs/execution-environments.md`, "Cluster desired policy".
fn conformance(
    policy: &RuntimeEnvironmentPolicy,
    observed: Option<&ExecutionPolicyObservation>,
) -> NodePolicyConformance {
    // A node that couldn't read the cluster policy it persisted refuses
    // everything; that's no evidence of which policy it would enforce.
    let Some(observed) = observed.filter(|observed| !observed.cluster_policy_unreadable()) else {
        return NodePolicyConformance::Unknown;
    };
    let (default_differs, executes_outside_policy) = match policy.default_environment {
        None => (false, Vec::new()),
        Some(default) => {
            let executing: BTreeSet<_> = policy.executing().collect();
            (
                observed.default_environment_reference() != Some(default),
                observed
                    .allowed_environment_references()
                    .iter()
                    .copied()
                    .filter(|environment| !executing.contains(environment))
                    .collect(),
            )
        }
    };
    // The cluster can revoke the legacy permission, never grant it, so a
    // node that refuses legacy work the cluster permits is only stricter.
    let allows_revoked_legacy_unbound =
        policy.legacy_unbound_allowed == Some(false) && observed.legacy_unbound_allowed();
    if !default_differs && executes_outside_policy.is_empty() && !allows_revoked_legacy_unbound {
        return NodePolicyConformance::Conforms;
    }
    NodePolicyConformance::Diverges {
        default_differs,
        executes_outside_policy,
        allows_revoked_legacy_unbound,
    }
}

#[derive(Debug, Serialize)]
struct NodeRuntimePolicyStatus {
    node_id: NodeId,
    name: Option<String>,
    role: NodeRole,
    substrate_node_id: Option<String>,
    /// Whether the node's status snapshot is within its `stale_after`.
    /// `null` when the node has never reported status. A stale node's
    /// conformance describes what it last published, not what it runs now.
    status_fresh: Option<bool>,
    /// Control-plane receipt time of the published policy.
    policy_received_at: Option<String>,
    /// The policy the node reports enforcing.
    enforced: Option<ExecutionPolicyObservation>,
    /// The cluster policy generation the node reports applying. `null` for a
    /// node enforcing its startup policy only (an older build, or the cluster
    /// serving no policy), or one that couldn't read the policy it persisted.
    applied_generation: Option<u64>,
    /// Whether `applied_generation` is the generation compared with. A node
    /// behind can still conform, when nothing it would change has changed.
    applied_current: Option<bool>,
    #[serde(flatten)]
    conformance: NodePolicyConformance,
}

#[derive(Debug, Serialize)]
struct NodeRuntimePolicyPage {
    /// The cluster policy each node was compared with.
    generation: u64,
    default_environment_ref: Option<EnvironmentReference>,
    nodes: Vec<NodeRuntimePolicyStatus>,
    /// Pass as `after` for the next page; `null` on the last page.
    next_after: Option<NodeId>,
}

#[derive(Debug, Deserialize)]
pub(super) struct NodeRuntimePolicyQuery {
    after: Option<NodeId>,
}

/// A snapshot is stale from `stale_after` on, as the node listing's `stale`
/// flag counts it.
fn status_fresh(stale_after: OffsetDateTime, now: OffsetDateTime) -> bool {
    stale_after > now
}

fn node_status(
    policy: &RuntimeEnvironmentPolicy,
    row: AdminNodeRow,
    now: OffsetDateTime,
) -> NodeRuntimePolicyStatus {
    let enforced = row.execution_policy();
    let applied_generation = enforced
        .as_ref()
        .and_then(|(policy, _)| policy.applied_generation());
    NodeRuntimePolicyStatus {
        node_id: row.id,
        name: row.name,
        role: row.role,
        substrate_node_id: row.substrate_node_id,
        status_fresh: row
            .status_stale_after
            .map(|stale_after| status_fresh(stale_after, now)),
        policy_received_at: enforced
            .as_ref()
            .and_then(|(_, received_at)| *received_at)
            .map(format_time),
        conformance: conformance(policy, enforced.as_ref().map(|(policy, _)| policy)),
        enforced: enforced.map(|(policy, _)| policy),
        applied_generation,
        applied_current: applied_generation.map(|applied| applied == policy.generation),
    }
}

/// The page of `rows` after `after`, in node id order, and the cursor for
/// the next page when one follows.
fn node_page(
    mut rows: Vec<AdminNodeRow>,
    after: Option<NodeId>,
) -> (Vec<AdminNodeRow>, Option<NodeId>) {
    rows.sort_unstable_by_key(|row| row.id);
    rows.retain(|row| after.is_none_or(|after| row.id > after));
    let next_after = if rows.len() > NODE_PAGE_SIZE {
        rows.truncate(NODE_PAGE_SIZE);
        rows.last().map(|row| row.id)
    } else {
        None
    };
    (rows, next_after)
}

/// `GET /runtime-environments/nodes`: each node's published execution
/// policy and how it compares with the cluster policy, a page at a time in
/// node id order.
///
/// Reads the admin node listing, which already joins the Direct-only node
/// table for names and soft deletion, and pages it here: shared code reads
/// that table only through an existing query (`controllers/substrate/AGENTS.md`).
/// That costs one full node listing per page, the same read `list-nodes`
/// makes without occupancy counts.
pub(super) async fn list_runtime_environment_nodes(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Query(query): Query<NodeRuntimePolicyQuery>,
) -> Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let policy = match policy_store::get(&state.pool).await {
        Ok(policy) => policy,
        Err(error) => return policy_read_error(error),
    };
    let rows = match AdminNodeStore::new(state.pool.clone())
        .list(&ListAdminNodesFilters::default())
        .await
    {
        Ok(rows) => rows,
        Err(err) => return read_db_error(err),
    };
    let (rows, next_after) = node_page(rows, query.after);
    let now = OffsetDateTime::now_utc();
    Json(NodeRuntimePolicyPage {
        generation: policy.generation,
        default_environment_ref: policy.default_environment,
        nodes: rows
            .into_iter()
            .map(|row| node_status(&policy, row, now))
            .collect(),
        next_after,
    })
    .into_response()
}

/// Work bound to one environment, or to none.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
struct PopulationCounts {
    sandboxes: u64,
    /// `sandboxes`, split by raw `sandboxes.lifecycle`.
    sandboxes_by_lifecycle: BTreeMap<String, u64>,
    /// How many of `sandboxes` can't run again: `cleanup_required` or
    /// `deleting`, or `failed` with no live evidence and nothing the repair
    /// loop could revert to.
    sandboxes_not_runnable: u64,
    template_releases: u64,
    /// How many of `template_releases` a tag points to or an enabled pool
    /// config pins: the ones that still create sandboxes.
    template_releases_in_use: u64,
}

impl PopulationCounts {
    fn add_sandboxes(&mut self, lifecycle: String, runnable: bool, count: u64) {
        self.sandboxes += count;
        if !runnable {
            self.sandboxes_not_runnable += count;
        }
        *self.sandboxes_by_lifecycle.entry(lifecycle).or_default() += count;
    }

    /// Sandboxes that can run again. One that can't, per
    /// [`store::SandboxPopulationRow::runnable`], is reported but strands
    /// nothing, so it never asks for `strand`.
    fn runnable_sandboxes(&self) -> u64 {
        self.sandboxes.saturating_sub(self.sandboxes_not_runnable)
    }

    /// Whether this work bound to a stopping environment needs `strand`.
    fn gates(&self) -> bool {
        self.runnable_sandboxes() > 0 || self.template_releases_in_use > 0
    }

    fn add(&mut self, other: &Self) {
        self.sandboxes += other.sandboxes;
        self.sandboxes_not_runnable += other.sandboxes_not_runnable;
        self.template_releases += other.template_releases;
        self.template_releases_in_use += other.template_releases_in_use;
        for (lifecycle, count) in &other.sandboxes_by_lifecycle {
            *self
                .sandboxes_by_lifecycle
                .entry(lifecycle.clone())
                .or_default() += count;
        }
    }
}

/// What a binding record says about the environment of one group of rows.
#[derive(Debug, Eq, PartialEq)]
enum Binding {
    Bound(EnvironmentReference),
    /// A record with no environment: legacy work.
    Unbound,
    /// No record at all: a sandbox its node publishes no observation of, a
    /// suspended or restoring one with no snapshot a restore could pick, or a
    /// failed one with no live
    /// observation and no repair snapshot, whether or not an execution is
    /// still live.
    Unrecorded,
    /// A recorded environment this build can't parse.
    Invalid,
}

fn binding(environment_ref: Option<&str>, recorded: bool, malformed: bool) -> Binding {
    if malformed {
        return Binding::Invalid;
    }
    match environment_ref {
        Some(value) => value.parse().map_or(Binding::Invalid, Binding::Bound),
        None if recorded => Binding::Unbound,
        None => Binding::Unrecorded,
    }
}

/// `COUNT(*)` is never negative; a value that somehow is counts as zero
/// rather than failing the read.
fn count(value: i64) -> u64 {
    u64::try_from(value).unwrap_or_default()
}

#[derive(Debug, Default, Eq, PartialEq)]
struct Population {
    environments: BTreeMap<EnvironmentReference, PopulationCounts>,
    unbound: PopulationCounts,
    unrecorded: PopulationCounts,
    invalid: PopulationCounts,
}

impl Population {
    fn from_rows(
        sandboxes: Vec<store::SandboxPopulationRow>,
        template_releases: Vec<store::TemplateReleasePopulationRow>,
    ) -> Self {
        let mut population = Self::default();
        for row in sandboxes {
            population
                .counts(binding(
                    row.environment_ref.as_deref(),
                    row.recorded,
                    row.malformed,
                ))
                .add_sandboxes(row.lifecycle, row.runnable, count(row.sandboxes));
        }
        for row in template_releases {
            // The release row is its own binding record.
            let counts =
                population.counts(binding(row.environment_ref.as_deref(), true, row.malformed));
            let releases = count(row.template_releases);
            counts.template_releases += releases;
            if row.in_use {
                counts.template_releases_in_use += releases;
            }
        }
        population
    }

    fn counts(&mut self, binding: Binding) -> &mut PopulationCounts {
        match binding {
            Binding::Bound(environment) => self.environments.entry(environment).or_default(),
            Binding::Unbound => &mut self.unbound,
            Binding::Unrecorded => &mut self.unrecorded,
            Binding::Invalid => &mut self.invalid,
        }
    }
}

async fn read_population(conn: &mut sqlx::PgConnection) -> Result<Population, sqlx::Error> {
    let sandboxes = store::sandbox_population(&mut *conn).await?;
    let template_releases = store::template_release_population(&mut *conn).await?;
    Ok(Population::from_rows(sandboxes, template_releases))
}

/// The work a policy write could strand: everything bound to the
/// environments that would stop executing, and the sandboxes whose
/// environment this server can't tell, because they have no binding, an
/// unreadable one, or no record.
///
/// A template release counts only while it's in use: a tag points to it or
/// an enabled pool config pins it. A superseded release stays until its
/// template is deleted, so counting it would make blocking an old environment
/// need `strand` long after every template was rebuilt; sandboxes already
/// made from it count on their own. An in-use release counts when it's bound
/// to a stopping environment, or when its environment can't be told, since
/// new creates from it may run on the one that stops. All of them are
/// reported.
#[derive(Debug, Eq, PartialEq, Serialize)]
struct AtRisk {
    /// The environments that would stop executing with work bound to them,
    /// whether or not that work needs `strand`.
    environments: Vec<EnvironmentReference>,
    /// The work bound to `environments`, summed.
    bound: PopulationCounts,
    unbound: PopulationCounts,
    unrecorded: PopulationCounts,
    invalid: PopulationCounts,
}

impl AtRisk {
    fn of<'a>(
        population: &Population,
        stopping: impl IntoIterator<Item = (&'a EnvironmentReference, &'a PopulationCounts)>,
    ) -> Self {
        let mut environments = Vec::new();
        let mut bound = PopulationCounts::default();
        // Every stopping environment with work bound to it is reported;
        // whether that work needs `strand` is `is_empty`'s question.
        for (environment, counts) in stopping {
            if counts.sandboxes > 0 || counts.template_releases > 0 {
                environments.push(*environment);
                bound.add(counts);
            }
        }
        Self {
            environments,
            bound,
            unbound: population.unbound.clone(),
            unrecorded: population.unrecorded.clone(),
            invalid: population.invalid.clone(),
        }
    }

    /// Blocking `environment` stops it executing.
    fn of_block(population: &Population, environment: EnvironmentReference) -> Self {
        Self::of(
            population,
            population
                .environments
                .iter()
                .filter(|(candidate, _)| **candidate == environment),
        )
    }

    /// The first default starts narrowing: every environment with no state,
    /// or with one that doesn't execute (a block only direct SQL can leave
    /// without a default), stops executing, other than the new default. Later
    /// defaults only add the new one as `allowed`, so they never stop
    /// anything.
    fn of_first_default(
        population: &Population,
        policy: &RuntimeEnvironmentPolicy,
        default: EnvironmentReference,
    ) -> Self {
        Self::of(
            population,
            population.environments.iter().filter(|(environment, _)| {
                **environment != default
                    && policy
                        .environments
                        .get(environment)
                        .is_none_or(|state| !state.executes())
            }),
        )
    }

    fn is_empty(&self) -> bool {
        !self.bound.gates()
            && self.unbound.runnable_sandboxes() == 0
            && self.unrecorded.runnable_sandboxes() == 0
            && self.invalid.runnable_sandboxes() == 0
            // A tag or pool on a release whose environment can't be told may
            // create on the one that stops.
            && self.unbound.template_releases_in_use == 0
            && self.invalid.template_releases_in_use == 0
    }
}

#[derive(Debug, Serialize)]
struct EnvironmentPopulationResponse {
    environment_ref: EnvironmentReference,
    /// `null` for an environment with work bound to it but no state row.
    state: Option<&'static str>,
    is_default: bool,
    #[serde(flatten)]
    counts: PopulationCounts,
}

/// `GET /runtime-environments/population`.
#[derive(Debug, Serialize)]
struct PopulationResponse {
    /// The policy generation `state` and `is_default` were read at.
    generation: u64,
    /// Every environment with a state or with work bound to it.
    environments: Vec<EnvironmentPopulationResponse>,
    /// Records with no environment binding: legacy work, which executes as
    /// the environment it matches.
    unbound: PopulationCounts,
    /// Sandboxes with no binding record: no observation from their node, a
    /// suspended or restoring one with no snapshot a restore could pick, or a
    /// failed one with no live observation and no repair snapshot.
    unrecorded: PopulationCounts,
    /// Records whose environment binding this build can't parse.
    invalid: PopulationCounts,
}

fn population_response(
    policy: &RuntimeEnvironmentPolicy,
    mut population: Population,
) -> PopulationResponse {
    for environment in policy.environments.keys() {
        population.environments.entry(*environment).or_default();
    }
    PopulationResponse {
        generation: policy.generation,
        environments: population
            .environments
            .into_iter()
            .map(|(environment, counts)| EnvironmentPopulationResponse {
                environment_ref: environment,
                state: policy
                    .environments
                    .get(&environment)
                    .map(|state| state.as_str()),
                is_default: policy.default_environment == Some(environment),
                counts,
            })
            .collect(),
        unbound: population.unbound,
        unrecorded: population.unrecorded,
        invalid: population.invalid,
    }
}

/// `GET /runtime-environments/population`: aggregate counts of sandboxes
/// and template releases per environment binding. Never a row list.
pub(super) async fn get_runtime_environment_population(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
) -> Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let policy = match policy_store::get(&state.pool).await {
        Ok(policy) => policy,
        Err(error) => return policy_read_error(error),
    };
    let mut conn = match state.pool.acquire().await {
        Ok(conn) => conn,
        Err(err) => return read_db_error(err),
    };
    match read_population(&mut conn).await {
        Ok(population) => Json(population_response(&policy, population)).into_response(),
        Err(err) => read_db_error(err),
    }
}

#[cfg(test)]
mod tests;
