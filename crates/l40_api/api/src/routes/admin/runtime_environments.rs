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
//! A write that commits also queues a `runtime-provisioning` reconcile on
//! every worker and builder ([`enqueue_policy_reconcile`]).
//!
//! A block also needs `strand: true` while an unfinished template runtime
//! upgrade campaign moves the environment's releases: blocking it mid-campaign
//! stops the releases the campaign hasn't given a twin on its target yet. As
//! for bound work, a block of an environment that already doesn't execute
//! stops nothing new and needs no `strand`, unless another policy write lands
//! while it waits, when the check runs in full. The first default needs
//! `strand: true` the same way while such a campaign moves any environment it
//! stops.
//!
//! Not yet checked: that every node can run a new default (per-node usable
//! environments are recorded, but a default change doesn't consult them yet),
//! that no unfinished template runtime upgrade campaign moves releases onto
//! the environment a block stops (a block checks a campaign's source, not its
//! target), and which nodes can't enforce a block (node status reports each
//! node's applied generation; refusing on it is a later change).

mod memory;
mod nodes;

pub(super) use memory::set_runtime_environment_memory;
pub(super) use nodes::list_runtime_environment_nodes;

#[cfg(test)]
use nodes::*;

use std::collections::{BTreeMap, BTreeSet};

use axum::{
    Extension, Json,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use pplx_space_accounts::UserId;
use pplx_space_execution_spec::EnvironmentReference;
use pplx_space_runtime_environment_policy_store::{
    self as policy_store, GetPolicyError, PolicyChange, PolicyChangeReason, RecordedChange,
    RuntimeEnvironmentPolicy, RuntimeEnvironmentPolicyRecord, RuntimeEnvironmentState,
    SetDefaultError, SetEnvironmentStateError,
};
use serde::{Deserialize, Serialize};

use pplx_space_runtime_upgrade_campaign_store::{self as campaign_store, CampaignId, CampaignKind};

use super::audit::{self, AdminRuntimeEnvironmentAuditEntry};
use super::runtime_environment_store as store;
use super::runtime_upgrade_campaigns::campaign_read_error;
use super::support::{
    ApiError, api_error, db_error, format_time, parse_environment_ref, parse_if_match_generation,
    parse_json_body, read_db_error, require_managing_org_admin,
};
use super::templates::requeue_reconcile_targets_tx;
use crate::middleware::auth::AuthUser;
use crate::models::User;
use crate::reconcile_queue::{ReconcileFacet, ReconcileReason, ReconcileRef};
use crate::routes::AppState;

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
    memory_retired: bool,
    memory_last_change: Option<RecordedChangeResponse>,
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
                memory_retired: policy.is_memory_retired(*environment),
                memory_last_change: record
                    .environment_memory_changes
                    .get(environment)
                    .map(RecordedChangeResponse::from),
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
    /// it stops (see [`AtRisk::of_first_default`]) or an unfinished template
    /// campaign moves one, or replace the default even though a `deprecated`
    /// environment with `memory_retired: true` lacks a certified `upgrade_edges`
    /// entry to the new default.
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

/// The audit outcome of a write whose node reconcile enqueue failed, which
/// rolls the write back.
const QUEUE_RECONCILE_FAILED: &str =
    "couldn't queue the node reconciles; the write was rolled back";

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

    /// Audit and answer a write refused because an unfinished template
    /// campaign still moves the releases of `environment`, which the write
    /// stops. `at_risk` is the work the write would also stop, if any, since
    /// the `strand: true` that overrides this refusal waives that check too.
    fn refuse_template_campaign(
        &self,
        campaign_id: CampaignId,
        environment: EnvironmentReference,
        write: StoppingWrite,
        at_risk: Option<AtRisk>,
    ) -> Response {
        // A first default the campaign waits on (its target) can't wait for
        // the campaign to complete, so the way through is keeping the source
        // executing: a state for it.
        let (stops, remedy, verb) = match write {
            StoppingWrite::Block => ("", "let it complete or abort it first".to_owned(), "block"),
            StoppingWrite::FirstDefault => (
                ", which the cluster's first default stops",
                format!(
                    "record a state for {environment} first (allowed or deprecated keeps it \
                     executing), or abort the campaign"
                ),
                "set the default",
            ),
        };
        let message = format!(
            "template runtime upgrade campaign {campaign_id} still moves environment \
             {environment}'s releases{stops}; {remedy}, or pass strand: true to {verb} \
             anyway{}",
            if at_risk.is_some() {
                ", which also stops the work at_risk counts"
            } else {
                ""
            }
        );
        self.record(None, Some(&message));
        let body = match at_risk {
            Some(at_risk) => serde_json::json!({ "error": message, "at_risk": at_risk }),
            None => serde_json::json!({ "error": message }),
        };
        (StatusCode::CONFLICT, Json(body)).into_response()
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

/// Queue a whole-facet `runtime-provisioning` ref and a reconcile trigger on
/// every worker and builder in the policy write's transaction, so the enqueue
/// commits or rolls back with the write and nodes fetch the new policy within
/// trigger latency. The enqueue lives in the handlers rather than the store's
/// verbs: which nodes a change reconciles is a use-case decision, and the
/// policy store writes only the policy's tables.
///
/// A ref already queued on a node, even one backing off after failures, is
/// queued again as a new request and is due at once
/// ([`requeue_reconcile_targets_tx`]). One a node holds a claim on keeps the
/// claim and runs again once the claim is released or goes stale.
///
/// The trigger reuses `node_updated` rather than a new
/// `state_reconciliation_reason` value. A controller that decodes the reasons
/// strictly fails every claim batch holding a value it doesn't know, which
/// would stall triggers across the fleet during a rolling deploy or after a
/// rollback. Claims tolerate unknown values from this change on, so a
/// dedicated reason is safe once no controller you could roll back to
/// predates it. The reason is diagnostic only; the queued ref decides what the
/// node reconciles.
async fn enqueue_policy_reconcile(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), sqlx::Error> {
    requeue_reconcile_targets_tx(
        tx,
        &[ReconcileRef::full(ReconcileFacet::RuntimeProvisioning)],
        ReconcileReason::NodeUpdated,
    )
    .await
}

/// The strand check's verdict on a block, read before the write.
enum BlockCheck {
    /// The environment already didn't execute at `generation`: it was
    /// blocked, or had no state under a cluster default. The block stops
    /// nothing new, unless another write lands before this one.
    AlreadyStopped { generation: u64 },
    /// What the block would stop, if anything, read at `generation`;
    /// `None` when the policy was unreadable.
    /// Boxed: an `AtRisk` is large next to the other variant.
    Checked {
        at_risk: Option<Box<AtRisk>>,
        generation: Option<u64>,
    },
}

/// What blocking `environment` would stop. A block of an environment that
/// already doesn't execute, say to update the reason, needs no
/// acknowledgement. The error is the response to give, boxed because a
/// `Response` is large.
async fn block_check(
    pool: &sqlx::PgPool,
    environment: EnvironmentReference,
    boots_template_twins: bool,
) -> Result<BlockCheck, Box<Response>> {
    let mut conn = pool
        .acquire()
        .await
        .map_err(|err| Box::new(read_db_error(err)))?;
    let mut generation = None;
    match policy_store::get(&mut *conn).await {
        Ok(policy) => {
            generation = Some(policy.generation);
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
    let population = read_population(&mut conn, boots_template_twins)
        .await
        .map_err(|err| Box::new(read_db_error(err)))?;
    Ok(BlockCheck::Checked {
        at_risk: Some(Box::new(AtRisk::of_block(&population, environment)))
            .filter(|at_risk| !at_risk.is_empty()),
        generation,
    })
}

/// What a default write would stop, when it's the cluster's first: `None`
/// when there's a default already or nothing is at risk. The error is the
/// response to give, boxed because a `Response` is large.
async fn first_default_at_risk(
    pool: &sqlx::PgPool,
    default: EnvironmentReference,
    boots_template_twins: bool,
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
    let population = read_population(&mut conn, boots_template_twins)
        .await
        .map_err(|err| Box::new(read_db_error(err)))?;
    Ok(
        Some(AtRisk::of_first_default(&population, &policy, default))
            .filter(|at_risk| !at_risk.is_empty()),
    )
}

/// A policy write that stops environments executing, as a refusal names it.
#[derive(Clone, Copy)]
enum StoppingWrite {
    Block,
    FirstDefault,
}

/// The unfinished template campaign, and its source, that moves an
/// environment the cluster's first default `default` stops, if any. Runs on
/// the write's transaction after the store's write, so it reads the policy as
/// the write leaves it: every environment with no executing state, other than
/// `default`, stops.
///
/// The candidate sources are every environment a template release is bound
/// to, and every unfinished template campaign's source. Each one that stops
/// is read under the advisory lock every campaign start for it takes
/// ([`campaign_store::unfinished_from_locked`]), so none can start between the
/// read and the commit. A campaign on any other environment had no release to
/// move when the candidates were read; one started there afterwards, on a
/// release registered after that read, isn't caught. The locks are taken in
/// order; a campaign start takes only one, and another policy write waits on
/// the policy row this transaction holds.
async fn first_default_template_campaign(
    tx: &mut sqlx::PgConnection,
    default: EnvironmentReference,
) -> Result<Option<(CampaignId, EnvironmentReference)>, Box<Response>> {
    let policy = policy_store::get(&mut *tx)
        .await
        .map_err(|error| Box::new(policy_read_error(error)))?;
    // An environment reference that doesn't parse can't be a campaign's
    // source.
    let mut sources: BTreeSet<EnvironmentReference> =
        pplx_space_template_release_store::lookup::release_environments(
            &mut *tx,
            pplx_space_execution_catalog::catalog::initial_environment_reference(),
        )
        .await
        .map_err(|err| Box::new(read_db_error(err)))?
        .iter()
        .filter_map(|environment| environment.parse().ok())
        .collect();
    sources.extend(
        campaign_store::list_unfinished(&mut *tx)
            .await
            .map_err(|error| Box::new(campaign_read_error(error)))?
            .into_iter()
            .filter(|campaign| campaign.kind == CampaignKind::Templates)
            .map(|campaign| campaign.from),
    );
    for source in sources {
        let stops = source != default
            && policy
                .environments
                .get(&source)
                .is_none_or(|state| !state.executes());
        if !stops {
            continue;
        }
        if let Some(campaign_id) =
            campaign_store::unfinished_from_locked(&mut *tx, CampaignKind::Templates, source)
                .await
                .map_err(|err| Box::new(read_db_error(err)))?
        {
            return Ok(Some((campaign_id, source)));
        }
    }
    Ok(None)
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
        first_default_at_risk(
            &state.pool,
            environment,
            state.sandbox_controller.boots_template_twins(),
        )
        .await
    };

    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(err) => return audit.fail("the write failed", db_error(err).into_response()),
    };
    // Whether this is the first default, read before the store's write takes
    // the policy row lock: a default set concurrently only makes the
    // campaign check below run when it needn't. Only the default, as for
    // `at_risk`.
    let first_default = if request.strand {
        Ok(false)
    } else {
        policy_store::get_default(&mut *tx)
            .await
            .map(|existing| existing.is_none())
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
        Err(SetDefaultError::MemoryRetired(environment)) => {
            return audit.refuse(
                StatusCode::CONFLICT,
                format!(
                    "environment {environment} has retired its checkpoint memory and can't \
                     become the default; unretire its memory with \
                     PUT /runtime-environments/{environment}/memory first"
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
    let at_risk = match at_risk {
        Ok(at_risk) => at_risk,
        Err(response) => {
            drop(tx);
            return audit.fail("couldn't check what the write would stop", *response);
        }
    };
    // As for a block: under the campaign start lock of each source the
    // default stops, after the read of what else it stops, so the refusal
    // reports that work too.
    let template_campaign = match first_default {
        Ok(true) => first_default_template_campaign(&mut tx, environment).await,
        Ok(false) => Ok(None),
        Err(error) => Err(Box::new(policy_read_error(error))),
    };
    match template_campaign {
        Ok(None) => {}
        Ok(Some((campaign_id, source))) => {
            drop(tx);
            return audit.refuse_template_campaign(
                campaign_id,
                source,
                StoppingWrite::FirstDefault,
                at_risk,
            );
        }
        Err(response) => {
            drop(tx);
            return audit.fail(
                "couldn't check for an unfinished template campaign",
                *response,
            );
        }
    }
    if let Some(at_risk) = at_risk {
        drop(tx);
        return audit.refuse_strand(at_risk);
    }
    if !request.strand {
        match memory::deprecated_retired_missing_edge_to_default(&mut tx, environment).await {
            Ok(None) => {}
            Ok(Some(retired)) => {
                drop(tx);
                return audit.refuse(
                    StatusCode::CONFLICT,
                    format!(
                        "deprecated environment {retired} has memory_retired = true and no \
                         certified upgrade edge to the new default {environment} in \
                         runtime_environments; certify an upgrade edge, unretire {retired}'s \
                         memory, or pass strand: true to move the default anyway"
                    ),
                );
            }
            Err(response) => {
                drop(tx);
                return audit.fail(
                    "couldn't check certified edges for memory-retired environments",
                    *response,
                );
            }
        }
    }
    // A refusal drops the transaction, which discards anything queued in it,
    // and so does a failed enqueue, rolling the write back with it. The
    // enqueue goes last so a refused write doesn't do it at all, and the
    // trigger rows it locks are held only until the commit.
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

    let boots_template_twins = state.sandbox_controller.boots_template_twins();
    // Read before the write's transaction, so a scan of every sandbox doesn't
    // hold the policy row lock and stall other policy writers. Sandboxes keep
    // changing either way: this is an acknowledgement, not a guarantee that
    // nothing is bound once the block lands. A failed read is answered only
    // after the store's own refusals.
    let check = if requested == RuntimeEnvironmentState::Blocked
        && !request.strand
        && !already_stale(&state.pool, expected).await
    {
        block_check(&state.pool, environment, boots_template_twins).await
    } else {
        Ok(BlockCheck::Checked {
            at_risk: None,
            generation: None,
        })
    };

    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(err) => return audit.fail("the write failed", db_error(err).into_response()),
    };
    // A block rechecks if another write landed since its check: one that
    // read its environment as already stopped, since the write may have let
    // it execute again, and, where creates boot the twin they resolve to, any
    // other, since which release a tag keeps in use then depends on the
    // policy. The recheck takes the policy row lock, so it waits for a write
    // in flight, and reads before this write, on this transaction's
    // connection: once the block is written, resolution no longer picks a
    // release in the environment, so a tag's twin there would read as
    // unused, and a second connection could wait forever on a pool this
    // transaction has exhausted. A rare path, so the scan's time under the
    // lock is acceptable. Unlike the check's, a failed recheck is answered
    // before the store's refusals, since it runs first.
    let checked_at = match &check {
        Ok(BlockCheck::AlreadyStopped { generation }) => Some(Some(*generation)),
        Ok(BlockCheck::Checked { generation, .. })
            if boots_template_twins
                && requested == RuntimeEnvironmentState::Blocked
                && !request.strand =>
        {
            Some(*generation)
        }
        _ => None,
    };
    let recheck = match checked_at {
        Some(checked_at) => {
            match recheck_block(
                &mut tx,
                environment,
                checked_at,
                expected,
                boots_template_twins,
            )
            .await
            {
                Ok(recheck) => recheck,
                Err(response) => {
                    drop(tx);
                    return audit.fail("couldn't check what the write would stop", *response);
                }
            }
        }
        None => None,
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
            Err(SetEnvironmentStateError::MemoryRetired(environment)) => {
                return audit.refuse(
                    StatusCode::CONFLICT,
                    format!(
                        "environment {environment} has retired its checkpoint memory and can't \
                         become allowed; unretire its memory with \
                         PUT /runtime-environments/{environment}/memory first"
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
    // This write raised the generation by exactly one, so nothing changed
    // the policy since it read the environment as stopped: the block stops
    // nothing new.
    let stops_nothing_new = matches!(
        check,
        Ok(BlockCheck::AlreadyStopped { generation: stopped_at }) if generation == stopped_at + 1
    );
    // Refused only after the store's own refusals, which the write above has
    // already returned, so an operator isn't asked to acknowledge stranding
    // work for a block that would be refused anyway. Dropping the transaction
    // rolls the write back.
    let at_risk = match check {
        Ok(check) => match block_at_risk(check, recheck, generation) {
            Ok(at_risk) => at_risk,
            Err(PolicyMovedDuringRecheck) => {
                drop(tx);
                return audit.refuse(
                    StatusCode::CONFLICT,
                    "the cluster policy changed while this block was being checked; retry"
                        .to_owned(),
                );
            }
        },
        Err(response) => {
            drop(tx);
            return audit.fail("couldn't check what the write would stop", *response);
        }
    };
    // Read inside the write's transaction under the advisory lock every
    // template campaign start for this environment takes, so no campaign can
    // start between this read and the block's commit. After the store's
    // refusals, the generation check and the read of what the block would
    // stop, so the refusal reports that work too: `strand: true` waives both.
    let template_campaign =
        if requested == RuntimeEnvironmentState::Blocked && !request.strand && !stops_nothing_new {
            campaign_store::unfinished_from_locked(&mut tx, CampaignKind::Templates, environment)
                .await
        } else {
            Ok(None)
        };
    match template_campaign {
        Ok(None) => {}
        Ok(Some(campaign_id)) => {
            drop(tx);
            return audit.refuse_template_campaign(
                campaign_id,
                environment,
                StoppingWrite::Block,
                at_risk,
            );
        }
        Err(err) => {
            drop(tx);
            return audit.fail(
                "couldn't check for an unfinished template campaign",
                db_error(err).into_response(),
            );
        }
    }
    if let Some(at_risk) = at_risk {
        drop(tx);
        return audit.refuse_strand(at_risk);
    }
    // As for the default: last before the commit.
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
    Json(SetStateResponse {
        generation,
        environment_ref: environment,
        state: requested.as_str(),
    })
    .into_response()
}

/// Work bound to one environment, or to none.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
struct PopulationCounts {
    sandboxes: u64,
    /// `sandboxes`, split by raw `sandboxes.lifecycle`.
    sandboxes_by_lifecycle: BTreeMap<String, u64>,
    /// How many of `sandboxes` can't run again: `deleting`, or `failed`
    /// with no live evidence and nothing the repair
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

/// What a block's recheck read before the write: the generation it read at,
/// and what blocking the environment would stop then.
struct Recheck {
    generation: u64,
    at_risk: Option<Box<AtRisk>>,
}

/// The recheck of a block checked at `checked_at`, read in `tx` under the
/// policy row lock before the block is written. `None` when no write landed
/// since, so the check stands; else what blocking `environment` would stop
/// under the policy as it stands. A check that couldn't read the policy
/// (`None`) is always rechecked. A write conditional on another generation
/// (`expected`) is refused with 412 before the recheck's result is used, so
/// it isn't rechecked.
async fn recheck_block(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    environment: EnvironmentReference,
    checked_at: Option<u64>,
    expected: Option<u64>,
    boots_template_twins: bool,
) -> Result<Option<Recheck>, Box<Response>> {
    let generation = policy_store::lock_generation(tx)
        .await
        .map_err(|error| Box::new(policy_read_error(error)))?;
    if checked_at == Some(generation) || expected.is_some_and(|expected| expected != generation) {
        return Ok(None);
    }
    let population = read_population(tx, boots_template_twins)
        .await
        .map_err(|err| Box::new(read_db_error(err)))?;
    Ok(Some(Recheck {
        generation,
        at_risk: Some(AtRisk::of_block(&population, environment))
            .filter(|at_risk| !at_risk.is_empty())
            .map(Box::new),
    }))
}

/// The block was written at a generation the recheck doesn't account for.
/// The recheck holds the policy row lock until the write, so this can't
/// happen; the handler refuses rather than guess if it does.
#[derive(Debug, PartialEq, Eq)]
struct PolicyMovedDuringRecheck;

/// What a block written at `generation` stops, given its `check` and, when
/// another write landed since the check, its `recheck`.
fn block_at_risk(
    check: BlockCheck,
    recheck: Option<Recheck>,
    generation: u64,
) -> Result<Option<AtRisk>, PolicyMovedDuringRecheck> {
    match (check, recheck) {
        // A write landed since the check: the recheck read what the block
        // stops just before this write. That other write may have changed
        // only some other environment, so this can ask for an acknowledgement
        // a retry wouldn't need.
        (
            _,
            Some(Recheck {
                generation: read_at,
                at_risk,
            }),
        ) if generation == read_at + 1 => Ok(at_risk.map(|at_risk| *at_risk)),
        (_, Some(_)) => Err(PolicyMovedDuringRecheck),
        (BlockCheck::Checked { at_risk, .. }, None) => Ok(at_risk.map(|at_risk| *at_risk)),
        // This write raised the generation by exactly one, so nothing changed
        // the policy since it read the environment as stopped.
        (
            BlockCheck::AlreadyStopped {
                generation: stopped_at,
            },
            None,
        ) if generation == stopped_at + 1 => Ok(None),
        (BlockCheck::AlreadyStopped { .. }, None) => Err(PolicyMovedDuringRecheck),
    }
}

/// `boots_template_twins`: whether this deployment's creates boot the twin
/// they resolve to ([`crate::controllers::substrate::Substrate::boots_template_twins`]),
/// so a tag stops counting for a release that has one.
async fn read_population(
    conn: &mut sqlx::PgConnection,
    boots_template_twins: bool,
) -> Result<Population, sqlx::Error> {
    let sandboxes = store::sandbox_population(&mut *conn).await?;
    let template_releases =
        store::template_release_population(&mut *conn, boots_template_twins).await?;
    Ok(Population::from_rows(sandboxes, template_releases))
}

/// The work a policy write could strand: everything bound to the
/// environments that would stop executing, and the sandboxes whose
/// environment this server can't tell, because they have no binding, an
/// unreadable one, or no record.
///
/// A template release counts only while it's in use: creates by tag run it
/// (where creates boot the twin they resolve to, the release the tagged build
/// resolves to) or an enabled pool config pins it. A superseded release stays until its
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
    match read_population(&mut conn, state.sandbox_controller.boots_template_twins()).await {
        Ok(population) => Json(population_response(&policy, population)).into_response(),
        Err(err) => read_db_error(err),
    }
}

#[cfg(test)]
mod tests;
