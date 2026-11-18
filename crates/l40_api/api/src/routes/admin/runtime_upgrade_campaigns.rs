//! `/runtime-upgrade-campaigns` operator surface: start, pause, resume,
//! abort and read runtime upgrade campaigns, which the cluster controller's
//! `runtime_upgrade` loop drives.
//!
//! Every write goes through `pplx_space_runtime_upgrade_campaign_store`,
//! which locks the campaign row, refuses a transition its phase doesn't
//! allow and raises the generation with every change. This module adds the
//! HTTP rules around it, following the runtime environment policy routes:
//!
//! * a managing-organization admin only, with a required `reason`;
//! * a pause, resume or abort that names a generation in `If-Match` applies only at
//!   that generation, compared under the row lock; a stale one answers 412;
//! * a start is refused (409) for a kind no cluster-controller driver moves
//!   ([`RUNTIME_UPGRADE_DRIVEN_KINDS`]), and while an unfinished campaign of
//!   the same kind moves the same source.
//!
//! Resume refuses a campaign whose failed items are at or over its failure
//! budget unless the request sets `retry_failed`, which returns them to
//! pending. Abort ends any unfinished campaign for good and frees its source
//! for a new start at once; its in-flight items drain under their leases,
//! and no other campaign leases those subjects until the leases lapse. See
//! the store's `admin` module.
//!
//! Each write is audited by the admin middleware and with its own
//! `space.admin.audit` entry, which carries the reason.

use axum::{
    Extension, Json,
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use pplx_space_accounts::UserId;
use pplx_space_execution_spec::EnvironmentReference;
use pplx_space_runtime_upgrade_campaign_store::{
    self as store, CampaignId, CampaignKind, CampaignRecord, ChangePhaseError, CreateCampaignError,
    ItemCounts, NewCampaign, PhaseChange, ReadCampaignError,
};
use serde::{Deserialize, Serialize};

use super::audit::{self, AdminRuntimeUpgradeCampaignAuditEntry};
use super::support::{
    ApiError, api_error, db_error, format_optional_time, format_time, parse_environment_ref,
    parse_if_match_generation, parse_json_body, read_db_error, require_managing_org_admin,
};
use crate::middleware::auth::AuthUser;
use crate::models::User;
use crate::routes::AppState;

/// Campaign kinds a cluster-controller driver moves, so the API starts
/// campaigns of them. None yet: the template driver and the sandbox driver
/// each add their kind here as they land, with the controller's
/// `runtime_upgrade::Drivers` registration. A campaign of a kind without a
/// driver would sit in canary forever.
pub const RUNTIME_UPGRADE_DRIVEN_KINDS: &[CampaignKind] = &[];

/// Campaigns per `GET /runtime-upgrade-campaigns` page.
const PAGE_SIZE: u32 = 100;

#[derive(Debug, Serialize)]
struct ItemCountsResponse {
    pending: u64,
    in_flight: u64,
    done: u64,
    failed: u64,
    skipped: u64,
}

impl From<ItemCounts> for ItemCountsResponse {
    fn from(counts: ItemCounts) -> Self {
        Self {
            pending: counts.pending,
            in_flight: counts.in_flight,
            done: counts.done,
            failed: counts.failed,
            skipped: counts.skipped,
        }
    }
}

#[derive(Debug, Serialize)]
struct CampaignResponse {
    id: uuid::Uuid,
    kind: &'static str,
    from_environment_ref: EnvironmentReference,
    to_environment_ref: EnvironmentReference,
    phase: &'static str,
    generation: u64,
    max_in_flight_per_node: u64,
    max_in_flight_per_cluster: u64,
    failure_budget: u64,
    include_running: bool,
    reason: String,
    created_at: String,
    created_by_user_id: Option<UserId>,
    updated_at: String,
    /// `null` when the reconciler made the last change.
    updated_by_user_id: Option<UserId>,
    finished_at: Option<String>,
    items: ItemCountsResponse,
    /// Pending items the campaign can't move: for a sandbox campaign, those
    /// recorded while the catalog certifies no upgrade edge from its source
    /// to its target. `null` for a kind that doesn't define it yet.
    stranded: Option<u64>,
}

/// Why a read failed, as a response.
fn campaign_read_error(error: ReadCampaignError) -> Response {
    match error {
        ReadCampaignError::Database(err) => read_db_error(err),
        error => {
            tracing::error!(%error, "read runtime upgrade campaign");
            api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("a runtime upgrade campaign can't be read: {error}"),
            )
            .into_response()
        }
    }
}

/// Why counting items failed, as a response.
fn item_counts_error(error: store::ItemCountsError) -> Response {
    match error {
        store::ItemCountsError::Database(err) => read_db_error(err),
        error => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("a campaign's items can't be read: {error}"),
        )
        .into_response(),
    }
}

/// `record` with its item `counts` and its stranded count, read now. The
/// error is the response to give, boxed because a `Response` is large.
async fn campaign_response(
    pool: &sqlx::PgPool,
    record: CampaignRecord,
    counts: ItemCounts,
) -> Result<CampaignResponse, Box<Response>> {
    let campaign = &record.campaign;
    let stranded = match campaign.kind {
        // Nothing pending is nothing stranded, edge or not, and a finished
        // campaign moves nothing more.
        CampaignKind::Sandboxes if counts.pending == 0 || campaign.phase.finished() => Some(0),
        CampaignKind::Sandboxes => {
            let moves = pplx_space_runtime_environment_catalog_store::upgrade_edge_certified(
                pool,
                campaign.from,
                campaign.to,
            )
            .await
            .map_err(|err| Box::new(read_db_error(err)))?;
            Some(if moves { 0 } else { counts.pending })
        }
        CampaignKind::Templates => None,
    };
    Ok(CampaignResponse {
        id: campaign.id.0,
        kind: campaign.kind.as_str(),
        from_environment_ref: campaign.from,
        to_environment_ref: campaign.to,
        phase: campaign.phase.as_str(),
        generation: campaign.generation,
        max_in_flight_per_node: campaign.max_in_flight_per_node,
        max_in_flight_per_cluster: campaign.max_in_flight_per_cluster,
        failure_budget: campaign.failure_budget,
        include_running: campaign.include_running,
        reason: record.reason,
        created_at: format_time(record.created_at),
        created_by_user_id: record.created_by,
        updated_at: format_time(record.updated_at),
        updated_by_user_id: record.updated_by,
        finished_at: format_optional_time(record.finished_at),
        items: counts.into(),
        stranded,
    })
}

#[derive(Debug, Deserialize)]
pub(super) struct ListQuery {
    /// Only canary, rolling and paused campaigns.
    #[serde(default)]
    unfinished: bool,
    after: Option<uuid::Uuid>,
}

#[derive(Debug, Serialize)]
struct CampaignPage {
    campaigns: Vec<CampaignResponse>,
    /// Pass as `after` for the next page; `null` on the last page.
    next_after: Option<uuid::Uuid>,
}

/// `GET /runtime-upgrade-campaigns`: campaigns in creation order, a page at
/// a time.
pub(super) async fn list_campaigns(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Query(query): Query<ListQuery>,
) -> Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    // One more than a page, to tell whether another follows.
    let mut records = match store::list_records(
        &state.pool,
        query.unfinished,
        query.after.map(CampaignId),
        PAGE_SIZE + 1,
    )
    .await
    {
        Ok(records) => records,
        Err(error) => return campaign_read_error(error),
    };
    let next_after = if records.len() > PAGE_SIZE as usize {
        records.truncate(PAGE_SIZE as usize);
        records.last().map(|record| record.campaign.id.0)
    } else {
        None
    };
    let ids: Vec<CampaignId> = records.iter().map(|record| record.campaign.id).collect();
    let mut counts = match store::item_counts_of(&state.pool, &ids).await {
        Ok(counts) => counts,
        Err(error) => return item_counts_error(error),
    };
    let mut campaigns = Vec::with_capacity(records.len());
    for record in records {
        let record_counts = counts.remove(&record.campaign.id).unwrap_or_default();
        match campaign_response(&state.pool, record, record_counts).await {
            Ok(campaign) => campaigns.push(campaign),
            Err(response) => return *response,
        }
    }
    Json(CampaignPage {
        campaigns,
        next_after,
    })
    .into_response()
}

fn parse_campaign_id(value: &str) -> Result<CampaignId, ApiError> {
    value.parse().map(CampaignId).map_err(|_| {
        api_error(
            StatusCode::BAD_REQUEST,
            format!("invalid campaign id {value:?}"),
        )
    })
}

/// `GET /runtime-upgrade-campaigns/{campaign_id}`.
pub(super) async fn get_campaign(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(spec): Path<String>,
) -> Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let campaign_id = match parse_campaign_id(&spec) {
        Ok(campaign_id) => campaign_id,
        Err(err) => return err.into_response(),
    };
    let record = match store::get_record(&state.pool, campaign_id).await {
        Ok(Some(record)) => record,
        Ok(None) => return not_found(campaign_id).into_response(),
        Err(error) => return campaign_read_error(error),
    };
    let counts = match store::item_counts(&state.pool, campaign_id).await {
        Ok(counts) => counts,
        Err(error) => return item_counts_error(error),
    };
    match campaign_response(&state.pool, record, counts).await {
        Ok(campaign) => Json(campaign).into_response(),
        Err(response) => *response,
    }
}

fn not_found(campaign_id: CampaignId) -> ApiError {
    api_error(
        StatusCode::NOT_FOUND,
        format!("no runtime upgrade campaign {campaign_id}"),
    )
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StartRequest {
    kind: String,
    from_environment_ref: String,
    to_environment_ref: String,
    /// Subjects upgraded before the campaign waits for a resume: `1`, the
    /// default, or `0` to start rolling.
    #[serde(default = "default_canary_size")]
    canary_size: u64,
    /// Items in flight at once across the cluster: the wave size.
    max_in_flight_per_cluster: u64,
    /// Items in flight at once on any one node, for kinds whose subjects
    /// have a node. Defaults to 1.
    #[serde(default = "default_per_node")]
    max_in_flight_per_node: u64,
    failure_budget: u64,
    #[serde(default)]
    include_running: bool,
    reason: String,
}

const fn default_canary_size() -> u64 {
    1
}

const fn default_per_node() -> u64 {
    1
}

#[derive(Debug, Serialize)]
struct StartResponse {
    id: uuid::Uuid,
    phase: &'static str,
    generation: u64,
}

fn parse_kind(value: &str) -> Result<CampaignKind, ApiError> {
    value.parse().map_err(|_| {
        let accepted = CampaignKind::ALL
            .iter()
            .map(|kind| kind.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        api_error(
            StatusCode::BAD_REQUEST,
            format!("unknown campaign kind {value:?}; expected one of: {accepted}"),
        )
    })
}

fn parse_start(request: StartRequest, actor: &User) -> Result<NewCampaign, ApiError> {
    let canary = match request.canary_size {
        0 => false,
        1 => true,
        _ => {
            return Err(api_error(
                StatusCode::BAD_REQUEST,
                "canary_size must be 0 or 1: a canary upgrades one subject",
            ));
        }
    };
    Ok(NewCampaign {
        from: parse_environment_ref(&request.from_environment_ref)?,
        to: parse_environment_ref(&request.to_environment_ref)?,
        kind: parse_kind(&request.kind)?,
        canary,
        max_in_flight_per_node: request.max_in_flight_per_node,
        max_in_flight_per_cluster: request.max_in_flight_per_cluster,
        failure_budget: request.failure_budget,
        include_running: request.include_running,
        reason: request.reason,
        created_by: Some(actor.id),
    })
}

/// What a write's audit entry needs besides its outcome.
struct AuditContext<'a> {
    actor: &'a User,
    action: &'static str,
    campaign_id: Option<CampaignId>,
    reason: &'a str,
}

impl AuditContext<'_> {
    fn record(
        &self,
        campaign_id: Option<CampaignId>,
        generation: Option<u64>,
        error: Option<&str>,
    ) {
        let campaign_id = campaign_id.or(self.campaign_id).map(|id| id.to_string());
        audit::record_runtime_upgrade_campaign(AdminRuntimeUpgradeCampaignAuditEntry {
            actor: self.actor,
            action: self.action,
            campaign_id: campaign_id.as_deref().unwrap_or(""),
            reason: self.reason,
            generation,
            success: error.is_none(),
            error,
        });
    }

    /// Audit a refusal and answer it.
    fn refuse(&self, status: StatusCode, message: String) -> Response {
        self.record(None, None, Some(&message));
        api_error(status, message).into_response()
    }

    /// Audit a write that failed rather than being refused, and give its
    /// response.
    fn fail(&self, what: &str, response: Response) -> Response {
        self.record(None, None, Some(what));
        response
    }
}

/// `POST /runtime-upgrade-campaigns`: start a campaign, as a one-subject
/// canary unless `canary_size` is 0.
pub(super) async fn start_campaign(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    body: Bytes,
) -> Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let request: StartRequest = match parse_json_body(&body, "runtime upgrade campaign") {
        Ok(request) => request,
        Err(err) => return err.into_response(),
    };
    let campaign = match parse_start(request, &actor) {
        Ok(campaign) => campaign,
        Err(err) => return err.into_response(),
    };
    let audit = AuditContext {
        actor: &actor,
        action: "runtime-upgrade-campaign-start",
        campaign_id: None,
        reason: &campaign.reason,
    };
    if !state.runtime_upgrade_driven_kinds.contains(&campaign.kind) {
        return audit.refuse(
            StatusCode::CONFLICT,
            format!(
                "no cluster-controller driver moves {} campaigns yet, so one would never \
                 progress",
                campaign.kind
            ),
        );
    }
    match store::create_campaign(&state.pool, &campaign).await {
        Ok(campaign_id) => {
            let phase = campaign.initial_phase();
            audit.record(Some(campaign_id), Some(0), None);
            (
                StatusCode::CREATED,
                Json(StartResponse {
                    id: campaign_id.0,
                    phase: phase.as_str(),
                    generation: 0,
                }),
            )
                .into_response()
        }
        Err(CreateCampaignError::Invalid(message)) => {
            audit.refuse(StatusCode::BAD_REQUEST, message.to_owned())
        }
        Err(CreateCampaignError::AlreadyUnfinished(existing)) => audit.refuse(
            StatusCode::CONFLICT,
            format!(
                "campaign {existing} already moves {} {}; let it complete or abort it first",
                campaign.from, campaign.kind
            ),
        ),
        Err(CreateCampaignError::Database(err)) => {
            audit.fail("the write failed", db_error(err).into_response())
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReasonRequest {
    reason: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResumeRequest {
    reason: String,
    /// Return failed items to pending, with their attempts reset, so a
    /// campaign at its failure budget can resume.
    #[serde(default)]
    retry_failed: bool,
}

#[derive(Debug, Serialize)]
struct PhaseResponse {
    id: uuid::Uuid,
    phase: &'static str,
    generation: u64,
    /// Failed items a resume returned to pending.
    retried: u64,
    /// In-flight items an abort left to drain: their workers stop at their
    /// next lease renewal.
    draining: u64,
}

/// The operator verbs: `{campaign_id}:pause`, `:resume` and `:abort`.
#[derive(Clone, Copy, Debug)]
enum Action {
    Pause,
    Resume,
    Abort,
}

impl Action {
    /// The verb after the colon in the path.
    const fn verb(self) -> &'static str {
        match self {
            Self::Pause => "pause",
            Self::Resume => "resume",
            Self::Abort => "abort",
        }
    }

    /// The action its audit entries name.
    const fn audit_action(self) -> &'static str {
        match self {
            Self::Pause => "runtime-upgrade-campaign-pause",
            Self::Resume => "runtime-upgrade-campaign-resume",
            Self::Abort => "runtime-upgrade-campaign-abort",
        }
    }
}

fn parse_action(spec: &str) -> Result<(CampaignId, Action), ApiError> {
    let Some((campaign_id, action)) = spec.rsplit_once(':') else {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "campaign action path must be /runtime-upgrade-campaigns/{campaign_id}:pause, \
             :resume or :abort",
        ));
    };
    let campaign_id = parse_campaign_id(campaign_id)?;
    let action = match [Action::Pause, Action::Resume, Action::Abort]
        .into_iter()
        .find(|candidate| candidate.verb() == action)
    {
        Some(action) => action,
        None => {
            let other = action;
            return Err(api_error(
                StatusCode::NOT_FOUND,
                format!("unknown campaign action {other:?}; expected pause, resume or abort"),
            ));
        }
    };
    Ok((campaign_id, action))
}

fn change_error(
    audit: &AuditContext<'_>,
    campaign_id: CampaignId,
    error: ChangePhaseError,
) -> Response {
    match error {
        ChangePhaseError::NotFound => audit.refuse(
            StatusCode::NOT_FOUND,
            format!("no runtime upgrade campaign {campaign_id}"),
        ),
        ChangePhaseError::Stale {
            generation,
            expected,
        } => audit.refuse(
            StatusCode::PRECONDITION_FAILED,
            format!(
                "the campaign is at generation {generation}, not {expected} as If-Match \
                 expects; read it again and retry"
            ),
        ),
        error @ (ChangePhaseError::Finished(_)
        | ChangePhaseError::AlreadyPaused
        | ChangePhaseError::NotResumable(_)
        | ChangePhaseError::AtFailureBudget { .. }) => {
            audit.refuse(StatusCode::CONFLICT, error.to_string())
        }
        error @ ChangePhaseError::UnknownValue(_) => {
            tracing::error!(%error, %campaign_id, "change runtime upgrade campaign phase");
            audit.refuse(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
        }
        ChangePhaseError::Database(err) => {
            audit.fail("the write failed", db_error(err).into_response())
        }
    }
}

/// `POST /runtime-upgrade-campaigns/{campaign_id}:pause`, `:resume` and
/// `:abort`.
pub(super) async fn dispatch_campaign_action(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(spec): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let (campaign_id, action) = match parse_action(&spec) {
        Ok(parsed) => parsed,
        Err(err) => return err.into_response(),
    };
    let expected_generation = match parse_if_match_generation(&headers) {
        Ok(expected) => expected,
        Err(message) => return api_error(StatusCode::BAD_REQUEST, message).into_response(),
    };
    let what = format!("campaign {}", action.verb());
    let parsed = match action {
        Action::Pause | Action::Abort => {
            parse_json_body::<ReasonRequest>(&body, &what).map(|request| (request.reason, false))
        }
        Action::Resume => parse_json_body::<ResumeRequest>(&body, &what)
            .map(|request| (request.reason, request.retry_failed)),
    };
    let (reason, retry_failed) = match parsed {
        Ok(parsed) => parsed,
        Err(err) => return err.into_response(),
    };
    if let Err(message) = store::check_reason(&reason) {
        return api_error(StatusCode::BAD_REQUEST, message).into_response();
    }
    let audit = AuditContext {
        actor: &actor,
        action: action.audit_action(),
        campaign_id: Some(campaign_id),
        reason: &reason,
    };
    let change = PhaseChange {
        actor: Some(actor.id),
        expected_generation,
    };
    let changed = match action {
        Action::Pause => store::pause_campaign(&state.pool, campaign_id, &change).await,
        Action::Resume => {
            store::resume_campaign(&state.pool, campaign_id, &change, retry_failed).await
        }
        Action::Abort => store::abort_campaign(&state.pool, campaign_id, &change).await,
    };
    match changed {
        Ok(changed) => {
            audit.record(None, Some(changed.generation), None);
            Json(PhaseResponse {
                id: campaign_id.0,
                phase: changed.phase.as_str(),
                generation: changed.generation,
                retried: changed.retried,
                draining: changed.draining,
            })
            .into_response()
        }
        Err(error) => change_error(&audit, campaign_id, error),
    }
}

#[cfg(test)]
mod tests;
