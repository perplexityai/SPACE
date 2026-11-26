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
//!   that generation, compared under the row lock; a stale one answers 412,
//!   unless the phase refuses the change anyway, which answers 409 first;
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
//! Each write is audited by the admin middleware, and each that passes
//! validation also with its own `space.admin.audit` entry, which carries the
//! reason. An invalid request is refused before that entry, as the runtime
//! environment routes refuse theirs.

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
/// campaigns of them: templates. The sandbox drivers add their kinds here as
/// they land, with the controller's `runtime_upgrade::Drivers` registration. A
/// campaign of a kind without a driver would sit in canary forever.
pub const RUNTIME_UPGRADE_DRIVEN_KINDS: &[CampaignKind] = &[CampaignKind::Templates];

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
    /// Pending items whose upgrade edge the catalog doesn't certify: for an
    /// unfinished sandbox campaign, all of them while the catalog certifies no
    /// upgrade edge from its source to its target, read now; 0 for a finished
    /// one. A certified edge doesn't promise a node can make the move: that's
    /// the driver's call. `null` for a kind that doesn't define it yet.
    stranded: Option<u64>,
    /// Why the reconciler can't move this unfinished campaign on this
    /// deployment, whatever its policy or catalog; `null` when nothing here
    /// holds it. Two holds are reported: `twins_not_booted` (the substrate)
    /// and `nodes_dont_read_twins` (its workers). The controller's holds that
    /// depend on the policy or the catalog (a target that isn't the default,
    /// or isn't compiled, say) aren't.
    held: Option<HeldResponse>,
}

/// A hold that no policy change clears, as the reconciler applies it.
#[derive(Debug, Serialize)]
struct HeldResponse {
    /// The reconciler's hold label, as its `runtime_upgrade.campaign.held`
    /// metric reports it.
    reason: &'static str,
    detail: &'static str,
}

/// The `twins_not_booted` hold: a template campaign moves nothing where
/// creates don't boot the twin they resolve to
/// (`crate::controllers::substrate::Substrate::boots_template_twins`).
const TWINS_NOT_BOOTED: HeldResponse = HeldResponse {
    reason: "twins_not_booted",
    detail: "this deployment's creates don't boot a template twin's environment: that needs \
             a substrate whose worker boots a create's required environment (Kubernetes) \
             running with SPACE_ENVIRONMENT_PLACEMENT_GATE=on, which the substrate accepts only \
             once its gap-table row for that mode closes; until then it refuses to start with it",
};

/// The `nodes_dont_read_twins` hold: a template campaign makes no twin until
/// every worker of the substrate reports that its template-manager reads a
/// twin's own metadata
/// (`pplx_space_runtime_upgrade_campaign_store::workers_not_reading_twins`).
const NODES_DONT_READ_TWINS: HeldResponse = HeldResponse {
    reason: "nodes_dont_read_twins",
    detail: "a worker of this deployment's substrate doesn't report that its template-manager \
             reads a template twin's own metadata.json (node-ctl's reads_twin_metadata), or its \
             node status isn't current, or no worker is registered; the cluster controller's \
             log names up to five of the holding workers",
};

/// Whether the substrate's workers hold template campaigns
/// (`NODES_DONT_READ_TWINS`), read only when one of `records` is an
/// unfinished template campaign that nothing else holds here.
async fn workers_hold_templates<'a>(
    state: &AppState,
    mut records: impl Iterator<Item = &'a CampaignRecord>,
) -> Result<bool, Box<Response>> {
    let boots_template_twins = state.sandbox_controller.boots_template_twins();
    if !boots_template_twins
        || !records.any(|record| {
            record.campaign.kind == CampaignKind::Templates && !record.campaign.phase.finished()
        })
    {
        return Ok(false);
    }
    store::workers_not_reading_twins(&state.pool, state.sandbox_controller.managed_by())
        .await
        .map(|readers| readers.holds())
        .map_err(|error| Box::new(super::support::read_db_error(error)))
}

/// Why a read failed, as a response.
pub(super) fn campaign_read_error(error: ReadCampaignError) -> Response {
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

/// The upgrade edge whose certification decides `record`'s stranded count,
/// or `None` when nothing is stranded whatever the catalog says: a template
/// campaign (stranded isn't defined for it), nothing pending, or finished.
fn edge_to_check(
    record: &CampaignRecord,
    counts: &ItemCounts,
) -> Option<(EnvironmentReference, EnvironmentReference)> {
    let campaign = &record.campaign;
    (matches!(
        campaign.kind,
        CampaignKind::Sandboxes
            | CampaignKind::SandboxForceRestarts
            | CampaignKind::SandboxMemoryDrops
    ) && counts.pending > 0
        && !campaign.phase.finished())
    .then_some((campaign.from, campaign.to))
}

/// Read whether the catalog certifies each campaign's edge, in one query:
/// `Some(answer)` for a campaign [`edge_to_check`] names one for. The error
/// is the response to give, boxed because a `Response` is large.
async fn edges_certified(
    pool: &sqlx::PgPool,
    campaigns: &[(CampaignRecord, ItemCounts)],
) -> Result<Vec<Option<bool>>, Box<Response>> {
    let edges: Vec<_> = campaigns
        .iter()
        .map(|(record, counts)| edge_to_check(record, counts))
        .collect();
    let to_read: Vec<_> = edges.iter().flatten().copied().collect();
    let mut answers =
        pplx_space_runtime_environment_catalog_store::upgrade_edges_certified(pool, &to_read)
            .await
            .map_err(|err| Box::new(read_db_error(err)))?
            .into_iter();
    Ok(edges
        .iter()
        .map(|edge| edge.and_then(|_| answers.next()))
        .collect())
}

/// `record` with its item `counts`, its stranded count and its hold.
/// `edge_certified` is the catalog's answer for [`edge_to_check`]'s edge,
/// `None` when it names none. `boots_template_twins`: whether this
/// deployment's creates boot the twin they resolve to. `workers_hold`:
/// whether its workers hold template campaigns ([`workers_hold_templates`]).
fn campaign_response(
    record: CampaignRecord,
    counts: ItemCounts,
    edge_certified: Option<bool>,
    boots_template_twins: bool,
    workers_hold: bool,
) -> CampaignResponse {
    let campaign = &record.campaign;
    let held = if campaign.kind != CampaignKind::Templates || campaign.phase.finished() {
        None
    } else if !boots_template_twins {
        Some(TWINS_NOT_BOOTED)
    } else if workers_hold {
        Some(NODES_DONT_READ_TWINS)
    } else {
        None
    };
    let stranded = match (campaign.kind, edge_certified) {
        (CampaignKind::Templates, _) => None,
        (
            CampaignKind::Sandboxes
            | CampaignKind::SandboxForceRestarts
            | CampaignKind::SandboxMemoryDrops,
            Some(false),
        ) => Some(counts.pending),
        // Certified, or nothing pending, or finished: nothing stranded.
        (
            CampaignKind::Sandboxes
            | CampaignKind::SandboxForceRestarts
            | CampaignKind::SandboxMemoryDrops,
            _,
        ) => Some(0),
    };
    CampaignResponse {
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
        held,
    }
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

/// `GET /runtime-upgrade-campaigns`: campaigns in id order (creation order to
/// the millisecond), a page at a time.
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
    let records: Vec<_> = records
        .into_iter()
        .map(|record| {
            let record_counts = counts.remove(&record.campaign.id).unwrap_or_default();
            (record, record_counts)
        })
        .collect();
    let certified = match edges_certified(&state.pool, &records).await {
        Ok(certified) => certified,
        Err(response) => return *response,
    };
    let workers_hold =
        match workers_hold_templates(&state, records.iter().map(|(record, _)| record)).await {
            Ok(hold) => hold,
            Err(response) => return *response,
        };
    let campaigns = records
        .into_iter()
        .zip(certified)
        .map(|((record, counts), certified)| {
            campaign_response(
                record,
                counts,
                certified,
                state.sandbox_controller.boots_template_twins(),
                workers_hold,
            )
        })
        .collect();
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
    let campaign = [(record, counts)];
    let certified = match edges_certified(&state.pool, &campaign).await {
        Ok(certified) => certified.into_iter().next().flatten(),
        Err(response) => return *response,
    };
    let workers_hold =
        match workers_hold_templates(&state, campaign.iter().map(|(record, _)| record)).await {
            Ok(hold) => hold,
            Err(response) => return *response,
        };
    let [(record, counts)] = campaign;
    Json(campaign_response(
        record,
        counts,
        certified,
        state.sandbox_controller.boots_template_twins(),
        workers_hold,
    ))
    .into_response()
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
    #[serde(default = "one")]
    canary_size: u64,
    /// Items in flight at once across the cluster: the wave size.
    max_in_flight_per_cluster: u64,
    /// Items in flight at once on any one node, for kinds that pace per node
    /// (`CampaignKind::paces_per_node`). Defaults to 1.
    #[serde(default = "one")]
    max_in_flight_per_node: u64,
    failure_budget: u64,
    #[serde(default)]
    include_running: bool,
    reason: String,
}

const fn one() -> u64 {
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
        max_in_flight_per_node: storable("max_in_flight_per_node", request.max_in_flight_per_node)?,
        max_in_flight_per_cluster: storable(
            "max_in_flight_per_cluster",
            request.max_in_flight_per_cluster,
        )?,
        failure_budget: storable("failure_budget", request.failure_budget)?,
        include_running: request.include_running,
        reason: request.reason,
        created_by: Some(actor.id),
    })
}

/// `value`, refused when the campaign's `BIGINT` column can't hold it.
fn storable(field: &str, value: u64) -> Result<u64, ApiError> {
    if i64::try_from(value).is_ok() {
        Ok(value)
    } else {
        Err(api_error(
            StatusCode::BAD_REQUEST,
            format!("{field} must be at most {}", i64::MAX),
        ))
    }
}

/// What a write's audit entry needs besides its outcome.
struct AuditContext<'a> {
    actor: &'a User,
    action: &'static str,
    campaign_id: Option<CampaignId>,
    reason: &'a str,
}

impl AuditContext<'_> {
    fn record(&self, generation: Option<u64>, error: Option<&str>) {
        let campaign_id = self.campaign_id.map(|id| id.to_string());
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
        self.record(None, Some(&message));
        api_error(status, message).into_response()
    }

    /// Audit a write that failed rather than being refused, and give its
    /// response.
    fn fail(&self, what: &str, response: Response) -> Response {
        self.record(None, Some(what));
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
    // Before the driver check, so an invalid start answers 400 whichever
    // kinds this build drives.
    if let Err(message) = store::validate(&campaign) {
        return api_error(StatusCode::BAD_REQUEST, message).into_response();
    }
    let mut audit = AuditContext {
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
            audit.campaign_id = Some(campaign_id);
            audit.record(Some(0), None);
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
                "campaign {existing} is already moving {} off {}; let it complete or abort it \
                 first",
                campaign.kind, campaign.from
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
    let action = [Action::Pause, Action::Resume, Action::Abort]
        .into_iter()
        .find(|candidate| candidate.verb() == action)
        .ok_or_else(|| {
            api_error(
                StatusCode::NOT_FOUND,
                format!("unknown campaign action {action:?}; expected pause, resume or abort"),
            )
        })?;
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
        | ChangePhaseError::NotResumable(_)) => {
            audit.refuse(StatusCode::CONFLICT, error.to_string())
        }
        error @ ChangePhaseError::AtFailureBudget { .. } => audit.refuse(
            StatusCode::CONFLICT,
            format!("{error} with retry_failed: true, which returns them to pending"),
        ),
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
            audit.record(Some(changed.generation), None);
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
