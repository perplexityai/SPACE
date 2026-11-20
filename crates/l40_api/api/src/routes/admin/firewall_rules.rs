//! `/firewall-rules` and `/firewall-rollouts`: read the adopted firewall
//! revision, and start a rollout of a reviewed candidate against it.
//!
//! `pplx_space_firewall_rules_store` compares the precondition under its lock
//! and owns every write. A started rollout adopts nothing.

use axum::{
    Extension, Json,
    body::Bytes,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use pplx_space_firewall_rules_model::RuleChange;
use pplx_space_firewall_rules_protocol::{decode_revision, encode_revision};
use pplx_space_firewall_rules_store::{
    self as store, AdoptedRevision, Review, RevisionId, StartError, StartRollout, StartedRollout,
    StoredStateError,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, value::RawValue};

use super::support::{
    api_error, db_error, parse_json_body, read_db_error, require_managing_org_admin,
};
use crate::middleware::auth::AuthUser;
use crate::routes::AppState;

#[derive(Debug, Serialize)]
struct AdoptedResponse {
    adopted_revision_id: Option<uuid::Uuid>,
    /// The canonical document.
    revision: Option<Box<RawValue>>,
}

/// `GET /firewall-rules`: the adopted revision, or nulls when none is.
pub(super) async fn get_adopted(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
) -> Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let adopted = match store::read_adopted(&state.pool).await {
        Ok(adopted) => adopted,
        Err(error) => return stored_state_error(error, read_db_error),
    };
    let (adopted_revision_id, revision) = match adopted {
        None => (None, None),
        Some((RevisionId(id), revision)) => {
            let document = RawValue::from_string(encode_revision(&revision));
            match document {
                Ok(document) => (Some(id), Some(document)),
                Err(error) => return internal(&error),
            }
        }
    };
    Json(AdoptedResponse {
        adopted_revision_id,
        revision,
    })
    .into_response()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StartRequest {
    /// Required though nullable: `null` expects that nothing is adopted.
    #[serde(deserialize_with = "Option::deserialize")]
    expected_adopted_revision_id: Option<uuid::Uuid>,
    reason: String,
    /// Raw, so the strict decoder sees any duplicate key.
    revision: Box<RawValue>,
}

#[derive(Debug, Serialize)]
struct StartedResponse {
    revision_id: uuid::Uuid,
    rollout_id: uuid::Uuid,
    generation: u64,
    review: ReviewResponse,
}

/// The change by rule ID: the caller holds both documents.
#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ReviewResponse {
    FirstPush,
    Against {
        base_revision_id: uuid::Uuid,
        changes: Vec<ChangeResponse>,
        reordered: bool,
    },
}

#[derive(Debug, Serialize)]
struct ChangeResponse {
    change: &'static str,
    rule_id: String,
}

fn started_response(started: StartedRollout) -> StartedResponse {
    let review = match started.review {
        Review::FirstPush => ReviewResponse::FirstPush,
        Review::Against { base, diff } => ReviewResponse::Against {
            base_revision_id: base.0,
            changes: diff
                .changes()
                .iter()
                .map(|change| {
                    let (change, rule) = match change {
                        RuleChange::Added(rule) => ("added", rule),
                        RuleChange::Replaced { after, .. } => ("replaced", after),
                        RuleChange::Removed(rule) => ("removed", rule),
                    };
                    ChangeResponse {
                        change,
                        rule_id: rule.id().to_string(),
                    }
                })
                .collect(),
            reordered: diff.reordered(),
        },
    };
    StartedResponse {
        revision_id: started.revision_id.0,
        rollout_id: started.rollout_id.0,
        generation: started.generation,
        review,
    }
}

/// `POST /firewall-rollouts`: start rolling out a complete candidate if the
/// adopted revision is still the one it was reviewed against.
pub(super) async fn start_rollout(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    body: Bytes,
) -> Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let request: StartRequest = match parse_json_body(&body, "firewall rollout") {
        Ok(request) => request,
        Err(err) => return err.into_response(),
    };
    let revision = match decode_revision(request.revision.get()) {
        Ok(revision) => revision,
        Err(error) => return start_error(StartError::Invalid(error)),
    };
    let start = StartRollout {
        actor: actor.id,
        reason: request.reason,
        revision,
        expected_adopted: request.expected_adopted_revision_id.map(RevisionId).into(),
    };
    let mut conn = match state.pool.acquire().await {
        Ok(conn) => conn,
        Err(err) => return read_db_error(err),
    };
    match store::start_rollout(&mut conn, &start).await {
        Ok(started) => (StatusCode::CREATED, Json(started_response(started))).into_response(),
        Err(error) => start_error(error),
    }
}

fn start_error(error: StartError) -> Response {
    match error {
        error @ (StartError::Invalid(_) | StartError::InvalidReason) => {
            api_error(StatusCode::BAD_REQUEST, error.to_string()).into_response()
        }
        error @ StartError::Stale { adopted } => {
            let adopted_revision_id = match adopted {
                AdoptedRevision::NoRevision => None,
                AdoptedRevision::Revision(RevisionId(id)) => Some(id),
            };
            let body = json!({
                "error": format!("{error}; read GET /firewall-rules and review the candidate again"),
                "adopted_revision_id": adopted_revision_id,
            });
            (StatusCode::CONFLICT, Json(body)).into_response()
        }
        error @ StartError::RolloutInProgress(id) => {
            let body = json!({ "error": error.to_string(), "rollout_id": id.0 });
            (StatusCode::CONFLICT, Json(body)).into_response()
        }
        error @ (StartError::SlotTaken | StartError::Unqualified(_) | StartError::NotEnabled) => {
            api_error(StatusCode::CONFLICT, error.to_string()).into_response()
        }
        StartError::Stored(error) => stored_state_error(error, |err| db_error(err).into_response()),
    }
}

/// A database error through `db`; any other fault without detail.
fn stored_state_error(
    error: StoredStateError,
    db: impl FnOnce(sqlx::Error) -> Response,
) -> Response {
    match error {
        StoredStateError::Database(err) => db(err),
        error => internal(&error),
    }
}

/// A fault, logged and answered without detail.
fn internal(error: &dyn std::error::Error) -> Response {
    tracing::error!(error = %error, "firewall rules admin route fault");
    api_error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error").into_response()
}

#[cfg(test)]
mod tests;
