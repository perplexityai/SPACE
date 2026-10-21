//! Native operations the manifest marks
//! [`IncubatingHidden`](pplx_space_public_api::routes::NativeOperationStatus::IncubatingHidden)
//! answer as unmounted routes unless the caller's org is enabled for them.
//!
//! An org is enabled when `SPACE_NATIVE_HIDDEN_OPERATIONS_ORGS` lists it (or
//! is `*`), or when the `space-native-hidden-operations` flag opens it. The
//! env list is the provider-free override for self-hosted deployments; the
//! flag is for opening one org, or one operation, without a redeploy.
//!
//! Two places enforce the same marker. [`hide_unless_enabled`] covers
//! operations that own their route, and runs before owner scoping so a hidden
//! route answers exactly as an unmounted one does, whatever sandbox it names.
//! Lifecycle actions share the `/sandboxes/{sandbox_id}` mount with actions
//! that stay visible, so `sandboxes::parse_action_segment` checks those.

use std::collections::BTreeSet;

use axum::{
    extract::{MatchedPath, Request, State},
    http::Method,
    middleware::Next,
    response::{IntoResponse, Response},
};
use pplx_space_common_core::types::OrgId;
use pplx_space_feature_flags::{Attributes, SPACE_NATIVE_HIDDEN_OPERATIONS_FLAG};
use pplx_space_public_api::routes::{NATIVE_V1_OPERATIONS, NativeHttpMethod, NativeOperation};

use super::error::PublicApiError;
use crate::{middleware::auth::AuthOrg, routes::AppState};

const HIDDEN_OPERATIONS_ORGS_ENV: &str = "SPACE_NATIVE_HIDDEN_OPERATIONS_ORGS";

/// Which orgs the deployment's environment enables for hidden operations.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum HiddenOperationsGate {
    /// Only the flag can enable an org.
    #[default]
    FlagOnly,
    /// Every org is enabled.
    AllOrgs,
    /// These orgs are enabled; the flag can enable others.
    Orgs(BTreeSet<OrgId>),
}

#[derive(Debug, thiserror::Error)]
#[error("{HIDDEN_OPERATIONS_ORGS_ENV} must be `*` or comma-separated org ids; {entry:?} is not")]
pub struct HiddenOperationsConfigError {
    entry: String,
}

impl HiddenOperationsGate {
    pub fn from_env() -> Result<Self, HiddenOperationsConfigError> {
        Self::parse(std::env::var(HIDDEN_OPERATIONS_ORGS_ENV).ok().as_deref())
    }

    fn parse(raw: Option<&str>) -> Result<Self, HiddenOperationsConfigError> {
        let raw = raw.unwrap_or_default().trim();
        if raw.is_empty() {
            return Ok(Self::FlagOnly);
        }
        if raw == "*" {
            return Ok(Self::AllOrgs);
        }
        raw.split(',')
            .map(str::trim)
            .map(|entry| {
                entry
                    .parse::<OrgId>()
                    .map_err(|_| HiddenOperationsConfigError {
                        entry: entry.to_owned(),
                    })
            })
            .collect::<Result<_, _>>()
            .map(Self::Orgs)
    }
}

/// Whether `org_id` may use `operation`. Every operation not marked hidden is
/// always visible.
pub(super) fn visible(state: &AppState, org_id: OrgId, operation: NativeOperation) -> bool {
    if !operation.status().is_hidden_by_default() {
        return true;
    }
    let listed = match &state.native_hidden_operations {
        HiddenOperationsGate::FlagOnly => false,
        HiddenOperationsGate::AllOrgs => true,
        HiddenOperationsGate::Orgs(orgs) => orgs.contains(&org_id),
    };
    // No env master: the flag only ever opens, and it falls back closed.
    listed
        || state.feature_flags.gate_enabled_with_attributes(
            true,
            SPACE_NATIVE_HIDDEN_OPERATIONS_FLAG,
            &org_id.to_string(),
            Attributes::from([(
                "operation_id".to_owned(),
                operation.operation_id().to_owned().into(),
            )]),
        )
}

/// The answer for a hidden operation, identical to an unmounted route's.
pub(super) fn not_found() -> PublicApiError {
    PublicApiError::not_found("route not found")
}

/// The hidden operation that owns the route `relative_runtime_path`, preferring
/// the one `method` names. `None` when any visible operation shares the route.
fn route_hidden_operation(relative_runtime_path: &str, method: &Method) -> Option<NativeOperation> {
    let mut on_route = NATIVE_V1_OPERATIONS
        .iter()
        .copied()
        .filter(|operation| operation.relative_runtime_path() == relative_runtime_path)
        .peekable();
    let first = *on_route.peek()?;
    let mut by_method = None;
    for operation in on_route {
        if !operation.status().is_hidden_by_default() {
            return None;
        }
        if serves(operation.method(), method) {
            by_method.get_or_insert(operation);
        }
    }
    Some(by_method.unwrap_or(first))
}

/// The methods [`super::method_filter`] mounts `native` on.
fn serves(native: NativeHttpMethod, method: &Method) -> bool {
    match native {
        NativeHttpMethod::Delete => method == Method::DELETE,
        NativeHttpMethod::Get => method == Method::GET || method == Method::HEAD,
        NativeHttpMethod::Patch => method == Method::PATCH,
        NativeHttpMethod::Post => method == Method::POST,
        NativeHttpMethod::Put => method == Method::PUT,
    }
}

pub(super) async fn hide_unless_enabled(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let hidden = request
        .extensions()
        .get::<MatchedPath>()
        .map(|matched| {
            let matched = matched.as_str();
            matched
                .strip_prefix(crate::routes::PUBLIC_API_SHARED_ROOT)
                .or_else(|| matched.strip_prefix("/v1"))
                .unwrap_or(matched)
        })
        .and_then(|path| route_hidden_operation(path, request.method()));
    if let Some(operation) = hidden {
        let Some(AuthOrg(org)) = request.extensions().get::<AuthOrg>() else {
            return PublicApiError::internal().into_response();
        };
        if !visible(&state, org.id, operation) {
            return not_found().into_response();
        }
    }
    next.run(request).await
}

#[cfg(test)]
mod tests;
