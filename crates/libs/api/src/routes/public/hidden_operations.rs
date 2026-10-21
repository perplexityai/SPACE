//! Native operations the manifest marks
//! [`IncubatingHidden`](pplx_space_public_api::routes::NativeOperationStatus::IncubatingHidden)
//! answer as if they did not exist unless the caller's org is enabled for them.
//!
//! An org is enabled when `SPACE_NATIVE_HIDDEN_OPERATIONS_ORGS` lists it (or
//! is `*`), or when the `space-native-hidden-operations` flag opens it. The
//! env list is the provider-free override for self-hosted deployments; the
//! flag is for opening one org, or one operation, without a redeploy. The org
//! is the request's effective org, the one that owns the data: for an
//! integration key asserting a federated owner, that owner's org, not the
//! key's.
//!
//! Two places enforce the same marker. A hidden operation that owns its mount
//! gets [`hide_unless_enabled`], layered on that mount outside owner scoping,
//! so it answers as an unmounted route whatever sandbox it names. Lifecycle
//! actions share the `/sandboxes/{sandbox_id}` mount with actions that stay
//! visible, so `sandboxes::parse_action_segment` answers a hidden one as it
//! answers an unknown action, after the same owner scoping.

use std::collections::BTreeSet;

use axum::{
    extract::{Request, State},
    middleware::Next,
    response::{IntoResponse, Response},
};
use pplx_space_common_core::types::OrgId;
use pplx_space_feature_flags::{Attributes, SPACE_NATIVE_HIDDEN_OPERATIONS_FLAG};
use pplx_space_public_api::routes::{self as native, NativeOperation};

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

/// The operation a mount's route-level gate checks: the mount's only
/// operation, when it is hidden.
///
/// # Panics
///
/// On a mount that shares a hidden operation with others, unless it is the
/// sandbox lifecycle-action mount, where `parse_action_segment` checks each
/// action. A hidden operation no check covers would be served to every org,
/// so building the router refuses it.
pub(super) fn route_gate(operations: &[NativeOperation]) -> Option<NativeOperation> {
    let hidden = operations
        .iter()
        .filter(|operation| operation.status().is_hidden_by_default());
    match (hidden.count(), operations) {
        (0, _) => None,
        (_, [operation]) => Some(*operation),
        _ => {
            assert!(
                operations.iter().all(|operation| {
                    operation.runtime_path() == native::START_SANDBOX.runtime_path()
                        && operation.method() == native::START_SANDBOX.method()
                }),
                "hidden operations share a mount no check covers: {operations:?}"
            );
            None
        }
    }
}

pub(super) async fn hide_unless_enabled(
    State((state, operation)): State<(AppState, NativeOperation)>,
    request: Request,
    next: Next,
) -> Response {
    let Some(AuthOrg(org)) = request.extensions().get::<AuthOrg>() else {
        return PublicApiError::internal().into_response();
    };
    if !visible(&state, org.id, operation) {
        return PublicApiError::not_found("route not found").into_response();
    }
    next.run(request).await
}

#[cfg(test)]
mod tests;
