//! Native operations the manifest marks
//! [`IncubatingHidden`](pplx_space_public_api::routes::NativeOperationStatus::IncubatingHidden)
//! answer as if they did not exist unless the caller's org is enabled for them.
//!
//! An org is enabled when `SPACE_NATIVE_HIDDEN_OPERATIONS_ORGS` lists it (or
//! is `*`), or when the `space-native-hidden-operations` flag opens it while
//! its env master, `SPACE_NATIVE_HIDDEN_OPERATIONS_FLAG_ENABLED`, is on (the
//! default). The env list is the provider-free override for self-hosted
//! deployments; the flag is for opening one org, or one operation, without a
//! redeploy, and turning its master off closes every org the flag opened. The
//! org is the request's effective org, the one that owns the data: for an
//! integration key asserting a federated owner, the org SPACE materialized for
//! that owner, not the key's org and not the upstream org id.
//!
//! Two places enforce the same marker. A hidden operation that owns its mount
//! gets [`hide_unless_enabled`], wrapped around that mount's whole method
//! router outside owner scoping, so it answers as an unmounted route whatever
//! method or sandbox it names. Lifecycle actions share the
//! `/sandboxes/{sandbox_id}` mount with actions that stay visible, so
//! `sandboxes::parse_action_segment` answers a hidden one as it answers an
//! unknown action, after the same owner scoping. [`route_gates`] refuses to
//! build a router where neither covers a hidden operation.

use std::collections::BTreeSet;
use std::env::VarError;

use axum::{
    extract::{Request, State},
    middleware::Next,
    response::{IntoResponse, Response},
};
use pplx_space_common_core::{env::env_flag, types::OrgId};
use pplx_space_feature_flags::{
    Attributes, FeatureFlagClient, SPACE_NATIVE_HIDDEN_OPERATIONS_FLAG,
};
use pplx_space_public_api::{
    SandboxLifecycleAction,
    routes::{self as native, NativeOperation, NativeSurface},
};

use super::error::PublicApiError;
use crate::{middleware::auth::AuthOrg, routes::AppState};

const HIDDEN_OPERATIONS_ORGS_ENV: &str = "SPACE_NATIVE_HIDDEN_OPERATIONS_ORGS";
const HIDDEN_OPERATIONS_FLAG_ENV: &str = "SPACE_NATIVE_HIDDEN_OPERATIONS_FLAG_ENABLED";

/// What the deployment's environment says about hidden operations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HiddenOperationsGate {
    orgs: EnabledOrgs,
    /// The flag's env master. Off, the flag cannot open any org; the env list
    /// still does.
    flag_enabled: bool,
}

/// The orgs `SPACE_NATIVE_HIDDEN_OPERATIONS_ORGS` enables.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
enum EnabledOrgs {
    #[default]
    None,
    All,
    Listed(BTreeSet<OrgId>),
}

impl Default for HiddenOperationsGate {
    fn default() -> Self {
        Self {
            orgs: EnabledOrgs::None,
            flag_enabled: true,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HiddenOperationsConfigError {
    #[error("{0} is not valid Unicode")]
    NotUnicode(&'static str),
    #[error(
        "{HIDDEN_OPERATIONS_ORGS_ENV} must be `*` or comma-separated org ids; {0:?} is not an org id"
    )]
    InvalidOrg(String),
    #[error("{HIDDEN_OPERATIONS_ORGS_ENV}: `*` must stand alone, not in a list of org ids")]
    WildcardInList,
    #[error(
        "{HIDDEN_OPERATIONS_FLAG_ENV} must be empty or an on/off value (such as `off`, `false` or `0`); {0:?} is not"
    )]
    InvalidFlagMaster(String),
}

impl HiddenOperationsGate {
    pub fn from_env() -> Result<Self, HiddenOperationsConfigError> {
        Self::from_vars(
            std::env::var(HIDDEN_OPERATIONS_ORGS_ENV),
            std::env::var(HIDDEN_OPERATIONS_FLAG_ENV),
        )
    }

    /// Every org enabled, for tests that sweep every mounted operation.
    #[cfg(test)]
    pub(crate) fn all_orgs() -> Self {
        Self {
            orgs: EnabledOrgs::All,
            ..Self::default()
        }
    }

    fn from_vars(
        orgs: Result<String, VarError>,
        flag_enabled: Result<String, VarError>,
    ) -> Result<Self, HiddenOperationsConfigError> {
        let value = |name, value: Result<String, VarError>| match value {
            Ok(value) => Ok(value),
            Err(VarError::NotPresent) => Ok(String::new()),
            Err(VarError::NotUnicode(_)) => Err(HiddenOperationsConfigError::NotUnicode(name)),
        };
        Ok(Self {
            orgs: parse_orgs(&value(HIDDEN_OPERATIONS_ORGS_ENV, orgs)?)?,
            flag_enabled: parse_flag_master(&value(HIDDEN_OPERATIONS_FLAG_ENV, flag_enabled)?)?,
        })
    }

    /// Whether `org_id` may use `operation`. Every operation not marked hidden
    /// is always visible. The env list opens an org whatever the flag says;
    /// the flag opens others only while its env master is on, and falls back
    /// closed.
    fn allows(&self, flags: &FeatureFlagClient, org_id: OrgId, operation: NativeOperation) -> bool {
        if !operation.status().is_hidden_by_default() {
            return true;
        }
        let listed = match &self.orgs {
            EnabledOrgs::None => false,
            EnabledOrgs::All => true,
            EnabledOrgs::Listed(orgs) => orgs.contains(&org_id),
        };
        listed
            || flags.gate_enabled_with_attributes(
                self.flag_enabled,
                SPACE_NATIVE_HIDDEN_OPERATIONS_FLAG,
                &org_id.to_string(),
                Attributes::from([(
                    "operation_id".to_owned(),
                    operation.operation_id().to_owned().into(),
                )]),
            )
    }
}

fn parse_orgs(raw: &str) -> Result<EnabledOrgs, HiddenOperationsConfigError> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(EnabledOrgs::None);
    }
    if raw == "*" {
        return Ok(EnabledOrgs::All);
    }
    raw.split(',')
        .map(str::trim)
        .map(|entry| match entry {
            "*" => Err(HiddenOperationsConfigError::WildcardInList),
            entry => entry
                .parse::<OrgId>()
                .map_err(|_| HiddenOperationsConfigError::InvalidOrg(entry.to_owned())),
        })
        .collect::<Result<_, _>>()
        .map(EnabledOrgs::Listed)
}

/// Unset or empty keeps the master on. Otherwise the value must be in the
/// shared on/off vocabulary of [`env_flag`] (`off`, `false`, `0`, `no`,
/// `disabled` and their opposites): this is a kill switch, so a typo fails
/// startup rather than leaving it on.
fn parse_flag_master(raw: &str) -> Result<bool, HiddenOperationsConfigError> {
    if raw.trim().is_empty() {
        return Ok(true);
    }
    // `env_flag` answers its default for a value outside its vocabulary, so a
    // value is in it exactly when the default does not change the answer.
    match (env_flag(Some(raw), true), env_flag(Some(raw), false)) {
        (on, off) if on == off => Ok(on),
        _ => Err(HiddenOperationsConfigError::InvalidFlagMaster(
            raw.to_owned(),
        )),
    }
}

/// Whether `org_id` may use `operation`; see [`HiddenOperationsGate::allows`].
pub(super) fn visible(state: &AppState, org_id: OrgId, operation: NativeOperation) -> bool {
    state
        .native_hidden_operations
        .allows(&state.feature_flags, org_id, operation)
}

/// The route-level gate for each of `v1_router`'s `(path, method)` mounts, in
/// order: the mount's only operation, when it is hidden.
///
/// # Panics
///
/// When a hidden operation would be served to every org because neither check
/// covers it:
///
/// - it is a data-plane operation, which this router does not serve;
/// - it shares its mount with other operations and is not a lifecycle action
///   `parse_action_segment` checks (`:set_timeout`, for one, is answered
///   before that parser runs);
/// - its mount is gated but another method on the same path is mounted too,
///   whose 405 and `Allow` header would still name the hidden method.
pub(super) fn route_gates(mounts: &[&[NativeOperation]]) -> Vec<Option<NativeOperation>> {
    route_gates_marking(mounts, native::NATIVE_V1_OPERATIONS, |operation| {
        operation.status().is_hidden_by_default()
    })
}

/// [`route_gates`] with the hidden operations chosen by `hidden`, so tests can
/// mark operations the manifest does not.
fn route_gates_marking(
    mounts: &[&[NativeOperation]],
    manifest: &[NativeOperation],
    hidden: impl Fn(NativeOperation) -> bool,
) -> Vec<Option<NativeOperation>> {
    let unserved = manifest.iter().find(|operation| {
        hidden(**operation) && operation.surface() != NativeSurface::ControlPlane
    });
    assert!(
        unserved.is_none(),
        "hidden operation {unserved:?} is not served by this router, so nothing gates it"
    );
    let gates = mounts
        .iter()
        .map(|operations| match operations {
            [operation] if hidden(*operation) => Some(*operation),
            _ => {
                let uncovered = operations
                    .iter()
                    .find(|operation| hidden(**operation) && !checked_as_action(**operation));
                assert!(
                    uncovered.is_none(),
                    "hidden operation {uncovered:?} shares a mount no check covers: {operations:?}"
                );
                None
            }
        })
        .collect::<Vec<_>>();
    for gated in gates.iter().flatten() {
        let path = gated.relative_runtime_path();
        let methods = mounts
            .iter()
            .filter(|operations| {
                operations
                    .iter()
                    .any(|operation| operation.relative_runtime_path() == path)
            })
            .count();
        assert_eq!(
            methods, 1,
            "hidden operation {gated:?} shares its path with another method"
        );
    }
    gates
}

/// Whether `parse_action_segment` checks `operation`: it is the operation of
/// a lifecycle action that parser accepts, on the lifecycle-action mount.
fn checked_as_action(operation: NativeOperation) -> bool {
    operation.runtime_path() == native::START_SANDBOX.runtime_path()
        && operation.method() == native::START_SANDBOX.method()
        && operation
            .path()
            .rsplit_once(':')
            .and_then(|(_, action)| SandboxLifecycleAction::parse_known(action).ok())
            .is_some_and(|action| {
                action != SandboxLifecycleAction::Delete
                    && super::sandboxes::lifecycle_operation(action) == operation
            })
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
