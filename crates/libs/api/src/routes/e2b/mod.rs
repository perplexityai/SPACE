use axum::{
    Extension, Json,
    extract::{Path, Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use base64::Engine;
use pplx_space_common_svc::ecw_credential::EcwCredentialKind;
use pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity;
use serde_json::Value;
use time::{Duration, OffsetDateTime, format_description::well_known::Rfc3339};

use super::AppState;
use crate::{
    controllers::sandbox::{
        AutoLifecycleTouchOutcome, FederatedSandboxCreateContext, SandboxAccessScope,
        SandboxPageCursor, lifecycle::SandboxAction,
    },
    e2b::{
        errors::E2bApiError,
        schema::{
            ConnectSandboxRequestE2b, CreateSandboxRequestE2b, ListedSandboxE2b,
            ListedSandboxesResponseE2b, SandboxDetailE2b, SandboxListQueryE2b,
            SandboxPauseRequestE2b, SandboxRefreshRequestE2b, SandboxResponseE2b,
            SandboxResumeRequestE2b, SandboxStateE2b, SandboxTimeoutRequestE2b, StringMap,
            e2b_timestamp,
        },
    },
    middleware::auth::AuthOrg,
    models::{
        CreateSandboxRequest, Organization, Sandbox, SandboxMetadata, SandboxMetadataValue,
        SandboxResponse,
    },
};

pub(crate) mod auth;
mod constants;
mod fork;
mod hosts;
mod lifecycle;
mod router;
mod template_build;
mod translate;
mod types;

use auth::*;
use constants::*;
use fork::*;
pub(crate) use hosts::EcwHostConfig;
use hosts::*;
use lifecycle::*;
pub(crate) use router::{dedicated_host_router, path_root_router};
#[cfg(test)]
use router::{
    dedicated_host_router_with_ecw_hosts, e2b_api_routes, path_root_router_with_ecw_hosts,
};
pub use template_build::TemplateBuildState;
#[cfg(test)]
use template_build::{
    file_upload_capability_org, file_upload_signing_secret, mint_file_upload_query,
};
pub(crate) use translate::translate_sandbox_state;
use translate::*;
use types::*;

#[cfg(test)]
mod tests;
