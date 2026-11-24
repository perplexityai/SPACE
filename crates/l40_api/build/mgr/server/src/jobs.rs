use pplx_space_accounts::JobId;
use pplx_space_build_mgr::metrics::TemplateBuildResult;
use pplx_space_build_mgr::{SystemPublicationGrantId, TemplateOrg, TenantPublicationGrantId};
use pplx_space_build_mgr_client::proto::BuildState;
use pplx_space_build_mgr_store::database::{self, ConnectionGuard, SharedConnection};
use pplx_space_build_mgr_store::jobs::{Job, JobError};
pub use pplx_space_build_mgr_store::{
    BUILD_JOB_KIND, BuildJobResult, BuildTerminalState, ParseBuildTerminalStateError,
    count_other_active_builds_for_subject, fail_expired_running_builds, get_build_result,
};
use serde::{Deserialize, Serialize};

// The job kind remains stable for queue accounting and scheduling. Nest the
// legacy fields so a downgraded worker cannot interpret selection as E0.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "schema", deny_unknown_fields)]
pub enum VersionedBuildJobPayload {
    #[serde(rename = "environment_v1")]
    Environment {
        environment: pplx_space_execution_spec::EnvironmentReference,
        build: BuildJobPayload,
    },
}

pub fn decode_payload(
    job: &Job,
) -> Result<
    (
        BuildJobPayload,
        Option<pplx_space_execution_spec::EnvironmentReference>,
    ),
    JobError,
> {
    if job.kind != BUILD_JOB_KIND {
        return Err(JobError::permanent("unsupported build job kind"));
    }
    // Dispatch on the envelope before decoding its body so malformed or future
    // envelopes cannot fall back to an ordinary build or erase the parse cause.
    let value = job.payload_as::<serde_json::Value>()?;
    if value.get("schema").is_some() {
        let VersionedBuildJobPayload::Environment { environment, build } = job.payload_as()?;
        Ok((build, Some(environment)))
    } else {
        Ok((job.payload_as()?, None))
    }
}

fn db_lock(db: &SharedConnection) -> Result<ConnectionGuard<'_>, database::Error> {
    db.lock().map_err(|error| {
        database::Error::to_sql_conversion(std::io::Error::other(format!(
            "build-manager db mutex poisoned: {error}"
        )))
    })
}

/// Legacy flat jobs retain additive compatibility. New execution-critical
/// semantics belong in a versioned envelope rather than an ignorable flat field.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BuildJobPayload {
    pub recipe_proto: Vec<u8>,
    pub recipe_label: String,
    pub template_id: String,
    pub force: bool,
    pub publish_tags: Vec<String>,
    pub template_org: Option<TemplateOrg>,
    #[serde(default)]
    pub system_publication_grant_id: Option<SystemPublicationGrantId>,
    /// Absent for jobs queued before tenant grants existed; ClusterApi then
    /// refuses their tenant publication.
    #[serde(default)]
    pub tenant_publication_grant_id: Option<TenantPublicationGrantId>,
}

pub fn metric_result(state: BuildTerminalState) -> TemplateBuildResult {
    match state {
        BuildTerminalState::Succeeded => TemplateBuildResult::Success,
        BuildTerminalState::Failed => TemplateBuildResult::Failed,
        BuildTerminalState::PublishFailed => TemplateBuildResult::PublishFailed,
    }
}

pub fn proto_build_state(state: BuildTerminalState) -> BuildState {
    match state {
        BuildTerminalState::Succeeded => BuildState::Succeeded,
        BuildTerminalState::Failed => BuildState::Failed,
        BuildTerminalState::PublishFailed => BuildState::PublishFailed,
    }
}

pub fn store_build_result(
    db: &SharedConnection,
    job_id: &JobId,
    result: &BuildJobResult,
) -> Result<(), database::Error> {
    let conn = db_lock(db)?;
    pplx_space_build_mgr_store::upsert_build_result(&conn, job_id, result)
}

pub fn store_build_result_if_lease_owner(
    db: &SharedConnection,
    job_id: &JobId,
    lease_owner: &str,
    result: &BuildJobResult,
) -> Result<bool, database::Error> {
    let conn = db_lock(db)?;
    pplx_space_build_mgr_store::record_build_result_as_lease_owner(
        &conn,
        job_id,
        lease_owner,
        result,
    )
}

#[cfg(test)]
mod tests;
