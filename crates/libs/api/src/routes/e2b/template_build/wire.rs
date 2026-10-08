//! Wire types matching the e2b Python/JS SDK's models verbatim.
//!
//! Field names and casing are the SDK's contract — see
//! `e2b/api/client/models/template_*.py` in the SDK. Anything we serialize
//! has to round-trip through the SDK's `from_dict`, so the camelCase keys
//! (`templateID`, `buildID`, `cpuCount`, `memoryMB`, `filesHash`, `logEntries`)
//! are not negotiable.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// POST /v3/templates
// ---------------------------------------------------------------------------

/// SDK source: `TemplateBuildRequestV3`. The SDK omits unset fields, so we
/// receive `Option<T>` everywhere.
///
/// `cpuCount` and `memoryMB` are caller-driven sizing, propagated into
/// the build VM's `BuildConfig` at start_build time.
/// Disk is not SDK-controlled; build-manager applies its build-VM default.
///
#[derive(Debug, Default, Deserialize)]
pub struct TemplateBuildRequestV3 {
    /// Deprecated upstream in favor of `name`. The SDK still accepts it.
    #[serde(default)]
    pub alias: Option<String>,
    #[serde(rename = "cpuCount", default)]
    pub cpu_count: Option<u32>,
    #[serde(rename = "memoryMB", default)]
    pub memory_mb: Option<u32>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    /// Deprecated E2B team selector. SPACE rejects a supplied value because
    /// template ownership is derived from the authenticated organization.
    #[serde(rename = "teamID", default)]
    pub team_id: Option<String>,
    /// Minimum free disk requested for the build VM. Zero has no additional
    /// sizing requirement; positive values are rejected until SPACE can carry
    /// the constraint through its persisted two-request build flow.
    #[serde(rename = "minFreeDiskMb", default)]
    pub min_free_disk_mb: Option<u32>,
}

/// SDK source: `TemplateRequestResponseV3`.
#[derive(Debug, Serialize)]
pub struct TemplateRequestResponseV3 {
    pub aliases: Vec<String>,
    #[serde(rename = "buildID")]
    pub build_id: String,
    pub names: Vec<String>,
    pub public: bool,
    pub tags: Vec<String>,
    #[serde(rename = "templateID")]
    pub template_id: String,
}

// ---------------------------------------------------------------------------
// GET /templates/aliases/{alias}
// ---------------------------------------------------------------------------

/// SDK source: `TemplateAliasResponse`.
#[derive(Debug, Serialize)]
pub struct TemplateAliasResponse {
    #[serde(rename = "templateID")]
    pub template_id: String,
    pub public: bool,
}

// ---------------------------------------------------------------------------
// GET /templates/{templateID}/files/{hash}
// ---------------------------------------------------------------------------

/// SDK source: `TemplateBuildFileUpload`. If `present` is true the SDK
/// skips the upload; we always return `present: false` for now (no cache
/// dedup across builds yet).
#[derive(Debug, Serialize)]
pub struct TemplateBuildFileUpload {
    pub present: bool,
    /// Relative URL the SDK will PUT the tar bytes to. The SDK joins it
    /// against its configured base URL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

// ---------------------------------------------------------------------------
// POST /v2/templates/{templateID}/builds/{buildID}
// ---------------------------------------------------------------------------

/// SDK source: `TemplateBuildStartV2`.
///
/// `readyCmd` only matters for E2B's warm-snapshot gate. It is intentionally
/// left in the open-object remainder: SPACE accepts it for SDK compatibility
/// but does not enforce it because SPACE has no snapshot publish path.
#[derive(Debug, Default, Deserialize)]
pub struct TemplateBuildStartV2 {
    #[serde(default)]
    pub force: bool,
    /// Public OCI image base.
    #[serde(rename = "fromImage", default)]
    pub from_image: Option<String>,
    /// Authenticated registries are not supported yet. This is modeled so
    /// callers receive an explicit error instead of losing credentials.
    #[serde(rename = "fromImageRegistry", default)]
    pub from_image_registry: Option<serde_json::Value>,
    #[serde(rename = "fromTemplate", default)]
    pub from_template: Option<String>,
    /// Translated into native template boot intent with template-only env.
    #[serde(rename = "startCmd", default)]
    pub start_cmd: Option<String>,
    #[serde(rename = "readyCmd", default)]
    pub ready_cmd: Option<String>,
    #[serde(default)]
    pub steps: Vec<TemplateStep>,
}

/// SDK source: `TemplateStep`. `type` is the only required field; the
/// remaining fields are optional per the SDK's openapi spec.
///
/// SPACE promotes per-step `force=true` to its build-wide cache switch. This
/// may rebuild more layers than E2B, but never reuses a layer the caller asked
/// to invalidate.
/// SDK-only upload hints such as `forceUpload`, `gzip`, and `resolveSymlinks`
/// remain in the open-object remainder; uploads already bypass an ECW cache.
#[derive(Debug, Default, Deserialize)]
pub struct TemplateStep {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub args: Vec<String>,
    /// Present on `COPY` steps; resolves to a previously-uploaded blob.
    #[serde(rename = "filesHash", default)]
    pub files_hash: Option<String>,
    #[serde(default)]
    pub force: bool,
}

impl TemplateBuildStartV2 {
    pub(super) fn force_build(&self) -> bool {
        self.force || self.steps.iter().any(|step| step.force)
    }
}

// ---------------------------------------------------------------------------
// GET /templates/{templateID}/builds/{buildID}/status
// ---------------------------------------------------------------------------

/// SDK source: `TemplateBuildInfo`. `status` is one of the four
/// `TemplateBuildStatus` enum values: `building` / `waiting` / `ready` /
/// `error`.
///
/// `logEntries` and `logs` are required-by-shape; the SDK calls
/// `len(build_status.log_entries)` to advance its `logsOffset`, so the
/// field must exist. We don't capture build-mgr logs through this surface
/// yet — both arrays serialize as empty. When log streaming arrives,
/// promote them to a typed entry.
#[derive(Debug, Serialize)]
pub struct TemplateBuildInfo {
    #[serde(rename = "buildID")]
    pub build_id: String,
    #[serde(rename = "logEntries")]
    pub log_entries: Vec<serde_json::Value>,
    pub logs: Vec<String>,
    pub status: &'static str,
    #[serde(rename = "templateID")]
    pub template_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<BuildStatusReason>,
}

/// SDK source: `BuildStatusReason`. We populate `message` from buildmgr's
/// error and leave `step`/`logEntries` empty for now — buildmgr doesn't
/// surface a step-level error attribution through `GetBuildStatusResponse`.
#[derive(Debug, Serialize)]
pub struct BuildStatusReason {
    pub message: String,
}
