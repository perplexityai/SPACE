//! E2B-compatible template-build endpoints.
//!
//! These mirror the five API calls the public e2b Python/JS SDK makes:
//!
//! 1. `POST /v3/templates`                              — synth templateID + buildID
//! 2. `GET  /templates/aliases/{alias}`                 — check the visible template registry
//! 3. `GET  /templates/{templateID}/files/{filesHash}`  — hand caller a URL to PUT a tar to
//!    The returned upload URL points to the internal
//!    `PUT /e2b/api/v1/internal/e2b-files/{templateID}/{filesHash}` callback,
//!    which receives the tar and remembers `(orgID, templateID, filesHash) → blob_sha256`.
//! 4. `POST /v2/templates/{templateID}/builds/{buildID}` — translates the e2b
//!    instruction list into a canonical `build.manager.v1.Recipe` and submits
//!    it through the native template controller
//! 5. `GET  /templates/{templateID}/builds/{buildID}/status` — polled by the
//!    SDK; reports `building`/`waiting`/`ready`/`error` and maps build-manager
//!    state through
//!
//! Why these endpoints sit here:
//!
//! The e2b SDK has no concept of "SPACE templates" — it speaks the e2b API,
//! and we want the existing `Template.build()` flow to drive a SPACE build
//! without any SDK changes. That means we have to:
//!
//! * stand up the e2b-shaped HTTP surface, AND
//! * translate the e2b instruction list (RUN/ENV/WORKDIR/USER/COPY +
//!   startCmd/readyCmd + fromTemplate) into a SPACE-native Recipe step list
//!   before handing it to the native template controller.
//!
//! Caveats baked in (all in the module sub-files):
//!
//! * **Snapshot semantics for `startCmd` are not yet implemented.** e2b runs
//!   `startCmd` once at the end of the build, waits for `readyCmd`, then
//!   snapshots the VM so every sandbox restore wakes up with the start
//!   process already running. SPACE has no warm-snapshot publish path yet;
//!   we approximate by writing a systemd unit at build time, which means
//!   every sandbox cold-boots through systemd. Functionally correct for
//!   the cgroup-setup-plus-background-daemon case we've exercised, with a
//!   performance gap vs. e2b.
//! * `readyCmd` is dropped (it only matters for the snapshot gate).
//! * **Postgres build + blob state**. `(orgID, templateID, buildID) → daemon_job_id`
//!   and `(orgID, templateID, filesHash) → blob_sha256` both live in Postgres so
//!   in-flight builds survive a space-api restart.
//! * **Base-template name aliasing**: the E2B base `code-interpreter-v1` is
//!   rewritten to `code-runtime` before forwarding to buildmgr.
//! * **Alias lookup accepts plain aliases only.** E2B's `name:tag` selectors
//!   are not yet supported by `GET /templates/aliases/{alias}`. The Harbor
//!   cache gate uses plain aliases; callers that need tag-specific existence
//!   checks must use the SPACE API until tagged lookup is added.
//! * **No e2b `namespace/alias` support.** Vendored e2b accepts a
//!   `team/template-name` form and resolves the namespace against the
//!   caller's team; we reject it with a clear error because SPACE has
//!   no team scoping in this MVP. Pass a flat alias instead. Add team
//!   support before re-enabling.

mod handlers;
mod state;
mod translate;
mod upload;
mod wire;

use crate::e2b::errors::E2bApiError;
use crate::template_build_jobs_store::TemplateBuildJobsStoreError;

/// Map a template-build store error onto the ECW surface.
///
/// Two things the bare `E2bApiError::internal(format!("{what}: {err}"))` these
/// call sites used got wrong.
///
/// A contention abort is not an internal failure: the build-spec and blob
/// statements are `INSERT ... ON CONFLICT` upserts on
/// `(template_id, build_id)` / `(template_id, files_hash)`, which block on a
/// concurrent unfinished insert of the same key, so the SDK's own retry of a
/// build submission can lose the race and the identical call then succeeds.
/// 503 with the SDK-visible retry hint says that; a 500 says the opposite.
///
/// Storage error details never reach the client. `E2bApiError`'s body carries
/// `message` verbatim at every status (`impl IntoResponse for E2bApiError`),
/// and `TemplateBuildJobsStoreError::Storage` renders the underlying
/// `sqlx::Error` — SQLSTATE, constraint name, and the failing statement's
/// detail line included. The log keeps all of it.
fn template_build_store_error(
    what: &'static str,
    error: TemplateBuildJobsStoreError,
) -> E2bApiError {
    if matches!(error, TemplateBuildJobsStoreError::OwnershipConflict) {
        return E2bApiError::new(axum::http::StatusCode::CONFLICT, error.to_string());
    }
    match error.as_sql() {
        Some(sql) => template_build_sql_error(what, sql),
        // A stored job row whose `daemon_job_id` will not parse. Not
        // contention and not retryable, but still not the client's to read.
        None => {
            tracing::error!(what, error = %error, "template build store failed");
            E2bApiError::internal("Internal server error")
        }
    }
}

/// [`template_build_store_error`] for the statements whose helper returns a
/// bare `sqlx::Error`.
fn template_build_sql_error(what: &'static str, error: &sqlx::Error) -> E2bApiError {
    if pplx_space_common_svc::sql::contention::is_lock_contention(error) {
        tracing::warn!(
            what,
            error = %error,
            "template build statement lost a lock race; retryable"
        );
        return E2bApiError::lock_contention();
    }
    tracing::error!(what, error = %error, "template build store failed");
    E2bApiError::internal("Internal server error")
}

#[cfg(test)]
pub(super) use handlers::BuildStatusQuery;
pub use handlers::{
    check_alias, create_template_v3, get_build_status, get_file_upload, start_build,
};
pub use state::TemplateBuildState;
pub(super) use upload::file_upload_capability_org;
pub use upload::put_uploaded_file;
#[cfg(test)]
pub(super) use upload::{file_upload_signing_secret, mint_file_upload_query};
