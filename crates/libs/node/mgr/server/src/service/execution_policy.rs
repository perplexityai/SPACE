//! The execution policy node-manager enforces: the startup settings, narrowed
//! by the cluster policy node-ctl forwards. The startup settings stay the
//! ceiling, so inventory discovery and artifact publication keep using them;
//! every admission decision reads the effective policy.

use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arc_swap::ArcSwap;
use pplx_space_common_core::types::{
    EXECUTION_POLICY_APPLIED_SCHEMA_VERSION, EXECUTION_POLICY_SCHEMA_VERSION, EnvironmentReference,
    ExecutionPolicyObservation, ExecutionPolicyObservationError,
};
use pplx_space_node_runtime_core::selection::{
    ClusterDefault, ClusterExecutionPolicy, DefaultOutcome, ExecutionPolicy,
};
use tonic::{Request, Response, Status};

use super::NodeManagerService;
use crate::proto::{ApplyExecutionPolicyRequest, ApplyExecutionPolicyResponse};

const ALLOW_POLICY_WRITER: &[pplx_space_common_svc::UdsPeerPrincipal] = &[
    pplx_space_common_svc::UdsPeerPrincipal::Root,
    pplx_space_common_svc::UdsPeerPrincipal::Service(
        pplx_space_common_svc::core::host::linux::identities::user_names::SPACE_NODECTL,
    ),
];
/// Twice the 4 MiB request bound of node-manager's gRPC server (tonic's
/// default): a record is about the size of the request it came from, so any
/// accepted policy fits, and `write_record` refuses one that wouldn't.
const MAX_RECORD_BYTES: u64 = 8 * 1024 * 1024;
/// With `RECORD_LOCK_BUDGET`, well under node-ctl's 10-second deadline for the
/// whole call, leaving room for the durable write. A slower check, such as a
/// first inventory discovery, refuses the default for this application
/// instead of letting a cancelled call delay the rest of the policy.
const USABILITY_BUDGET: std::time::Duration = std::time::Duration::from_secs(3);
/// Waiting for the previous application's record write, so a hung write can't
/// stop a later policy being enforced, only persisted.
const RECORD_LOCK_BUDGET: std::time::Duration = std::time::Duration::from_secs(1);
const RECORD_SCHEMA_VERSION: u32 = 1;

/// The applied cluster policy, as received, so a restart re-narrows it against
/// the startup settings of the new process.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyRecord {
    schema_version: u32,
    cluster: ClusterExecutionPolicy,
    /// The cluster default was installed and verified when last applied. A
    /// restart reuses that verdict rather than discovering inventory before
    /// the service starts; the next application checks it again.
    default_verified: bool,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum RecordError {
    #[error("read execution policy record {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("open execution policy record {path}: {status}")]
    Open { path: PathBuf, status: Status },
    #[error("execution policy record {0} exceeds {MAX_RECORD_BYTES} bytes")]
    TooLarge(PathBuf),
    #[error("decode execution policy record {path}: {source}")]
    Decode {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("unsupported execution policy record schema {0}")]
    Schema(u32),
}

/// The outcome of checking a cluster default on this node.
enum Verification {
    Usable,
    Unusable,
    /// The check timed out or failed in a way that can succeed on retry.
    Undetermined,
}

/// The record file as this process last read or wrote it.
#[derive(Debug, PartialEq, Eq)]
enum OnDisk {
    /// Unreadable; the next application replaces or removes it, whatever it
    /// says.
    Unreadable,
    /// `None`: no record.
    Record(Option<PolicyRecord>),
}

pub(crate) struct ExecutionPolicyState {
    startup: ExecutionPolicy,
    effective: ArcSwap<ExecutionPolicy>,
    record_path: PathBuf,
    /// Serializes applications, and holds what's on disk so an unchanged
    /// policy isn't rewritten on every tick. The record write owns the guard,
    /// so a write that outlives a cancelled call still finishes before the
    /// next application can persist a newer policy.
    persisted: Arc<tokio::sync::Mutex<OnDisk>>,
    /// Applications in arrival order; one enforces only if no later arrival
    /// already has, so a slow call can't swap a superseded policy back in.
    arrivals: std::sync::atomic::AtomicU64,
    enforced_arrival: std::sync::Mutex<u64>,
    /// The cluster default last verified usable, by this process or by the
    /// one that wrote the record. It survives an undetermined check.
    verified_default: std::sync::Mutex<Option<EnvironmentReference>>,
}

impl std::fmt::Debug for ExecutionPolicyState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExecutionPolicyState")
            .field("effective", &self.effective.load())
            .field("record_path", &self.record_path)
            .finish_non_exhaustive()
    }
}

impl ExecutionPolicyState {
    /// Enforce the startup settings, narrowed by the persisted cluster policy
    /// when one is recorded. An unreadable record fails closed: nothing new
    /// executes until node-ctl next applies the cluster policy, so a damaged
    /// file can never lift a cluster block.
    pub(crate) fn load(startup: ExecutionPolicy, record_path: PathBuf) -> Self {
        let record = match read_record(&record_path) {
            Ok(record) => record,
            Err(error) => {
                tracing::error!(%error, "unreadable execution policy record; admitting no new work until the cluster policy is reapplied");
                return Self::new(
                    startup.refuse_all(),
                    startup,
                    record_path,
                    OnDisk::Unreadable,
                );
            }
        };
        // The recorded verdict counts until a check this process completes
        // says otherwise, so a slow first discovery can't flap it.
        let verified = record
            .as_ref()
            .filter(|record| record.default_verified)
            .and_then(|record| record.cluster.default_reference());
        let effective = match &record {
            Some(record) => {
                let narrowed = startup.narrow(&record.cluster, record.default_verified);
                log_applied(
                    &narrowed.policy,
                    narrowed.default,
                    &record.cluster,
                    "restored",
                );
                narrowed.policy
            }
            None => startup.clone(),
        };
        let state = Self::new(effective, startup, record_path, OnDisk::Record(record));
        *state
            .verified_default
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = verified;
        state
    }

    fn new(
        effective: ExecutionPolicy,
        startup: ExecutionPolicy,
        record_path: PathBuf,
        on_disk: OnDisk,
    ) -> Self {
        Self {
            startup,
            effective: ArcSwap::from_pointee(effective),
            record_path,
            persisted: Arc::new(tokio::sync::Mutex::new(on_disk)),
            arrivals: std::sync::atomic::AtomicU64::new(0),
            enforced_arrival: std::sync::Mutex::new(0),
            verified_default: std::sync::Mutex::new(None),
        }
    }

    /// Enforce `policy`, and record its default `verdict` if it reached one,
    /// unless a later arrival already enforced. Returns the previously
    /// enforced policy, or `None` when superseded, which changes neither.
    fn enforce(
        &self,
        arrival: u64,
        policy: Arc<ExecutionPolicy>,
        verdict: Option<Option<EnvironmentReference>>,
    ) -> Option<Arc<ExecutionPolicy>> {
        let mut enforced = self
            .enforced_arrival
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *enforced > arrival {
            return None;
        }
        *enforced = arrival;
        if let Some(verdict) = verdict {
            *self
                .verified_default
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = verdict;
        }
        Some(self.effective.swap(policy))
    }

    /// The policy every admission decision reads. Each check loads it whole,
    /// and the final check of a decision re-loads it after any await, so a
    /// policy applied meanwhile still gates the decision.
    pub(crate) fn effective(&self) -> Arc<ExecutionPolicy> {
        self.effective.load_full()
    }

    /// The node's own settings, the upper bound on what it may execute.
    pub(crate) fn startup(&self) -> &ExecutionPolicy {
        &self.startup
    }
}

pub(crate) fn observation(
    policy: &ExecutionPolicy,
) -> Result<ExecutionPolicyObservation, ExecutionPolicyObservationError> {
    let schema_version =
        if policy.applied_generation().is_some() || policy.cluster_policy_unreadable() {
            EXECUTION_POLICY_APPLIED_SCHEMA_VERSION
        } else {
            EXECUTION_POLICY_SCHEMA_VERSION
        };
    ExecutionPolicyObservation::new(
        schema_version,
        policy.allowed_references().collect(),
        policy.default_reference(),
        policy.legacy_unbound_allowed(),
        policy.applied_generation(),
        policy.cluster_policy_unreadable(),
    )
}

/// Absence is checked by name, then the record is opened beneath its pinned
/// parent without following links, as the write publishes it.
fn read_record(path: &Path) -> Result<Option<PolicyRecord>, RecordError> {
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(RecordError::Read {
                path: path.to_owned(),
                source,
            });
        }
        Ok(_) => {}
    }
    let file = super::artifact_fs::parent(path)
        .and_then(|(directory, name)| super::trusted_paths::open_regular_file(&directory, name))
        .map_err(|status| RecordError::Open {
            path: path.to_owned(),
            status,
        })?;
    let mut bytes = Vec::new();
    file.take(MAX_RECORD_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| RecordError::Read {
            path: path.to_owned(),
            source,
        })?;
    if bytes.len() as u64 > MAX_RECORD_BYTES {
        return Err(RecordError::TooLarge(path.to_owned()));
    }
    let record: PolicyRecord =
        serde_json::from_slice(&bytes).map_err(|source| RecordError::Decode {
            path: path.to_owned(),
            source,
        })?;
    if record.schema_version != RECORD_SCHEMA_VERSION {
        return Err(RecordError::Schema(record.schema_version));
    }
    Ok(Some(record))
}

fn write_record(path: &Path, record: Option<&PolicyRecord>) -> Result<(), Status> {
    match record {
        Some(record) => {
            let bytes = serde_json::to_vec(record)
                .map_err(|error| Status::internal(format!("encode policy record: {error}")))?;
            // Never persist what a restart would refuse to read.
            if bytes.len() as u64 > MAX_RECORD_BYTES {
                return Err(Status::invalid_argument(format!(
                    "execution policy record would exceed {MAX_RECORD_BYTES} bytes"
                )));
            }
            super::artifact_fs::write_atomic(path, |file| {
                use std::io::Write as _;
                (&*file)
                    .write_all(&bytes)
                    .map_err(|error| Status::unavailable(format!("write policy record: {error}")))
            })
            .map(|_| ())
        }
        None => {
            match std::fs::symlink_metadata(path) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(Status::unavailable(format!(
                        "inspect policy record: {error}"
                    )));
                }
                Ok(_) => super::artifact_fs::unlink(path)?,
            }
            // Durable like the write, so a crash can't bring it back. Sync
            // even when it's already gone, in case an earlier sync failed.
            let (parent, _) = match super::artifact_fs::parent(path) {
                Ok(parent) => parent,
                Err(status) if status.code() == tonic::Code::NotFound => return Ok(()),
                Err(status) => return Err(status),
            };
            parent.sync_all().map_err(|error| {
                Status::unavailable(format!("sync policy record directory: {error}"))
            })
        }
    }
}

/// Read the wire policy without refusing it, since a refusal would leave a
/// block unenforced. References this node can't parse or doesn't compile are
/// ignored, an unparsable default is refused as not allowed while the
/// executing set still narrows, and the default always executes.
fn parse_cluster_policy(policy: crate::proto::ClusterExecutionPolicy) -> ClusterExecutionPolicy {
    let mut executing = std::collections::BTreeSet::new();
    let mut unparsable = 0_usize;
    for raw in &policy.executing_environment_references {
        match raw.parse::<EnvironmentReference>() {
            Ok(reference) => {
                executing.insert(reference);
            }
            Err(_) => unparsable += 1,
        }
    }
    if unparsable > 0 {
        tracing::warn!(
            unparsable,
            "ignoring unparsable executing environment references"
        );
    }
    let default = policy.default_environment_reference.as_deref().map(|raw| {
        raw.parse::<EnvironmentReference>()
            .map(ClusterDefault::Environment)
            .unwrap_or_else(|error| {
                tracing::warn!(%error, "refusing an unparsable cluster default execution environment");
                ClusterDefault::Unrecognized
            })
    });
    let mut cluster = ClusterExecutionPolicy {
        generation: policy.generation,
        default,
        executing,
        legacy_unbound_allowed: policy.legacy_unbound_allowed,
    };
    cluster.executing.extend(cluster.default_reference());
    cluster
}

fn outcome_proto(outcome: DefaultOutcome) -> crate::proto::ClusterDefaultOutcome {
    use crate::proto::ClusterDefaultOutcome as Proto;
    match outcome {
        DefaultOutcome::Unset => Proto::Unset,
        DefaultOutcome::Adopted => Proto::Adopted,
        DefaultOutcome::NotAllowed => Proto::NotAllowed,
        DefaultOutcome::Unusable => Proto::Unusable,
    }
}

fn log_applied(
    policy: &ExecutionPolicy,
    outcome: DefaultOutcome,
    cluster: &ClusterExecutionPolicy,
    action: &'static str,
) {
    let executing = policy.allowed_references().collect::<Vec<_>>();
    if outcome.diverges() {
        tracing::warn!(
            action,
            generation = cluster.generation,
            cluster_default = ?cluster.default,
            enforced_default = ?policy.default_reference(),
            %outcome,
            ?executing,
            legacy_unbound_allowed = policy.legacy_unbound_allowed(),
            "refused the cluster default execution environment"
        );
    } else {
        tracing::info!(
            action,
            generation = cluster.generation,
            cluster_default = ?cluster.default,
            enforced_default = ?policy.default_reference(),
            %outcome,
            ?executing,
            legacy_unbound_allowed = policy.legacy_unbound_allowed(),
            "enforcing cluster execution policy"
        );
    }
}

impl NodeManagerService {
    pub(super) fn execution_policy(&self) -> Arc<ExecutionPolicy> {
        self.execution_policy.effective()
    }

    /// Replace the startup settings, keeping the record path, with no cluster
    /// policy applied.
    #[cfg(test)]
    pub(super) fn set_startup_execution_policy_for_test(&mut self, startup: ExecutionPolicy) {
        let path = self.execution_policy.record_path.clone();
        self.execution_policy = Arc::new(ExecutionPolicyState::new(
            startup.clone(),
            startup,
            path,
            OnDisk::Record(None),
        ));
    }

    /// Enforce what an unreadable record leaves: nothing new.
    #[cfg(test)]
    pub(super) fn refuse_all_execution_for_test(&mut self) {
        let startup = self.execution_policy.startup.clone();
        let path = self.execution_policy.record_path.clone();
        self.execution_policy = Arc::new(ExecutionPolicyState::new(
            startup.refuse_all(),
            startup,
            path,
            OnDisk::Unreadable,
        ));
    }

    /// Enforce the startup settings narrowed by `cluster`, its default
    /// verified by this process.
    #[cfg(test)]
    pub(super) fn narrow_execution_for_test(&mut self, cluster: &ClusterExecutionPolicy) {
        let startup = self.execution_policy.startup.clone();
        let path = self.execution_policy.record_path.clone();
        let state = ExecutionPolicyState::new(
            startup.narrow(cluster, true).policy,
            startup,
            path,
            OnDisk::Record(None),
        );
        *state.verified_default.lock().unwrap() = cluster.default_reference();
        self.execution_policy = Arc::new(state);
    }

    pub async fn apply_execution_policy(
        &self,
        request: Request<ApplyExecutionPolicyRequest>,
    ) -> Result<Response<ApplyExecutionPolicyResponse>, Status> {
        self.authorize_peer(&request, "ApplyExecutionPolicy", ALLOW_POLICY_WRITER)?;
        let cluster = request.into_inner().policy.map(parse_cluster_policy);
        let state = &self.execution_policy;
        let arrival = state
            .arrivals
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        // A verdict to record once this application is enforced.
        let mut verdict = None;
        let (policy, outcome, record) = match cluster {
            None => {
                // No policy, so no default: an earlier verdict is stale.
                verdict = Some(None);
                (state.startup.clone(), DefaultOutcome::Unset, None)
            }
            Some(cluster) => {
                let verified = match cluster.default_reference() {
                    Some(reference) if state.startup.allows(reference) => {
                        match self.verify_environment(reference).await {
                            Verification::Usable => {
                                verdict = Some(Some(reference));
                                true
                            }
                            Verification::Unusable => {
                                verdict = Some(None);
                                false
                            }
                            // A slow or retryable check keeps the verdict on
                            // the same default, so an inventory rescan can't
                            // flap it. A verdict on another default is stale.
                            Verification::Undetermined => {
                                let kept = *state
                                    .verified_default
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                                    == Some(reference);
                                if !kept {
                                    verdict = Some(None);
                                }
                                kept
                            }
                        }
                    }
                    // No default to verify: an earlier verdict is stale.
                    _ => {
                        verdict = Some(None);
                        false
                    }
                };
                let narrowed = state.startup.narrow(&cluster, verified);
                let record = PolicyRecord {
                    schema_version: RECORD_SCHEMA_VERSION,
                    cluster,
                    default_verified: verified,
                };
                (narrowed.policy, narrowed.default, Some(record))
            }
        };
        // Only the previous application's record write holds this; checking
        // the default happens before it, and arrival order keeps enforcement
        // ordered.
        let persisted =
            tokio::time::timeout(RECORD_LOCK_BUDGET, state.persisted.clone().lock_owned())
                .await
                .ok();
        let enforced = observation(&policy).map_err(|error| Status::internal(error.to_string()))?;
        // Enforce before persisting: a failed write must not delay a block.
        // The record is retried on the next application.
        let policy = Arc::new(policy);
        let Some(previous) = state.enforce(arrival, policy.clone(), verdict) else {
            tracing::info!("execution policy application superseded by a later one; not enforced");
            return Err(Status::aborted(
                "superseded by a later execution policy application",
            ));
        };
        let previous =
            observation(&previous).map_err(|error| Status::internal(error.to_string()))?;
        if enforced != previous {
            match &record {
                Some(record) => log_applied(&policy, outcome, &record.cluster, "applied"),
                None => {
                    tracing::info!("cluster serves no execution policy; enforcing startup settings")
                }
            }
            self.pool.mark_node_status_dirty();
        } else if outcome.diverges() {
            tracing::debug!(%outcome, "cluster default execution environment still refused");
        }
        let Some(mut persisted) = persisted else {
            tracing::warn!(budget = ?RECORD_LOCK_BUDGET, "execution policy enforced but not persisted: the previous record write is still running");
            return Err(Status::unavailable(
                "execution policy enforced but not persisted: the previous record write is still running",
            ));
        };
        if !matches!(&*persisted, OnDisk::Record(on_disk) if *on_disk == record) {
            let path = state.record_path.clone();
            self.off_runtime("persist execution policy", move || {
                // A write can change the disk and still fail (a rename or
                // unlink lands, then the directory sync doesn't), so the disk
                // is unknown until it succeeds and the next application must
                // rewrite.
                *persisted = OnDisk::Unreadable;
                write_record(&path, record.as_ref())?;
                *persisted = OnDisk::Record(record);
                Ok(())
            })
            .await?;
        }
        Ok(Response::new(ApplyExecutionPolicyResponse {
            enforced: Some((&enforced).into()),
            default_outcome: outcome_proto(outcome) as i32,
        }))
    }

    /// Whether the environment's artifacts are installed and verified here,
    /// by the resolution a new cold boot runs, under the startup settings.
    async fn verify_environment(&self, reference: EnvironmentReference) -> Verification {
        let resolution = async {
            self.resolve_new_cold_boot(self.execution_policy.startup(), Some(reference))
                .await
        };
        match tokio::time::timeout(USABILITY_BUDGET, resolution).await {
            Ok(Ok(_)) => Verification::Usable,
            Err(_elapsed) => {
                tracing::warn!(%reference, budget = ?USABILITY_BUDGET, "timed out verifying the cluster default execution environment");
                Verification::Undetermined
            }
            // Inventory reports a retryable failure as `Internal`; the others
            // come from cancelled, timed-out or contended blocking work.
            Ok(Err(error))
                if matches!(
                    error.code(),
                    tonic::Code::Internal
                        | tonic::Code::Unavailable
                        | tonic::Code::Cancelled
                        | tonic::Code::DeadlineExceeded
                        | tonic::Code::ResourceExhausted
                        | tonic::Code::Aborted
                ) =>
            {
                tracing::warn!(%reference, %error, "couldn't verify the cluster default execution environment");
                Verification::Undetermined
            }
            Ok(Err(error)) => {
                tracing::warn!(%reference, %error, "cluster default execution environment is not usable");
                Verification::Unusable
            }
        }
    }
}

#[cfg(test)]
mod tests;
