//! `Suspend` RPC admission, completed-suspend retries, suspend/wake arbitration,
//! and suspend preparation once the locks are held.

use super::*;

fn completed_suspend_retry_response(
    sandbox: &crate::db::SandboxRow,
    blocking: bool,
    requested_format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat,
) -> Result<Option<SuspendResponse>, Status> {
    if sandbox.state != SandboxState::Suspended
        || sandbox.current_local_artifact_kind != Some(ArtifactKind::Full)
        || sandbox.current_local_artifact_origin != Some(ArtifactOrigin::Suspend)
    {
        return Ok(None);
    }
    let Some(local_artifact_id) = sandbox.current_local_artifact_id else {
        return Ok(None);
    };
    if requested_format != pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::Unspecified {
        let sandbox_dir = std::path::PathBuf::from(format!("{CACHE_DIR}/{}", sandbox.id));
        let snapshot_dir = canonical_suspend_snapshot_dir(&sandbox_dir);
        let recorded_format = reupload_format_from_sidecar(&snapshot_dir.to_string_lossy())?;
        ensure_completed_suspend_retry_format(requested_format, recorded_format)?;
    }
    Ok(Some(SuspendResponse {
        published_artifact: blocking.then(|| crate::proto::PublishedArtifact {
            local_artifact_id: local_artifact_id.to_string(),
            artifact_kind: crate::proto::ArtifactKind::Full as i32,
            origin: crate::proto::ArtifactOrigin::Suspend as i32,
            snapshot_name: None,
        }),
    }))
}

pub(super) fn ensure_completed_suspend_retry_format(
    requested_format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat,
    recorded_format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat,
) -> Result<(), Status> {
    if requested_format != pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::Unspecified
        && requested_format != recorded_format
    {
        return Err(Status::failed_precondition(format!(
            "cannot reuse completed suspend artifact encoded as {recorded_format:?}: request \
             explicitly requires {requested_format:?}",
        )));
    }
    Ok(())
}

/// How soon after a completed wake an incoming suspend is flagged as a
/// possible wake/suspend oscillation.
const SUSPEND_AFTER_WAKE_WARN_THRESHOLD: std::time::Duration =
    std::time::Duration::from_millis(5_000);

/// Suspend-after-wake observation derived from the coordinator's
/// wake-completion record. `ms_since_wake` always lands on the
/// `suspend_requested` event when a prior wake is known;
/// `within_warn_threshold` additionally drives a `tracing::warn!` so the
/// oscillation signature is searchable without event-stream tooling.
pub(in crate::service) struct SuspendAfterWake {
    pub(in crate::service) ms_since_wake: u64,
    pub(in crate::service) within_warn_threshold: bool,
}

pub(in crate::service) fn classify_suspend_after_wake(
    since_wake: Option<std::time::Duration>,
    warn_threshold: std::time::Duration,
) -> Option<SuspendAfterWake> {
    let since_wake = since_wake?;
    Some(SuspendAfterWake {
        // Saturate rather than zero on overflow: a delta that large is by
        // definition not "shortly after a wake".
        ms_since_wake: u64::try_from(since_wake.as_millis()).unwrap_or(u64::MAX),
        within_warn_threshold: since_wake <= warn_threshold,
    })
}

const ENV_SUSPEND_WAKE_ARBITRATION: &str = "SPACE_SUSPEND_WAKE_ARBITRATION";

/// Whether a suspend that queued behind a concurrent wake is rejected as
/// superseded (the default) or allowed through. Resolved per-request so ops
/// can disable arbitration via a deployment env change without a daemonset
/// rollout. Default `true`: this is a kill switch, so unset/unrecognized
/// values fail toward the fixed behavior; only an explicit falsy value
/// disables it.
pub(in crate::service) fn suspend_wake_arbitration_enabled() -> bool {
    pplx_space_common_core::env::env_flag_var(ENV_SUSPEND_WAKE_ARBITRATION, true)
}

/// A suspend is superseded when its request arrived at this process before
/// the sandbox's most recent wake completed: the suspend queued behind the
/// wake on the wake lock, so the state it was issued against no longer
/// exists, and executing it would tear down a sandbox some caller just paid
/// a restore to bring up.
pub(in crate::service) fn suspend_superseded_by_wake(
    arrived_at: std::time::Instant,
    last_wake_completed_at: Option<std::time::Instant>,
) -> bool {
    last_wake_completed_at.is_some_and(|wake_completed_at| wake_completed_at > arrived_at)
}

pub(super) fn emit_suspend_failed_event(
    sandbox_id: &str,
    base_details: &SandboxEventDetails,
    operation: &'static str,
    reason: &'static str,
    error: impl Into<String>,
) {
    emit_sandbox_event(
        sandbox_id,
        SandboxEventType::SuspendFailed,
        SandboxEventSource::SbxManager,
        base_details
            .clone()
            .operation(operation)
            .reason(reason)
            .error(error),
    );
}

impl SandboxManagerService {
    pub(in crate::service) async fn suspend(
        &self,
        request: Request<SuspendRequest>,
    ) -> Result<Response<SuspendResponse>, Status> {
        let arrived_at = std::time::Instant::now();
        self.authorize_peer(&request, "Suspend", ALLOW_SANDBOX_OPERATOR)?;
        let publish_node_state = !should_skip_node_publish(&request);
        let req = request.get_ref();
        crate::proto::SuspendSnapshotFormat::try_from(req.format).map_err(|_| {
            Status::invalid_argument(format!("unknown suspend format value {}", req.format))
        })?;
        let mut context = SuspendRequestContext::new(
            req.sandbox_id.clone(),
            req.blocking,
            req.format,
            publish_node_state,
            sandbox_event_trigger(&request),
        );

        let sandbox = self.lookup_sandbox(&context.sandbox_id)?;
        self.ensure_teardown_suspend_owner_for_durability(&sandbox.id, context.durability)?;
        self.ensure_not_pool_sandbox_for_snapshots(&sandbox, "suspend")?;
        self.ensure_no_plaintext_pool_cow_for_snapshots(&sandbox, "suspend")?;
        self.ensure_snapshot_restore_supported("suspend")?;
        context.sandbox_id = sandbox.id.clone();
        let wake_lock = self.auto_lifecycle.wake_lock(&sandbox.id);
        let wake_guard = wake_lock.lock_owned().await;
        // Suspend-vs-wake arbitration: if a wake completed after this request
        // arrived, the request queued behind that wake on the wake lock — the
        // caller decided to suspend a sandbox that has since been woken
        // (typically by a data-plane access restoring it). Executing it would
        // tear the sandbox down underneath the waker, which in the wild
        // oscillates the sandbox through suspend/restore cycles (each paying
        // a full suspend snapshot) while the data plane 502s. Reject; a
        // caller that still wants the sandbox suspended re-issues against the
        // now-running sandbox and wins cleanly.
        if suspend_wake_arbitration_enabled()
            && suspend_superseded_by_wake(
                arrived_at,
                self.auto_lifecycle.last_wake_completed_at(&sandbox.id),
            )
        {
            let mut details =
                SandboxEventDetails::default().with_from_state(sandbox.state.to_string());
            if let Some(trigger) = context.event_trigger {
                details = details.trigger(trigger);
            }
            if let Some(after_wake) = classify_suspend_after_wake(
                self.auto_lifecycle.since_last_wake_completed(&sandbox.id),
                SUSPEND_AFTER_WAKE_WARN_THRESHOLD,
            ) {
                details = details.ms_since_wake(after_wake.ms_since_wake.to_string());
            }
            emit_suspend_failed_event(
                &sandbox.id,
                &details,
                "arbitration",
                "superseded_by_concurrent_wake",
                "suspend queued behind a concurrent wake",
            );
            tracing::warn!(
                sandbox_id = %sandbox.id,
                "rejecting suspend that queued behind a concurrent wake"
            );
            return Err(Status::aborted(
                "suspend superseded by a concurrent wake: the sandbox was woken while this \
                 suspend request waited on the wake lock; re-issue the suspend if the sandbox \
                 should still be suspended",
            ));
        }
        let artifact_lock = self.auto_lifecycle.artifact_lock(&sandbox.id);
        let artifact_guard = artifact_lock.lock_owned().await;
        // A blocking suspend commits the full artifact before replying. If the
        // caller loses the response or its control-plane marker write fails,
        // retrying must return that exact committed artifact instead of trying
        // to suspend an already-suspended VM again.
        let sandbox = self.lookup_sandbox(&context.sandbox_id)?;
        if let Some(response) =
            completed_suspend_retry_response(&sandbox, context.blocking, context.requested_format)?
        {
            return Ok(Response::new(response));
        }
        let (blocking_local_artifact_id, publication) = self
            .prepare_suspend_snapshot_publication_after_locks(
                &context,
                &wake_guard,
                &artifact_guard,
            )
            .await?;
        if !context.blocking {
            let service = self.clone();
            let background_sandbox_id = publication.sandbox.id.clone();
            let span_sandbox_id = background_sandbox_id.clone();
            self.spawn_detached_state_writer("SuspendPublication", &span_sandbox_id, async move {
                let _wake_guard = wake_guard;
                let _artifact_guard = artifact_guard;
                if let Err(error) = service
                    .complete_suspend_snapshot_publication(publication)
                    .await
                {
                    tracing::warn!(
                        sandbox_id = %background_sandbox_id,
                        code = %error.code(),
                        error = %error.message(),
                        "asynchronous suspend publication failed"
                    );
                    super::super::drain::record_detached_failure(error.message());
                }
            })?;
            return Ok(Response::new(SuspendResponse {
                published_artifact: None,
            }));
        }

        self.complete_suspend_snapshot_publication(publication)
            .await?;
        drop(artifact_guard);
        drop(wake_guard);

        Ok(Response::new(SuspendResponse {
            published_artifact: Some(crate::proto::PublishedArtifact {
                local_artifact_id: blocking_local_artifact_id,
                artifact_kind: crate::proto::ArtifactKind::Full as i32,
                origin: crate::proto::ArtifactOrigin::Suspend as i32,
                snapshot_name: None,
            }),
        }))
    }

    pub(in crate::service) async fn prepare_suspend_snapshot_publication_after_locks(
        &self,
        context: &SuspendRequestContext,
        _wake_guard: &crate::auto_lifecycle::LifecycleMutexGuard,
        _artifact_guard: &crate::auto_lifecycle::LifecycleMutexGuard,
    ) -> Result<(String, SuspendSnapshotPublicationContext), Status> {
        self.ensure_snapshot_restore_supported("suspend")?;
        // Cleanup takes its fence before any artifact lock. Never wait for it
        // while holding the wake/artifact locks: an admitted delete must win,
        // and retrying avoids an inverted lock-order cycle.
        let CleanupLockTryResult::Acquired(cleanup_fence) =
            self.try_cleanup_fence(&context.sandbox_id)
        else {
            return Err(Status::unavailable(
                "sandbox cleanup is active; retry suspend",
            ));
        };
        let sandbox = self.lookup_sandbox(&context.sandbox_id)?;
        self.ensure_not_pool_sandbox_for_snapshots(&sandbox, "suspend")?;
        self.ensure_no_plaintext_pool_cow_for_snapshots(&sandbox, "suspend")?;
        let local_artifact_id = LocalArtifactId::new();
        // Stamp encryption on every event this suspend emits: it selects the
        // suspend and restore code path and is immutable, so a timeline that
        // omits it cannot say which path ran.
        let mut suspend_event_details = SandboxEventDetails::default()
            .with_from_state(sandbox.state.to_string())
            .bool_field(
                SandboxEventBoolField::VolumeEncryption,
                sandbox.volume_encryption,
            );
        if let Some(trigger) = context.event_trigger {
            suspend_event_details = suspend_event_details.trigger(trigger);
        }
        if context.durability.is_remote_before_teardown() {
            suspend_event_details =
                suspend_event_details.operation(crate::db::PLANNED_TEARDOWN_SUSPEND_OPERATION);
        }
        if let Some(execution_generation) = context.durability.execution_generation() {
            suspend_event_details =
                suspend_event_details.execution_generation(execution_generation.to_string());
        }
        // Suspend-after-wake oscillation signal: a suspend landing moments
        // after a completed wake usually means an external caller is
        // fighting wake-on-access, re-suspending the sandbox over and over
        // and paying a full suspend snapshot per cycle. Signal only — never
        // delays or rejects the suspend.
        if let Some(after_wake) = classify_suspend_after_wake(
            self.auto_lifecycle.since_last_wake_completed(&sandbox.id),
            SUSPEND_AFTER_WAKE_WARN_THRESHOLD,
        ) {
            suspend_event_details =
                suspend_event_details.ms_since_wake(after_wake.ms_since_wake.to_string());
            if after_wake.within_warn_threshold {
                tracing::warn!(
                    sandbox_id = %sandbox.id,
                    ms_since_wake = after_wake.ms_since_wake,
                    "suspend requested shortly after a completed wake; \
                     possible wake/suspend oscillation"
                );
            }
        }
        emit_sandbox_event(
            &sandbox.id,
            SandboxEventType::SuspendRequested,
            SandboxEventSource::SbxManager,
            suspend_event_details.clone(),
        );

        let create_kind = "suspend";
        let create_started_at = std::time::Instant::now();
        tracing::info!(
            sandbox_id = %sandbox.id,
            blocking = context.blocking,
            "snapshot create started"
        );
        pplx_space_metrics::snapshot::create_started(
            create_kind,
            context.create_codec,
            sandbox.volume_encryption,
        );

        // Auto-pause if started
        if sandbox.state == SandboxState::Started {
            let pause_started_at = std::time::Instant::now();
            let mut node_client = connect_node(&self.node_manager).await.map_err(|e| {
                pplx_space_metrics::snapshot::create_failed(
                    create_kind,
                    context.create_codec,
                    sandbox.volume_encryption,
                    "pause",
                    "node_mgr_unreachable",
                    create_started_at,
                );
                let error = format!("cannot connect to node-manager: {e}");
                emit_suspend_failed_event(
                    &sandbox.id,
                    &suspend_event_details,
                    "pause",
                    "node_mgr_unreachable",
                    error.clone(),
                );
                Status::internal(error)
            })?;
            self.ensure_teardown_suspend_owner_for_durability(&sandbox.id, context.durability)?;
            {
                let db = self.db_conn()?;
                require_state_write(
                    crate::db::update_state(&db, &sandbox.id, SandboxState::Pausing),
                    "begin checkpoint pause",
                )?;
            }
            self.watchdog.record_lifecycle_edge(&sandbox.id).await;
            let guest_preparation_budget = context.guest_preparation_budget();
            if let Err(e) =
                prepare_guest_for_vm_snapshot_via_spaced_host(&sandbox.id, guest_preparation_budget)
                    .await
            {
                let db = self.db_conn()?;
                if let Err(error) = require_state_write(
                    crate::db::update_state(&db, &sandbox.id, SandboxState::Started),
                    "roll back checkpoint pause preparation",
                ) {
                    tracing::warn!(sandbox_id = %sandbox.id, %error, "failed to roll back checkpoint pause state");
                }
                pplx_space_metrics::snapshot::create_failed(
                    create_kind,
                    context.create_codec,
                    sandbox.volume_encryption,
                    "pause",
                    "guest_fs_flush_failed",
                    create_started_at,
                );
                emit_suspend_failed_event(
                    &sandbox.id,
                    &suspend_event_details,
                    "guest_filesystem_flush",
                    "guest_fs_flush_failed",
                    format!("guest filesystem flush before suspend failed: {e}"),
                );
                return Err(Status::internal(format!(
                    "guest snapshot preparation before suspend failed: {e}"
                )));
            }
            let sandbox_id = sandbox
                .id
                .parse::<SandboxId>()
                .map_err(|error| Status::internal(format!("invalid sandbox ID: {error}")))?;
            let pause_state = pause_with_reconciliation(
                &mut node_client,
                sandbox.node_execution_fence(),
                &self.db,
                &sandbox_id,
                "auto-pause failed",
                guest_preparation_budget,
                &self.node_manager,
                self.config.balloon_hint_timeout_ms,
            )
            .await;
            if pause_state != SandboxState::Paused {
                self.watchdog.record_lifecycle_edge(&sandbox.id).await;
                let reason = format!("auto-pause failed; sandbox reconciled to {pause_state}");
                pplx_space_metrics::snapshot::create_failed(
                    create_kind,
                    context.create_codec,
                    sandbox.volume_encryption,
                    "pause",
                    "fc_pause_failed",
                    create_started_at,
                );
                emit_suspend_failed_event(
                    &sandbox.id,
                    &suspend_event_details,
                    "pause",
                    "fc_pause_failed",
                    reason.clone(),
                );
                return Err(Status::internal(reason));
            }
            {
                let db = self.db_conn()?;
                require_state_write(
                    crate::db::update_state(&db, &sandbox.id, SandboxState::Paused),
                    "complete checkpoint pause",
                )?;
            }
            pplx_space_metrics::snapshot::create_step(
                create_kind,
                context.create_codec,
                sandbox.volume_encryption,
                "pause",
                pause_started_at,
            );
        } else if sandbox.state != SandboxState::Paused {
            pplx_space_metrics::snapshot::create_failed(
                create_kind,
                context.create_codec,
                sandbox.volume_encryption,
                "pause",
                "invalid_state",
                create_started_at,
            );
            // For the `failed` state the cluster-controller's node-drain
            // evacuation classifies this rejection on
            // `SUSPEND_SANDBOX_FAILED_MARKER` to give up and flip the DB row to
            // `failed` (so the node can finish draining) instead of retrying
            // suspend forever, so the failed branch must render that exact
            // phrase. Reference the shared constant rather than re-typing the
            // literal so a reword stays in lockstep with the consumer.
            let error = if sandbox.state == SandboxState::Failed {
                format!(
                    "cannot suspend: {}",
                    pplx_space_common_core::node_drain_signals::SUSPEND_SANDBOX_FAILED_MARKER
                )
            } else {
                format!(
                    "cannot suspend: sandbox is {}, must be started or paused",
                    sandbox.state
                )
            };
            emit_suspend_failed_event(
                &sandbox.id,
                &suspend_event_details,
                "pause",
                "invalid_state",
                error.clone(),
            );
            return Err(Status::failed_precondition(error));
        }

        // Now paused — create snapshot
        self.ensure_teardown_suspend_owner_for_durability(&sandbox.id, context.durability)?;
        {
            let db = self.db_conn()?;
            match context.durability {
                SuspendDurability::NodeLocal => {
                    let applied = crate::db::begin_suspend(&db, &sandbox.id)
                        .map_err(|e| Status::internal(format!("db error: {e}")))?;
                    if !applied {
                        return Err(Status::aborted("suspend admission superseded"));
                    }
                    Ok(())
                }
                SuspendDurability::RemoteBeforeTeardown {
                    execution_generation,
                } => crate::db::begin_teardown_suspend(&db, &sandbox.id, execution_generation)
                    .map(|_| ()),
            }
            .map_err(|e| Status::internal(format!("db error: {e}")))?;
        }

        let blocking_local_artifact_id = local_artifact_id.to_string();
        let publication = SuspendSnapshotPublicationContext {
            cleanup_fence,
            sandbox,
            requested_format: context.requested_format,
            suspend_format: context.suspend_format,
            create_codec: context.create_codec,
            local_artifact_id,
            event_details: suspend_event_details,
            started_at: create_started_at,
            publish_node_state: context.publish_node_state,
            durability: context.durability,
        };
        Ok((blocking_local_artifact_id, publication))
    }
}
