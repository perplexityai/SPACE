//! Manual guest maintenance is owned here, behind the same execution and
//! lifecycle fences as suspend/delete. A host socket executes a decision; it
//! does not authorize one. Recovery never wakes a dormant execution.
use super::*;
use crate::db::guest_updates::{
    self as store, Action, Admission, GuestIdentity, Intent, Outcome, Phase, Record,
};
use crate::service::cleanup_support::CleanupLockTryResult;
use pplx_space_spaced_common::proto::update as wire;
use pplx_space_spaced_host_client::RawSpacedHostGuestUpdateClient as Client;
use pplx_space_spaced_update as model;

mod observation;
mod policy;

// Includes transfer, stop/start and fallback; tightened only after production
// selector qualification on the supported seed. No lifecycle clock is extended.
const OPERATION_BUDGET: u64 = 360;
const ACTIVATION_BUDGET: u64 = 210;
const LIFECYCLE_MARGIN: u64 = 30;
const RPC_BUDGET: Duration = Duration::from_secs(5);

fn internal(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}
fn request<T>(value: T) -> Request<T> {
    let mut request = Request::new(value);
    request.set_timeout(RPC_BUDGET);
    request
}
fn uuid(value: &str) -> Result<Uuid, Status> {
    value
        .parse::<Uuid>()
        .ok()
        .filter(|v| !v.is_nil())
        .ok_or_else(|| Status::invalid_argument("non-nil UUID required"))
}
fn owner(record: &Record) -> wire::HoldRequest {
    wire::HoldRequest {
        operation_id: record.intent.operation.to_string(),
        execution_id: record.intent.execution.to_string(),
    }
}
fn identity(status: &wire::Status) -> Result<GuestIdentity, Status> {
    Ok(GuestIdentity {
        boot: uuid(&status.boot_id)?,
        incarnation: uuid(&status.incarnation)?,
        committed: status
            .committed
            .clone()
            .ok_or_else(|| Status::data_loss("missing committed identity"))?
            .try_into()?,
        attempt: status.attempt.clone().map(TryInto::try_into).transpose()?,
        forward_ports: status.forward_ports.clone(),
    })
}
fn phase(status: &wire::Status) -> Result<wire::Phase, Status> {
    match wire::Phase::try_from(status.phase) {
        Ok(wire::Phase::Unspecified) | Err(_) => {
            Err(Status::data_loss("unknown guest update phase"))
        }
        Ok(value) => Ok(value),
    }
}
fn response(record: &Record, guest: Option<wire::Status>) -> crate::proto::GuestUpdateOperation {
    use crate::proto::GuestUpdateOperationPhase as P;
    crate::proto::GuestUpdateOperation {
        sandbox_id: record.intent.sandbox.to_string(),
        execution_id: record.intent.execution.to_string(),
        operation_id: Some(record.intent.operation.to_string()),
        phase: match record.phase {
            Phase::Pending => P::Pending,
            Phase::Staged => P::Staged,
            Phase::Activating => P::Activating,
            Phase::Settling => P::Settling,
            Phase::Complete => P::Complete,
        } as i32,
        outcome: record.outcome.map(|v| v.to_string()),
        guest,
    }
}

impl SandboxManagerService {
    pub(super) async fn guest_update_status_rpc(
        &self,
        req: Request<crate::proto::GuestUpdateStatusRequest>,
    ) -> Result<Response<crate::proto::GuestUpdateOperation>, Status> {
        self.authorize_peer(&req, "GuestUpdateStatus", ALLOW_LOCAL_ARTIFACT_OPERATOR)?;
        let req = req.into_inner();
        let sandbox = self.lookup_sandbox(&req.sandbox_id)?;
        let record = if let Some(id) = req.operation_id {
            crate::db::read::guest_update(&*self.db_conn()?, uuid(&id)?).map_err(internal)?
        } else {
            crate::db::read::active_guest_update(&*self.db_conn()?, &sandbox.id)
                .map_err(internal)?
        };
        if record
            .as_ref()
            .is_some_and(|r| r.intent.sandbox.to_string() != sandbox.id)
        {
            return Err(Status::invalid_argument(
                "operation belongs to another sandbox",
            ));
        }
        let guest = if sandbox.state.is_unowned(SandboxState::Started) {
            match async {
                let mut client = self.connect_spaced_host(&sandbox).await?.guest_update();
                client
                    .get_status(request(wire::StatusRequest {}))
                    .await
                    .map(Response::into_inner)
            }
            .await
            {
                Ok(status) => Some(status),
                Err(error) if record.is_some() => {
                    tracing::debug!(%error, "guest maintenance receipt available without live guest status");
                    None
                }
                Err(error) => return Err(error),
            }
        } else {
            None
        };
        Ok(Response::new(match record {
            Some(record) => response(&record, guest),
            None => crate::proto::GuestUpdateOperation {
                sandbox_id: sandbox.id,
                execution_id: sandbox
                    .node_execution_id
                    .map(|v| v.to_string())
                    .unwrap_or_default(),
                guest,
                ..Default::default()
            },
        }))
    }

    pub(super) async fn apply_guest_update_rpc(
        &self,
        req: Request<crate::proto::ApplyGuestUpdateRequest>,
    ) -> Result<Response<crate::proto::GuestUpdateOperation>, Status> {
        self.authorize_peer(&req, "ApplyGuestUpdate", ALLOW_LOCAL_ARTIFACT_OPERATOR)?;
        let req = req.into_inner();
        let requested = policy::promotion(
            req.promotion
                .ok_or_else(|| Status::invalid_argument("promotion is required"))?,
        )?;
        let sandbox = self.lookup_sandbox(&req.sandbox_id)?;
        if let Some(receipt) = self.replay_guest_update(
            uuid(&req.operation_id)?,
            &sandbox,
            &req.expected_execution_id,
            req.force,
            Some(&requested),
        )? {
            return Ok(Response::new(receipt));
        }
        let bundle = policy::load().await?;
        if requested != bundle.policy.promotion {
            return Err(Status::permission_denied(
                "promotion is not the configured host authority",
            ));
        }
        qualify(&sandbox, Some(&bundle.policy), OPERATION_BUDGET)?;
        self.probe_update_contract(&sandbox).await?;
        self.admit_guest_update(Intent {
            operation: uuid(&req.operation_id)?,
            sandbox: sandbox.id.parse().map_err(internal)?,
            execution: req.expected_execution_id.parse().map_err(internal)?,
            action: Action::Apply {
                promotion: Box::new(requested),
                authorization: bundle.policy.authorization,
            },
            force: req.force,
            configuration_revision: 1,
        })
    }

    pub(super) async fn restart_guest_rpc(
        &self,
        req: Request<crate::proto::RestartGuestRequest>,
    ) -> Result<Response<crate::proto::GuestUpdateOperation>, Status> {
        self.authorize_peer(&req, "RestartGuest", ALLOW_LOCAL_ARTIFACT_OPERATOR)?;
        let req = req.into_inner();
        let sandbox = self.lookup_sandbox(&req.sandbox_id)?;
        if let Some(receipt) = self.replay_guest_update(
            uuid(&req.operation_id)?,
            &sandbox,
            &req.expected_execution_id,
            req.force,
            None,
        )? {
            return Ok(Response::new(receipt));
        }
        qualify(&sandbox, None, ACTIVATION_BUDGET)?;
        self.probe_update_contract(&sandbox).await?;
        self.admit_guest_update(Intent {
            operation: uuid(&req.operation_id)?,
            sandbox: sandbox.id.parse().map_err(internal)?,
            execution: req.expected_execution_id.parse().map_err(internal)?,
            action: Action::Restart,
            force: req.force,
            configuration_revision: 1,
        })
    }

    fn replay_guest_update(
        &self,
        operation: Uuid,
        sandbox: &crate::db::SandboxRow,
        execution: &str,
        force: bool,
        promotion: Option<&model::policy::Promotion>,
    ) -> Result<Option<crate::proto::GuestUpdateOperation>, Status> {
        let Some(record) =
            crate::db::read::guest_update(&*self.db_conn()?, operation).map_err(internal)?
        else {
            return Ok(None);
        };
        let action_matches = match (promotion, &record.intent.action) {
            (Some(requested), Action::Apply { promotion, .. }) => requested == promotion.as_ref(),
            (None, Action::Restart) => true,
            _ => false,
        };
        if record.intent.sandbox.to_string() != sandbox.id
            || record.intent.execution.to_string() != execution
            || record.intent.force != force
            || !action_matches
        {
            return Err(Status::failed_precondition(
                "guest maintenance operation identity was reused",
            ));
        }
        // A receipt reports an already-admitted intent; it does not grant new
        // authority. Reconciliation still checks current policy and execution.
        Ok(Some(response(&record, None)))
    }

    fn admit_guest_update(
        &self,
        intent: Intent,
    ) -> Result<Response<crate::proto::GuestUpdateOperation>, Status> {
        let _writer = self.state_writer_lease("GuestUpdate")?;
        let result = store::admit(
            &*self.db_conn()?,
            intent,
            jiff::Timestamp::now().as_second() + OPERATION_BUDGET as i64,
        )
        .map_err(internal)?;
        let record = match result {
            Admission::Accepted(value) => *value,
            Admission::Refused(reason) => {
                return Err(Status::failed_precondition(format!(
                    "guest maintenance deferred: {reason:?}"
                )));
            }
        };
        if record.phase != Phase::Complete
            && let Ok(permit) = self.guest_update_sem.clone().try_acquire_owned()
        {
            let service = self.clone();
            let operation = record.intent.operation;
            self.task_tracker.spawn(async move {
                let _permit = permit;
                if let Err(error) = service.run_guest_update(operation).await {
                    tracing::warn!(%operation, %error, "guest update awaits reconciliation");
                }
            });
        }
        Ok(Response::new(response(&record, None)))
    }

    async fn probe_update_contract(&self, sandbox: &crate::db::SandboxRow) -> Result<(), Status> {
        let status = self
            .connect_spaced_host(sandbox)
            .await?
            .guest_update()
            .get_status(request(wire::StatusRequest {}))
            .await?
            .into_inner();
        if status.seed_contract != model::SEED_CONTRACT {
            return Err(Status::failed_precondition(
                "guest selector contract is not supported",
            ));
        }
        Ok(())
    }

    fn update_record(&self, operation: Uuid) -> Result<Record, Status> {
        crate::db::read::guest_update(&*self.db_conn()?, operation)
            .map_err(internal)?
            .ok_or_else(|| Status::not_found("guest update receipt missing"))
    }
    fn update_execution(&self, record: &Record) -> Result<crate::db::SandboxRow, Status> {
        let sandbox = self.lookup_sandbox(&record.intent.sandbox.to_string())?;
        if sandbox.node_execution_id != Some(record.intent.execution)
            || !sandbox.state.is_unowned(SandboxState::Started)
            || sandbox.cleanup_completion.is_some()
        {
            return Err(Status::failed_precondition(
                "guest update execution is no longer running",
            ));
        }
        Ok(sandbox)
    }
    async fn update_client(&self, record: &Record) -> Result<Client, Status> {
        let sandbox = self.update_execution(record)?;
        let client = self.connect_spaced_host(&sandbox).await?.guest_update();
        self.update_execution(record)?;
        Ok(client)
    }
    fn update_advance(
        &self,
        record: &Record,
        to: Phase,
        seen: Option<&GuestIdentity>,
    ) -> Result<Record, Status> {
        if !store::advance(&*self.db_conn()?, record, to, seen).map_err(internal)? {
            return Err(Status::aborted("guest update phase or execution changed"));
        }
        self.update_record(record.intent.operation)
    }

    async fn run_guest_update(&self, operation: Uuid) -> Result<(), Status> {
        let record = self.update_record(operation)?;
        if record.phase == Phase::Complete {
            return Ok(());
        }
        if store::supersede(&*self.db_conn()?, operation).map_err(internal)? {
            return Ok(());
        }
        let id = record.intent.sandbox.to_string();
        // Capture must not overlap selection, but timeout intent and activity
        // touches remain writable throughout the trial. Stop/delete take the
        // cleanup fence only around a bounded Arm/Restart/Commit RPC below;
        // transfers and status polling never retain that fence.
        let artifact = self.auto_lifecycle.artifact_lock(&id);
        let priority = artifact.subscribe_waiters();
        let Ok(_artifact) = artifact.try_lock_owned() else {
            return Ok(());
        };
        let _writer = self.state_writer_lease("GuestUpdateRecovery")?;
        self.drive_guest_update(record, false, priority).await
    }

    /// Called with the artifact lock before a lifecycle transition. Pending
    /// maintenance yields; an already committed decision wins, otherwise the
    /// local trial is cancelled and its running fallback is configured first.
    pub(super) async fn settle_guest_update_for_lifecycle(
        &self,
        sandbox: &str,
    ) -> Result<(), Status> {
        let Some(record) =
            crate::db::read::active_guest_update(&*self.db_conn()?, sandbox).map_err(internal)?
        else {
            return Ok(());
        };
        let priority = self
            .auto_lifecycle
            .artifact_lock(sandbox)
            .subscribe_waiters();
        self.drive_guest_update(record, true, priority).await
    }

    fn guest_update_execution_fence(
        &self,
        record: &Record,
    ) -> Result<cleanup_support::OwnedCleanupFenceGuard, Status> {
        let fence = match self.try_cleanup_fence(&record.intent.sandbox.to_string()) {
            CleanupLockTryResult::Acquired(fence) => fence,
            CleanupLockTryResult::Busy => {
                return Err(Status::aborted("cleanup supersedes guest maintenance"));
            }
        };
        self.update_execution(record)?;
        Ok(fence)
    }

    async fn drive_guest_update(
        &self,
        mut record: Record,
        lifecycle: bool,
        mut priority: tokio::sync::watch::Receiver<()>,
    ) -> Result<(), Status> {
        let operation = record.intent.operation;
        let sandbox = self.update_execution(&record)?;
        let mut client = self.update_client(&record).await?;
        let mut observed = client
            .get_status(request(wire::StatusRequest {}))
            .await?
            .into_inner();
        if observed.seed_contract != model::SEED_CONTRACT {
            return Err(Status::failed_precondition(
                "guest selector contract is not qualified",
            ));
        }
        if (lifecycle || jiff::Timestamp::now().as_second() >= record.deadline)
            && matches!(record.phase, Phase::Pending | Phase::Staged)
        {
            client.hold(request(owner(&record))).await?;
            return self
                .finish_guest_update(record, Outcome::Cancelled, observed, &mut client)
                .await;
        }
        // A lost Stage reply leaves a durable guest receipt. Reuse that exact
        // fence instead of changing its deadline and retransferring the bytes.
        if record.phase == Phase::Pending
            && phase(&observed)? == wire::Phase::Staged
            && observed
                .attempt
                .as_ref()
                .is_some_and(|attempt| attempt.operation_id == operation.to_string())
        {
            if maintenance_decision(&record, &observed)? != Decision::Arm {
                return Err(Status::failed_precondition(
                    "staging receipt conflicts with intent",
                ));
            }
            record = self.update_advance(&record, Phase::Staged, None)?;
        }
        if record.phase == Phase::Pending
            && let Action::Apply {
                promotion,
                authorization,
            } = &record.intent.action
        {
            let bundle = match policy::load().await {
                Ok(bundle)
                    if &bundle.policy.promotion == promotion.as_ref()
                        && &bundle.policy.authorization == authorization =>
                {
                    bundle
                }
                authority => {
                    tracing::warn!(operation = %operation, error = ?authority.err(), "guest promotion authority withdrawn before staging");
                    client.hold(request(owner(&record))).await?;
                    return self
                        .finish_guest_update(record, Outcome::Cancelled, observed, &mut client)
                        .await;
                }
            };
            if !crate::db::read::guest_update_retry_ready(&*self.db_conn()?, &record)
                .map_err(internal)?
            {
                if !observed.customer_admission_held {
                    return Ok(());
                }
                // Backoff leaves admission open, including after a guest crash
                // while waiting. The durable operation owns reconfiguration.
                client.hold(request(owner(&record))).await?;
                self.configure_update(&record, &observed, &mut client, false)
                    .await?;
                client
                    .release(request(wire::ReleaseRequest {
                        owner: Some(owner(&record)),
                        expected_incarnation: observed.incarnation,
                        configuration_revision: record.intent.configuration_revision,
                    }))
                    .await?;
                return Ok(());
            }
            if qualify(&sandbox, Some(&bundle.policy), OPERATION_BUDGET).is_err()
                || (!record.intent.force && !observed.defer_reasons.is_empty())
            {
                client.hold(request(owner(&record))).await?;
                return self
                    .finish_guest_update(record, Outcome::Deferred, observed, &mut client)
                    .await;
            }
            let committed = identity(&observed)?.committed;
            if committed.digest == promotion.target {
                client.hold(request(owner(&record))).await?;
                return self
                    .finish_guest_update(record, Outcome::Committed, observed, &mut client)
                    .await;
            }
            let guest_version = observed
                .committed_version
                .parse::<semver::Version>()
                .map_err(internal)?;
            if !promotion
                .should_install(&guest_version, committed.digest)
                .map_err(|error| Status::failed_precondition(error.to_string()))?
            {
                client.hold(request(owner(&record))).await?;
                return self
                    .finish_guest_update(record, Outcome::NotRequired, observed, &mut client)
                    .await;
            }
            let attempt = model::Attempt {
                operation,
                execution: uuid(&record.intent.execution.to_string())?,
                boot: uuid(&observed.boot_id)?,
                incarnation: uuid(&observed.incarnation)?,
                configuration_revision: record.intent.configuration_revision,
                target: model::Selection {
                    slot: committed.slot.other(),
                    digest: promotion.target,
                },
                confirmation_deadline_ms: observed
                    .boot_time_ms
                    .saturating_add(OPERATION_BUDGET * 1000),
            };
            observed = yield_staging_to_capture(&mut priority, stage(&mut client, bundle, attempt))
                .await?;
            self.update_execution(&record)?;
            record = self.update_advance(&record, Phase::Staged, None)?;
        }
        if matches!(record.phase, Phase::Pending | Phase::Staged) {
            qualify(&self.update_execution(&record)?, None, ACTIVATION_BUDGET)?;
            observed = client.hold(request(owner(&record))).await?.into_inner();
            if !record.intent.force && !observed.defer_reasons.is_empty() {
                return self
                    .finish_guest_update(record, Outcome::Deferred, observed, &mut client)
                    .await;
            }
            record =
                self.update_advance(&record, Phase::Activating, Some(&identity(&observed)?))?;
        } else {
            // A new spaced-host process lost its in-memory hold. Re-establish
            // ownership before configuration or a commit can proceed.
            client.hold(request(owner(&record))).await?;
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(ACTIVATION_BUDGET);
        let finish_by = now_secs().saturating_add(ACTIVATION_BUDGET);
        loop {
            let current = self.update_execution(&record)?;
            let lifecycle = lifecycle
                || priority.has_changed().unwrap_or(true)
                || lifecycle_preempts(current.next_auto_event_at, finish_by);
            if record.phase == Phase::Settling {
                return self
                    .finish_guest_update(
                        record.clone(),
                        record
                            .outcome
                            .ok_or_else(|| Status::data_loss("settlement lacks outcome"))?,
                        observed,
                        &mut client,
                    )
                    .await;
            }
            match maintenance_decision(&record, &observed)? {
                Decision::Arm => {
                    let authority = policy::load().await;
                    let authorized = authority.as_ref().is_ok_and(|bundle| matches!(&record.intent.action,
                        Action::Apply { promotion, authorization } if promotion.as_ref() == &bundle.policy.promotion && authorization == &bundle.policy.authorization));
                    if !authorized {
                        return self
                            .finish_guest_update(record, Outcome::Cancelled, observed, &mut client)
                            .await;
                    }
                    if lifecycle || jiff::Timestamp::now().as_second() >= record.deadline {
                        return self
                            .finish_guest_update(record, Outcome::Deferred, observed, &mut client)
                            .await;
                    }
                    let attempt = observed
                        .attempt
                        .clone()
                        .ok_or_else(|| Status::data_loss("staged attempt missing"))?;
                    let _execution = self.guest_update_execution_fence(&record)?;
                    match client
                        .arm(request(wire::ArmRequest {
                            attempt: Some(attempt),
                            force: record.intent.force,
                        }))
                        .await
                    {
                        Ok(value) => observed = value.into_inner(),
                        Err(e) if e.code() == tonic::Code::FailedPrecondition => {
                            return self
                                .finish_guest_update(
                                    record,
                                    Outcome::Deferred,
                                    observed,
                                    &mut client,
                                )
                                .await;
                        }
                        Err(_) => {}
                    }
                }
                Decision::Restart => {
                    if lifecycle || jiff::Timestamp::now().as_second() >= record.deadline {
                        return self
                            .finish_guest_update(record, Outcome::Deferred, observed, &mut client)
                            .await;
                    }
                    let _execution = self.guest_update_execution_fence(&record)?;
                    match client
                        .restart(request(wire::RestartRequest {
                            operation_id: operation.to_string(),
                            execution_id: record.intent.execution.to_string(),
                            expected_boot_id: observed.boot_id.clone(),
                            expected_incarnation: observed.incarnation.clone(),
                            force: record.intent.force,
                        }))
                        .await
                    {
                        Ok(value) => observed = value.into_inner(),
                        Err(e) if e.code() == tonic::Code::FailedPrecondition => {
                            return self
                                .finish_guest_update(
                                    record,
                                    Outcome::Deferred,
                                    observed,
                                    &mut client,
                                )
                                .await;
                        }
                        Err(_) => {}
                    }
                }
                Decision::Confirm => {
                    let current_authority = policy::load().await;
                    let authorized = current_authority.as_ref().is_ok_and(|bundle| matches!(&record.intent.action,
                        Action::Apply { promotion, authorization } if promotion.as_ref() == &bundle.policy.promotion && authorization == &bundle.policy.authorization));
                    if lifecycle
                        || jiff::Timestamp::now().as_second() >= record.deadline
                        || !authorized
                    {
                        if let Err(error) = client
                            .abort(request(wire::AbortRequest {
                                attempt: observed.attempt.clone(),
                            }))
                            .await
                        {
                            tracing::debug!(%operation, %error, "trial cancellation awaits status reconciliation");
                        }
                    } else {
                        self.configure_update(&record, &observed, &mut client, true)
                            .await?;
                        let _execution = self.guest_update_execution_fence(&record)?;
                        if let Err(error) = client
                            .commit(request(wire::CommitRequest {
                                attempt: observed.attempt.clone(),
                            }))
                            .await
                        {
                            tracing::debug!(%operation, %error, "trial confirmation awaits status reconciliation");
                        }
                    }
                }
                Decision::Settle(outcome) => {
                    let outcome = if lifecycle
                        && matches!(outcome, Outcome::AwaitingHost | Outcome::CandidateFailed)
                    {
                        Outcome::Cancelled
                    } else {
                        outcome
                    };
                    return self
                        .finish_guest_update(record, outcome, observed, &mut client)
                        .await;
                }
                Decision::Wait => {}
            }
            tokio::select! {
                _ = self.shutdown_token.cancelled() => return Ok(()),
                _ = tokio::time::sleep(Duration::from_millis(200)) => {}
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Status::deadline_exceeded(
                    "guest update requires recovery; hold remains owned",
                ));
            }
            // Re-attest on a host restart; never use a channel that reconnects
            // transparently to a new process while carrying the old fence.
            match client.get_status(request(wire::StatusRequest {})).await {
                Ok(value) => observed = value.into_inner(),
                Err(_) => {
                    client = self.update_client(&record).await?;
                    continue;
                }
            }
        }
    }

    async fn configure_update(
        &self,
        record: &Record,
        seen: &wire::Status,
        client: &mut Client,
        trial: bool,
    ) -> Result<(), Status> {
        let sandbox = self.update_execution(record)?;
        client
            .configure(request(wire::ConfigureRequest {
                attempt: trial.then(|| seen.attempt.clone()).flatten(),
                environment: sandbox.process_env.into_iter().collect(),
                forward_ports: record
                    .prior
                    .as_ref()
                    .map(|v| v.forward_ports.clone())
                    .unwrap_or_else(|| seen.forward_ports.clone()),
                owner: (!trial).then(|| owner(record)),
                expected_incarnation: seen.incarnation.clone(),
                configuration_revision: record.intent.configuration_revision,
                expected_boot_id: seen.boot_id.clone(),
            }))
            .await?;
        self.update_execution(record)?;
        Ok(())
    }

    async fn finish_guest_update(
        &self,
        mut record: Record,
        outcome: Outcome,
        mut seen: wire::Status,
        client: &mut Client,
    ) -> Result<(), Status> {
        if outcome == Outcome::Committed
            && seen
                .attempt
                .as_ref()
                .is_some_and(|attempt| attempt.operation_id == record.intent.operation.to_string())
        {
            // A durable commit can precede link repair and READY. Repeating
            // that decision finishes the guest side before reopening traffic.
            let _execution = self.guest_update_execution_fence(&record)?;
            seen = client
                .commit(request(wire::CommitRequest {
                    attempt: seen.attempt.clone(),
                }))
                .await?
                .into_inner();
        }
        if matches!(outcome, Outcome::Deferred | Outcome::Cancelled)
            && phase(&seen)? == wire::Phase::Staged
            && seen
                .attempt
                .as_ref()
                .is_some_and(|a| a.operation_id == record.intent.operation.to_string())
        {
            seen = client
                .abort(request(wire::AbortRequest {
                    attempt: seen.attempt.clone(),
                }))
                .await?
                .into_inner();
        }
        self.configure_update(&record, &seen, client, false).await?;
        if record.phase != Phase::Settling {
            if !store::settle(&*self.db_conn()?, &record, outcome, Some(&identity(&seen)?))
                .map_err(internal)?
            {
                return Err(Status::aborted("guest update settlement superseded"));
            }
            record = self.update_record(record.intent.operation)?;
        }
        client
            .release(request(wire::ReleaseRequest {
                owner: Some(owner(&record)),
                expected_incarnation: seen.incarnation,
                configuration_revision: record.intent.configuration_revision,
            }))
            .await?;
        self.update_advance(&record, Phase::Complete, None)?;
        if let Ok(current) = self.update_execution(&record)
            && let Some(due) = current.next_auto_event_at
            && now_secs() > due
        {
            tracing::warn!(operation = %record.intent.operation, lateness_seconds = now_secs().saturating_sub(due), "guest maintenance settled after the lifecycle deadline");
        }
        Ok(())
    }

    pub fn start_guest_update_recovery(&self, shutdown: CancellationToken, tracker: &TaskTracker) {
        let service = self.clone();
        tracker.spawn(async move {
            loop {
                let mut after = None;
                loop {
                    let page = service.db_conn().and_then(|db| crate::db::scan::unfinished_guest_updates(&db, after).map_err(internal));
                    let records = match page { Ok(value) => value, Err(error) => { tracing::warn!(%error, "cannot scan guest update recovery"); break; } };
                    if records.is_empty() { break; }
                    use futures_util::StreamExt;
                    after = records.last().map(|record| record.intent.operation);
                    let work = futures_util::stream::iter(records).for_each_concurrent(1, |record| {
                        let service = &service;
                        async move {
                            let Ok(_permit) = service.guest_update_sem.clone().try_acquire_owned() else { return; };
                            if let Err(error) = service.run_guest_update(record.intent.operation).await {
                                tracing::warn!(operation = %record.intent.operation, %error, "guest update recovery deferred");
                            }
                        }
                    });
                    tokio::select! { _ = shutdown.cancelled() => return, _ = work => {} }
                }
                tokio::select! { _ = shutdown.cancelled() => return, _ = tokio::time::sleep(Duration::from_secs(5)) => {} }
            }
        });
    }
}

fn qualify(
    sandbox: &crate::db::SandboxRow,
    policy: Option<&policy::Policy>,
    budget: u64,
) -> Result<(), Status> {
    if !sandbox.state.is_unowned(SandboxState::Started)
        || sandbox.pool_template.is_some()
        || matches!(
            sandbox.user.kind(),
            SandboxOwnerKind::TemplateManager
                | SandboxOwnerKind::Builder
                | SandboxOwnerKind::Orphaned
        )
    {
        return Err(Status::failed_precondition(
            "guest maintenance requires a running customer execution",
        ));
    }
    if sandbox.next_auto_event_at.is_some_and(|deadline| {
        deadline
            <= now_secs()
                .saturating_add(budget)
                .saturating_add(LIFECYCLE_MARGIN)
    }) {
        return Err(Status::failed_precondition(
            "insufficient lifecycle deadline headroom",
        ));
    }
    if policy.is_some_and(|p| {
        !sandbox
            .template_hash
            .as_ref()
            .is_some_and(|hash| p.qualified_templates.contains(hash))
    }) {
        return Err(Status::failed_precondition(
            "installed template has not been qualified for this selector contract",
        ));
    }
    Ok(())
}

fn lifecycle_preempts(due: Option<u64>, finish_by: u64) -> bool {
    due.is_some_and(|due| due <= finish_by.saturating_add(LIFECYCLE_MARGIN))
}

#[derive(Debug, Eq, PartialEq)]
enum Decision {
    Arm,
    Restart,
    Confirm,
    Settle(Outcome),
    Wait,
}
fn maintenance_decision(record: &Record, seen: &wire::Status) -> Result<Decision, Status> {
    let identity = identity(seen)?;
    let running: model::Selection = seen
        .running
        .clone()
        .ok_or_else(|| Status::data_loss("missing running executable identity"))?
        .try_into()?;
    if phase(seen)? == wire::Phase::RolledBack && running != identity.committed {
        // Abort writes its decision while the candidate still serves. Only
        // systemd's next selector/start establishes the running fallback.
        return Ok(Decision::Wait);
    }
    let same = identity.attempt.as_ref().is_some_and(|a| {
        a.operation == record.intent.operation
            && a.execution.to_string() == record.intent.execution.to_string()
    });
    if same && let Action::Apply { promotion, .. } = &record.intent.action {
        let attempt = identity
            .attempt
            .as_ref()
            .ok_or_else(|| Status::data_loss("missing trial identity"))?;
        if attempt.target.digest != promotion.target
            || attempt.configuration_revision != record.intent.configuration_revision
        {
            return Err(Status::failed_precondition(
                "candidate identity conflicts with authorized attempt",
            ));
        }
        if phase(seen)? == wire::Phase::Trial
            && (attempt.boot != identity.boot || attempt.incarnation != identity.incarnation)
        {
            return Err(Status::failed_precondition(
                "candidate generation conflicts with journal",
            ));
        }
    }
    match (&record.intent.action, phase(seen)?) {
        (_, wire::Phase::RecoveryRequired) => Ok(Decision::Settle(Outcome::RecoveryRequired)),
        (Action::Apply { promotion, .. }, wire::Phase::Committed)
            if same && identity.committed.digest == promotion.target =>
        {
            Ok(Decision::Settle(Outcome::Committed))
        }
        (Action::Apply { .. }, wire::Phase::Staged) if same => Ok(Decision::Arm),
        (Action::Apply { .. }, wire::Phase::Trial) if same => Ok(Decision::Confirm),
        (Action::Apply { .. }, wire::Phase::Armed) if same => Ok(Decision::Wait),
        (Action::Apply { .. }, wire::Phase::RolledBack) if same => Ok(Decision::Settle(
            if seen.progress == wire::TrialProgress::AwaitingHost as i32 {
                Outcome::AwaitingHost
            } else {
                Outcome::CandidateFailed
            },
        )),
        (Action::Restart, wire::Phase::Restarted)
            if seen.restart.as_ref().is_some_and(|r| {
                r.operation_id == record.intent.operation.to_string()
                    && r.execution_id == record.intent.execution.to_string()
                    && r.previous_incarnation != seen.incarnation
            }) =>
        {
            Ok(Decision::Settle(Outcome::Restarted))
        }
        (Action::Restart, wire::Phase::Restarting)
            if seen
                .restart
                .as_ref()
                .is_some_and(|r| r.operation_id == record.intent.operation.to_string()) =>
        {
            Ok(Decision::Wait)
        }
        (
            Action::Restart,
            wire::Phase::Stable
            | wire::Phase::Committed
            | wire::Phase::RolledBack
            | wire::Phase::Restarted
            | wire::Phase::Staged,
        ) if record
            .prior
            .as_ref()
            .is_some_and(|p| p.boot == identity.boot && p.incarnation == identity.incarnation) =>
        {
            Ok(Decision::Restart)
        }
        (
            _,
            wire::Phase::Stable
            | wire::Phase::Committed
            | wire::Phase::RolledBack
            | wire::Phase::Restarted,
        ) if identity.attempt.is_none() && running == identity.committed => {
            // Missing receipt authority cannot justify another mutation, but
            // a stable committed executable is safe to configure and capture.
            // Retire this operation and quarantine its target for inspection.
            Ok(Decision::Settle(Outcome::RecoveryRequired))
        }
        _ => Err(Status::failed_precondition(
            "guest journal no longer belongs to this operation",
        )),
    }
}

// Dropping a transfer cannot authorize a restart. Partial bytes remain in the
// inactive slot; lifecycle settlement observes/aborts any completed Staged
// receipt before capture. Once armed, the driver instead waits for rollback.
async fn yield_staging_to_capture<T>(
    priority: &mut tokio::sync::watch::Receiver<()>,
    transfer: impl std::future::Future<Output = Result<T, Status>>,
) -> Result<T, Status> {
    tokio::select! {
        biased;
        _ = priority.changed() => Err(Status::aborted("guest staging yields to lifecycle capture")),
        result = transfer => result,
    }
}

async fn stage(
    client: &mut Client,
    bundle: policy::Bundle,
    attempt: model::Attempt,
) -> Result<wire::Status, Status> {
    use tokio::io::AsyncReadExt;
    let (sender, receiver) = tokio::sync::mpsc::channel(2);
    let descriptor = bundle.descriptor.clone();
    let send = async move {
        sender
            .send(wire::StageRequest {
                part: Some(wire::stage_request::Part::Header(Box::new(
                    wire::StageHeader {
                        descriptor: Some(descriptor.into()),
                        attempt: Some(attempt.into()),
                    },
                ))),
            })
            .await
            .map_err(internal)?;
        for (binary, file) in [
            (wire::Binary::Service, bundle.service),
            (wire::Binary::Cli, bundle.cli),
        ] {
            let mut file = tokio::fs::File::from_std(file);
            let mut offset = 0;
            let mut buffer = vec![0; 64 * 1024];
            loop {
                let read = file.read(&mut buffer).await.map_err(internal)?;
                if read == 0 {
                    break;
                }
                if offset + read as u64 > model::MAX_BINARY_BYTES {
                    return Err(Status::resource_exhausted(
                        "host agent payload exceeds bound",
                    ));
                }
                sender
                    .send(wire::StageRequest {
                        part: Some(wire::stage_request::Part::Chunk(wire::Chunk {
                            binary: binary as i32,
                            offset,
                            data: buffer
                                .get(..read)
                                .ok_or_else(|| Status::internal("invalid payload read length"))?
                                .to_vec(),
                        })),
                    })
                    .await
                    .map_err(internal)?;
                offset += read as u64;
            }
        }
        Ok::<_, Status>(())
    };
    let transfer = async {
        tokio::try_join!(
            send,
            client.stage(tokio_stream::wrappers::ReceiverStream::new(receiver))
        )
        .map(|(_, result)| result.into_inner())
    };
    tokio::time::timeout(Duration::from_secs(130), transfer)
        .await
        .map_err(|_| Status::deadline_exceeded("guest bundle transfer timed out"))?
}

#[cfg(test)]
mod tests;
