//! Maintenance bypasses ordinary readiness, never customer admission. Accepted
//! mutations outlive their RPC observer; disk work runs off the async executor.
use super::*;
use pplx_space_spaced_common::proto::{
    bridge_guest::guest_update_server::GuestUpdate, update as wire,
};
use pplx_space_spaced_guest_protocol::update::{binary, journal_status};
use pplx_space_spaced_guest_update::{SlotStore, UPDATE_ROOT, UpdateError};
use pplx_space_spaced_update::{Attempt, BundleDescriptor, JournalState};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Mutex;
use uuid::Uuid;

pub(crate) struct UpdateRuntime {
    root: std::path::PathBuf,
    clock: fn() -> Result<(Uuid, u64), UpdateError>,
    enabled: AtomicBool,
    gated: AtomicBool,
    boot: std::sync::OnceLock<Uuid>,
    hold: Mutex<Option<pplx_space_spaced_guest_execution::MaintenanceHold>>,
    configured: Mutex<Option<Attempt>>,
    serial: Arc<Semaphore>,
    disk_serial: Arc<Mutex<()>>,
    tasks: tokio_util::task::TaskTracker,
    events: watch::Sender<u64>,
    subscribers: Arc<Semaphore>,
    cached_status: std::sync::Mutex<Option<wire::Status>>,
}

impl UpdateRuntime {
    pub(crate) fn new() -> Self {
        Self {
            root: UPDATE_ROOT.into(),
            clock,
            enabled: AtomicBool::new(false),
            gated: AtomicBool::new(false),
            boot: std::sync::OnceLock::new(),
            hold: Mutex::new(None),
            configured: Mutex::new(None),
            serial: Arc::new(Semaphore::new(1)),
            disk_serial: Arc::new(Mutex::new(())),
            tasks: tokio_util::task::TaskTracker::new(),
            events: watch::channel(0).0,
            subscribers: Arc::new(Semaphore::new(1)),
            cached_status: std::sync::Mutex::new(None),
        }
    }
    pub(crate) fn trial_gated(&self) -> bool {
        self.gated.load(Ordering::Acquire)
    }
    pub(crate) async fn drain(&self) {
        self.tasks.close();
        self.tasks.wait().await;
    }
}

fn status(error: UpdateError) -> Status {
    match error {
        UpdateError::Busy => Status::unavailable(error.to_string()),
        UpdateError::Descriptor(_) | UpdateError::Json(_) | UpdateError::TooLarge => {
            Status::invalid_argument(error.to_string())
        }
        UpdateError::Io(_) => Status::internal(error.to_string()),
        _ => Status::failed_precondition(error.to_string()),
    }
}

fn required_attempt(value: Option<wire::Attempt>) -> Result<Attempt, Status> {
    value
        .ok_or_else(|| Status::invalid_argument("attempt is required"))?
        .try_into()
}

async fn send_status(
    sender: &tokio::sync::mpsc::Sender<Result<wire::Status, Status>>,
    value: Result<wire::Status, Status>,
    shutdown: &CancellationToken,
) -> bool {
    let value = match value {
        Ok(value) => value,
        Err(error) => {
            // Stream errors mean generation loss to the host. A failed status
            // read must not close customer admission on a still-live guest.
            tracing::warn!(%error, "guest update status read failed; awaiting next event");
            return !shutdown.is_cancelled() && !sender.is_closed();
        }
    };
    tokio::select! {
        biased;
        _ = shutdown.cancelled() => false,
        sent = sender.send(Ok(value)) => sent.is_ok(),
    }
}

enum StageInput {
    Chunk(wire::Chunk),
    Finish,
}

#[cfg(target_os = "linux")]
fn clock() -> Result<(Uuid, u64), UpdateError> {
    pplx_space_spaced_guest_update::boot_clock()
}
#[cfg(not(target_os = "linux"))]
fn clock() -> Result<(Uuid, u64), UpdateError> {
    Err(UpdateError::Layout)
}

impl UpdateRuntime {
    async fn disk<T: Send + 'static>(
        &self,
        work: impl FnOnce(SlotStore) -> Result<T, UpdateError> + Send + 'static,
    ) -> Result<T, Status> {
        let root = self.root.clone();
        let permit = self.disk_serial.clone().lock_owned().await;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            work(SlotStore::open(&root)?)
        })
        .await
        .map_err(|e| Status::internal(format!("update worker failed: {e}")))?
        .map_err(status)
    }
}

impl SpacedGuestService {
    pub(crate) async fn prepare_update_startup(&self) -> Result<(), Status> {
        if !cfg!(target_os = "linux") {
            return Ok(());
        }
        match tokio::fs::symlink_metadata(self.inner.update.root.join("committed")).await {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(Status::internal(e.to_string())),
            Ok(_) => {}
        }
        let incarnation = self
            .inner
            .service_generation
            .parse()
            .map_err(|_| Status::internal("invalid service incarnation"))?;
        let clock = self.inner.update.clock;
        let (boot, trial) = self
            .inner
            .update
            .disk(move |store| {
                let (boot, now) = clock()?;
                Ok((boot, store.adopt_trial(incarnation, boot, now)?))
            })
            .await?;
        self.inner
            .update
            .boot
            .set(boot)
            .map_err(|_| Status::internal("update startup called twice"))?;
        if let Some(attempt) = trial {
            // No listeners exist yet. Exercise the real filesystem worker before
            // closing admission; this read creates no customer retry obligation.
            use pplx_space_spaced_common::proto::bridge_guest::filesystem_server::Filesystem;
            let response = tokio::time::timeout(
                Duration::from_secs(10),
                self.inner
                    .execution
                    .filesystem_service()
                    .stat_info(Request::new(
                        pplx_space_spaced_common::proto::bridge_guest::StatRequest {
                            path: "/".into(),
                            user: None,
                        },
                    )),
            )
            .await
            .map_err(|_| Status::deadline_exceeded("candidate filesystem self-test timed out"))??;
            if response.into_inner().info.is_none() {
                return Err(Status::internal(
                    "candidate filesystem self-test omitted root",
                ));
            }
            *self.inner.update.hold.lock().await =
                Some(self.inner.execution.hold_for_update(attempt.operation)?);
            self.inner.update.gated.store(true, Ordering::Release);
            self.inner
                .update
                .disk(move |store| store.self_test_passed(&attempt))
                .await?;
        }
        self.inner.update.enabled.store(true, Ordering::Release);
        Ok(())
    }

    fn require_updates(&self) -> Result<(), Status> {
        if !self.inner.update.enabled.load(Ordering::Acquire) {
            return Err(Status::unimplemented(
                "installed guest has no qualified update contract",
            ));
        }
        Ok(())
    }

    async fn update_status(&self) -> Result<wire::Status, Status> {
        self.require_updates()?;
        let root = self.inner.update.root.clone();
        // Short reads queue behind short writes instead of making a durable
        // mutation fail Busy. The permit lives inside the blocking job so RPC
        // cancellation cannot release it before the filesystem lock is gone.
        let permit = self.inner.update.disk_serial.clone().lock_owned().await;
        let fresh = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let store = match SlotStore::open(&root) {
                Ok(store) => store,
                Err(UpdateError::Busy) => return Ok(None),
                Err(error) => return Err(error),
            };
            let journal = store.journal()?;
            let version = store.descriptor(journal.committed.slot)?.version;
            let mut status = journal_status(journal);
            status.committed_version = version.to_string();
            status.running = Some(store.running_selection()?.into());
            Ok::<_, UpdateError>(Some(status))
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .map_err(status)?;
        // Staging holds the writer lock while bytes arrive. Metadata must stay
        // available during that transfer, or the host would mistake staging
        // for a crash and interrupt ordinary customer work. A stale observation
        // cannot authorize a mutation: each mutation rechecks the disk fence.
        let mut result = {
            let mut cached = self
                .inner
                .update
                .cached_status
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(fresh) = fresh {
                *cached = Some(fresh);
            }
            cached
                .clone()
                .ok_or_else(|| Status::unavailable("initial update status is not available"))?
        };
        result.boot_id = self
            .inner
            .update
            .boot
            .get()
            .ok_or_else(|| Status::internal("missing boot identity"))?
            .to_string();
        result.incarnation = self.inner.service_generation.clone();
        result.forward_ports = self.inner.loopback_bridge.authorized_ports().await;
        result.boot_time_ms = (self.inner.update.clock)().map_err(status)?.1;
        result.customer_admission_held = self.inner.update.hold.lock().await.is_some();
        result.defer_reasons = self
            .update_blockers()
            .await
            .into_iter()
            .map(|v| v as i32)
            .collect();
        Ok(result)
    }

    async fn update_owned<F, Fut>(&self, action: F) -> Result<Response<wire::Status>, Status>
    where
        F: FnOnce(Self) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<wire::Status, Status>> + Send + 'static,
    {
        self.require_updates()?;
        let permit = self
            .inner
            .update
            .serial
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::unavailable("another update transition is active"))?;
        let service = self.clone();
        self.inner
            .update
            .tasks
            .spawn(async move {
                let _permit = permit;
                let result = action(service.clone()).await;
                service
                    .inner
                    .update
                    .events
                    .send_modify(|v| *v = v.wrapping_add(1));
                result.map(Response::new)
            })
            .await
            .map_err(|e| Status::internal(format!("update transition task failed: {e}")))?
    }

    async fn update_blockers(&self) -> Vec<wire::DeferReason> {
        let reasons = self.inner.execution.update_defer_reasons().await;
        #[cfg(target_os = "linux")]
        let mut reasons = reasons;
        // The leaf excludes the holder; populated includes nested descendants.
        // An unreadable cgroup cannot establish idleness, in status or Arm.
        #[cfg(target_os = "linux")]
        match tokio::fs::read_to_string(
            "/sys/fs/cgroup/space.slice/space-workloads.service/workloads/cgroup.events",
        )
        .await
        {
            Ok(events) if events.lines().any(|v| v == "populated 0") => {}
            _ => reasons.push(wire::DeferReason::Descendants),
        }
        reasons
    }

    async fn hold_update_admission(&self, owner: Uuid, force: bool) -> Result<(), Status> {
        let mut guard = self.inner.update.hold.lock().await;
        if guard.is_some() {
            return Err(Status::failed_precondition(
                "update admission is already held",
            ));
        }
        let hold = self.inner.execution.hold_for_update(owner)?;
        if !force && !self.update_blockers().await.is_empty() {
            return Err(Status::failed_precondition(
                "guest has active, retained or unverified workload obligations",
            ));
        }
        *guard = Some(hold);
        Ok(())
    }
}

#[tonic::async_trait]
impl GuestUpdate for SpacedGuestService {
    type SubscribeStream =
        std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<wire::Status, Status>> + Send>>;

    async fn subscribe(
        &self,
        _: Request<wire::StatusRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        self.require_updates()?;
        let permit = self
            .inner
            .update
            .subscribers
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                Status::resource_exhausted("guest metadata subscriber already connected")
            })?;
        let mut events = self.inner.update.events.subscribe();
        let first = self.update_status().await?;
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        sender
            .try_send(Ok(first))
            .map_err(|_| Status::internal("metadata stream setup failed"))?;
        let service = self.clone();
        self.inner.update.tasks.spawn(async move {
            let _permit = permit;
            loop {
                tokio::select! {
                    biased;
                    _ = service.inner.shutdown.cancelled() => break,
                    _ = sender.closed() => break,
                    changed = events.changed() => if changed.is_err() { break; },
                }
                let value = service.update_status().await;
                if !send_status(&sender, value, &service.inner.shutdown).await {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(receiver),
        )))
    }

    async fn get_status(
        &self,
        _: Request<wire::StatusRequest>,
    ) -> Result<Response<wire::Status>, Status> {
        Ok(Response::new(self.update_status().await?))
    }

    async fn stage(
        &self,
        request: Request<tonic::Streaming<wire::StageRequest>>,
    ) -> Result<Response<wire::Status>, Status> {
        let mut incoming = request.into_inner();
        self.update_owned(move |service| async move {
            let first = tokio::time::timeout(
                pplx_space_spaced_guest_protocol::update::STAGE_HEADER_BUDGET,
                incoming.message(),
            )
            .await
            .map_err(|_| Status::deadline_exceeded("stage header deadline"))??;
            let Some(wire::stage_request::Part::Header(header)) = first.and_then(|v| v.part) else {
                return Err(Status::invalid_argument("stage header required"));
            };
            let descriptor: BundleDescriptor = header
                .descriptor
                .ok_or_else(|| Status::invalid_argument("descriptor required"))?
                .try_into()?;
            let attempt = required_attempt(header.attempt)?;
            if Some(&attempt.boot) != service.inner.update.boot.get()
                || attempt.incarnation.to_string() != service.inner.service_generation
            {
                return Err(Status::failed_precondition(
                    "staging execution observation is stale",
                ));
            }
            let (sender, mut receiver) = tokio::sync::mpsc::channel::<StageInput>(2);
            let root = service.inner.update.root.clone();
            let permit = service.inner.update.disk_serial.clone().lock_owned().await;
            let writer = tokio::task::spawn_blocking(move || {
                let mut stage = SlotStore::open(&root)
                    .map_err(status)?
                    .begin_stage(descriptor, attempt)
                    .map_err(status)?;
                // Streaming retains the OS writer lock but lets observations
                // return cached metadata. update_owned excludes other writers.
                drop(permit);
                while let Some(input) = receiver.blocking_recv() {
                    match input {
                        StageInput::Chunk(chunk) => stage
                            .write(binary(chunk.binary)?, chunk.offset, &chunk.data)
                            .map_err(status)?,
                        StageInput::Finish => return stage.finish().map(Some).map_err(status),
                    }
                }
                // Channel closure abandons partial bytes. The receiver below
                // preserves the specific transport error that ended the stream.
                Ok(None)
            });
            let transfer = tokio::time::timeout(
                pplx_space_spaced_guest_protocol::update::STAGE_TRANSFER_BUDGET,
                async {
                    while let Some(message) = incoming.message().await? {
                        let Some(wire::stage_request::Part::Chunk(chunk)) = message.part else {
                            return Err(Status::invalid_argument("unexpected stage header"));
                        };
                        if chunk.data.is_empty() || chunk.data.len() > 64 * 1024 {
                            return Err(Status::invalid_argument("invalid stage chunk length"));
                        }
                        sender.send(StageInput::Chunk(chunk)).await.map_err(|_| {
                            Status::failed_precondition("stage writer rejected payload")
                        })?;
                    }
                    sender.send(StageInput::Finish).await.map_err(|_| {
                        Status::failed_precondition("stage writer rejected payload")
                    })?;
                    Ok::<_, Status>(())
                },
            )
            .await
            .map_err(|_| Status::deadline_exceeded("stage transfer deadline"))
            .and_then(|v| v);
            drop(sender);
            // Preserve actual disk/validation errors rather than replacing
            // them with the sender's generic "writer rejected" error.
            let written = writer
                .await
                .map_err(|e| Status::internal(e.to_string()))??;
            transfer?;
            written.ok_or_else(|| Status::cancelled("stage ended without successful EOF"))?;
            service.update_status().await
        })
        .await
    }

    async fn arm(
        &self,
        request: Request<wire::ArmRequest>,
    ) -> Result<Response<wire::Status>, Status> {
        let request = request.into_inner();
        let attempt = required_attempt(request.attempt)?;
        self.update_owned(move |service| async move {
            let existing = service.update_status().await?;
            if existing.phase == wire::Phase::Armed as i32
                && existing.attempt == Some(attempt.clone().into())
            {
                service.inner.shutdown.cancel();
                return Ok(existing);
            }
            if existing.boot_id != attempt.boot.to_string()
                || existing.incarnation != attempt.incarnation.to_string()
                || existing.boot_time_ms >= attempt.confirmation_deadline_ms
            {
                return Err(Status::failed_precondition(
                    "activation observation expired",
                ));
            }
            service
                .hold_update_admission(attempt.operation, request.force)
                .await?;
            let result = service
                .inner
                .update
                .disk(move |store| store.arm(&attempt))
                .await;
            if let Err(error) = result {
                service.inner.update.hold.lock().await.take();
                return Err(error);
            }
            let response = service.update_status().await;
            service.inner.shutdown.cancel();
            response
        })
        .await
    }

    async fn configure(
        &self,
        request: Request<wire::ConfigureRequest>,
    ) -> Result<Response<wire::Status>, Status> {
        let request = request.into_inner();
        let attempt = request.attempt.map(TryInto::try_into).transpose()?;
        let env = parse_env(request.environment);
        if request.forward_ports.len() > 128 {
            return Err(Status::invalid_argument("too many forwarded ports"));
        }
        let ports: Vec<u16> = request
            .forward_ports
            .into_iter()
            .map(|p| {
                u16::try_from(p)
                    .ok()
                    .filter(|p| *p != 0)
                    .ok_or_else(|| Status::invalid_argument("invalid forwarded port"))
            })
            .collect::<Result<_, _>>()?;
        self.update_owned(move |service| async move {
            let current = service.update_status().await?;
            let Some(attempt): Option<Attempt> = attempt else {
                if current.incarnation != request.expected_incarnation
                    || current.boot_id != request.expected_boot_id
                    || current.running != current.committed
                    || matches!(
                        wire::Phase::try_from(current.phase),
                        Ok(wire::Phase::Trial | wire::Phase::Armed | wire::Phase::Restarting)
                    )
                {
                    return Err(Status::failed_precondition(
                        "committed configuration observation changed",
                    ));
                }
                let owner = request
                    .owner
                    .ok_or_else(|| Status::invalid_argument("configuration owner required"))?;
                let operation = owner
                    .operation_id
                    .parse::<Uuid>()
                    .map_err(|_| Status::invalid_argument("invalid configuration owner"))?;
                let execution = owner
                    .execution_id
                    .parse::<Uuid>()
                    .map_err(|_| Status::invalid_argument("invalid configuration execution"))?;
                if execution.is_nil() {
                    return Err(Status::invalid_argument("invalid configuration execution"));
                }
                let hold = service.inner.execution.hold_for_update(operation)?;
                service
                    .inner
                    .execution
                    .configure_process_env_for_update(&hold, env)
                    .await?;
                service.inner.loopback_bridge.replace_ports(&ports).await?;
                drop(hold);
                return service.update_status().await;
            };
            if current.phase != wire::Phase::Trial as i32
                || current.attempt != Some(attempt.clone().into())
            {
                return Err(Status::failed_precondition(
                    "configuration trial fence changed",
                ));
            }
            let guard = service.inner.update.hold.lock().await;
            let hold = guard
                .as_ref()
                .ok_or_else(|| Status::failed_precondition("trial admission is not held"))?;
            service
                .inner
                .execution
                .configure_process_env_for_update(hold, env)
                .await?;
            service.inner.loopback_bridge.replace_ports(&ports).await?;
            *service.inner.update.configured.lock().await = Some(attempt);
            drop(guard);
            service.update_status().await
        })
        .await
    }

    async fn commit(
        &self,
        request: Request<wire::CommitRequest>,
    ) -> Result<Response<wire::Status>, Status> {
        let attempt = required_attempt(request.into_inner().attempt)?;
        self.update_owned(move |service| async move {
            let current = service.update_status().await?;
            let committed = current.phase == wire::Phase::Committed as i32
                && current.attempt == Some(attempt.clone().into());
            if !committed && service.inner.update.configured.lock().await.as_ref() != Some(&attempt)
            {
                return Err(Status::failed_precondition(
                    "trial configuration has not been acknowledged",
                ));
            }
            let clock = service.inner.update.clock;
            service
                .inner
                .update
                .disk(move |store| {
                    let (boot, now) = clock()?;
                    store.commit(&attempt, boot, now)
                })
                .await?;
            service.inner.update.hold.lock().await.take();
            service.inner.update.gated.store(false, Ordering::Release);
            service.ready();
            service.update_status().await
        })
        .await
    }

    async fn abort(
        &self,
        request: Request<wire::AbortRequest>,
    ) -> Result<Response<wire::Status>, Status> {
        let attempt = required_attempt(request.into_inner().attempt)?;
        self.update_owned(move |service| async move {
            let journal = service
                .inner
                .update
                .disk(move |store| store.abort(&attempt))
                .await?;
            let terminal_commit = matches!(journal.state, JournalState::Committed { .. });
            let response = service.update_status().await;
            if !terminal_commit && service.inner.update.trial_gated() {
                service.inner.shutdown.cancel();
            } else if !terminal_commit {
                service.inner.update.hold.lock().await.take();
            }
            response
        })
        .await
    }

    async fn restart(
        &self,
        request: Request<wire::RestartRequest>,
    ) -> Result<Response<wire::Status>, Status> {
        let request = request.into_inner();
        let parse = |value: &str| {
            value
                .parse::<Uuid>()
                .map_err(|_| Status::invalid_argument("invalid restart identity"))
        };
        let restart = pplx_space_spaced_update::journal::Restart {
            operation: parse(&request.operation_id)?,
            execution: parse(&request.execution_id)?,
            boot: parse(&request.expected_boot_id)?,
            incarnation: parse(&request.expected_incarnation)?,
        };
        self.update_owned(move |service| async move {
            let current = service.update_status().await?;
            if let Some(receipt) = &current.restart
                && receipt.operation_id == request.operation_id
                && receipt.execution_id == request.execution_id
            {
                if receipt.boot_id != request.expected_boot_id
                    || receipt.previous_incarnation != request.expected_incarnation
                {
                    return Err(Status::failed_precondition(
                        "restart operation identity was reused",
                    ));
                }
                if current.phase == wire::Phase::Restarting as i32 {
                    service.inner.shutdown.cancel();
                }
                return Ok(current);
            }
            if current.incarnation != request.expected_incarnation
                || current.boot_id != request.expected_boot_id
            {
                return Err(Status::failed_precondition("restart observation is stale"));
            }
            service
                .hold_update_admission(restart.operation, request.force)
                .await?;
            if let Err(error) = service
                .inner
                .update
                .disk(move |store| store.restart(restart))
                .await
            {
                service.inner.update.hold.lock().await.take();
                return Err(error);
            }
            let response = service.update_status().await;
            service.inner.shutdown.cancel();
            response
        })
        .await
    }
}

#[cfg(test)]
mod tests;
