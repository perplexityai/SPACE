//! Operation-owned host admission and pre-readiness maintenance transport.
use super::*;
use pplx_space_spaced_common::proto::{
    bridge_guest::guest_update_client::GuestUpdateClient, update as wire,
};
use proto::guest_update_server::GuestUpdate;
use std::sync::Mutex;
use tokio::sync::{Notify, Semaphore, watch};
use tokio::time::Instant as Deadline;

use pplx_space_spaced_common::update::ACTIVATION_BUDGET as ADMISSION_WAIT_BUDGET;

// Only an explicit Finish represents upstream EOF. Dropping a forwarding
// task (input error, timeout or cancellation) must reject the guest upload.
enum StageForward {
    Message(wire::StageRequest),
    Finish,
}

fn stage_forward_stream(
    receiver: tokio::sync::mpsc::Receiver<StageForward>,
) -> impl futures_util::Stream<Item = wire::StageRequest> {
    futures_util::stream::unfold((receiver, false), |(mut receiver, ended)| async move {
        if ended {
            return None;
        }
        match receiver.recv().await {
            Some(StageForward::Message(message)) => Some((message, (receiver, false))),
            Some(StageForward::Finish) => None,
            None => Some((wire::StageRequest { part: None }, (receiver, true))),
        }
    })
}

async fn forward_stage(
    mut input: impl futures_util::Stream<Item = Result<wire::StageRequest, Status>> + Unpin,
    sender: tokio::sync::mpsc::Sender<StageForward>,
) -> Result<(), Status> {
    use futures_util::TryStreamExt;
    while let Some(message) = input.try_next().await? {
        if sender.send(StageForward::Message(message)).await.is_err() {
            // The guest may reject before draining the upload. Its RPC owns
            // the response; a local channel close must not mask that status.
            return Ok(());
        }
    }
    if sender.send(StageForward::Finish).await.is_err() {
        return Ok(());
    }
    Ok(())
}

use pplx_space_spaced_common::update::HoldOwner as Owner;

#[derive(Default)]
struct Admission {
    owner: Option<Owner>,
    hold_deadline: Option<Deadline>,
    active: usize,
    configuring: bool,
    incarnation: Option<String>,
    configured_revision: Option<u64>,
    paused: bool,
}

impl Admission {
    fn hold(&mut self, owner: Owner) -> Result<(), Status> {
        if self
            .owner
            .as_ref()
            .is_some_and(|current| !owner.may_replace(current))
        {
            return Err(Status::failed_precondition(
                "another operation owns admission",
            ));
        }
        // Repeating Hold adopts the existing deadline, never a fresh wait budget.
        if self.owner.as_ref() != Some(&owner) {
            self.hold_deadline = (owner.operation != owner.execution)
                .then(|| Deadline::now() + ADMISSION_WAIT_BUDGET);
        }
        self.owner = Some(owner);
        Ok(())
    }

    fn observe_incarnation(&mut self, incarnation: &str) {
        match self.incarnation.as_deref() {
            None => self.incarnation = Some(incarnation.to_owned()),
            Some(seen) if seen != incarnation => {
                self.configuring = true;
                self.configured_revision = None;
            }
            _ => {}
        }
    }
    fn annotate_status(&mut self, value: &mut wire::Status, observe_generation: bool) {
        if observe_generation {
            self.observe_incarnation(&value.incarnation);
        }
        value.customer_admission_held |= self.customer_admission_held();
        if self.active != 0 {
            value
                .defer_reasons
                .push(wire::DeferReason::Operations as i32);
        }
    }

    fn customer_admission_held(&self) -> bool {
        self.owner.is_some() || self.configuring
    }
}

/// A new host adopts surviving guest forwarding before replacing configuration.
/// Preserve opens/closes racing that first observation; thereafter the host's
/// remembered intent survives a guest-only restart with an empty port list.
#[derive(Default)]
struct Forwarding {
    desired: Option<std::collections::BTreeSet<u32>>,
    pending: std::collections::BTreeMap<u32, bool>,
}
impl Forwarding {
    fn remember(&mut self, port: u32, open: bool) {
        if let Some(ports) = &mut self.desired {
            if open {
                ports.insert(port);
            } else {
                ports.remove(&port);
            }
        } else {
            self.pending.insert(port, open);
        }
    }
    fn observe(&mut self, guest: &[u32]) -> Vec<u32> {
        let ports = self.desired.get_or_insert_with(|| {
            let mut ports: std::collections::BTreeSet<_> = guest.iter().copied().collect();
            for (port, open) in std::mem::take(&mut self.pending) {
                if open {
                    ports.insert(port);
                } else {
                    ports.remove(&port);
                }
            }
            ports
        });
        ports.iter().copied().collect()
    }
}

pub(super) struct HostUpdate {
    admission: Arc<Mutex<Admission>>,
    transition: Arc<Semaphore>,
    subscriber: Arc<Semaphore>,
    changed: Notify,
    admission_changed: watch::Sender<()>,
    tasks: tokio_util::task::TaskTracker,
    ports: Mutex<Forwarding>,
}

impl HostUpdate {
    pub(super) fn new() -> Self {
        Self {
            admission: Arc::default(),
            transition: Arc::new(Semaphore::new(1)),
            subscriber: Arc::new(Semaphore::new(1)),
            changed: Notify::new(),
            admission_changed: watch::channel(()).0,
            tasks: tokio_util::task::TaskTracker::new(),
            ports: Mutex::default(),
        }
    }

    pub(super) async fn drain(&self) {
        self.tasks.close();
        self.tasks.wait().await;
    }

    pub(super) fn remember_port(&self, port: u32, open: bool) {
        self.ports
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remember(port, open);
    }

    fn forwarding(&self, guest: &[u32]) -> Vec<u32> {
        self.ports
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .observe(guest)
    }

    /// Only operation-owned holds wait; ordinary generation configuration keeps
    /// its existing refusal behavior. Waiters own no guest dispatch or payload.
    pub(super) async fn admit_when_ready(
        &self,
        caller_deadline: Option<Deadline>,
        shutdown: &CancellationToken,
    ) -> Result<DispatchGuard, Status> {
        let mut changed = self.admission_changed.subscribe();
        let mut deadline = caller_deadline.unwrap_or(Deadline::now() + ADMISSION_WAIT_BUDGET);
        let mut waiting_for: Option<Owner> = None;
        loop {
            if shutdown.is_cancelled() {
                return Err(Status::cancelled("host is shutting down"));
            }
            if Deadline::now() >= deadline {
                return Err(admission_wait_expired(caller_deadline));
            }
            {
                let mut state = self
                    .admission
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                // A waiter cannot acquire a bridge after capture quiescence.
                // Ordinary dispatch retains the existing lifecycle routing rule.
                if state.paused && (waiting_for.is_some() || state.customer_admission_held()) {
                    return Err(admission_refused(
                        "guest lifecycle changed during maintenance",
                    ));
                }
                if !state.customer_admission_held() {
                    state.active += 1;
                    return Ok(DispatchGuard(self.admission.clone()));
                }
                let Some(owner) = state
                    .owner
                    .as_ref()
                    .filter(|owner| owner.operation != owner.execution)
                else {
                    return Err(admission_refused("guest configuration admission is held"));
                };
                if waiting_for
                    .as_ref()
                    .is_some_and(|previous| previous != owner)
                {
                    return Err(admission_refused("guest maintenance owner changed"));
                }
                waiting_for = Some(owner.clone());
                deadline =
                    deadline.min(state.hold_deadline.ok_or_else(|| {
                        admission_refused("guest maintenance has no wait budget")
                    })?);
            }
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return Err(Status::cancelled("host is shutting down")),
                _ = tokio::time::sleep_until(deadline) => return Err(admission_wait_expired(caller_deadline)),
                result = changed.changed() => {
                    if result.is_err() {
                        return Err(admission_refused("guest admission closed"));
                    }
                },
            }
        }
    }

    fn require_owner(&self, owner: &Owner) -> Result<(), Status> {
        if self
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .owner
            .as_ref()
            != Some(owner)
        {
            return Err(Status::failed_precondition(
                "update does not own host admission",
            ));
        }
        Ok(())
    }

    pub(super) async fn pause_observation(&self) -> Result<ObservationPause<'_>, Status> {
        let was_paused = {
            let mut state = self
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::replace(&mut state.paused, true)
        };
        // The guard also covers cancellation while awaiting quiescence or the
        // caller's filesystem flush. Only successful capture preparation keeps
        // the pause until NotifyResume.
        let pause = ObservationPause((!was_paused).then_some(self));
        self.changed.notify_one();
        self.admission_changed.send_replace(());
        let permit = tokio::time::timeout(
            Duration::from_secs(5),
            self.subscriber.clone().acquire_owned(),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("metadata subscription did not quiesce"))?
        .map_err(|_| Status::unavailable("metadata subscription closed"))?;
        drop(permit);
        Ok(pause)
    }

    async fn until_observation_pauses<T>(
        &self,
        work: impl std::future::Future<Output = T>,
        shutdown: &CancellationToken,
    ) -> Result<T, Status> {
        tokio::pin!(work);
        loop {
            let changed = self.changed.notified();
            if self
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .paused
            {
                return Err(Status::failed_precondition(
                    "metadata observation is paused for lifecycle capture",
                ));
            }
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return Err(Status::cancelled("host is shutting down")),
                _ = changed => continue,
                value = &mut work => return Ok(value),
            }
        }
    }

    async fn forward_observation(
        &self,
        mut stream: impl futures_util::Stream<Item = Result<wire::Status, Status>> + Unpin,
        sender: tokio::sync::mpsc::Sender<Result<wire::Status, Status>>,
        shutdown: &CancellationToken,
    ) {
        loop {
            let changed = self.changed.notified();
            if self
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .paused
            {
                break;
            }
            let next = tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                _ = sender.closed() => break,
                _ = changed => continue,
                value = futures_util::StreamExt::next(&mut stream) => value,
            };
            match next {
                Some(Ok(mut value)) => {
                    {
                        let mut state = self
                            .admission
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        state.annotate_status(&mut value, true);
                    }
                    value.forward_ports = self.forwarding(&value.forward_ports);
                    if !self
                        .until_observation_pauses(sender.send(Ok(value)), shutdown)
                        .await
                        .is_ok_and(|sent| sent.is_ok())
                    {
                        break;
                    }
                }
                _ => {
                    // A transport disconnect is not evidence of a guest
                    // restart. The next observed incarnation decides that.
                    let _ = sender.try_send(Err(Status::unavailable(
                        "guest metadata generation disconnected",
                    )));
                    break;
                }
            }
        }
    }

    pub(super) fn resume_observation(&self) {
        self.admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .paused = false;
        self.changed.notify_one();
    }

    pub(super) fn capture_transition(&self) -> Result<tokio::sync::OwnedSemaphorePermit, Status> {
        self.transition
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::unavailable("another maintenance or capture transition is active"))
    }
}

fn admission_refused(message: &'static str) -> Status {
    pplx_space_common_grpc::refused_before_start(
        Status::unavailable(message),
        Duration::from_secs(1),
    )
}

fn admission_wait_expired(caller_deadline: Option<Deadline>) -> Status {
    let status = if caller_deadline.is_some_and(|deadline| Deadline::now() >= deadline) {
        Status::deadline_exceeded("request deadline elapsed before guest dispatch")
    } else {
        Status::unavailable("guest maintenance wait budget exhausted")
    };
    pplx_space_common_grpc::refused_before_start(status, Duration::from_secs(1))
}

pub(super) struct ObservationPause<'a>(Option<&'a HostUpdate>);
impl ObservationPause<'_> {
    pub(super) fn keep_paused(mut self) {
        self.0.take();
    }
}
impl Drop for ObservationPause<'_> {
    fn drop(&mut self) {
        if let Some(update) = self.0.take() {
            update.resume_observation();
        }
    }
}

pub(super) struct DispatchGuard(Arc<Mutex<Admission>>);
impl Drop for DispatchGuard {
    fn drop(&mut self) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active -= 1;
    }
}

impl SpacedHostService {
    async fn update_client(
        &self,
    ) -> Result<
        (
            GuestUpdateClient<tonic::transport::Channel>,
            Option<pplx_space_spaced_common::PinnedConnection>,
        ),
        Status,
    > {
        use pplx_space_common_core::types::guest_image::QualifiedGuestAgent;
        if !matches!(
            self.inner.guest_agent,
            QualifiedGuestAgent::LinuxV1 | QualifiedGuestAgent::WindowsV1
        ) {
            return Err(Status::unimplemented(
                "guest update requires a qualified native guest contract",
            ));
        }
        let (channel, pinned) =
            tokio::time::timeout(Duration::from_secs(5), self.connect_bridge_guest_channel())
                .await
                .map_err(|_| Status::deadline_exceeded("maintenance connection deadline"))?
                .map_err(|e| Status::unavailable(e.to_string()))?;
        Ok((
            GuestUpdateClient::new(channel)
                .max_decoding_message_size(128 * 1024)
                .max_encoding_message_size(128 * 1024),
            pinned,
        ))
    }

    pub(super) async fn guest_update_status(&self) -> Result<wire::Status, Status> {
        self.read_guest_update_status(true).await
    }

    pub(super) async fn read_guest_update_status(
        &self,
        observe_generation: bool,
    ) -> Result<wire::Status, Status> {
        let (mut client, _pinned) = self.update_client().await?;
        let mut request = Request::new(wire::StatusRequest {});
        request.set_timeout(Duration::from_secs(5));
        let mut result = client.get_status(request).await?.into_inner();
        result.forward_ports = self.inner.update.forwarding(&result.forward_ports);
        let mut state = self
            .inner
            .update
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Capture is a read-only check, including pool/builder executions that
        // have no configuration observer to release a newly latched hold.
        state.annotate_status(&mut result, observe_generation);
        Ok(result)
    }

    fn update_owner(&self, attempt: Option<&wire::Attempt>) -> Result<Owner, Status> {
        let attempt = attempt.ok_or_else(|| Status::invalid_argument("missing update attempt"))?;
        let owner = Owner::try_from(&wire::HoldRequest {
            operation_id: attempt.operation_id.clone(),
            execution_id: attempt.execution_id.clone(),
        })?;
        Owner::validate_operation(owner.operation, owner.execution)?;
        self.inner.update.require_owner(&owner)?;
        Ok(owner)
    }
}

#[tonic::async_trait]
impl GuestUpdate for SpacedHostService {
    type SubscribeStream =
        std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<wire::Status, Status>> + Send>>;

    async fn get_status(
        &self,
        _: Request<wire::StatusRequest>,
    ) -> Result<Response<wire::Status>, Status> {
        Ok(Response::new(self.guest_update_status().await?))
    }

    async fn hold(
        &self,
        request: Request<wire::HoldRequest>,
    ) -> Result<Response<wire::Status>, Status> {
        let owner = Owner::try_from(request.get_ref())?;
        {
            let mut state = self
                .inner
                .update
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.hold(owner)?;
        }
        self.inner.update.admission_changed.send_replace(());
        // A failed probe cannot release an accepted hold; the same manager
        // operation reconciles it explicitly after transport recovery.
        Ok(Response::new(self.guest_update_status().await?))
    }

    async fn release(
        &self,
        request: Request<wire::ReleaseRequest>,
    ) -> Result<Response<wire::Status>, Status> {
        let request = request.into_inner();
        let owner = Owner::try_from(
            request
                .owner
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("missing hold owner"))?,
        )?;
        self.inner.update.require_owner(&owner)?;
        let result = self.guest_update_status().await?;
        if result.incarnation != request.expected_incarnation
            || result.running.is_none()
            || result.running != result.committed
            || matches!(
                wire::Phase::try_from(result.phase),
                Ok(wire::Phase::Trial
                    | wire::Phase::Armed
                    | wire::Phase::Restarting
                    | wire::Phase::Unspecified)
                    | Err(_)
            )
        {
            return Err(Status::failed_precondition(
                "guest is not the settled configured generation",
            ));
        }
        {
            let mut state = self
                .inner
                .update
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.owner.as_ref() != Some(&owner) {
                return Err(Status::failed_precondition("hold owner changed"));
            }
            if state.incarnation.as_deref() != Some(&request.expected_incarnation)
                || state.configured_revision != Some(request.configuration_revision)
            {
                return Err(Status::failed_precondition(
                    "guest configuration has not been acknowledged",
                ));
            }
            state.owner = None;
            state.hold_deadline = None;
            state.configuring = false;
        }
        self.inner.update.admission_changed.send_replace(());
        Ok(Response::new(self.guest_update_status().await?))
    }

    async fn stage(
        &self,
        request: Request<tonic::Streaming<wire::StageRequest>>,
    ) -> Result<Response<wire::Status>, Status> {
        // Staging itself does not hold customer traffic. Manager serialization
        // and the guest's one-writer lock own it; no activation is implied.
        let _permit = self
            .inner
            .update
            .transition
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::unavailable("update transfer already active"))?;
        let (mut client, _pinned) = self.update_client().await?;
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        let forward = forward_stage(request.into_inner(), sender);
        let call = client.stage(stage_forward_stream(receiver));
        let transfer = async { tokio::try_join!(forward, call).map(|(_, response)| response) };
        // Guest header + streaming allow 130s; leave verification/flush margin.
        // Slow local I/O can still finish after this deadline: reconcile status.
        tokio::time::timeout(
            pplx_space_spaced_common::update::STAGE_HOST_BUDGET,
            transfer,
        )
        .await
        .map_err(|_| Status::deadline_exceeded("host stage deadline"))?
    }

    async fn arm(
        &self,
        request: Request<wire::ArmRequest>,
    ) -> Result<Response<wire::Status>, Status> {
        self.update_owner(request.get_ref().attempt.as_ref())?;
        if !request.get_ref().force
            && self
                .inner
                .update
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .active
                != 0
        {
            return Err(Status::failed_precondition(
                "host dispatches remain in flight",
            ));
        }
        let (mut client, _pinned) = self.update_client().await?;
        client.arm(request).await
    }

    async fn configure(
        &self,
        request: Request<wire::ConfigureRequest>,
    ) -> Result<Response<wire::Status>, Status> {
        let revision = if let Some(attempt) = request.get_ref().attempt.as_ref() {
            self.update_owner(Some(attempt))?;
            attempt.configuration_revision
        } else {
            let owner = Owner::try_from(
                request
                    .get_ref()
                    .owner
                    .as_ref()
                    .ok_or_else(|| Status::invalid_argument("missing configuration owner"))?,
            )?;
            self.inner.update.require_owner(&owner)?;
            request.get_ref().configuration_revision
        };
        let ports = request.get_ref().forward_ports.clone();
        let (mut client, _pinned) = self.update_client().await?;
        let result = client.configure(request).await?;
        *self
            .inner
            .update
            .ports
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Forwarding {
            desired: Some(ports.into_iter().collect()),
            pending: Default::default(),
        };
        let mut state = self
            .inner
            .update
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.incarnation = Some(result.get_ref().incarnation.clone());
        state.configured_revision = Some(revision);
        Ok(result)
    }

    async fn commit(
        &self,
        request: Request<wire::CommitRequest>,
    ) -> Result<Response<wire::Status>, Status> {
        self.update_owner(request.get_ref().attempt.as_ref())?;
        let (mut client, _pinned) = self.update_client().await?;
        client.commit(request).await
    }

    async fn abort(
        &self,
        request: Request<wire::AbortRequest>,
    ) -> Result<Response<wire::Status>, Status> {
        self.update_owner(request.get_ref().attempt.as_ref())?;
        let (mut client, _pinned) = self.update_client().await?;
        client.abort(request).await
    }

    async fn restart(
        &self,
        request: Request<wire::RestartRequest>,
    ) -> Result<Response<wire::Status>, Status> {
        let owner = Owner::try_from(&wire::HoldRequest {
            operation_id: request.get_ref().operation_id.clone(),
            execution_id: request.get_ref().execution_id.clone(),
        })?;
        Owner::validate_operation(owner.operation, owner.execution)?;
        self.inner.update.require_owner(&owner)?;
        if !request.get_ref().force
            && self
                .inner
                .update
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .active
                != 0
        {
            return Err(Status::failed_precondition(
                "host dispatches remain in flight",
            ));
        }
        let (mut client, _pinned) = self.update_client().await?;
        client.restart(request).await
    }

    async fn subscribe(
        &self,
        _: Request<wire::StatusRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let permit = self
            .inner
            .update
            .subscriber
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("metadata subscriber already connected"))?;
        let setup = async {
            let (mut client, pinned) = self.update_client().await?;
            let initial = tokio::time::timeout(
                Duration::from_secs(5),
                client.subscribe(wire::StatusRequest {}),
            )
            .await
            .map_err(|_| Status::deadline_exceeded("guest metadata subscription deadline"))??
            .into_inner();
            Ok::<_, Status>((initial, pinned))
        };
        let (initial, pinned) = self
            .inner
            .update
            .until_observation_pauses(setup, &self.inner.shutdown)
            .await??;
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        let service = self.clone();
        // The response receiver owns cancellation; shutdown and PrepareSuspend
        // also end acquisition before bridge invalidation/capture.
        self.inner.update.tasks.spawn(async move {
            let _permit = permit;
            let _pinned = pinned;
            service
                .inner
                .update
                .forward_observation(initial, sender, &service.inner.shutdown)
                .await;
        });
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(receiver),
        )))
    }
}

#[cfg(test)]
mod tests;
