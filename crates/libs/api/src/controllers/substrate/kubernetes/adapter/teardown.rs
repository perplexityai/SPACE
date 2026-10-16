//! Delete flow: pod identity fences, fenced reads, and execution teardown,
//! including retirement when no active execution remains.

use super::*;

/// The address a fenced teardown may send its in-VM stop to, derived from the
/// execution row and the pod the fence check already verified.
///
/// This exists because `resolve_pod_ip` is not read-only: on an absent or
/// terminal pod it deletes the object and releases generation tracking, and on
/// a metadata drift it persists replacement identity. Calling it from a fenced
/// teardown lets those mutations run against whatever is current *before* the
/// fence's own preconditioned delete gets a say — the exact ordering the fence
/// exists to prevent. It also asks the database and the apiserver for a row
/// and a pod the fence check just read.
///
/// So the fenced path derives the address itself, from values already in hand,
/// applying the same filters `resolve_pod_ip` applies and none of its writes.
/// Every filter resolves to "no runtime to stop", which is always safe: the
/// teardown proceeds to the pod delete instead of talking to a VM.
fn fenced_runtime_pod_ip(
    lifecycle: &str,
    state: ActiveExecutionState,
    live_pod: Option<&SandboxPodView>,
) -> Option<PodIp> {
    if is_suspended_lifecycle(lifecycle) || execution_pod_lookup_in_flight(state) {
        return None;
    }
    let live_pod = live_pod?;
    if live_pod.is_deleting()
        || terminal_pod_phase(live_pod).is_some()
        || live_pod.unschedulable_message().is_some()
    {
        return None;
    }
    assigned_pod_ip(live_pod).ok().flatten()
}

/// Project the world as it is now into the same shape the fence carries, so
/// the two can be compared as whole values rather than field by field. One
/// constructor, so the comparison and every database guard read the same
/// fields.
fn current_execution_identity(
    active: &ActiveExecutionStatusRow,
    live_pod: Option<&SandboxPodView>,
) -> ExecutionIdentity {
    ExecutionIdentity {
        pod_name: PodName::parse(&active.pod_name)
            // The column is written by `sandbox_pod_name`, so this cannot
            // normally fail; a value the newtype rejects is schema drift, and
            // an unparseable name matches no fence, which fails closed.
            .unwrap_or_else(|_| PodName::parse("space-sbx-unparseable").expect("static pod name")),
        generation: Some(active.generation),
        activation_attempt_id: active.activation_attempt_id,
        recorded: RecordedExecution::Read {
            pod_uid: active.pod_uid.clone(),
            route_activation_id: active.route_activation_id,
        },
        pod: match live_pod {
            None => ObservedPod::Absent,
            Some(pod) => match pod.uid().and_then(|uid| PodUid::parse(uid).ok()) {
                Some(pod_uid) => ObservedPod::Seen {
                    pod_uid,
                    resource_version: pod
                        .resource_version()
                        .and_then(|version| PodResourceVersion::parse(version).ok()),
                    terminal: terminal_pod_phase(pod).is_some(),
                },
                // A live object we cannot identify is not something any fence
                // can be proven against.
                None => ObservedPod::Unobserved,
            },
        },
    }
}

/// What a passing pod-identity fence check proved, carried forward so the
/// teardown's destructive steps are pinned to the execution the caller
/// observed instead of re-asking what is active by the time each one runs.
/// How current the pod read behind a fence check has to be.
///
/// The teardown path may trust the reflector for a pod it will not stop,
/// because its own delete carries the identity as a precondition and so
/// discovers a stale read before mutating anything. A caller-side probe has no
/// such backstop: its answer authorizes a state transition that commits
/// *before* any delete runs, so a cached pod that the apiserver has already
/// replaced would approve a fence the transition then cannot be followed
/// through on. These probes are rare — only revert and reap candidates reach
/// one — so the extra read is bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FenceReadFreshness {
    CacheIsEnough,
    MustBeAuthoritative,
}

#[derive(Debug)]
pub(super) struct FencedTeardown {
    /// The identity the fence matched, as the *row* holds it. Carried whole so
    /// both database guards bind the same predicate, and so the UID they bind
    /// is the row's — not the live pod's, which is absent precisely when the
    /// cleanup is most needed.
    identity: ExecutionIdentity,
    /// The address the in-VM stop may use, derived from the pod the fence
    /// verified rather than from whatever is active at stop time. `None` means
    /// there is no runtime to stop and the teardown goes straight to the pod
    /// delete.
    runtime_pod_ip: Option<PodIp>,
    /// The pod the check *proved* is there, if any — which is what the delete
    /// carries as its precondition.
    ///
    /// Read from the object the comparison just accepted rather than from the
    /// caller's fence, because those differ exactly where it matters: the
    /// filtered reflector drops terminal pods, so a terminal pod always
    /// arrives behind a name-only fence, and taking the precondition from the
    /// fence would leave it undeletable forever. Deriving it here also keeps
    /// "do we have an identity to delete under" from being a second, separate
    /// judgement that can disagree with the fence check.
    verified_pod_uid: Option<PodUid>,
}

pub(super) enum PodFenceCheck {
    /// Boxed: this variant is much larger than the other two, and the enum is
    /// returned by value from every fence check.
    Holds(Box<FencedTeardown>),
    /// No active execution at all. The retire's goal is already met — another
    /// teardown or a suspend finalizer got there first — so this is success,
    /// not supersession. Calling it a mismatch would make a compensating
    /// caller undo a transition that was correct, which is the opposite of
    /// what the fence is for. Same reading as
    /// [`Self::classify_lost_tracking_release`]'s `None` arm.
    AlreadyRetired,
    Mismatch(PodFenceMismatch),
}

impl KubernetesSubstrate {
    /// Check a caller's pod-identity fence before anything destructive runs.
    ///
    /// Two questions, two authorities. The control plane answers "is the
    /// fenced pod still the active execution?" — a successor generation lives
    /// under a different pod name, so no precondition attached to the observed
    /// pod would ever see it. The apiserver answers "is the object at that
    /// name still the one the caller saw?" via UID and, when supplied,
    /// resourceVersion.
    ///
    /// Running this before the in-VM stop is the point: the fence exists to
    /// keep a retire off a sandbox that recovered, and by the time the pod
    /// delete carries its precondition the VM has already been stopped and its
    /// disk backed up. The delete's precondition still matters — it closes the
    /// gap between this check and the delete — but it cannot be the only gate.
    ///
    /// A passing check hands back the facts every later step is pinned to, so
    /// none of them has to re-ask "what is active now?" and get the successor
    /// as an answer.
    pub(super) async fn check_pod_fence(
        &self,
        sandbox_id: SandboxId,
        fence: &PodIdentityFence,
        freshness: FenceReadFreshness,
    ) -> Result<PodFenceCheck, SubstrateError> {
        // One read answers "is the fenced pod still the active execution?" and
        // supplies the generation and runtime the rest of the teardown needs.
        let Some(active) = self.store.active_execution_status(sandbox_id).await? else {
            return Ok(PodFenceCheck::AlreadyRetired);
        };
        let live_pod = self
            .read_fenced_pod(&active.lifecycle, active.state, &fence.pod_name, freshness)
            .await?;
        // One comparison, over the whole identity. Every earlier bug on this
        // path was a site that compared a different subset of these fields, or
        // read one of them being absent as "no expectation here".
        let current = current_execution_identity(&active, live_pod.as_deref());
        if let Err(mismatch) = fence.observed().reconcile(&current) {
            if mismatch == PodFenceMismatch::PodIdentityUnverified
                && let Some(observed_uid) = live_pod.as_deref().and_then(SandboxPodView::uid)
            {
                self.hydrate_materialized_pod_uid(
                    sandbox_id,
                    &current_execution_identity(&active, live_pod.as_deref()),
                    observed_uid,
                )
                .await;
            }
            return Ok(PodFenceCheck::Mismatch(mismatch));
        }
        Ok(PodFenceCheck::Holds(Box::new(FencedTeardown {
            verified_pod_uid: match &current.pod {
                ObservedPod::Seen { pod_uid, .. } => Some(pod_uid.clone()),
                ObservedPod::Absent | ObservedPod::Unobserved => None,
            },
            identity: current_execution_identity(&active, live_pod.as_deref()),
            runtime_pod_ip: fenced_runtime_pod_ip(
                &active.lifecycle,
                active.state,
                live_pod.as_deref(),
            ),
        })))
    }

    /// The pod object the fence decision is made against.
    ///
    /// The reflector is the cheap read every hot path uses, and a fence check
    /// runs over a candidate set large enough that adding an apiserver GET per
    /// candidate is its own outage risk. For a decision that concludes "no
    /// runtime to stop" the cache is good enough: the only destructive step
    /// left is the delete, and the apiserver evaluates that precondition
    /// itself.
    ///
    /// It is not good enough when the decision is about to authorize a
    /// blocking in-VM stop. A lagging cache can still match a fence the
    /// apiserver would reject — a same-name replacement, or the same pod
    /// having moved on — and by the time the delete says so the VM is already
    /// stopped. So a cached object that *would* authorize a stop is re-read
    /// authoritatively, which bounds the extra GET to candidates that actually
    /// have a live runtime. A cache miss is re-read for the same reason it
    /// always was: treating "not cached" as "no pod" would silently pass the
    /// fence.
    async fn read_fenced_pod(
        &self,
        lifecycle: &str,
        state: ActiveExecutionState,
        pod_name: &PodName,
        freshness: FenceReadFreshness,
    ) -> Result<Option<Arc<SandboxPodView>>, SubstrateError> {
        let cached = self.lookup_sandbox_pod(pod_name.as_str());
        let authorizes_stop = cached
            .as_deref()
            .is_some_and(|pod| fenced_runtime_pod_ip(lifecycle, state, Some(pod)).is_some());
        if cached.is_some() && !authorizes_stop && freshness == FenceReadFreshness::CacheIsEnough {
            return Ok(cached);
        }
        fetch_sandbox_pod_uncached(&self.k8s, pod_name.as_str())
            .await
            .map_err(|err| SubstrateError::Failed {
                message: format!("failed to read fenced sandbox pod: {err}"),
            })
    }

    /// Record the UID of a live pod that no fence can describe.
    ///
    /// A name-only fence refuses a live pod, which is correct but not by
    /// itself convergent: if pod creation succeeded and the process died
    /// before persisting the pod's metadata, nothing else ever writes that
    /// UID, so every later candidate carries the same unusable fence and the
    /// execution is never cleaned up — worse than the unfenced retire this
    /// replaced, which at least made progress.
    ///
    /// Writing the UID here is the create path's missed write, made
    /// generation-scoped so it cannot land on a successor. It authorizes
    /// nothing on its own: this call is refusing the teardown, and the *next*
    /// candidate select carries the UID into a fence that can be enforced.
    async fn hydrate_materialized_pod_uid(
        &self,
        sandbox_id: SandboxId,
        identity: &ExecutionIdentity,
        observed_uid: &str,
    ) {
        match self
            .store
            .backfill_missing_pod_uid(sandbox_id, identity, observed_uid)
            .await
        {
            Ok(_) => tracing::info!(
                sandbox_id = %sandbox_id,
                generation = ?identity.generation,
                "recorded the pod uid a name-only fence could not verify; the next sweep \
                 can fence on it"
            ),
            Err(error) => tracing::warn!(
                sandbox_id = %sandbox_id,
                generation = ?identity.generation,
                %error,
                "failed to record the pod uid a name-only fence could not verify; cleanup \
                 will keep deferring until it lands"
            ),
        }
    }

    /// Shared body of [`SubstrateAdapter::delete_execution`] and
    /// [`SubstrateAdapter::retire_execution`]; `mode` decides whether the
    /// in-VM cleanup also destroys node-local sandbox state, and `fence` pins
    /// the whole teardown to one observed pod identity (see
    /// [`Self::check_pod_fence`]).
    pub(super) async fn teardown_execution(
        &self,
        sandbox_id: SandboxId,
        mode: ExecutionTeardown,
        fence: &RetireFence,
    ) -> Result<RetireExecutionOutcome, SubstrateError> {
        let sandbox_uuid = sandbox_id;
        let pod_fence = match fence {
            RetireFence::Unobserved => None,
            RetireFence::NoExecution => {
                // Having observed no execution authorizes exactly one thing:
                // clearing the placement state such a sandbox is left holding.
                // Not the in-VM stop, and not the tracked-pod sweep — both act
                // on objects this caller never saw, and the pod list would
                // include a successor's.
                return self.retire_with_no_active_execution(sandbox_id, mode).await;
            }
            RetireFence::Pod(pod_fence) => Some(pod_fence),
        };
        let fenced = match pod_fence {
            Some(pod_fence) => match self
                .check_pod_fence(sandbox_id, pod_fence, FenceReadFreshness::CacheIsEnough)
                .await?
            {
                PodFenceCheck::AlreadyRetired => {
                    // No active execution names the pod, but create compensation
                    // still holds its UID, and the execution rows can be gone for
                    // a reason that proves nothing about the pod: a best-effort
                    // user DELETE can remove the sandbox row, and its executions
                    // cascade with it, when its own pod delete failed. The
                    // UID precondition makes this delete unable to touch any
                    // other pod.
                    if mode.confirms_pod_absence()
                        && let Some(pod_uid) = pod_fence.pod_uid.as_ref()
                    {
                        match self
                            .delete_fenced_pod_confirmed(&pod_fence.pod_name, pod_uid, None)
                            .await?
                        {
                            FencedPodDeleteOutcome::Deleted
                            | FencedPodDeleteOutcome::NotFound
                            // Another pod holds the name, so the fenced one is gone.
                            | FencedPodDeleteOutcome::PreconditionFailed => {}
                        }
                    }
                    return self.retire_with_no_active_execution(sandbox_id, mode).await;
                }
                PodFenceCheck::Mismatch(mismatch) => {
                    return Ok(self.refuse_fenced_teardown(
                        sandbox_id,
                        pod_fence,
                        mismatch,
                        TeardownProgress::NothingMutated,
                    ));
                }
                PodFenceCheck::Holds(fenced) => Some(fenced),
            },
            None => None,
        };
        // A fenced teardown already has its address, derived from the pod the
        // fence verified. The unfenced path resolves one now: best-effort, and
        // never letting a missing runtime block the teardown. When the tracked
        // pod has nothing to talk to — no IP yet, or `Pending+Unschedulable` —
        // `resolve_pod_ip` returns `Unavailable`; treat that as "no live
        // runtime" and fall through to the pod-delete loop instead of
        // short-circuiting. Genuine infra faults still propagate.
        let pod_ip = match &fenced {
            Some(fenced) => fenced.runtime_pod_ip,
            None => match self.resolve_pod_ip(sandbox_uuid).await {
                Ok(pod_ip) => pod_ip,
                Err(err) if delete_can_skip_runtime_cleanup(&err) => {
                    tracing::info!(
                        sandbox_id = %sandbox_id,
                        reason = %err,
                        "tracked sandbox pod has no reachable runtime; skipping in-VM cleanup and deleting pod object"
                    );
                    None
                }
                Err(err) => return Err(err),
            },
        };
        // Tracks whether anything has actually been done yet, so a refusal
        // arriving later can tell a compensating caller whether undoing its
        // own transition is still honest.
        let mut progress = TeardownProgress::NothingMutated;
        if let Some(pod_ip) = pod_ip {
            // Retire preserves the stopped local disk by stopping the runtime
            // and enqueueing an explicit current-instance backup before pod
            // tracking is forgotten. Once tracking is inactive, the
            // failed-cleanup retry scan no longer has a source execution to
            // address; abort on stop/backup failure so the retry loop keeps the
            // pod and execution row visible. Delete goes straight to sbx-mgr's
            // guarded delete: a pre-delete Stop would turn an in-flight
            // `Suspending` row into `Stopping` and erase the publication fence.
            let endpoint_addr = format!("{pod_ip}:{}", self.k8s.sandbox_proxy_port);
            let client = Self::pod_worker_client(
                pod_ip,
                self.k8s.sandbox_proxy_port,
                self.budgets.teardown_worker_connect,
            )
            .await;
            if let Some(mut client) = client {
                if mode.sends_sandbox_stop() {
                    let stop_result = client
                        .stop_sandbox(pplx_space_sbx_mgr_client::proto::StopRequest {
                            sandbox_id: sandbox_id.to_string(),
                            template_filter: None,
                            blocking: true,
                            immediate_kill: false,
                        })
                        .await;
                    if let Err(err) = stop_result {
                        return Err(SubstrateError::Failed {
                            message: format!(
                                "in-VM stop failed during execution retire; keeping execution tracked for retry: {err}"
                            ),
                        });
                    }
                    // Past this point the sandbox is stopped whatever any
                    // later fence check says, so a refusal from here on must
                    // not read as "nothing happened".
                    progress = TeardownProgress::RuntimeStopped;
                    match enqueue_artifact_backup_with_client(
                        &mut client,
                        &endpoint_addr,
                        sandbox_id,
                        ArtifactKind::Disk,
                        BackupSource::CurrentInstance,
                    )
                    .await
                    {
                        Ok(_) => {}
                        Err(err) if retire_can_skip_backup_enqueue_error(&err) => {
                            tracing::info!(
                                sandbox_id = %sandbox_id,
                                error = %err,
                                "stopped-disk backup skipped during execution retire because \
                                 sbx-mgr has no sandbox row or terminally refused the backup"
                            );
                        }
                        Err(err) => {
                            return Err(SubstrateError::Failed {
                                message: format!(
                                    "stopped-disk backup enqueue failed during execution retire; keeping execution tracked for retry: {err}"
                                ),
                            });
                        }
                    }
                }
                if mode.sends_sandbox_delete()
                    && let Err(error) = client
                        .delete_sandbox(pplx_space_sbx_mgr_client::proto::DeleteRequest {
                            sandbox_id: sandbox_id.to_string(),
                            template_filter: None,
                            blocking: true,
                        })
                        .await
                {
                    match classify_sandbox_delete_rpc_error(&error) {
                        SandboxDeleteRpcErrorDisposition::DeferTeardown => {
                            return Err(SubstrateError::SuspendInFlight {
                                message: format!(
                                    "sandbox {sandbox_id} node-local delete deferred while suspend publication is pending: {error}"
                                ),
                            });
                        }
                        SandboxDeleteRpcErrorDisposition::RetainedFilesystemOnlyCopy => {
                            return Err(SubstrateError::RetainedFilesystemOnlyCopy {
                                message: format!(
                                    "sandbox {sandbox_id} node-local delete refused because the retained filesystem may be the only copy: {error}"
                                ),
                            });
                        }
                        SandboxDeleteRpcErrorDisposition::ContinueTeardown => {
                            tracing::warn!(
                                sandbox_id = %sandbox_id,
                                error = %error,
                                "best-effort sandbox delete RPC failed during execution teardown"
                            );
                        }
                    }
                }
            } else if mode.requires_stop_success() {
                return Err(SubstrateError::Failed {
                    message: "pod worker unreachable for in-VM stop during execution retire; \
                              keeping execution tracked for retry"
                        .to_string(),
                });
            }
        }

        let placement_clock = if mode.preserves_placement_freshness() {
            PlacementClock::Preserve
        } else {
            PlacementClock::Bump
        };

        // A fenced teardown deletes exactly the pod the caller observed, under
        // its precondition, and nothing else. The unfenced loop below sweeps
        // every *tracked* name, which on a race includes a successor
        // generation's pod — a name the caller never saw and no precondition
        // attached to the observed pod could protect.
        if let Some((fence, fenced)) = pod_fence.zip(fenced.as_ref()) {
            // The check proved no pod, which means it held because the pod was
            // *absent* — and between that check and here it can materialize.
            // There is nothing to precondition on, so there is nothing safe to
            // delete: an unconditional delete would remove a live pod nobody
            // identified. Releasing tracking would be no
            // better — a materialized pod with no active execution row is an
            // orphan the failed-cleanup pass can no longer collect — so absence
            // is re-proven authoritatively first, as late as possible.
            if fenced.verified_pod_uid.is_none()
                && let Some(materialized) =
                    fetch_sandbox_pod_uncached(&self.k8s, fence.pod_name.as_str())
                        .await
                        .map_err(|err| SubstrateError::Failed {
                            message: format!("failed to re-read an unfenceable sandbox pod: {err}"),
                        })?
            {
                if let Some(observed_uid) = materialized.uid() {
                    self.hydrate_materialized_pod_uid(sandbox_id, &fenced.identity, observed_uid)
                        .await;
                }
                return Ok(self.refuse_fenced_teardown(
                    sandbox_id,
                    fence,
                    PodFenceMismatch::PodIdentityUnverified,
                    progress,
                ));
            }
            if let Some(pod_uid) = fenced.verified_pod_uid.as_ref() {
                let deleted = if mode.confirms_pod_absence() {
                    self.delete_fenced_pod_confirmed(
                        &fence.pod_name,
                        pod_uid,
                        fence.resource_version.as_ref(),
                    )
                    .await?
                } else {
                    delete_sandbox_pod_fenced(
                        &self.k8s,
                        &fence.pod_name,
                        pod_uid,
                        fence.resource_version.as_ref(),
                    )
                    .await
                    .map_err(|err| SubstrateError::Failed {
                        message: format!("failed to delete fenced sandbox pod: {err}"),
                    })?
                };
                match deleted {
                    // A delete that removed the object is destructive too, so
                    // a refusal from here on must not report otherwise. Never
                    // downgrades a stop that already ran.
                    FencedPodDeleteOutcome::Deleted => {
                        if progress == TeardownProgress::NothingMutated {
                            progress = TeardownProgress::PodDeleted;
                        }
                    }
                    FencedPodDeleteOutcome::NotFound => {}
                    FencedPodDeleteOutcome::PreconditionFailed => {
                        return Ok(self.refuse_fenced_teardown(
                            sandbox_id,
                            fence,
                            self.classify_delete_precondition_rejection(pod_uid, &fence.pod_name)
                                .await,
                            progress,
                        ));
                    }
                }
            }
            // Strictly-older generations are provably not the successor, and
            // nothing else reliably collects them once this sandbox leaves the
            // failed-cleanup scan. The successor's own generation is excluded
            // by the bound, not by a name comparison.
            self.delete_tracked_pods(
                self.store
                    .tracked_execution_pod_names_before_generation(
                        sandbox_id,
                        fenced.identity.generation.unwrap_or(i64::MIN),
                    )
                    .await?,
            )
            .await?;

            // Generation-guarded, so a successor installed while this teardown
            // ran keeps its execution row instead of being retired out from
            // under itself. The guard is a CAS, which makes this the one step
            // that closes its own window rather than narrowing it.
            if !self
                .store
                .forget_fenced_pod_tracking(sandbox_uuid, &fenced.identity, placement_clock)
                .await?
            {
                return self
                    .classify_lost_tracking_release(sandbox_id, fence, progress)
                    .await;
            }
            return Ok(RetireExecutionOutcome::Retired);
        }

        self.delete_tracked_pods(self.store.tracked_execution_pod_names(sandbox_id).await?)
            .await?;

        self.store
            .forget_pod_tracking(sandbox_uuid, placement_clock)
            .await?;

        Ok(RetireExecutionOutcome::Retired)
    }

    /// Idempotent best-effort delete of each tracked pod name. An already-gone
    /// pod is the expected case, not a failure.
    async fn delete_tracked_pods(&self, pod_names: Vec<String>) -> Result<(), SubstrateError> {
        for pod_name in pod_names {
            if let Err(err) = delete_sandbox_pod(&self.k8s, &pod_name).await
                && !is_not_found_error(&err)
            {
                return Err(SubstrateError::Failed {
                    message: format!("failed to delete sandbox pod {pod_name}: {err}"),
                });
            }
        }
        Ok(())
    }

    /// Decide what a lost tracking-release CAS means.
    ///
    /// Losing it is not automatically a supersession. The teardown's own
    /// `resolve_pod_ip` releases tracking when it finds the pod
    /// authoritatively absent, so an ordinary already-gone pod reaches the CAS
    /// with nothing left to claim. Reporting that as a defer would tell the
    /// sweeper nothing was torn down while the execution had in fact just been
    /// retired, and meter a pod-identity race that never happened. Only an
    /// execution that is *active under another generation* is a supersession.
    async fn classify_lost_tracking_release(
        &self,
        sandbox_id: SandboxId,
        fence: &PodIdentityFence,
        progress: TeardownProgress,
    ) -> Result<RetireExecutionOutcome, SubstrateError> {
        match self.store.active_execution_status(sandbox_id).await? {
            None => Ok(RetireExecutionOutcome::Retired),
            Some(_) => Ok(self.refuse_fenced_teardown(
                sandbox_id,
                fence,
                PodFenceMismatch::ExecutionSuperseded,
                progress,
            )),
        }
    }

    /// [`delete_sandbox_pod_fenced`], then wait until the apiserver no longer
    /// has the pod, bounded by `k8s_blocking_delete_timeout`. A delete that is
    /// accepted but not yet complete is an error, so the caller keeps the
    /// tracking a retry needs to find the pod again.
    async fn delete_fenced_pod_confirmed(
        &self,
        pod_name: &PodName,
        pod_uid: &PodUid,
        resource_version: Option<&PodResourceVersion>,
    ) -> Result<FencedPodDeleteOutcome, SubstrateError> {
        let resource_version = resource_version.map(PodResourceVersion::to_string);
        match self
            .k8s_blocking_delete_sandbox_pod_for_operation(
                pod_name.as_str(),
                "create_compensation",
                Some((pod_uid.as_str(), resource_version.as_deref())),
            )
            .await
        {
            Ok(K8sBlockingDeleteOutcome::Deleted) => Ok(FencedPodDeleteOutcome::Deleted),
            Ok(K8sBlockingDeleteOutcome::NotFound) => Ok(FencedPodDeleteOutcome::NotFound),
            Ok(K8sBlockingDeleteOutcome::PreconditionFailed) => {
                Ok(FencedPodDeleteOutcome::PreconditionFailed)
            }
            Ok(K8sBlockingDeleteOutcome::TimedOut) => Err(SubstrateError::Timeout {
                message: format!(
                    "fenced sandbox pod {pod_name} was not confirmed gone after its delete \
                     was accepted; keeping execution tracking"
                ),
                phase: None,
            }),
            Ok(K8sBlockingDeleteOutcome::ObserveError) => Err(SubstrateError::Unavailable {
                message: format!(
                    "could not confirm fenced sandbox pod {pod_name} deletion; keeping \
                     execution tracking"
                ),
            }),
            Err(err) => Err(SubstrateError::Failed {
                message: format!("failed to delete fenced sandbox pod: {err}"),
            }),
        }
    }

    /// Finish a teardown that found no active execution.
    ///
    /// Both ways of arriving here — a caller that observed no execution, and a
    /// fence whose row went inactive since — leave the same two jobs, and must
    /// do both. Pods recorded on *inactive* rows, then the placement state the
    /// sandbox is left holding; without the second a revert can leave a
    /// dormant `suspended` row still marked as placed, holding a pool slot. No successor can own one, because a
    /// successor's pod is on the active row, and nothing else retries them:
    /// the failed-cleanup pass selects active rows only. That is where a
    /// create cleanup leaves an orphan when it marks its row inactive and then
    /// fails its best-effort delete, and an orphan pod holds a VM and node
    /// capacity until the node is recycled.
    ///
    /// One function so the two paths cannot answer this differently.
    async fn retire_with_no_active_execution(
        &self,
        sandbox_id: SandboxId,
        mode: ExecutionTeardown,
    ) -> Result<RetireExecutionOutcome, SubstrateError> {
        // Pods first, tracking second. The rows naming these pods are what a
        // later attempt would read to find them again, so clearing tracking
        // ahead of a delete that then fails strands the pod with nothing left
        // pointing at it — the failed-cleanup pass selects active rows only.
        // Deleting first is safe in the other direction: a successor's pod is
        // recorded on the *active* row, so nothing here can be one, even if an
        // execution appears while this runs.
        let inactive = self.store.inactive_execution_pod_names(sandbox_id).await?;
        if !inactive.is_empty() {
            self.delete_tracked_pods(inactive).await?;
        }
        // Then the placement state the sandbox is left holding. The clear
        // re-proves the absence inside its own lock, so an execution created
        // since claims nothing — and its refusal is the one way this path
        // still reports a mismatch.
        let placement_clock = if mode.preserves_placement_freshness() {
            PlacementClock::Preserve
        } else {
            PlacementClock::Bump
        };
        if !self
            .store
            .forget_pod_tracking_if_no_active_execution(sandbox_id, placement_clock)
            .await?
        {
            tracing::info!(
                sandbox_id = %sandbox_id,
                "an execution appeared while retiring a sandbox that had none; \
                 refusing the teardown"
            );
            return Ok(RetireExecutionOutcome::PodIdentityChanged {
                mismatch: PodFenceMismatch::ExecutionMissing,
                progress: TeardownProgress::NothingMutated,
            });
        }
        Ok(RetireExecutionOutcome::Retired)
    }

    /// Turn the apiserver's one 409 into the two things it can actually mean.
    ///
    /// A preconditioned delete is rejected both when the pod the caller
    /// observed simply moved — a Pending pod coming up changes its
    /// resourceVersion — and when a different pod now holds that name. Those
    /// call for opposite decisions from a caller that has already stopped a
    /// runtime and is deciding whether to undo its own state transition: the
    /// first says the sandbox it stopped is the live one, the second says it
    /// stopped a predecessor. Picking either as the default is wrong half the
    /// time, so read the object and answer with the unambiguous mismatch. The
    /// read is authoritative and only happens on a 409, which is rare.
    async fn classify_delete_precondition_rejection(
        &self,
        fenced_uid: &PodUid,
        pod_name: &PodName,
    ) -> PodFenceMismatch {
        match fetch_sandbox_pod_uncached(&self.k8s, pod_name.as_str()).await {
            // Gone between the rejection and this read: whatever the 409 was
            // about, the object the caller wanted removed is not there. Report
            // it as the pod having changed, which is the conservative half.
            Ok(None) => PodFenceMismatch::PodUidChanged,
            Ok(Some(pod)) => match pod.uid() {
                Some(uid) if uid == fenced_uid.as_str() => {
                    PodFenceMismatch::PodResourceVersionChanged
                }
                Some(_) => PodFenceMismatch::PodUidChanged,
                // An object with no UID is not something to reason about.
                None => PodFenceMismatch::PodIdentityUnverified,
            },
            Err(err) => {
                tracing::info!(
                    pod_name = %pod_name,
                    error = %err,
                    "could not read the pod behind a rejected delete precondition; \
                     treating it as replaced, which is the half that defers"
                );
                PodFenceMismatch::DeletePreconditionRejected
            }
        }
    }

    /// Log and report a teardown the fence refused. Every refusal path funnels
    /// through here so they all leave the same trace and none can drift into
    /// reporting a teardown that did not happen.
    fn refuse_fenced_teardown(
        &self,
        sandbox_id: SandboxId,
        fence: &PodIdentityFence,
        mismatch: PodFenceMismatch,
        progress: TeardownProgress,
    ) -> RetireExecutionOutcome {
        tracing::info!(
            sandbox_id = %sandbox_id,
            pod_name = %fence.pod_name,
            mismatch = %mismatch,
            progress = ?progress,
            "pod-identity fence rejected an execution teardown; the sandbox changed \
             since the caller observed it, so nothing further was torn down"
        );
        RetireExecutionOutcome::PodIdentityChanged { mismatch, progress }
    }
}
