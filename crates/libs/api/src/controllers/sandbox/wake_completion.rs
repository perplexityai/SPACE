//! Cancellation shield for the wake/restore completion write.
//!
//! The `restoring -> started` completion CAS (and the pod-metadata hydration
//! feeding it) runs inside the HTTP request that triggered the wake. When the
//! client disconnects mid-wake, axum drops the handler future and that write
//! silently never happens — while the node, whose restore work is not tied to
//! the caller's connection, finishes activating the VM anyway. The row then
//! sits at `restoring` until the transient-lifecycle sweeper's age threshold
//! passes and its hydrate-then-converge repair runs (~10 minutes), even
//! though the sandbox has been serving the whole time.
//!
//! [`WakeCompletionGuard`] closes that gap with the same idiom the create
//! path uses for cancelled pod creation (`CancelGuard` in the Kubernetes
//! substrate): a drop-guard armed just before the substrate wake call and
//! defused on every explicit exit — substrate error return or the completion
//! CAS committing. A `Drop` while armed therefore means the request future
//! was cancelled, and the guard spawns one bounded, fire-and-forget redrive
//! ([`redrive_wake_completion`]) that finishes the job: poll the substrate's
//! kube-verified execution hydration until the execution is routable, then
//! run a routability-fenced completion CAS. Everything the redrive writes is
//! guard-railed the same way the sweeper's repair is — the CAS claims
//! nothing unless the row is still `restoring` *and* a routable execution
//! backs it at flip time.
//!
//! Process death during the redrive still loses the write; the sweeper
//! remains the backstop, exactly as `CancelGuard` defers to the reconciler
//! when it cannot clean up itself.

use pplx_space_common_core::sql;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use futures_util::future::BoxFuture;
use pplx_space_common_core::types::{SandboxId, SandboxLifecycle};
use pplx_space_common_svc::sql::sandbox_routability::routable_execution_or_non_k8s;
use pplx_space_events::{
    SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
};
use pplx_space_metrics::sandbox::api::{WakeCompletionAction, WakeCompletionRedriveOutcome};
use pplx_space_sandbox_store as sandbox_store;
use sqlx::PgPool;
use tokio::sync::Semaphore;

use crate::controllers::substrate::{Substrate, SubstrateCapabilities};

/// How often the redrive re-checks the row and re-attempts hydration. The
/// node-side restore this waits on takes seconds to low tens of seconds;
/// 2s keeps the redrive responsive without hammering Postgres or the kube
/// apiserver for the (rare, ~1% of wakes) cancellation population.
const REDRIVE_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Per-attempt budget for the substrate's uncached pod GET inside
/// [`hydrate once`](Substrate)-style calls; mirrors the sweeper's probe
/// timeout scale.
const REDRIVE_HYDRATE_BUDGET: Duration = Duration::from_secs(10);

/// Cap on concurrently-running redrive tasks across the process. The shield
/// fires exactly on the cancellation population, so a genuine `/connect`
/// cancel storm would otherwise fan out into an unbounded pool of long-lived
/// pollers — the same self-reinforcing cascade the create path's
/// cancel-rate tracker exists to dampen. Past the cap, cancellations shed
/// (`outcome=shed`) and the transient-lifecycle sweeper backstops those rows
/// exactly as it did before the shield existed. Sized well above the
/// steady-state cancellation rate (~1-2/minute) times the redrive lifetime.
const MAX_CONCURRENT_REDRIVES: usize = 64;

static REDRIVE_SLOTS: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(MAX_CONCURRENT_REDRIVES)));

/// Total time one redrive may run. Must exceed the user-restore lease TTL
/// (the node-RPC timeout): the cancelled attempt's own unexpired lease
/// blocks the fenced completion CAS — lease parity with every other
/// `restoring -> started` writer — so the redrive has to outlive that lease
/// to converge a row nothing else picked up. The slack past the TTL covers
/// hydration polling after expiry.
pub(super) fn redrive_budget() -> Duration {
    crate::controllers::substrate::NODE_RPC_TIMEOUT + Duration::from_secs(60)
}

/// Whether the cancellation shield applies to this wake: only restore-shaped
/// wakes (`suspended`/`restoring` pre-wake lifecycle) on a substrate that
/// leaves `restoring -> started` to the caller's completion CAS (Kubernetes).
/// Everything else keeps existing semantics:
/// * the direct substrate does not guarantee this: its worker-local
///   wake leaves the pre-wake lifecycle (`suspended`, or `restoring` kept by
///   an earlier lease restore) for the completion CAS, and its lease restore
///   writes `started` itself, so a cancelled direct wake leaves `suspended`,
///   `started`, or a `restoring` row that a later restore reclaims once any
///   lease expires;
/// * pause/resume and stop/start wakes don't take the restore claim and have
///   no execution to hydrate — their (much rarer) cancellation class is a
///   separate piece of work.
pub(super) fn shield_applies(
    capabilities: SubstrateCapabilities,
    pre_wake_lifecycle: SandboxLifecycle,
) -> bool {
    let restore_shaped = matches!(
        pre_wake_lifecycle,
        SandboxLifecycle::Suspended | SandboxLifecycle::Restoring
    );
    restore_shaped && capabilities.caller_completes_claimed_restore
}

/// Drop-guard that owns the boxed redrive future while the wake handler is
/// in flight. `arm` before the substrate call; `defuse` on every explicit
/// exit. A `Drop` while armed is cancellation: the guard spawns the redrive
/// (fire-and-forget — its outcome is observable via the redrive metric and
/// the sandbox event stream, and the sweeper backstops a lost task).
pub(super) struct WakeCompletionGuard {
    sandbox_id: SandboxId,
    action: WakeCompletionAction,
    completion: Option<BoxFuture<'static, ()>>,
    /// The redrive concurrency slots this guard draws from. Production
    /// guards share the process-global pool; tests inject a private
    /// semaphore so cap-exhaustion tests can't starve unrelated tests
    /// running in the same process.
    slots: Arc<Semaphore>,
}

impl WakeCompletionGuard {
    pub(super) fn unarmed(sandbox_id: SandboxId, action: WakeCompletionAction) -> Self {
        Self::with_slots(sandbox_id, action, Arc::clone(&REDRIVE_SLOTS))
    }

    fn with_slots(
        sandbox_id: SandboxId,
        action: WakeCompletionAction,
        slots: Arc<Semaphore>,
    ) -> Self {
        Self {
            sandbox_id,
            action,
            completion: None,
            slots,
        }
    }

    /// Arm with the redrive to spawn on cancellation. Called once the wake
    /// is about to be dispatched to the substrate — the point after which
    /// the node will finish the restore regardless of the caller's fate.
    pub(super) fn arm(&mut self, completion: BoxFuture<'static, ()>) {
        self.completion = Some(completion);
    }

    /// Disarm on an explicit exit (substrate error, completion CAS done):
    /// the handler survived to make its own decision, so cancellation
    /// recovery must not run.
    pub(super) fn defuse(&mut self) {
        self.completion = None;
    }

    #[cfg(test)]
    pub(super) fn is_armed(&self) -> bool {
        self.completion.is_some()
    }
}

impl Drop for WakeCompletionGuard {
    fn drop(&mut self) {
        let Some(completion) = self.completion.take() else {
            return;
        };
        pplx_space_metrics::sandbox::api::wake_completion_cancelled(self.action);
        // Drop must never panic: without a Tokio runtime (teardown paths,
        // non-async test code) there is nowhere to run the redrive — shed
        // and let the sweeper backstop the row.
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            pplx_space_metrics::sandbox::api::wake_completion_redrive(
                WakeCompletionRedriveOutcome::Shed,
            );
            tracing::warn!(
                sandbox_id = %self.sandbox_id,
                action = %self.action,
                "wake request cancelled mid-flight but no Tokio runtime is \
                 available to run the redrive; shedding — the \
                 transient-lifecycle sweeper backstops this row"
            );
            return;
        };
        let permit = match Arc::clone(&self.slots).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                // Cancel storm: every slot is already redriving. Shed rather
                // than amplify — the sweeper backstops this row exactly as it
                // did before the shield existed.
                pplx_space_metrics::sandbox::api::wake_completion_redrive(
                    WakeCompletionRedriveOutcome::Shed,
                );
                tracing::warn!(
                    sandbox_id = %self.sandbox_id,
                    action = %self.action,
                    cap = MAX_CONCURRENT_REDRIVES,
                    "wake request cancelled mid-flight but the redrive cap is full; \
                     shedding — the transient-lifecycle sweeper backstops this row"
                );
                return;
            }
        };
        tracing::warn!(
            sandbox_id = %self.sandbox_id,
            action = %self.action,
            "wake request cancelled mid-flight — spawning shielded completion redrive"
        );
        let span = tracing::info_span!(
            "sandbox.wake_completion_redrive",
            sandbox_id = %self.sandbox_id,
            action = %self.action,
        );
        runtime.spawn(tracing::Instrument::instrument(
            async move {
                let _slot = permit;
                completion.await;
            },
            span,
        ));
    }
}

/// The completion CAS the redrive runs: same shape as the handler's post-wake
/// write, hardened for running without a live attempt behind it — it claims
/// only a row still in `restoring`, and only when a routable execution (or a
/// non-k8s substrate) backs the wake target at flip time. The shared
/// routability predicate is the same fragment the transient-lifecycle
/// sweeper's converge CAS and the node published-state convergence use.
/// The restore-lease guard makes the redrive honor the same ownership
/// invariant as the sweeper's converge CAS and `begin_restore_lifecycle`:
/// an unexpired lease means a restore attempt — most likely a client retry
/// that fired after our caller hung up — is actively driving the row and
/// will run its own completion. The cancelled attempt's *own* lease also
/// blocks the CAS until it expires (the lease TTL is the node-RPC timeout),
/// which is why [`redrive_budget`] must exceed that TTL.
fn fenced_completion_cas() -> String {
    let routable = routable_execution_or_non_k8s("sandboxes");
    format!(
        "UPDATE sandboxes
            SET lifecycle = 'started', placement = 'on_node'
          WHERE id = $1
            AND lifecycle = 'restoring'
            AND (restore_lease_expires_at IS NULL OR restore_lease_expires_at <= NOW())
            AND {routable}"
    )
}

/// Build the production redrive future for a cancelled wake: polls the
/// Kubernetes substrate's kube-verified execution hydration (the same repair
/// primitive the sweeper uses) until the execution is routable, then runs
/// the fenced completion CAS. Bounded by `budget`.
pub(super) fn wake_completion_redrive(
    pool: PgPool,
    substrate: Substrate,
    sandbox_id: SandboxId,
    action: WakeCompletionAction,
    budget: Duration,
) -> BoxFuture<'static, ()> {
    Box::pin(async move {
        let hydrate = || async {
            match &substrate {
                Substrate::Direct(_) => Ok(true),
                #[cfg(feature = "kubernetes")]
                Substrate::Kubernetes(kube) => {
                    use crate::controllers::substrate::kubernetes::ExecutionHydrationOutcome;
                    match kube
                        .hydrate_restored_execution(sandbox_id, REDRIVE_HYDRATE_BUDGET)
                        .await
                    {
                        Ok(
                            ExecutionHydrationOutcome::Hydrated
                            | ExecutionHydrationOutcome::AlreadyRoutable,
                        ) => Ok(true),
                        Ok(ExecutionHydrationOutcome::NotRoutable) => Ok(false),
                        Err(err) => Err(err.to_string()),
                    }
                }
            }
        };
        let outcome = redrive_wake_completion(
            &pool,
            sandbox_id,
            action,
            budget,
            REDRIVE_POLL_INTERVAL,
            REDRIVE_HYDRATE_BUDGET,
            hydrate,
        )
        .await;
        pplx_space_metrics::sandbox::api::wake_completion_redrive(outcome);
        match outcome {
            WakeCompletionRedriveOutcome::Completed => tracing::warn!(
                sandbox_id = %sandbox_id,
                "shielded wake completion converged the row the cancelled request abandoned"
            ),
            WakeCompletionRedriveOutcome::AlreadyResolved => tracing::info!(
                sandbox_id = %sandbox_id,
                "shielded wake completion found the row already resolved by another actor"
            ),
            WakeCompletionRedriveOutcome::Abandoned => tracing::warn!(
                sandbox_id = %sandbox_id,
                budget_secs = budget.as_secs(),
                "shielded wake completion gave up within its budget; the row stays \
                 restoring and the transient-lifecycle sweeper remains the backstop"
            ),
            // Shed is decided (and metered) in the guard's Drop before the
            // redrive future is spawned; the loop never returns it.
            WakeCompletionRedriveOutcome::Shed => debug_assert!(
                false,
                "redrive loop must not return the drop-side shed outcome"
            ),
        }
    })
}

/// The completed-event type for the action the guard was armed with — the
/// same mapping the non-cancelled handler path applies via
/// `SandboxAction::completed_event`, so a cancelled wake still pairs
/// `wake_requested` with `wake_completed` on the event stream (and a
/// cancelled restore with `restore_completed`).
const fn completed_event_for(action: WakeCompletionAction) -> SandboxEventType {
    match action {
        WakeCompletionAction::Wake => SandboxEventType::WakeCompleted,
        WakeCompletionAction::Restore => SandboxEventType::RestoreCompleted,
    }
}

/// Re-enqueue the sandbox's desired-state reconciliation when it has open
/// port exposures — suspension resets their observations to `pending`, and
/// the normal wake completion re-applies them right after the lifecycle CAS.
/// The recovery paths must do the same or a healed sandbox serves with its
/// durable ports unapplied. One sandbox-scoped enqueue covers every open
/// exposure (the per-exposure path enqueues the identical desired-state
/// ref). Best-effort: on failure the per-node exposure reconciler's periodic
/// tick remains the backstop.
async fn enqueue_open_port_exposure_reconcile(pool: &PgPool, sandbox_id: SandboxId) {
    let has_open = match sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS (
             SELECT 1 FROM sandbox_port_exposures
             WHERE sandbox_id = $1 AND desired_state = 'open'
         )"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await
    {
        Ok(has_open) => has_open,
        Err(err) => {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                error = %err,
                "shielded wake completion failed to check open port exposures"
            );
            return;
        }
    };
    if !has_open {
        return;
    }
    match crate::reconcile_queue::enqueue_sandbox_reconcile_and_trigger(
        pool,
        sandbox_id,
        &[crate::reconcile_queue::ReconcileRef::sandbox_desired_state(
            sandbox_id,
        )],
        crate::reconcile_queue::ReconcileReason::SandboxPortExposureUpdated,
    )
    .await
    {
        Ok(Some(_)) => {}
        Ok(None) => tracing::warn!(
            sandbox_id = %sandbox_id,
            "shielded wake completion could not enqueue exposure reconciliation: \
             sandbox has no active node"
        ),
        Err(err) => tracing::warn!(
            sandbox_id = %sandbox_id,
            error = %err,
            "shielded wake completion failed to enqueue exposure reconciliation"
        ),
    }
}

/// Redrive loop, parameterized over the hydration probe so the Postgres
/// behavior is testable without a kube cluster. `hydrate` answers "is the
/// active execution routable now?" — `Ok(true)` permits the completion CAS,
/// `Ok(false)` and `Err` both wait for the next poll (a transient apiserver
/// error must not abandon a redrive that would succeed one tick later). A
/// missing row is terminal: a concurrent delete resolved the sandbox, and
/// polling on would only pin a redrive slot with nothing left to converge.
///
/// The whole future — the loop and the post-resolution exposure enqueue — is
/// hard-bounded by `budget` plus one-await grace via `timeout_at`, so a
/// blocked query or a never-resolving hydration cannot hold a redrive slot
/// past the deadline and starve later cancellations into shedding. The
/// graceful deadline check between awaits remains the common exit.
///
/// `hydrate_grace` is how long one in-flight hydration attempt may run past
/// the graceful deadline; it belongs to the caller because it is the budget
/// the caller's own `hydrate` is bounded by (production: the per-attempt
/// [`REDRIVE_HYDRATE_BUDGET`] the Kubernetes probe is given).
pub(super) async fn redrive_wake_completion<H, Fut>(
    pool: &PgPool,
    sandbox_id: SandboxId,
    action: WakeCompletionAction,
    budget: Duration,
    poll_interval: Duration,
    hydrate_grace: Duration,
    hydrate: H,
) -> WakeCompletionRedriveOutcome
where
    H: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<bool, String>>,
{
    let deadline = tokio::time::Instant::now() + budget;
    // Grace covers a single in-flight await (one hydration attempt or one
    // query) past the graceful deadline; the hard bound fires only when an
    // await is genuinely stuck.
    let hard_deadline = deadline + hydrate_grace + poll_interval;
    let run = async {
        let outcome =
            redrive_loop(pool, sandbox_id, action, deadline, poll_interval, hydrate).await;
        match outcome {
            WakeCompletionRedriveOutcome::Completed => {
                enqueue_open_port_exposure_reconcile(pool, sandbox_id).await;
            }
            WakeCompletionRedriveOutcome::AlreadyResolved => {
                // The converging actor (published-state convergence, a client
                // retry that itself was cancelled, the sweeper) does not
                // re-enqueue exposures; if the row landed on `started`, own
                // that follow-up here.
                if let Ok(Some(record)) = sandbox_store::get(pool, sandbox_id).await
                    && record.lifecycle == SandboxLifecycle::Started.as_str()
                {
                    enqueue_open_port_exposure_reconcile(pool, sandbox_id).await;
                }
            }
            _ => {}
        }
        outcome
    };
    match tokio::time::timeout_at(hard_deadline, run).await {
        Ok(outcome) => outcome,
        Err(_) => {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                budget_secs = budget.as_secs(),
                "shielded wake completion hit its hard bound (a stuck query or \
                 hydration); abandoning to free the redrive slot"
            );
            WakeCompletionRedriveOutcome::Abandoned
        }
    }
}

async fn redrive_loop<H, Fut>(
    pool: &PgPool,
    sandbox_id: SandboxId,
    action: WakeCompletionAction,
    deadline: tokio::time::Instant,
    poll_interval: Duration,
    hydrate: H,
) -> WakeCompletionRedriveOutcome
where
    H: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<bool, String>>,
{
    loop {
        match sandbox_store::get(pool, sandbox_id)
            .await
            .map(|record| record.map(|record| record.lifecycle))
        {
            Err(err) => {
                // Transient read error: resolves on the next poll; fall
                // through to the deadline check below.
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    error = %err,
                    "shielded wake completion failed to read lifecycle; retrying"
                );
            }
            Ok(None) => {
                // The row is gone: a concurrent delete resolved the sandbox.
                // Terminal — do not sit out the budget holding a redrive slot.
                return WakeCompletionRedriveOutcome::AlreadyResolved;
            }
            Ok(Some(lifecycle)) if lifecycle != SandboxLifecycle::Restoring.as_str() => {
                // A client retry, the published-state convergence, or the
                // sweeper moved the row. Nothing left to shield.
                return WakeCompletionRedriveOutcome::AlreadyResolved;
            }
            Ok(Some(_)) => match hydrate().await {
                Ok(true) => {
                    match sqlx::query(sqlx::AssertSqlSafe(sql!(fenced_completion_cas())))
                        .bind(sandbox_id)
                        .execute(pool)
                        .await
                    {
                        Ok(claimed) if claimed.rows_affected() > 0 => {
                            SandboxEvent::new(
                                sandbox_id,
                                completed_event_for(action),
                                SandboxEventSource::Api,
                            )
                            .emit_with(
                                SandboxEventDetails::default()
                                    .reason("wake_request_cancelled")
                                    .operation("wake_completion_redrive")
                                    .trigger(SandboxEventTrigger::System)
                                    .with_from_state(SandboxLifecycle::Restoring.as_str())
                                    .with_to_state(SandboxLifecycle::Started.as_str()),
                            );
                            return WakeCompletionRedriveOutcome::Completed;
                        }
                        Ok(_) => {
                            // Routable a moment ago but the CAS claimed
                            // nothing: either the row moved on (a concurrent
                            // completion won — resolved) or the execution was
                            // replaced between hydration and the CAS (the
                            // fence refused; poll again).
                        }
                        Err(err) => {
                            tracing::warn!(
                                sandbox_id = %sandbox_id,
                                error = %err,
                                "shielded wake completion CAS failed; retrying"
                            );
                        }
                    }
                }
                Ok(false) => {}
                Err(err) => {
                    tracing::debug!(
                        sandbox_id = %sandbox_id,
                        error = %err,
                        "shielded wake completion hydration attempt failed; retrying"
                    );
                }
            },
        }
        if tokio::time::Instant::now() >= deadline {
            return WakeCompletionRedriveOutcome::Abandoned;
        }
        tokio::time::sleep(poll_interval).await;
    }
}

#[cfg(test)]
mod tests;
