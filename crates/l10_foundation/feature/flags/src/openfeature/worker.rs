//! The bridge between the synchronous [`crate::FeatureFlagProvider`] contract
//! and OpenFeature's asynchronous provider SPI.
//!
//! Every method on OpenFeature's `FeatureProvider` is `async fn`, and even a
//! provider that resolves purely from local state awaits real Tokio primitives
//! to get at it — flagd's in-process store takes `tokio::sync::RwLock` reads on
//! the way to a flag it already holds in memory. So something has to cross from
//! sync to async, and the two obvious ways both break:
//!
//! * `Handle::block_on` panics with "Cannot start a runtime from within a
//!   runtime" whenever a flag is read from inside an async context, which is
//!   most of SPACE.
//! * `tokio::task::block_in_place` panics with "can call blocking only when
//!   running on the multi-threaded runtime" under the *default* `#[tokio::test]`
//!   flavour, which is current-thread. This repository has far more
//!   current-thread tests than multi-threaded ones, so that choice would pass in
//!   production and fail nearly everywhere in the suite.
//!
//! What works in every context is a handoff: the adapter owns a thread, that
//! thread owns a current-thread runtime, and a read sends a request to it and
//! blocks on a reply channel. The caller's thread never needs a runtime of its
//! own, so a plain sync function, a current-thread test, and a multi-threaded
//! server all behave the same.
//!
//! **The thread is the important half, not just the runtime.** A `Runtime` held
//! as a struct field panics on drop inside any async context — "Cannot drop a
//! runtime in a context where blocking is not allowed" — and a flag client is
//! dropped exactly there: at server shutdown, and at the end of every
//! `#[tokio::test]` that built one. Owning the thread instead means `Drop` only
//! closes a channel and joins, which is safe from anywhere.

use std::future::Future;
use std::sync::Arc;
use std::sync::mpsc::{RecvTimeoutError, SyncSender, sync_channel};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use open_feature::provider::{FeatureProvider, ProviderStatus};
use open_feature::{EvaluationContext, EvaluationError, EvaluationErrorCode};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use super::convert;
use crate::Attributes;

/// How long a read waits for the worker before giving up on it.
///
/// Sized for the contract the trait states rather than for any particular
/// backend: reads are served from locally-held state, so the handoff is a
/// thread wake-up and the budget is enormous by comparison. It exists to bound
/// a wedged or saturated worker, not to pace a slow backend — an OpenFeature
/// provider configured to call out over the network on every read will trip it
/// under load, and should be run in a local-evaluation mode instead.
pub(super) const EVALUATION_TIMEOUT: Duration = Duration::from_millis(500);

/// Which typed read a request is for. One request type covers all five so the
/// worker needs a single queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ValueKind {
    Bool,
    Integer,
    Float,
    String,
    Json,
}

/// A resolved value, already converted out of OpenFeature's types so nothing
/// vendor-shaped crosses back to the synchronous side.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum Resolved {
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Json(serde_json::Value),
}

/// Ties a Rust value type to the read it comes from.
///
/// Without it a read names its kind twice — once as a [`ValueKind`] telling
/// the worker which resolve to call, once as a closure narrowing the answer
/// back to a type — and nothing makes the two agree. Asking for
/// [`ValueKind::Integer`] and narrowing to [`Resolved::Float`] compiles, and
/// fails at runtime as a wrong-type error. Keyed on the return type, the
/// pairing is stated once and the compiler checks it.
pub(super) trait FlagValue: Sized {
    const KIND: ValueKind;

    /// The answer for [`Self::KIND`], or nothing if the worker somehow
    /// returned a different shape — impossible by construction now that the
    /// kind and this function come from the same impl.
    fn from_resolved(resolved: Resolved) -> Option<Self>;
}

impl FlagValue for bool {
    const KIND: ValueKind = ValueKind::Bool;
    fn from_resolved(resolved: Resolved) -> Option<Self> {
        match resolved {
            Resolved::Bool(value) => Some(value),
            _ => None,
        }
    }
}

impl FlagValue for i64 {
    const KIND: ValueKind = ValueKind::Integer;
    fn from_resolved(resolved: Resolved) -> Option<Self> {
        match resolved {
            Resolved::Integer(value) => Some(value),
            _ => None,
        }
    }
}

impl FlagValue for f64 {
    const KIND: ValueKind = ValueKind::Float;
    fn from_resolved(resolved: Resolved) -> Option<Self> {
        match resolved {
            Resolved::Float(value) => Some(value),
            _ => None,
        }
    }
}

impl FlagValue for String {
    const KIND: ValueKind = ValueKind::String;
    fn from_resolved(resolved: Resolved) -> Option<Self> {
        match resolved {
            Resolved::String(value) => Some(value),
            _ => None,
        }
    }
}

impl FlagValue for serde_json::Value {
    const KIND: ValueKind = ValueKind::Json;
    fn from_resolved(resolved: Resolved) -> Option<Self> {
        match resolved {
            Resolved::Json(value) => Some(value),
            _ => None,
        }
    }
}

/// What the provider said about one flag.
#[derive(Clone, Debug)]
pub(super) enum Outcome {
    /// The provider resolved a value of the requested type.
    Assigned(Resolved),
    /// The provider has no assignment for this subject. Distinct from a
    /// failure, and reported as a different [`crate::Unresolved`] reason.
    Unassigned,
    /// The provider tried and failed: unreadable configuration, a type the flag
    /// does not have, a backend that is not ready.
    Failed(EvaluationError),
}

/// The worker could not answer at all, which is neither an assignment nor a
/// provider verdict.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Unavailable {
    /// No reply within [`EVALUATION_TIMEOUT`].
    Timeout,
    /// The reply channel closed before this caller's deadline: the worker
    /// thread is gone, or the task serving this one read panicked. A
    /// disconnect *at or after* the deadline is reported as [`Self::Timeout`]
    /// instead, because the worker closes the channel when it abandons an
    /// expired request and that is the same event as the timeout, not a
    /// separate failure.
    Closed,
}

impl Unavailable {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Closed => "reply channel closed",
        }
    }
}

/// The provider's own readiness, callable from whichever thread asks for it.
///
/// A closure over the provider rather than a sixth request kind:
/// `FeatureProvider::status` is a synchronous method on a value the SPI
/// already requires to be `Send + Sync`, so any thread may call it — and
/// [`crate::FeatureFlagClient`] asks for the state on *every* read, so routing
/// it through the single worker thread would double the round trips a read
/// costs to answer a question the provider can answer in place.
pub(super) type StatusHandle = Arc<dyn Fn() -> ProviderStatus + Send + Sync>;

/// Failure to stand the worker up. Evaluation failures are [`Outcome`]s; this
/// is only ever returned from construction.
#[derive(Debug, thiserror::Error)]
pub enum OpenFeatureProviderError {
    /// The OS refused a thread.
    #[error("failed to spawn the OpenFeature worker thread")]
    SpawnThread(#[source] std::io::Error),
    /// Tokio refused a runtime on that thread.
    #[error("failed to build the OpenFeature worker runtime")]
    BuildRuntime(#[source] std::io::Error),
    /// The provider's own constructor failed. For flagd in-process mode this is
    /// where an unreachable sync source lands: the store's initial sync has a
    /// deadline, and missing it fails construction rather than every later read.
    #[error("failed to construct the OpenFeature provider")]
    BuildProvider(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// The worker thread ended before reporting either outcome.
    #[error("the OpenFeature worker thread stopped before it finished starting")]
    WorkerStopped,
}

struct Request {
    kind: ValueKind,
    flag_key: String,
    /// Built on the calling thread, not here. The caller is parked on the
    /// reply anyway, while the worker is the one serialization point every
    /// flag read in the process passes through — so the conversion, and the
    /// string clones it makes, belong on the caller's side of the handoff.
    context: EvaluationContext,
    /// The instant the caller stops waiting, set before the request is
    /// queued. One clock for both sides: the worker uses it to skip a request
    /// whose caller has already gone, and to bound the evaluation by what is
    /// *left* rather than by a fresh budget starting whenever it happened to
    /// be dequeued.
    deadline: Instant,
    reply: SyncSender<Outcome>,
}

/// Owns the worker thread and the queue into it.
///
/// The queue is unbounded because the sending side is synchronous and a
/// bounded `tokio::sync::mpsc` offers no send it can use: `send` is `async`,
/// and `try_send` would drop reads under exactly the burst a limit is meant to
/// absorb.
///
/// What keeps an unbounded queue from growing without limit is not the
/// callers — a caller that times out does **not** withdraw its request, and is
/// free to read again immediately, so depth is only bounded by thread count
/// while the worker keeps up. It is [`serve`] discarding requests whose
/// deadline has passed: once service rate falls below offered rate the backlog
/// ages out at the head as fast as it arrives at the tail, and the worker
/// stops spending its one thread producing replies nobody is waiting for.
pub(super) struct Worker {
    requests: Option<UnboundedSender<Request>>,
    /// Released before the thread is joined; [`Drop`] says why that ordering
    /// matters.
    status: Option<StatusHandle>,
    thread: Option<JoinHandle<()>>,
}

impl Worker {
    /// Build the provider on the worker's own runtime and start serving.
    ///
    /// The provider is constructed inside the worker because OpenFeature SDK
    /// constructors are themselves async — flagd's connects to its sync source
    /// and waits for a first flag set — and a caller has no runtime to build one
    /// on. Construction blocks until the provider is ready or has failed, so a
    /// misconfigured backend is a startup error rather than a silent stream of
    /// unresolved flags.
    pub(super) fn start<F, Fut, P>(build: F) -> Result<Self, OpenFeatureProviderError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<P, Box<dyn std::error::Error + Send + Sync>>>,
        P: FeatureProvider,
    {
        let (requests, receiver) = unbounded_channel();
        let (started, start_result) = sync_channel(1);

        let thread = std::thread::Builder::new()
            .name("openfeature-provider".to_owned())
            .spawn(move || run(build, &started, receiver))
            .map_err(OpenFeatureProviderError::SpawnThread)?;

        // Both failure branches join before returning, so a failed start
        // leaves no thread running behind it. The join result is discarded on
        // purpose: the error being returned already names what went wrong, and
        // it is more specific than "the thread ended".
        match start_result.recv() {
            Ok(Ok(status)) => Ok(Self {
                requests: Some(requests),
                status: Some(status),
                thread: Some(thread),
            }),
            Ok(Err(error)) => {
                drop(thread.join());
                Err(error)
            }
            // The thread ended without reporting either way, which in practice
            // means it panicked before reaching the report.
            Err(_) => {
                drop(thread.join());
                Err(OpenFeatureProviderError::WorkerStopped)
            }
        }
    }

    /// What the provider says about its own readiness, asked directly rather
    /// than over the queue.
    ///
    /// Answers [`ProviderStatus::NotReady`] once the handle has been released,
    /// which happens only while the worker is being torn down.
    pub(super) fn status(&self) -> ProviderStatus {
        self.status
            .as_ref()
            .map_or(ProviderStatus::NotReady, |status| status())
    }

    /// Ask the provider for one flag and wait for its answer.
    pub(super) fn resolve(
        &self,
        kind: ValueKind,
        flag_key: &str,
        subject_key: &str,
        attributes: &Attributes,
    ) -> Result<Outcome, Unavailable> {
        // Capacity one and a single use: the worker never blocks sending a
        // reply, even if this caller has already given up and gone away.
        // Convert first, then stamp: the deadline has to start at the same
        // instant for both sides, and building the context is the caller's
        // own work, not time the worker should be charged for. Stamping
        // earlier leaves the caller waiting past the instant the worker
        // abandons the request, and a genuine worker death in that trailing
        // window gets classified as an ordinary timeout.
        let context = convert::evaluation_context(subject_key, attributes);
        let (reply, answer) = sync_channel(1);
        let deadline = Instant::now() + EVALUATION_TIMEOUT;
        let request = Request {
            kind,
            flag_key: flag_key.to_owned(),
            context,
            deadline,
            reply,
        };

        self.requests
            .as_ref()
            .ok_or(Unavailable::Closed)?
            .send(request)
            .map_err(|_| Unavailable::Closed)?;

        answer
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .map_err(|error| match error {
                RecvTimeoutError::Timeout => Unavailable::Timeout,
                // The worker drops the reply channel the moment it abandons a
                // request whose deadline has passed, so a disconnect at that
                // point is this caller's own timeout arriving by a different
                // route — a race between two clocks measuring the same
                // instant. Reporting it as a dead worker would blame the
                // wrong thing for ordinary overload.
                RecvTimeoutError::Disconnected if Instant::now() >= deadline => {
                    Unavailable::Timeout
                }
                RecvTimeoutError::Disconnected => Unavailable::Closed,
            })
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        // Release this side's handle on the provider before the join, so the
        // worker's own is the last one and the provider's destructor runs on
        // the worker thread while its runtime is still there. Held past the
        // join, it would instead drop the provider here — on whatever thread
        // dropped the flag client, with no reactor under whatever Tokio
        // resources it holds.
        drop(self.status.take());
        // Closing the queue ends the serve loop, which ends `block_on`, which
        // drops the runtime *on the worker's own thread* — the one place
        // dropping it is always allowed.
        drop(self.requests.take());
        if let Some(thread) = self.thread.take()
            && thread.join().is_err()
        {
            // A panicking worker has already stranded every in-flight read as
            // a disconnect, and each of those logged its own failure. Say so
            // once more here, because this is the only place the cause —
            // the worker itself — is visible.
            tracing::error!("the OpenFeature worker thread panicked");
        }
    }
}

/// The worker thread: build a runtime, build the provider on it, then serve.
fn run<F, Fut, P>(
    build: F,
    started: &SyncSender<Result<StatusHandle, OpenFeatureProviderError>>,
    requests: UnboundedReceiver<Request>,
) where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<P, Box<dyn std::error::Error + Send + Sync>>>,
    P: FeatureProvider,
{
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let _ = started.send(Err(OpenFeatureProviderError::BuildRuntime(error)));
            return;
        }
    };

    runtime.block_on(async move {
        let mut provider = match build().await {
            Ok(provider) => provider,
            Err(error) => {
                let _ = started.send(Err(OpenFeatureProviderError::BuildProvider(error)));
                return;
            }
        };

        // Initialization is part of starting up, so it happens before the first
        // request is served rather than racing it. Requests queue meanwhile.
        provider.initialize(&EvaluationContext::default()).await;
        if provider.status() == ProviderStatus::Error {
            // Not fatal: a provider that reports `Error` may still recover, and
            // its own reads will say so per flag. Worth saying once, loudly,
            // because every flag will sit at its declared fallback until it does.
            //
            // Inert for flagd, which overrides neither `status` nor
            // `initialize` and so is always `Ready`; there, construction
            // failing is the startup signal. This is here for providers that
            // do implement the lifecycle.
            tracing::warn!(
                provider = provider.metadata().name,
                "the OpenFeature provider initialized into an error state; flags will resolve to their declared fallbacks until it recovers"
            );
        }

        // Hooks are the one part of the OpenFeature lifecycle this adapter
        // does not implement: reads call the provider's resolvers directly
        // rather than going through the SDK client that would run `before`,
        // `after`, `error`, and `finally` around them. A `before` hook can
        // rewrite the evaluation context or refuse the evaluation outright,
        // so a provider that relies on hooks would resolve differently here
        // than it would through the SDK — silently, and only for the flags
        // whose outcome the hooks change.
        //
        // Said loudly at startup rather than left to be discovered: flagd
        // supplies none, so this never fires for the reference backend, and
        // anyone installing a provider that does supply them learns before
        // the first read instead of from a wrong assignment.
        //
        // `error!` where the `ProviderStatus::Error` case above is `warn!`,
        // even though that one means every flag is defaulting *now* and this
        // one only means some may differ. The split is by who has to act: a
        // provider in an error state may recover on its own and the warning
        // is then noise, while an unrun hook never fixes itself and needs
        // someone to change the provider or accept the gap.
        if !provider.hooks().is_empty() {
            tracing::error!(
                provider = provider.metadata().name,
                hooks = provider.hooks().len(),
                "provider supplies hooks, which this adapter does not run; its flags may resolve differently here than through the OpenFeature SDK"
            );
        }

        let provider = Arc::new(provider);
        let readiness = Arc::clone(&provider);
        let _ = started.send(Ok(Arc::new(move || readiness.status())));
        serve(provider, requests).await;
    });
}

/// Serve requests until the queue closes.
///
/// Each request is spawned rather than awaited in line: a provider that is slow
/// for one flag — a cache miss that goes upstream, say — must not hold up reads
/// of every other flag behind it. Awaiting the queue rather than blocking on it
/// is what lets the provider's own background work run, which for flagd is the
/// task that keeps the local flag set in sync.
///
/// Two things bound the work, both driven by the caller's deadline rather than
/// by a budget of the worker's own.
///
/// A request already past its deadline is dropped without evaluating it. Its
/// caller has given up and reported the read as a failure, so resolving it
/// produces a reply nobody receives — and under sustained overload, spending
/// the single worker thread on those is how goodput reaches zero and stays
/// there. Discarding at the head is also the only thing keeping the unbounded
/// queue from growing without limit.
///
/// An evaluation that is still wanted runs under the deadline's *remaining*
/// time. Without a bound here a provider that stops answering leaves a task
/// alive forever holding a provider handle, a context, and its strings, and
/// every later read adds another. Dropping the future is what cancels the
/// provider call, so the bound has to wrap it rather than sit beside it.
async fn serve<P: FeatureProvider>(provider: Arc<P>, mut requests: UnboundedReceiver<Request>) {
    while let Some(request) = requests.recv().await {
        if request.deadline <= Instant::now() {
            // Silently: the caller already logged this read when its own wait
            // ran out, and a second line per abandoned request is exactly the
            // wrong thing to emit while overloaded.
            continue;
        }
        let provider = Arc::clone(&provider);
        tokio::spawn(async move {
            // `timeout_at`, not `timeout(remaining)`: a relative timeout starts
            // counting at the task's first poll, and on a current-thread
            // runtime the loop can dequeue and spawn many requests before any
            // of them is polled. Measured from that later instant, the budget
            // would overrun the caller's deadline — under exactly the overload
            // this is meant to bound.
            let outcome = tokio::time::timeout_at(
                tokio::time::Instant::from_std(request.deadline),
                resolve_one(provider.as_ref(), &request),
            )
            .await;
            // Out of time, so the caller has stopped waiting. Ending the task
            // is the whole point.
            let Ok(outcome) = outcome else {
                return;
            };
            // A caller that timed out has dropped its receiver. Nothing to do.
            let _ = request.reply.send(outcome);
        });
    }
}

async fn resolve_one<P: FeatureProvider>(provider: &P, request: &Request) -> Outcome {
    let context = &request.context;
    let flag_key = request.flag_key.as_str();

    let resolved = match request.kind {
        ValueKind::Bool => provider
            .resolve_bool_value(flag_key, context)
            .await
            .map(|details| Resolved::Bool(details.value)),
        ValueKind::Integer => provider
            .resolve_int_value(flag_key, context)
            .await
            .map(|details| Resolved::Integer(details.value)),
        ValueKind::Float => provider
            .resolve_float_value(flag_key, context)
            .await
            .map(|details| Resolved::Float(details.value)),
        ValueKind::String => provider
            .resolve_string_value(flag_key, context)
            .await
            .map(|details| Resolved::String(details.value)),
        ValueKind::Json => provider
            .resolve_struct_value(flag_key, context)
            .await
            .map(|details| Resolved::Json(convert::struct_value_to_json(details.value))),
    };

    match resolved {
        Ok(value) => Outcome::Assigned(value),
        // `FlagNotFound` is OpenFeature's "nothing here for you", which is the
        // same thing Eppo reports as `Ok(None)`. Everything else is a failure
        // the provider wants attributed to it. Note that flagd folds a disabled
        // flag into `FlagNotFound` as well, so a flag turned off at the source
        // counts as unassigned rather than as an error.
        Err(error) if error.code == EvaluationErrorCode::FlagNotFound => Outcome::Unassigned,
        Err(error) => Outcome::Failed(error),
    }
}
