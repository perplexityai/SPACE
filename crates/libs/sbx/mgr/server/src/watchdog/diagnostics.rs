//! Evidence for a sandbox health transition, so a responder can tell from
//! the logs alone which layer stopped answering.
//!
//! Every composed or guest health transition is logged at once, from probe
//! state the watchdog already holds and without IO: each component's last
//! outcome and error text, its failure streak, and how long ago it last
//! succeeded. The line carries a `transition_id`.
//!
//! A degrading transition also asks for an evidence report, logged as a
//! second line with the same `sandbox_id` and `transition_id`. [`admission`]
//! bounds how many reports wait; a fixed set of collectors gathers each
//! within [`EVIDENCE_DEADLINE`], and on expiry logs what it has, marked
//! partial. A report holds:
//!
//! - a raw vsock check: a bare Firecracker `CONNECT` to the spaced-guest port.
//!   `accepted` means the guest kernel still services vsock, so a failing
//!   agent RPC points at the agent; `timeout` means the kernel did not answer;
//! - a resource sample of the VM's jailer cgroup (CPU use and throttling over
//!   a short window, CPU and memory pressure, memory use, OOM kills), to tell
//!   a starved or memory-exhausted guest from one that stalled while idle;
//! - when the composed status became `Degraded`, at most once per
//!   [`CONSOLE_TAIL_MIN_INTERVAL`] per sandbox, bounded tails of the
//!   Firecracker log and of the guest console file, when one exists.
//!
//! Sandbox IDs appear only in log lines; metrics are tagged by outcome alone.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use pplx_space_common_core::host::linux::paths;
use pplx_space_metrics::sandbox::worker::{
    SandboxHealthEvidenceOutcome, SandboxRawVsockCheckOutcome,
};
use pplx_space_spaced_common::ExpectedUdsPeer;
use tokio::sync::{Mutex, Semaphore};
use tokio_util::sync::CancellationToken;

use super::{
    ComposedHealthStatus, GuestHealthCategory, HealthComponent, HealthProbeOutcome, RUN_DIR,
    SandboxHealth, SidecarInfo,
};

pub(super) mod admission;
mod tail;

use admission::{Admission, EvidenceQueue, EvidenceRequest};
use tail::{TailBuffers, TailWindow};

/// Shorter than Firecracker's own 2 s deadline for a host-initiated
/// connection request, so a guest that never answers reads as `timeout`
/// rather than as a connection Firecracker closed by itself.
const RAW_VSOCK_CHECK_TIMEOUT: Duration = Duration::from_secs(1);
/// CPU use is measured as the cgroup's usage delta over at least this window.
const RESOURCE_SAMPLE_WINDOW: Duration = Duration::from_secs(1);
/// Collection of one report, from its first host read to its last. A report
/// normally takes about [`RESOURCE_SAMPLE_WINDOW`]; the margin covers slow
/// small reads, not a stuck one.
const EVIDENCE_DEADLINE: Duration = Duration::from_secs(3);
pub(super) const CONSOLE_TAIL_MIN_INTERVAL: Duration = Duration::from_secs(15 * 60);
const CONSOLE_TAIL_MAX_BYTES: usize = 4 * 1024;
/// Upper bound on one cgroup file read; `memory.stat`, the largest, is a few
/// KiB.
const SMALL_FILE_MAX_BYTES: u64 = 16 * 1024;
/// Blocking host reads in flight across all collectors. A read stuck in the
/// kernel outlives its report's deadline on a blocking-pool thread; this
/// caps how many such threads collectors can strand.
const HOST_READS_IN_FLIGHT: usize = admission::EVIDENCE_COLLECTORS;
const GUEST_CONSOLE_LOG_FILE: &str = "guest-console.log";
const MIB: u64 = 1024 * 1024;

/// Correlates a transition line with its evidence line.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct TransitionId(uuid::Uuid);

impl TransitionId {
    fn new() -> Self {
        Self(uuid::Uuid::now_v7())
    }
}

impl std::fmt::Display for TransitionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ConsoleTails {
    /// The transition does not take the composed status to `Degraded`.
    NotRequested,
    Included,
    /// An earlier degraded transition carried tails too recently.
    RateLimited,
}

impl ConsoleTails {
    fn as_str(self) -> &'static str {
        match self {
            Self::NotRequested => "not_requested",
            Self::Included => "included",
            Self::RateLimited => "rate_limited",
        }
    }

    /// The tails a report covering both transitions carries: a transition
    /// that claimed the sandbox's tail allowance keeps it.
    fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::Included, _) | (_, Self::Included) => Self::Included,
            (Self::RateLimited, _) | (_, Self::RateLimited) => Self::RateLimited,
            (Self::NotRequested, Self::NotRequested) => Self::NotRequested,
        }
    }
}

/// One component's probe state when the transition was observed.
#[derive(Debug)]
struct ComponentSnapshot {
    component: HealthComponent,
    degraded: bool,
    outcome: Option<HealthProbeOutcome>,
    detail: String,
    latency_micros: u64,
    consecutive_failures: u32,
    last_success_age: Option<Duration>,
}

impl ComponentSnapshot {
    fn new(health: &SandboxHealth, component: HealthComponent, now: SystemTime) -> Self {
        let component_health = health.health_components.get(component);
        let last = component_health.observations.back();
        Self {
            component,
            degraded: component_health.degraded,
            outcome: last.map(|observation| observation.outcome),
            detail: last.map_or_else(String::new, |observation| observation.detail.clone()),
            latency_micros: last.map_or(0, |observation| observation.latency_micros),
            consecutive_failures: component_health.consecutive_failures,
            last_success_age: component_health
                .last_success_at
                .map(|at| now.duration_since(at).unwrap_or_default()),
        }
    }

    fn name(&self) -> &'static str {
        self.component.as_metric().as_str()
    }

    fn outcome_str(&self) -> &'static str {
        self.outcome
            .map_or("unprobed", |outcome| outcome.as_metric().as_str())
    }
}

/// A composed or guest health change, with what is needed to log it.
#[derive(Debug)]
pub(super) struct HealthTransition {
    id: TransitionId,
    sandbox_id: String,
    lifecycle: Option<crate::state::SandboxState>,
    previous: ComposedHealthStatus,
    current: ComposedHealthStatus,
    guest_previous: GuestHealthCategory,
    guest_current: GuestHealthCategory,
    components: Vec<ComponentSnapshot>,
    gather_evidence: bool,
    console_tails: ConsoleTails,
}

impl HealthTransition {
    /// Compare the statuses before and after one probe result. Returns `None`
    /// when neither changed, so steady states cost nothing. Records the
    /// console-tail rate limit on `health` when this transition claims it,
    /// even if its report is later dropped.
    pub(super) fn observe(
        sandbox_id: &str,
        lifecycle: Option<crate::state::SandboxState>,
        previous: ComposedHealthStatus,
        guest_previous: GuestHealthCategory,
        health: &mut SandboxHealth,
        now: Instant,
    ) -> Option<Self> {
        let current = health.health_status;
        let guest_current = super::classify_guest_health(super::GuestHealthProbe::from(&*health));
        if current == previous && guest_current == guest_previous {
            return None;
        }
        let became_degraded =
            current == ComposedHealthStatus::Degraded && previous != ComposedHealthStatus::Degraded;
        let guest_degraded = guest_previous == GuestHealthCategory::Healthy
            && guest_current != GuestHealthCategory::Healthy;
        let console_tails = if !became_degraded {
            ConsoleTails::NotRequested
        } else if health
            .last_console_tail_at
            .is_some_and(|at| now.saturating_duration_since(at) < CONSOLE_TAIL_MIN_INTERVAL)
        {
            ConsoleTails::RateLimited
        } else {
            health.last_console_tail_at = Some(now);
            ConsoleTails::Included
        };
        let observed_at = SystemTime::now();
        Some(Self {
            id: TransitionId::new(),
            sandbox_id: sandbox_id.to_string(),
            lifecycle,
            previous,
            current,
            guest_previous,
            guest_current,
            components: HealthComponent::ALL
                .into_iter()
                .map(|component| ComponentSnapshot::new(health, component, observed_at))
                .collect(),
            gather_evidence: became_degraded || guest_degraded,
            console_tails,
        })
    }

    /// The evidence report this transition asks for. Only degrading
    /// transitions ask.
    pub(super) fn evidence_request(&self) -> Option<EvidenceRequest> {
        self.gather_evidence.then(|| EvidenceRequest {
            sandbox_id: self.sandbox_id.clone(),
            transition_id: self.id,
            console_tails: self.console_tails,
            queued_at: Instant::now(),
            merged_transitions: 0,
        })
    }

    fn component(&self, component: HealthComponent) -> Option<&ComponentSnapshot> {
        self.components
            .iter()
            .find(|snapshot| snapshot.component == component)
    }

    /// `firecracker=success spaced_guest=timeout(failures=4,last_ok=97s,"probe timed out") ...`
    fn probe_summary(&self) -> String {
        let mut summary = String::new();
        for snapshot in &self.components {
            if !summary.is_empty() {
                summary.push(' ');
            }
            let _ = write!(summary, "{}={}", snapshot.name(), snapshot.outcome_str());
            if snapshot.consecutive_failures == 0 && !snapshot.degraded {
                continue;
            }
            let _ = write!(summary, "(failures={}", snapshot.consecutive_failures);
            match snapshot.last_success_age {
                Some(age) => {
                    let _ = write!(summary, ",last_ok={}s", age.as_secs());
                }
                None => summary.push_str(",last_ok=never"),
            }
            if !snapshot.detail.is_empty() {
                let _ = write!(summary, ",{:?}", snapshot.detail);
            }
            summary.push(')');
        }
        summary
    }

    fn degraded_components(&self) -> String {
        self.components
            .iter()
            .filter(|snapshot| snapshot.degraded)
            .map(ComponentSnapshot::name)
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Log the transition. `admission` is what became of its evidence
    /// request, when it made one.
    pub(super) fn emit(&self, admission: Option<Admission>) {
        let guest = self.component(HealthComponent::SpacedGuest);
        macro_rules! emit_transition {
            ($message:literal) => {
                tracing::info!(
                    target: super::LOG_TARGET,
                    sandbox_id = %self.sandbox_id,
                    transition_id = %self.id,
                    lifecycle = self.lifecycle.map(|state| state.as_str()),
                    previous = ?self.previous,
                    current = ?self.current,
                    guest_health_previous = self.guest_previous.as_str(),
                    guest_health = self.guest_current.as_str(),
                    degraded_components = %self.degraded_components(),
                    probes = %self.probe_summary(),
                    guest_probe_outcome = guest.map(ComponentSnapshot::outcome_str),
                    guest_probe_detail = guest.map(|snapshot| snapshot.detail.as_str()),
                    guest_probe_latency_ms = guest.map(|snapshot| millis(snapshot.latency_micros)),
                    guest_probe_consecutive_failures = guest.map(|snapshot| snapshot.consecutive_failures),
                    guest_probe_last_success_age_s = guest.and_then(|snapshot| snapshot.last_success_age).map(|age| age.as_secs()),
                    evidence = admission.map_or("not_requested", Admission::as_str),
                    console_tails = self.console_tails.as_str(),
                    $message
                )
            };
        }
        if self.previous != self.current {
            emit_transition!("sandbox composed health changed");
        } else {
            emit_transition!("sandbox guest health changed");
        }
    }
}

/// Log `transition` and admit its evidence request, without waiting: the
/// line never depends on collectors keeping up.
pub(super) fn report_transition(queue: &EvidenceQueue, transition: &HealthTransition) {
    let admission = transition.evidence_request().map(|request| {
        let admission = queue.admit(request);
        let outcome = match admission {
            Admission::Queued => None,
            Admission::Merged => Some(SandboxHealthEvidenceOutcome::Merged),
            Admission::Dropped => Some(SandboxHealthEvidenceOutcome::Dropped),
        };
        if let Some(outcome) = outcome {
            pplx_space_metrics::sandbox::worker::sandbox_health_evidence_report(outcome);
        }
        admission
    });
    transition.emit(admission);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RawVsockOutcome {
    Accepted,
    Refused,
    UnexpectedReply,
    Timeout,
    HostSocketUnavailable,
    NotAttempted,
}

impl RawVsockOutcome {
    fn as_metric(self) -> SandboxRawVsockCheckOutcome {
        match self {
            Self::Accepted => SandboxRawVsockCheckOutcome::Accepted,
            Self::Refused => SandboxRawVsockCheckOutcome::Refused,
            Self::UnexpectedReply => SandboxRawVsockCheckOutcome::UnexpectedReply,
            Self::Timeout => SandboxRawVsockCheckOutcome::Timeout,
            Self::HostSocketUnavailable => SandboxRawVsockCheckOutcome::HostSocketUnavailable,
            Self::NotAttempted => SandboxRawVsockCheckOutcome::NotAttempted,
        }
    }

    fn as_str(self) -> &'static str {
        self.as_metric().as_str()
    }
}

/// Whether the process behind the VM's vsock socket is the Firecracker the
/// sandbox manager recorded. The check proceeds either way: a mismatch is
/// itself evidence of a host-side identity problem, and a bare `CONNECT`
/// sends nothing the listener could misuse.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PeerCheck {
    Matched,
    Mismatched,
    Unknown,
}

impl PeerCheck {
    fn of(stream: &tokio::net::UnixStream, expected: Option<ExpectedUdsPeer>) -> Self {
        let (Some(expected), Ok(credentials)) = (expected, stream.peer_cred()) else {
            return Self::Unknown;
        };
        if credentials.pid() == Some(expected.pid) && credentials.uid() == expected.uid {
            Self::Matched
        } else {
            Self::Mismatched
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Matched => "matched",
            Self::Mismatched => "mismatched",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug)]
pub(super) struct RawVsockCheck {
    pub(super) outcome: RawVsockOutcome,
    latency: Duration,
    pub(super) peer: PeerCheck,
    detail: String,
}

/// Connect to the VM's host-side vsock socket and run only Firecracker's
/// `CONNECT` handshake to the spaced-guest port, within `timeout` end to end.
/// The guest kernel answers it without the agent: the agent's listener only
/// has to exist, not to call `accept`.
pub(super) async fn raw_vsock_check(
    vsock_uds_path: &Path,
    expected_peer: Option<ExpectedUdsPeer>,
    timeout: Duration,
) -> RawVsockCheck {
    let started = Instant::now();
    let mut peer = PeerCheck::Unknown;
    let result = tokio::time::timeout(timeout, async {
        let mut stream = tokio::net::UnixStream::connect(vsock_uds_path)
            .await
            .map_err(|error| (RawVsockOutcome::HostSocketUnavailable, error))?;
        peer = PeerCheck::of(&stream, expected_peer);
        pplx_space_spaced_common::firecracker_vsock_handshake(
            &mut stream,
            pplx_space_spaced_common::BRIDGE_GUEST_VSOCK_PORT,
        )
        .await
        .map_err(|error| {
            let outcome = match error.kind() {
                std::io::ErrorKind::UnexpectedEof
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::BrokenPipe => RawVsockOutcome::Refused,
                std::io::ErrorKind::InvalidData => RawVsockOutcome::UnexpectedReply,
                _ => RawVsockOutcome::HostSocketUnavailable,
            };
            (outcome, error)
        })
    })
    .await;
    let latency = started.elapsed();
    let (outcome, detail) = match result {
        Ok(Ok(())) => (RawVsockOutcome::Accepted, String::new()),
        Ok(Err((outcome, error))) => (outcome, super::bounded_health_detail(error.to_string())),
        Err(_) => (RawVsockOutcome::Timeout, String::new()),
    };
    RawVsockCheck {
        outcome,
        latency,
        peer,
        detail,
    }
}

/// What collection needs from a sandbox's sidecar record, copied under the
/// sidecar lock in place of the whole record. The VM's cgroup follows from
/// the sandbox ID.
#[derive(Debug)]
pub(super) struct EvidenceTarget {
    vsock_uds_path: Option<PathBuf>,
    expected_peer: Option<ExpectedUdsPeer>,
}

impl EvidenceTarget {
    pub(super) fn of(info: &SidecarInfo) -> Self {
        Self {
            vsock_uds_path: info.vsock_uds_path.as_deref().map(PathBuf::from),
            expected_peer: info
                .runtime_pid
                .and_then(|pid| i32::try_from(pid).ok())
                .zip(info.uid)
                .map(|(pid, uid)| ExpectedUdsPeer::new(pid, uid)),
        }
    }
}

/// Host files a report reads for one sandbox.
#[derive(Debug)]
pub(super) struct EvidencePaths {
    cgroup: PathBuf,
    firecracker_log: PathBuf,
    guest_console: PathBuf,
}

impl EvidencePaths {
    fn for_sandbox(sandbox_id: &str) -> Self {
        Self {
            cgroup: pplx_space_node_runtime_core::cgroup_paths::jailer_cgroup_path(sandbox_id),
            // The launch configures Firecracker's own log inside the jail.
            firecracker_log: paths::jail_root(sandbox_id).join(paths::FIRECRACKER_LOG_FILE),
            // Created only when `SPACE_ENABLE_GUEST_CONSOLE_LOGS` is set, as
            // the jailer's output. Guest serial output reaches it only if
            // Firecracker's own stdout does, which a daemonizing jailer
            // redirects to `/dev/null`.
            guest_console: PathBuf::from(format!(
                "{RUN_DIR}/{sandbox_id}/{GUEST_CONSOLE_LOG_FILE}"
            )),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct EvidenceLimits {
    deadline: Duration,
    raw_vsock_timeout: Duration,
    sample_window: Duration,
}

impl EvidenceLimits {
    const PRODUCTION: Self = Self {
        deadline: EVIDENCE_DEADLINE,
        raw_vsock_timeout: RAW_VSOCK_CHECK_TIMEOUT,
        sample_window: RESOURCE_SAMPLE_WINDOW,
    };
}

/// Blocking host reads, at most [`HOST_READS_IN_FLIGHT`] at a time across
/// collectors. A read holds its slot until it actually returns, so a stuck
/// read keeps one blocking-pool thread and later reads queue for a slot
/// within their own report's deadline.
#[derive(Clone)]
pub(super) struct HostReads {
    slots: Arc<Semaphore>,
}

impl HostReads {
    pub(super) fn new(slots: usize) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(slots)),
        }
    }

    async fn blocking<T: Send + 'static>(
        &self,
        read: impl FnOnce() -> T + Send + 'static,
    ) -> Option<T> {
        let slot = Arc::clone(&self.slots).acquire_owned().await.ok()?;
        tokio::task::spawn_blocking(move || {
            let _slot = slot;
            read()
        })
        .await
        .ok()
    }

    /// A small cgroup or proc file, up to [`SMALL_FILE_MAX_BYTES`]; absent or
    /// unreadable reads as `None`.
    async fn small(&self, path: PathBuf) -> Option<String> {
        self.blocking(move || {
            use std::io::Read as _;
            let mut content = String::new();
            std::fs::File::open(path)
                .ok()?
                .take(SMALL_FILE_MAX_BYTES)
                .read_to_string(&mut content)
                .ok()?;
            Some(content)
        })
        .await
        .flatten()
    }

    /// The newest lines of a log file within `window`. `None` when the file
    /// does not exist; a read error is reported in place of the text.
    async fn tail(&self, path: PathBuf, window: TailWindow) -> Option<String> {
        self.blocking(move || {
            use std::io::{Seek as _, SeekFrom};
            let read = || {
                let mut file = std::fs::File::open(&path)?;
                let start = file.metadata()?.len().saturating_sub(window.scan_bytes);
                file.seek(SeekFrom::Start(start))?;
                tail::tail_lines(file, start > 0, window, &mut TailBuffers::new(window))
            };
            match read() {
                Ok(text) => Some(text),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => Some(format!("<unreadable: {error}>")),
            }
        })
        .await
        .flatten()
    }
}

/// How far collection got; a partial report names the step its deadline
/// interrupted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum EvidenceStage {
    #[default]
    CpuBefore,
    RawVsock,
    SampleWindow,
    CpuAfter,
    ResourceFiles,
    ConsoleTails,
    Done,
}

impl EvidenceStage {
    fn as_str(self) -> &'static str {
        match self {
            Self::CpuBefore => "cpu_before",
            Self::RawVsock => "raw_vsock",
            Self::SampleWindow => "sample_window",
            Self::CpuAfter => "cpu_after",
            Self::ResourceFiles => "resource_files",
            Self::ConsoleTails => "console_tails",
            Self::Done => "done",
        }
    }
}

/// One sample of the VM's jailer cgroup, which holds only the Firecracker
/// process and its vCPU threads. Every field is best effort: a missing file
/// (for example PSI disabled on the host) leaves it unset.
#[derive(Debug, Default)]
struct ResourceSample {
    window: Option<Duration>,
    /// CPU time used over `window`, as a percentage of one CPU.
    cpu_util_pct: Option<f64>,
    /// Time throttled by `cpu.max` over `window`, as a percentage of it.
    cpu_throttled_pct: Option<f64>,
    cpu_max: Option<String>,
    cpu_pressure_some_avg10: Option<f64>,
    cpu_pressure_some_avg60: Option<f64>,
    memory_current_mib: Option<u64>,
    /// Anonymous memory: the guest RAM Firecracker has touched.
    memory_anon_mib: Option<u64>,
    memory_max_mib: Option<u64>,
    memory_pressure_some_avg10: Option<f64>,
    /// Times usage hit `memory.max`, and OOM kills, over the cgroup's life.
    memory_max_events: Option<u64>,
    memory_oom_kills: Option<u64>,
}

#[derive(Clone, Copy, Debug)]
struct CpuCounters {
    usage_usec: Option<u64>,
    throttled_usec: Option<u64>,
}

impl CpuCounters {
    async fn read(reads: &HostReads, cgroup: &Path) -> Self {
        let content = reads.small(cgroup.join("cpu.stat")).await;
        let content = content.as_deref().unwrap_or_default();
        Self {
            usage_usec: keyed_u64(content, "usage_usec"),
            throttled_usec: keyed_u64(content, "throttled_usec"),
        }
    }
}

impl ResourceSample {
    fn record_cpu_window(&mut self, before: CpuCounters, after: CpuCounters, window: Duration) {
        let window_usec = window.as_micros() as f64;
        let percent_of_window = |start: Option<u64>, end: Option<u64>| {
            let delta = end?.checked_sub(start?)?;
            (window_usec > 0.0).then(|| round_tenths(delta as f64 * 100.0 / window_usec))
        };
        self.window = Some(window);
        self.cpu_util_pct = percent_of_window(before.usage_usec, after.usage_usec);
        self.cpu_throttled_pct = percent_of_window(before.throttled_usec, after.throttled_usec);
    }

    /// Read the remaining cgroup files, filling each field as its file
    /// arrives so a deadline keeps what was read.
    async fn read_files(&mut self, reads: &HostReads, cgroup: &Path) {
        let mib = |value: String| value.trim().parse::<u64>().ok().map(|bytes| bytes / MIB);
        self.cpu_max = reads
            .small(cgroup.join("cpu.max"))
            .await
            .map(|value| value.trim().to_string());
        if let Some(content) = reads.small(cgroup.join("cpu.pressure")).await {
            self.cpu_pressure_some_avg10 = psi_average(&content, "some", "avg10");
            self.cpu_pressure_some_avg60 = psi_average(&content, "some", "avg60");
        }
        self.memory_current_mib = reads
            .small(cgroup.join("memory.current"))
            .await
            .and_then(mib);
        self.memory_anon_mib = reads
            .small(cgroup.join("memory.stat"))
            .await
            .and_then(|content| keyed_u64(&content, "anon"))
            .map(|bytes| bytes / MIB);
        self.memory_max_mib = reads.small(cgroup.join("memory.max")).await.and_then(mib);
        self.memory_pressure_some_avg10 = reads
            .small(cgroup.join("memory.pressure"))
            .await
            .and_then(|content| psi_average(&content, "some", "avg10"));
        if let Some(content) = reads.small(cgroup.join("memory.events")).await {
            self.memory_max_events = keyed_u64(&content, "max");
            self.memory_oom_kills = keyed_u64(&content, "oom_kill");
        }
    }
}

#[derive(Debug, Default)]
struct ConsoleTailText {
    firecracker_log: Option<String>,
    guest_console: Option<String>,
}

/// Evidence gathered for one report. Fields stay unset when collection
/// stopped before reaching them.
#[derive(Debug, Default)]
pub(super) struct TransitionEvidence {
    /// Whether the deadline expired, at `stage`.
    partial: bool,
    pub(super) stage: EvidenceStage,
    pub(super) raw_vsock: Option<RawVsockCheck>,
    resources: ResourceSample,
    console_tails: Option<ConsoleTailText>,
    elapsed: Duration,
}

impl TransitionEvidence {
    /// Gather evidence for `request` within `limits.deadline`, returning what
    /// was collected when it expires. The raw vsock check runs inside the CPU
    /// sampling window, so a full report takes about that window.
    pub(super) async fn gather(
        request: &EvidenceRequest,
        target: Option<&EvidenceTarget>,
        paths: &EvidencePaths,
        limits: EvidenceLimits,
        reads: &HostReads,
    ) -> Self {
        let started = Instant::now();
        let mut evidence = Self::default();
        evidence.partial = tokio::time::timeout(
            limits.deadline,
            evidence.collect(request, target, paths, limits, reads),
        )
        .await
        .is_err();
        evidence.elapsed = started.elapsed();
        evidence
    }

    async fn collect(
        &mut self,
        request: &EvidenceRequest,
        target: Option<&EvidenceTarget>,
        paths: &EvidencePaths,
        limits: EvidenceLimits,
        reads: &HostReads,
    ) {
        self.stage = EvidenceStage::CpuBefore;
        let before = CpuCounters::read(reads, &paths.cgroup).await;
        let window_started = Instant::now();
        self.stage = EvidenceStage::RawVsock;
        let raw_vsock = match target.and_then(|target| target.vsock_uds_path.as_deref()) {
            Some(path) => {
                raw_vsock_check(
                    path,
                    target.and_then(|target| target.expected_peer),
                    limits.raw_vsock_timeout,
                )
                .await
            }
            None => RawVsockCheck {
                outcome: RawVsockOutcome::NotAttempted,
                latency: Duration::ZERO,
                peer: PeerCheck::Unknown,
                detail: "v.sock path is absent".to_string(),
            },
        };
        pplx_space_metrics::sandbox::worker::sandbox_health_raw_vsock_check(
            raw_vsock.outcome.as_metric(),
        );
        self.raw_vsock = Some(raw_vsock);
        self.stage = EvidenceStage::SampleWindow;
        tokio::time::sleep_until((window_started + limits.sample_window).into()).await;
        self.stage = EvidenceStage::CpuAfter;
        let after = CpuCounters::read(reads, &paths.cgroup).await;
        self.resources
            .record_cpu_window(before, after, window_started.elapsed());
        self.stage = EvidenceStage::ResourceFiles;
        self.resources.read_files(reads, &paths.cgroup).await;
        if request.console_tails == ConsoleTails::Included {
            self.stage = EvidenceStage::ConsoleTails;
            let tails = self.console_tails.insert(ConsoleTailText::default());
            tails.firecracker_log = reads
                .tail(paths.firecracker_log.clone(), FIRECRACKER_LOG_TAIL)
                .await;
            tails.guest_console = reads
                .tail(paths.guest_console.clone(), GUEST_CONSOLE_TAIL)
                .await;
        }
        self.stage = EvidenceStage::Done;
    }

    pub(super) fn is_partial(&self) -> bool {
        self.partial
    }

    fn emit(&self, request: &EvidenceRequest, queued: Duration) {
        let raw = self.raw_vsock.as_ref();
        let resources = &self.resources;
        let tails = self.console_tails.as_ref();
        tracing::info!(
            target: super::LOG_TARGET,
            sandbox_id = %request.sandbox_id,
            transition_id = %request.transition_id,
            merged_transitions = request.merged_transitions,
            evidence_partial = self.is_partial(),
            evidence_stalled_at = self.is_partial().then(|| self.stage.as_str()),
            evidence_queue_ms = duration_ms(queued),
            evidence_elapsed_ms = duration_ms(self.elapsed),
            raw_vsock_check = raw.map_or("not_run", |raw| raw.outcome.as_str()),
            raw_vsock_latency_ms = raw.map(|raw| round_tenths(raw.latency.as_secs_f64() * 1_000.0)),
            raw_vsock_peer = raw.map(|raw| raw.peer.as_str()),
            raw_vsock_detail = raw.map(|raw| raw.detail.as_str()).filter(|detail| !detail.is_empty()),
            vm_cpu_util_pct = resources.cpu_util_pct,
            vm_cpu_throttled_pct = resources.cpu_throttled_pct,
            vm_cpu_max = resources.cpu_max.as_deref(),
            vm_cpu_pressure_some_avg10 = resources.cpu_pressure_some_avg10,
            vm_cpu_pressure_some_avg60 = resources.cpu_pressure_some_avg60,
            vm_memory_current_mib = resources.memory_current_mib,
            vm_memory_anon_mib = resources.memory_anon_mib,
            vm_memory_max_mib = resources.memory_max_mib,
            vm_memory_pressure_some_avg10 = resources.memory_pressure_some_avg10,
            vm_memory_max_events = resources.memory_max_events,
            vm_memory_oom_kills = resources.memory_oom_kills,
            resource_window_ms = resources.window.map(duration_ms),
            console_tails = request.console_tails.as_str(),
            fc_log_tail = tails.and_then(|tails| tails.firecracker_log.as_deref()),
            guest_console_tail = tails.and_then(|tails| tails.guest_console.as_deref()),
            "sandbox health evidence"
        );
    }
}

/// One of the [`admission::EVIDENCE_COLLECTORS`] long-lived collectors: take
/// the oldest waiting report, look up the sandbox's current sidecar
/// authority, so evidence reflects the endpoint the next probe would use,
/// and log the report. Returns on shutdown, abandoning a report in flight.
pub(super) async fn run_evidence_collector(
    queue: Arc<EvidenceQueue>,
    sidecars: Arc<Mutex<HashMap<String, SidecarInfo>>>,
    reads: HostReads,
    shutdown_token: CancellationToken,
) {
    loop {
        let request = tokio::select! {
            biased;
            _ = shutdown_token.cancelled() => return,
            request = queue.next() => request,
        };
        let queued = request.queued_at.elapsed();
        let target = sidecars
            .lock()
            .await
            .get(&request.sandbox_id)
            .map(EvidenceTarget::of);
        let paths = EvidencePaths::for_sandbox(&request.sandbox_id);
        let evidence = tokio::select! {
            biased;
            _ = shutdown_token.cancelled() => return,
            evidence = TransitionEvidence::gather(
                &request,
                target.as_ref(),
                &paths,
                EvidenceLimits::PRODUCTION,
                &reads,
            ) => evidence,
        };
        evidence.emit(&request, queued);
        pplx_space_metrics::sandbox::worker::sandbox_health_evidence_report(
            if evidence.is_partial() {
                SandboxHealthEvidenceOutcome::Partial
            } else {
                SandboxHealthEvidenceOutcome::Complete
            },
        );
    }
}

pub(super) fn host_reads() -> HostReads {
    HostReads::new(HOST_READS_IN_FLIGHT)
}

const FIRECRACKER_LOG_TAIL: TailWindow = TailWindow {
    scan_bytes: 64 * 1024,
    keep_bytes: CONSOLE_TAIL_MAX_BYTES,
    noise: tail::firecracker_state_poll,
};

const GUEST_CONSOLE_TAIL: TailWindow = TailWindow {
    scan_bytes: CONSOLE_TAIL_MAX_BYTES as u64,
    keep_bytes: CONSOLE_TAIL_MAX_BYTES,
    noise: tail::no_noise,
};

/// The value of a `key value` line, as in `cpu.stat`, `memory.stat`, and
/// `memory.events`.
fn keyed_u64(content: &str, key: &str) -> Option<u64> {
    content.lines().find_map(|line| {
        let (name, value) = line.split_once(' ')?;
        (name == key).then(|| value.trim().parse().ok()).flatten()
    })
}

/// One average from a PSI file line such as
/// `some avg10=1.50 avg60=0.20 avg300=0.00 total=12345`.
fn psi_average(content: &str, kind: &str, field: &str) -> Option<f64> {
    let line = content
        .lines()
        .find(|line| line.split_whitespace().next() == Some(kind))?;
    line.split_whitespace().find_map(|pair| {
        let (name, value) = pair.split_once('=')?;
        (name == field).then(|| value.parse().ok()).flatten()
    })
}

fn millis(micros: u64) -> f64 {
    round_tenths(micros as f64 / 1_000.0)
}

fn round_tenths(value: f64) -> f64 {
    (value * 10.0).round() / 10.0
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests;
