//! Evidence attached to a sandbox health transition, so a responder can tell
//! from the log line alone which layer stopped answering.
//!
//! Every composed or guest health transition logs the probe state the watchdog
//! already holds: each component's last outcome and error text, its failure
//! streak, and how long ago it last succeeded. A degrading transition also
//! gathers, once and within a bounded budget:
//!
//! - a raw vsock check: a bare Firecracker `CONNECT` to the spaced-guest port.
//!   `accepted` means the guest kernel still services vsock, so a failing
//!   agent RPC points at the agent; `timeout` means the kernel did not answer;
//! - a resource sample of the VM's jailer cgroup (CPU use and throttling over
//!   a short window, CPU and memory pressure, memory use, OOM kills), to tell
//!   a starved or memory-exhausted guest from one that stalled while idle;
//! - when the composed status becomes `Degraded`, at most once per
//!   [`CONSOLE_TAIL_MIN_INTERVAL`] per sandbox, bounded tails of the
//!   Firecracker log and, if console capture is enabled, the guest console.
//!
//! Sandbox IDs appear only in the log line; the one metric is tagged by
//! outcome alone.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

use pplx_space_metrics::sandbox::worker::SandboxRawVsockCheckOutcome;
use pplx_space_spaced_common::ExpectedUdsPeer;

use super::{
    ComposedHealthStatus, GuestHealthCategory, HealthComponent, HealthProbeOutcome, RUN_DIR,
    SandboxHealth, SidecarInfo,
};

/// Shorter than Firecracker's own 2 s deadline for a host-initiated
/// connection request, so a guest that never answers reads as `timeout`
/// rather than as a connection Firecracker closed by itself.
const RAW_VSOCK_CHECK_TIMEOUT: Duration = Duration::from_secs(1);
/// CPU use is measured as the cgroup's usage delta over at least this window,
/// which also bounds how long a degrading transition's log line is delayed.
const RESOURCE_SAMPLE_WINDOW: Duration = Duration::from_secs(1);
pub(super) const CONSOLE_TAIL_MIN_INTERVAL: Duration = Duration::from_secs(15 * 60);
const CONSOLE_TAIL_MAX_BYTES: usize = 4 * 1024;
/// Degrading transitions gathering evidence at once. A node-wide event
/// degrades many sandboxes together; extra transitions wait for a slot.
pub(super) const EVIDENCE_CONCURRENCY: usize = 4;
const GUEST_CONSOLE_LOG_FILE: &str = "guest-console.log";
const MIB: u64 = 1024 * 1024;

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
    /// console-tail rate limit on `health` when this transition claims it.
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

    /// Whether this transition needs [`TransitionEvidence::gather`] before
    /// it is logged. Only degrading transitions pay for it.
    pub(super) fn needs_evidence(&self) -> bool {
        self.gather_evidence
    }

    pub(super) fn sandbox_id(&self) -> &str {
        &self.sandbox_id
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

    /// Log the transition, with `evidence` when it [needs](Self::needs_evidence)
    /// it.
    pub(super) fn emit(&self, evidence: Option<&TransitionEvidence>) {
        let guest = self.component(HealthComponent::SpacedGuest);
        let raw = evidence.map(|evidence| &evidence.raw_vsock);
        let resources = evidence.map(|evidence| &evidence.resources);
        let tails = evidence.and_then(|evidence| evidence.console_tails.as_ref());
        macro_rules! emit_transition {
            ($message:literal) => {
                tracing::info!(
                    target: super::LOG_TARGET,
                    sandbox_id = %self.sandbox_id,
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
                    raw_vsock_check = raw.map_or("not_run", |raw| raw.outcome.as_str()),
                    raw_vsock_latency_ms = raw.map(|raw| round_tenths(raw.latency.as_secs_f64() * 1_000.0)),
                    raw_vsock_peer = raw.map(|raw| raw.peer.as_str()),
                    raw_vsock_detail = raw.map(|raw| raw.detail.as_str()).filter(|detail| !detail.is_empty()),
                    vm_cpu_util_pct = resources.and_then(|sample| sample.cpu_util_pct),
                    vm_cpu_throttled_pct = resources.and_then(|sample| sample.cpu_throttled_pct),
                    vm_cpu_max = resources.and_then(|sample| sample.cpu_max.as_deref()),
                    vm_cpu_pressure_some_avg10 = resources.and_then(|sample| sample.cpu_pressure_some_avg10),
                    vm_cpu_pressure_some_avg60 = resources.and_then(|sample| sample.cpu_pressure_some_avg60),
                    vm_memory_current_mib = resources.and_then(|sample| sample.memory_current_mib),
                    vm_memory_anon_mib = resources.and_then(|sample| sample.memory_anon_mib),
                    vm_memory_max_mib = resources.and_then(|sample| sample.memory_max_mib),
                    vm_memory_pressure_some_avg10 = resources.and_then(|sample| sample.memory_pressure_some_avg10),
                    vm_memory_max_events = resources.and_then(|sample| sample.memory_max_events),
                    vm_memory_oom_kills = resources.and_then(|sample| sample.memory_oom_kills),
                    resource_window_ms = resources.map(|sample| u64::try_from(sample.window.as_millis()).unwrap_or(u64::MAX)),
                    console_tails = self.console_tails.as_str(),
                    fc_log_tail = tails.and_then(|tails| tails.firecracker_log.as_deref()),
                    guest_console_tail = tails.and_then(|tails| tails.guest_console.as_deref()),
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

/// One sample of the VM's jailer cgroup, which holds only the Firecracker
/// process and its vCPU threads. Every field is best effort: a missing file
/// (for example PSI disabled on the host) leaves it unset.
#[derive(Debug, Default)]
struct ResourceSample {
    window: Duration,
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
    async fn read(cgroup: &Path) -> Self {
        let content = read_small(&cgroup.join("cpu.stat")).await;
        let content = content.as_deref().unwrap_or_default();
        Self {
            usage_usec: keyed_u64(content, "usage_usec"),
            throttled_usec: keyed_u64(content, "throttled_usec"),
        }
    }
}

impl ResourceSample {
    async fn read(
        cgroup: &Path,
        before: CpuCounters,
        after: CpuCounters,
        window: Duration,
    ) -> Self {
        let window_usec = window.as_micros() as f64;
        let percent_of_window = |start: Option<u64>, end: Option<u64>| {
            let delta = end?.checked_sub(start?)?;
            (window_usec > 0.0).then(|| round_tenths(delta as f64 * 100.0 / window_usec))
        };
        let cpu_pressure = read_small(&cgroup.join("cpu.pressure")).await;
        let memory_stat = read_small(&cgroup.join("memory.stat")).await;
        let memory_events = read_small(&cgroup.join("memory.events")).await;
        let memory_pressure = read_small(&cgroup.join("memory.pressure")).await;
        Self {
            window,
            cpu_util_pct: percent_of_window(before.usage_usec, after.usage_usec),
            cpu_throttled_pct: percent_of_window(before.throttled_usec, after.throttled_usec),
            cpu_max: read_small(&cgroup.join("cpu.max"))
                .await
                .map(|value| value.trim().to_string()),
            cpu_pressure_some_avg10: cpu_pressure
                .as_deref()
                .and_then(|content| psi_average(content, "some", "avg10")),
            cpu_pressure_some_avg60: cpu_pressure
                .as_deref()
                .and_then(|content| psi_average(content, "some", "avg60")),
            memory_current_mib: read_small(&cgroup.join("memory.current"))
                .await
                .and_then(|value| value.trim().parse::<u64>().ok())
                .map(|bytes| bytes / MIB),
            memory_anon_mib: memory_stat
                .as_deref()
                .and_then(|content| keyed_u64(content, "anon"))
                .map(|bytes| bytes / MIB),
            memory_max_mib: read_small(&cgroup.join("memory.max"))
                .await
                .and_then(|value| value.trim().parse::<u64>().ok())
                .map(|bytes| bytes / MIB),
            memory_pressure_some_avg10: memory_pressure
                .as_deref()
                .and_then(|content| psi_average(content, "some", "avg10")),
            memory_max_events: memory_events
                .as_deref()
                .and_then(|content| keyed_u64(content, "max")),
            memory_oom_kills: memory_events
                .as_deref()
                .and_then(|content| keyed_u64(content, "oom_kill")),
        }
    }
}

#[derive(Debug)]
struct ConsoleTailText {
    firecracker_log: Option<String>,
    guest_console: Option<String>,
}

/// Evidence gathered for a degrading transition.
#[derive(Debug)]
pub(super) struct TransitionEvidence {
    pub(super) raw_vsock: RawVsockCheck,
    resources: ResourceSample,
    console_tails: Option<ConsoleTailText>,
}

impl TransitionEvidence {
    /// Gather evidence for `transition` in about [`RESOURCE_SAMPLE_WINDOW`]:
    /// the raw vsock check runs inside the CPU sampling window, and the
    /// remaining reads are a few small files.
    pub(super) async fn gather(transition: &HealthTransition, info: Option<&SidecarInfo>) -> Self {
        let sandbox_id = transition.sandbox_id.as_str();
        let cgroup = pplx_space_node_runtime_core::cgroup_paths::jailer_cgroup_path(sandbox_id);
        let before = CpuCounters::read(&cgroup).await;
        let window_started = Instant::now();
        let raw_vsock = match info.and_then(|info| info.vsock_uds_path.as_deref()) {
            Some(path) => {
                raw_vsock_check(
                    Path::new(path),
                    expected_peer(info),
                    RAW_VSOCK_CHECK_TIMEOUT,
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
        tokio::time::sleep_until((window_started + RESOURCE_SAMPLE_WINDOW).into()).await;
        let after = CpuCounters::read(&cgroup).await;
        let resources =
            ResourceSample::read(&cgroup, before, after, window_started.elapsed()).await;
        let console_tails = if transition.console_tails == ConsoleTails::Included {
            Some(ConsoleTailText {
                firecracker_log: read_tail(&firecracker_log_path(sandbox_id), FIRECRACKER_LOG_TAIL)
                    .await,
                guest_console: read_tail(&guest_console_log_path(sandbox_id), GUEST_CONSOLE_TAIL)
                    .await,
            })
        } else {
            None
        };
        Self {
            raw_vsock,
            resources,
            console_tails,
        }
    }
}

fn expected_peer(info: Option<&SidecarInfo>) -> Option<ExpectedUdsPeer> {
    let info = info?;
    let pid = i32::try_from(info.runtime_pid?).ok()?;
    Some(ExpectedUdsPeer::new(pid, info.uid?))
}

/// Firecracker's own log, which the launch configures as `/fc.log` inside
/// the jail.
fn firecracker_log_path(sandbox_id: &str) -> PathBuf {
    PathBuf::from(format!(
        "{}/firecracker/{sandbox_id}/root/fc.log",
        pplx_space_common_core::host::linux::paths::FIRECRACKER_JAIL_DIR
    ))
}

/// Written only when `SPACE_ENABLE_GUEST_CONSOLE_LOGS` attaches the serial
/// console at launch.
fn guest_console_log_path(sandbox_id: &str) -> PathBuf {
    PathBuf::from(format!("{RUN_DIR}/{sandbox_id}/{GUEST_CONSOLE_LOG_FILE}"))
}

/// Read a small cgroup or proc file; absent or unreadable reads as `None`.
async fn read_small(path: &Path) -> Option<String> {
    tokio::fs::read_to_string(path).await.ok()
}

/// What a tail keeps of a log file: the newest whole lines, up to
/// `keep_bytes`, found in its last `scan_bytes`, skipping lines `noise`
/// accepts. Scanning more than is kept stops periodic noise from crowding out
/// the events around it.
#[derive(Clone, Copy)]
struct TailWindow {
    scan_bytes: u64,
    keep_bytes: usize,
    noise: fn(&mut NoiseFilter, &str) -> bool,
}

/// Per-tail state for a [`TailWindow::noise`] filter that pairs lines.
#[derive(Default)]
struct NoiseFilter {
    skipped_request: bool,
}

/// Firecracker logs every API request and its response at `Info`. The
/// watchdog reads VM state (`GET /`) on every probe, so without this a tail
/// is little but those pairs. Any other request, such as a pause, is kept
/// with its response.
fn firecracker_state_poll(filter: &mut NoiseFilter, line: &str) -> bool {
    let follows_skipped_request = std::mem::take(&mut filter.skipped_request);
    if line.contains(":fc_api] The API server received a Get request on \"/\".") {
        filter.skipped_request = true;
        return true;
    }
    follows_skipped_request && line.contains(":fc_api] The request was executed successfully.")
}

fn no_noise(_: &mut NoiseFilter, _: &str) -> bool {
    false
}

const FIRECRACKER_LOG_TAIL: TailWindow = TailWindow {
    scan_bytes: 64 * 1024,
    keep_bytes: CONSOLE_TAIL_MAX_BYTES,
    noise: firecracker_state_poll,
};

const GUEST_CONSOLE_TAIL: TailWindow = TailWindow {
    scan_bytes: CONSOLE_TAIL_MAX_BYTES as u64,
    keep_bytes: CONSOLE_TAIL_MAX_BYTES,
    noise: no_noise,
};

/// The newest lines of a log file within `window`. Invalid UTF-8 and control
/// characters other than newlines and tabs become `?`, which never lengthens
/// the text. `None` when the file does not exist; a read error is reported in
/// place of the text.
async fn read_tail(path: &Path, window: TailWindow) -> Option<String> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    let read = async {
        let mut file = tokio::fs::File::open(path).await?;
        let length = file.metadata().await?.len();
        let start = length.saturating_sub(window.scan_bytes);
        file.seek(std::io::SeekFrom::Start(start)).await?;
        let mut bytes = Vec::new();
        file.take(window.scan_bytes).read_to_end(&mut bytes).await?;
        Ok::<_, std::io::Error>((bytes, start > 0))
    };
    match read.await {
        Ok((bytes, truncated)) => Some(tail_text(&bytes, truncated, window)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => Some(format!("<unreadable: {error}>")),
    }
}

fn tail_text(bytes: &[u8], truncated: bool, window: TailWindow) -> String {
    let text = String::from_utf8_lossy(bytes)
        .chars()
        .map(|character| {
            if character == char::REPLACEMENT_CHARACTER
                || (character.is_control() && character != '\n' && character != '\t')
            {
                '?'
            } else {
                character
            }
        })
        .collect::<String>();
    let mut filter = NoiseFilter::default();
    let lines = text
        .split_inclusive('\n')
        // A scan that starts mid-file starts mid-line.
        .skip(usize::from(truncated))
        .filter(|line| !(window.noise)(&mut filter, line))
        .collect::<Vec<_>>();
    let mut kept = 0;
    let newest = lines
        .iter()
        .rev()
        .take_while(|line| {
            kept += line.len();
            kept <= window.keep_bytes
        })
        .count();
    lines
        .get(lines.len().saturating_sub(newest)..)
        .unwrap_or_default()
        .concat()
}

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

#[cfg(test)]
mod tests;
