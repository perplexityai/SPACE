//! Read-only node-local disk metrics emitted by node-manager.

mod rootfs_backing;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use futures_util::{StreamExt, stream};
use pplx_space_common_core::types::SandboxId;
use serde::Serialize;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{info, warn};

use super::disk::{DirectoryBudget, btrfs_du_exclusive};
use super::{CACHE_DIR, TEMPLATE_DIR};

const DISK_USAGE_CONCURRENCY: usize = 8;
const DISK_EVIDENCE_MAX_AGE: Duration = Duration::from_secs(600);
const DEFAULT_TOP_CONSUMER_COUNT: usize = 20;
const DEFAULT_TOP_CONSUMER_COUNT_PER_CATEGORY: usize = 10;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SandboxDiskUsageLookup {
    Usage(u64),
    Missing,
    Unknown,
}

#[derive(Debug, Default)]
struct SandboxDiskUsageSnapshot {
    outcomes: BTreeMap<SandboxId, (tokio::time::Instant, DiskMeasurementOutcome)>,
}

#[derive(Clone)]
pub struct SandboxDiskUsageCache {
    snapshot: Arc<ArcSwap<SandboxDiskUsageSnapshot>>,
}

impl Default for SandboxDiskUsageCache {
    fn default() -> Self {
        Self {
            snapshot: Arc::new(ArcSwap::from_pointee(SandboxDiskUsageSnapshot::default())),
        }
    }
}

impl SandboxDiskUsageCache {
    pub fn lookup(&self, sandbox_id: SandboxId) -> SandboxDiskUsageLookup {
        let snapshot = self.snapshot.load();
        let Some((observed_at, outcome)) = snapshot.outcomes.get(&sandbox_id) else {
            return SandboxDiskUsageLookup::Unknown;
        };
        if observed_at.elapsed() > DISK_EVIDENCE_MAX_AGE {
            return SandboxDiskUsageLookup::Unknown;
        }
        match outcome {
            DiskMeasurementOutcome::Usage(bytes) => SandboxDiskUsageLookup::Usage(*bytes),
            DiskMeasurementOutcome::Missing => SandboxDiskUsageLookup::Missing,
            DiskMeasurementOutcome::Error | DiskMeasurementOutcome::Unmeasured => {
                SandboxDiskUsageLookup::Unknown
            }
        }
    }

    fn replace(
        &self,
        targets: &DiskMetricTargets,
        measurement: &DiskMeasurementReport,
        observed_at: tokio::time::Instant,
    ) {
        let previous = self.snapshot.load();
        let outcomes = targets
            .active_sandboxes
            .iter()
            .filter_map(|path| {
                let id = active_sandbox_identifier(path)?;
                let observation = match measurement.outcome(path)? {
                    // A scheduling budget is not a failed probe. Keep recent
                    // evidence without advancing its age, bounded by this census.
                    DiskMeasurementOutcome::Unmeasured => previous.outcomes.get(&id).cloned()?,
                    outcome => (observed_at, outcome.clone()),
                };
                (observation.0.elapsed() <= DISK_EVIDENCE_MAX_AGE).then_some((id, observation))
            })
            .collect();
        self.snapshot
            .store(Arc::new(SandboxDiskUsageSnapshot { outcomes }));
    }

    #[cfg(test)]
    pub(super) fn replace_for_test(
        &self,
        outcomes: impl IntoIterator<Item = (SandboxId, SandboxDiskUsageLookup)>,
    ) {
        let outcomes = outcomes
            .into_iter()
            .filter_map(|(sandbox_id, outcome)| {
                let outcome = match outcome {
                    SandboxDiskUsageLookup::Usage(bytes) => DiskMeasurementOutcome::Usage(bytes),
                    SandboxDiskUsageLookup::Missing => DiskMeasurementOutcome::Missing,
                    SandboxDiskUsageLookup::Unknown => return None,
                };
                Some((sandbox_id, (tokio::time::Instant::now(), outcome)))
            })
            .collect();
        self.snapshot
            .store(Arc::new(SandboxDiskUsageSnapshot { outcomes }));
    }
}

#[derive(Debug, Clone, Copy)]
pub struct NodeDiskMetricsConfig {
    interval: Duration,
    top_consumer_count: usize,
    top_consumer_count_per_category: usize,
}

impl Default for NodeDiskMetricsConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(300),
            top_consumer_count: DEFAULT_TOP_CONSUMER_COUNT,
            top_consumer_count_per_category: DEFAULT_TOP_CONSUMER_COUNT_PER_CATEGORY,
        }
    }
}

#[derive(Debug, Default, Eq, PartialEq)]
struct DiskMetricTargets {
    templates: Vec<String>,
    snapshotted_sandboxes: Vec<String>,
    active_sandboxes: Vec<String>,
    btrfs_snapshots: Vec<String>,
}

impl DiskMetricTargets {
    fn all_paths(&self) -> Vec<String> {
        self.templates
            .iter()
            .chain(self.snapshotted_sandboxes.iter())
            .chain(self.active_sandboxes.iter())
            .chain(self.btrfs_snapshots.iter())
            .cloned()
            .collect()
    }
}

#[derive(Debug, Default)]
struct DiskMetricSnapshot {
    templates_bytes: u64,
    snapshotted_sandbox_bytes: u64,
    active_sandbox_bytes: u64,
    btrfs_snapshot_bytes: u64,
    template_count: u64,
    snapshotted_sandbox_count: u64,
    active_sandbox_count: u64,
    btrfs_snapshot_count: u64,
    measurement_counts: MeasurementCounts,
    measurement_counts_by_category: BTreeMap<DiskUsageCategory, MeasurementCounts>,
}

impl DiskMetricSnapshot {
    fn emit(self) {
        // If btrfs measurement errors or the measurement deadline hit a
        // category, that category's sum is partial. Leave its byte gauge stale
        // rather than publishing a misleading drop toward zero that looks like
        // reclaimed disk.
        if self.should_emit_byte_gauge(DiskUsageCategory::Template) {
            pplx_space_metrics::node_disk::templates_bytes(self.templates_bytes);
        }
        if self.should_emit_byte_gauge(DiskUsageCategory::SuspendCheckpoint) {
            pplx_space_metrics::node_disk::snapshotted_sandbox_bytes(
                self.snapshotted_sandbox_bytes,
            );
        }
        if self.should_emit_byte_gauge(DiskUsageCategory::ActiveInstance) {
            pplx_space_metrics::node_disk::active_sandbox_bytes(self.active_sandbox_bytes);
        }
        if self.should_emit_byte_gauge(DiskUsageCategory::BtrfsSnapshot) {
            pplx_space_metrics::node_disk::btrfs_snapshot_bytes(self.btrfs_snapshot_bytes);
        }
        pplx_space_metrics::node_disk::template_count(self.template_count);
        pplx_space_metrics::node_disk::snapshotted_sandbox_count(self.snapshotted_sandbox_count);
        pplx_space_metrics::node_disk::btrfs_snapshot_count(self.btrfs_snapshot_count);
        pplx_space_metrics::node_disk::measurement_success_count(
            self.measurement_counts.success_count,
        );
        pplx_space_metrics::node_disk::measurement_missing_count(
            self.measurement_counts.missing_count,
        );
        pplx_space_metrics::node_disk::measurement_error_count(self.measurement_counts.error_count);
        pplx_space_metrics::node_disk::measurement_unmeasured_count(
            self.measurement_counts.unmeasured_count,
        );
    }

    fn should_emit_byte_gauge(&self, category: DiskUsageCategory) -> bool {
        self.measurement_counts_by_category
            .get(&category)
            .map(MeasurementCounts::is_complete)
            .unwrap_or(true)
    }
}

enum DiskUsageProbe {
    Usage { path: String, exclusive_bytes: u64 },
    Missing { path: String },
    Error { path: String, error: String },
    Unmeasured { path: String },
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum DiskMeasurementOutcome {
    Usage(u64),
    Missing,
    Error,
    Unmeasured,
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Serialize)]
struct MeasurementCounts {
    success_count: u64,
    missing_count: u64,
    error_count: u64,
    unmeasured_count: u64,
}

impl MeasurementCounts {
    fn is_complete(&self) -> bool {
        self.error_count == 0 && self.unmeasured_count == 0
    }

    fn record(&mut self, outcome: &DiskMeasurementOutcome) {
        match outcome {
            DiskMeasurementOutcome::Usage(_) => self.success_count += 1,
            DiskMeasurementOutcome::Missing => self.missing_count += 1,
            DiskMeasurementOutcome::Error => self.error_count += 1,
            DiskMeasurementOutcome::Unmeasured => self.unmeasured_count += 1,
        }
    }
}

#[derive(Debug, Default)]
struct DiskMeasurementReport {
    outcomes: BTreeMap<String, DiskMeasurementOutcome>,
    counts: MeasurementCounts,
}

impl DiskMeasurementReport {
    fn record_usage(&mut self, path: String, exclusive_bytes: u64) {
        self.record(path, DiskMeasurementOutcome::Usage(exclusive_bytes));
    }

    fn record_missing(&mut self, path: String) {
        self.record(path, DiskMeasurementOutcome::Missing);
    }

    fn record_error(&mut self, path: String) {
        self.record(path, DiskMeasurementOutcome::Error);
    }

    fn record(&mut self, path: String, outcome: DiskMeasurementOutcome) {
        self.counts.record(&outcome);
        self.outcomes.insert(path, outcome);
    }

    fn usage_bytes(&self, path: &str) -> Option<u64> {
        match self.outcomes.get(path)? {
            DiskMeasurementOutcome::Usage(exclusive_bytes) => Some(*exclusive_bytes),
            DiskMeasurementOutcome::Missing
            | DiskMeasurementOutcome::Error
            | DiskMeasurementOutcome::Unmeasured => None,
        }
    }

    fn outcome(&self, path: &str) -> Option<&DiskMeasurementOutcome> {
        self.outcomes.get(path)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
enum DiskUsageCategory {
    Template,
    SuspendCheckpoint,
    BtrfsSnapshot,
    ActiveInstance,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
struct DiskUsageDiagnosticEntry {
    category: DiskUsageCategory,
    identifier: String,
    exclusive_bytes: u64,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
struct DiskUsageDiagnostic {
    top_consumers: Vec<DiskUsageDiagnosticEntry>,
    top_consumers_by_category: BTreeMap<DiskUsageCategory, Vec<DiskUsageDiagnosticEntry>>,
}

/// Spawn the read-only node disk metrics loop.
pub fn start_node_disk_metrics(
    disk_usage_cache: SandboxDiskUsageCache,
    cfg: NodeDiskMetricsConfig,
    shutdown_token: CancellationToken,
    task_tracker: &TaskTracker,
) {
    task_tracker.spawn(async move {
        run_loop(cfg, disk_usage_cache, shutdown_token).await;
    });
}

async fn run_loop(
    cfg: NodeDiskMetricsConfig,
    disk_usage_cache: SandboxDiskUsageCache,
    shutdown: CancellationToken,
) {
    info!(
        interval_secs = cfg.interval.as_secs(),
        "node disk metrics loop starting",
    );

    let mut interval = tokio::time::interval(cfg.interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // One scheduler owns continuation; no durable state is needed. A pathname
    // cursor remains useful when entries disappear or new ones sort before it.
    let mut cursor = None;

    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            _ = interval.tick() => {
                let result = emit_tick(&cfg, &disk_usage_cache, &mut cursor).await;
                // A failed tick publishes none of the byte, target-count or
                // measurement gauges, so without this a stale byte gauge looks
                // like a node that stopped reporting.
                pplx_space_metrics::node_disk::metrics_tick_success(result.is_ok());
                if let Err(error) = result {
                    warn!(%error, "node disk metrics tick failed");
                }
            }
        }
    }
}

async fn emit_tick(
    cfg: &NodeDiskMetricsConfig,
    disk_usage_cache: &SandboxDiskUsageCache,
    cursor: &mut Option<String>,
) -> Result<(), String> {
    // Age from the beginning: a long census must not publish old measurements
    // as freshly observed just because its final command finished now.
    let observed_at = tokio::time::Instant::now();
    let targets = tokio::task::spawn_blocking(|| {
        // Metadata-only adoption inventory is independent of the much slower
        // extent accounting below and never runs on a lifecycle request.
        rootfs_backing::emit_inventory(Path::new(CACHE_DIR), Path::new(TEMPLATE_DIR));
        collect_targets()
    })
    .await
    .map_err(|error| format!("collect node disk metric targets task failed: {error}"))?
    .map_err(|error| {
        format!("node disk inventory incomplete; retaining previous evidence: {error}")
    })?;
    let measurement = measure_paths(targets.all_paths(), cursor).await;
    disk_usage_cache.replace(&targets, &measurement, observed_at);
    let snapshot = build_snapshot(&targets, &measurement);
    if !snapshot.measurement_counts.is_complete() {
        warn!(
            measurement_success_count = snapshot.measurement_counts.success_count,
            measurement_missing_count = snapshot.measurement_counts.missing_count,
            measurement_error_count = snapshot.measurement_counts.error_count,
            measurement_unmeasured_count = snapshot.measurement_counts.unmeasured_count,
            measurement_counts_by_category =
                %json_string(&snapshot.measurement_counts_by_category),
            "node disk metrics measurement incomplete; suppressing byte gauges for affected categories",
        );
    }
    emit_disk_usage_diagnostic(&snapshot, &targets, &measurement, cfg);
    snapshot.emit();
    info!(
        templates = targets.templates.len(),
        snapshotted_sandboxes = targets.snapshotted_sandboxes.len(),
        active_sandboxes = targets.active_sandboxes.len(),
        btrfs_snapshots = targets.btrfs_snapshots.len(),
        "node disk metrics tick complete",
    );
    Ok(())
}

fn collect_targets() -> std::io::Result<DiskMetricTargets> {
    // An absent cache mount cannot produce authoritative zero-byte totals.
    std::fs::metadata(CACHE_DIR)?;
    std::fs::metadata(TEMPLATE_DIR)?;
    classify_disk_metric_targets(Path::new(CACHE_DIR), Path::new(TEMPLATE_DIR))
}

async fn measure_paths(paths: Vec<String>, cursor: &mut Option<String>) -> DiskMeasurementReport {
    measure_paths_with(paths, cursor, |path, budget| async move {
        tokio::task::spawn_blocking(move || btrfs_du_exclusive(&path, budget))
            .await
            .map_err(|error| format!("join btrfs disk usage task: {error}"))?
    })
    .await
}

async fn measure_paths_with<F, Fut>(
    mut paths: Vec<String>,
    cursor: &mut Option<String>,
    mut probe: F,
) -> DiskMeasurementReport
where
    F: FnMut(String, Duration) -> Fut,
    Fut: std::future::Future<Output = Result<Option<super::disk::BtrfsDiskUsage>, String>>,
{
    if paths.is_empty() {
        return DiskMeasurementReport::default();
    }

    paths.sort();
    if let Some(previous) = cursor.as_ref() {
        let start = paths.partition_point(|path| path <= previous);
        let len = paths.len();
        paths.rotate_left(start % len);
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let probes = stream::iter(paths)
        .map(|path| {
            // Advance at admission, not completion (which is unordered), and
            // never advance over skipped entries. Failures get a turn too.
            let work = if tokio::time::Instant::now() < deadline {
                *cursor = Some(path.clone());
                Some(probe(
                    path.clone(),
                    deadline.saturating_duration_since(tokio::time::Instant::now()),
                ))
            } else {
                None
            };
            async move {
                let Some(work) = work else {
                    return DiskUsageProbe::Unmeasured { path };
                };
                match work.await {
                    Ok(Some(usage)) => DiskUsageProbe::Usage {
                        path,
                        exclusive_bytes: usage.exclusive_bytes,
                    },
                    Ok(None) => DiskUsageProbe::Missing { path },
                    Err(error) => DiskUsageProbe::Error { path, error },
                }
            }
        })
        .buffer_unordered(DISK_USAGE_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;

    let mut measurement = DiskMeasurementReport::default();
    for probe in probes {
        match probe {
            DiskUsageProbe::Unmeasured { path } => {
                measurement.record(path, DiskMeasurementOutcome::Unmeasured)
            }
            DiskUsageProbe::Usage {
                path,
                exclusive_bytes,
            } => {
                measurement.record_usage(path, exclusive_bytes);
            }
            DiskUsageProbe::Missing { path } => {
                warn!(path, "node disk metric path missing during measurement");
                measurement.record_missing(path);
            }
            DiskUsageProbe::Error { path, error } => {
                if let Some(sandbox_id) = active_sandbox_identifier(&path) {
                    warn!(
                        sandbox_id = %sandbox_id,
                        instance_path = path,
                        error,
                        "sandbox disk usage measurement failed; byte gauge for this category will be suppressed",
                    );
                } else {
                    warn!(
                        path,
                        error,
                        "node disk metric path measurement failed; byte gauge for this category will be suppressed",
                    );
                }
                measurement.record_error(path);
            }
        }
    }
    measurement
}

fn build_snapshot(
    targets: &DiskMetricTargets,
    measurement: &DiskMeasurementReport,
) -> DiskMetricSnapshot {
    let measurement_counts_by_category = measurement_counts_by_category(targets, measurement);
    DiskMetricSnapshot {
        templates_bytes: sum_usage(&targets.templates, measurement),
        snapshotted_sandbox_bytes: sum_usage(&targets.snapshotted_sandboxes, measurement),
        active_sandbox_bytes: sum_usage(&targets.active_sandboxes, measurement),
        btrfs_snapshot_bytes: sum_usage(&targets.btrfs_snapshots, measurement),
        template_count: targets.templates.len() as u64,
        snapshotted_sandbox_count: targets.snapshotted_sandboxes.len() as u64,
        active_sandbox_count: targets.active_sandboxes.len() as u64,
        btrfs_snapshot_count: targets.btrfs_snapshots.len() as u64,
        measurement_counts: measurement.counts,
        measurement_counts_by_category,
    }
}

fn sum_usage(paths: &[String], measurement: &DiskMeasurementReport) -> u64 {
    paths
        .iter()
        .filter_map(|path| measurement.usage_bytes(path))
        .sum()
}

fn measurement_counts_by_category(
    targets: &DiskMetricTargets,
    measurement: &DiskMeasurementReport,
) -> BTreeMap<DiskUsageCategory, MeasurementCounts> {
    [
        (DiskUsageCategory::Template, &targets.templates),
        (
            DiskUsageCategory::SuspendCheckpoint,
            &targets.snapshotted_sandboxes,
        ),
        (DiskUsageCategory::BtrfsSnapshot, &targets.btrfs_snapshots),
        (DiskUsageCategory::ActiveInstance, &targets.active_sandboxes),
    ]
    .into_iter()
    .map(|(category, paths)| {
        let mut counts = MeasurementCounts::default();
        for path in paths {
            if let Some(outcome) = measurement.outcome(path) {
                counts.record(outcome);
            }
        }
        (category, counts)
    })
    .collect()
}

fn emit_disk_usage_diagnostic(
    snapshot: &DiskMetricSnapshot,
    targets: &DiskMetricTargets,
    measurement: &DiskMeasurementReport,
    cfg: &NodeDiskMetricsConfig,
) {
    let diagnostic = build_disk_usage_diagnostic(
        targets,
        measurement,
        cfg.top_consumer_count,
        cfg.top_consumer_count_per_category,
    );

    info!(
        top_n = cfg.top_consumer_count,
        top_per_category_n = cfg.top_consumer_count_per_category,
        templates_bytes = snapshot.templates_bytes,
        suspend_checkpoint_bytes = snapshot.snapshotted_sandbox_bytes,
        btrfs_snapshot_bytes = snapshot.btrfs_snapshot_bytes,
        active_instance_bytes = snapshot.active_sandbox_bytes,
        template_count = snapshot.template_count,
        suspend_checkpoint_count = snapshot.snapshotted_sandbox_count,
        btrfs_snapshot_count = snapshot.btrfs_snapshot_count,
        active_instance_count = snapshot.active_sandbox_count,
        measurement_success_count = snapshot.measurement_counts.success_count,
        measurement_missing_count = snapshot.measurement_counts.missing_count,
        measurement_error_count = snapshot.measurement_counts.error_count,
        measurement_unmeasured_count = snapshot.measurement_counts.unmeasured_count,
        measurement_counts_by_category = %json_string(&snapshot.measurement_counts_by_category),
        top_consumers = %json_string(&diagnostic.top_consumers),
        top_consumers_by_category = %json_string(&diagnostic.top_consumers_by_category),
        "node disk metrics largest local disk consumers",
    );
}

fn build_disk_usage_diagnostic(
    targets: &DiskMetricTargets,
    measurement: &DiskMeasurementReport,
    top_consumer_count: usize,
    top_consumer_count_per_category: usize,
) -> DiskUsageDiagnostic {
    let entries = measured_diagnostic_entries(targets, measurement);
    let top_consumers = select_top_consumers(entries.clone(), top_consumer_count);
    let mut top_consumers_by_category = BTreeMap::new();

    for category in [
        DiskUsageCategory::Template,
        DiskUsageCategory::SuspendCheckpoint,
        DiskUsageCategory::BtrfsSnapshot,
        DiskUsageCategory::ActiveInstance,
    ] {
        let category_entries = entries
            .iter()
            .filter(|entry| entry.category == category)
            .cloned()
            .collect::<Vec<_>>();
        top_consumers_by_category.insert(
            category,
            select_top_consumers(category_entries, top_consumer_count_per_category),
        );
    }

    DiskUsageDiagnostic {
        top_consumers,
        top_consumers_by_category,
    }
}

fn measured_diagnostic_entries(
    targets: &DiskMetricTargets,
    measurement: &DiskMeasurementReport,
) -> Vec<DiskUsageDiagnosticEntry> {
    targets
        .templates
        .iter()
        .filter_map(|path| {
            measured_diagnostic_entry(
                DiskUsageCategory::Template,
                template_identifier(path),
                path,
                measurement,
            )
        })
        .chain(targets.snapshotted_sandboxes.iter().filter_map(|path| {
            measured_diagnostic_entry(
                DiskUsageCategory::SuspendCheckpoint,
                sandbox_artifact_identifier(path),
                path,
                measurement,
            )
        }))
        .chain(targets.btrfs_snapshots.iter().filter_map(|path| {
            measured_diagnostic_entry(
                DiskUsageCategory::BtrfsSnapshot,
                btrfs_snapshot_identifier(path),
                path,
                measurement,
            )
        }))
        .chain(targets.active_sandboxes.iter().filter_map(|path| {
            measured_diagnostic_entry(
                DiskUsageCategory::ActiveInstance,
                sandbox_artifact_identifier(path),
                path,
                measurement,
            )
        }))
        .collect()
}

fn measured_diagnostic_entry(
    category: DiskUsageCategory,
    identifier: Option<String>,
    path: &str,
    measurement: &DiskMeasurementReport,
) -> Option<DiskUsageDiagnosticEntry> {
    Some(DiskUsageDiagnosticEntry {
        category,
        identifier: identifier.unwrap_or_else(|| path.to_string()),
        exclusive_bytes: measurement.usage_bytes(path)?,
    })
}

fn select_top_consumers(
    mut entries: Vec<DiskUsageDiagnosticEntry>,
    limit: usize,
) -> Vec<DiskUsageDiagnosticEntry> {
    entries.sort_by(|left, right| {
        right
            .exclusive_bytes
            .cmp(&left.exclusive_bytes)
            .then_with(|| left.category.cmp(&right.category))
            .then_with(|| left.identifier.cmp(&right.identifier))
    });
    entries.truncate(limit);
    entries
}

fn template_identifier(path: &str) -> Option<String> {
    path_component(Path::new(path))
}

fn sandbox_artifact_identifier(path: &str) -> Option<String> {
    Path::new(path).parent().and_then(path_component)
}

fn active_sandbox_identifier(path: &str) -> Option<SandboxId> {
    let path = Path::new(path);
    if path.file_name()?.to_str()? != "instance" {
        return None;
    }
    SandboxId::from_str(path.parent()?.file_name()?.to_str()?).ok()
}

fn btrfs_snapshot_identifier(path: &str) -> Option<String> {
    let path = Path::new(path);
    let snapshot_name = path_component(path)?;
    let sandbox_id = path.parent()?.parent().and_then(path_component)?;
    Some(format!("{sandbox_id}/{snapshot_name}"))
}

fn path_component(path: &Path) -> Option<String> {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
}

fn json_string<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value).unwrap_or_else(|error| {
        serde_json::json!({ "serialization_error": error.to_string() }).to_string()
    })
}

fn classify_disk_metric_targets(
    sandbox_cache_dir: &Path,
    template_dir: &Path,
) -> std::io::Result<DiskMetricTargets> {
    let mut budget = DirectoryBudget::default();
    let mut targets = DiskMetricTargets::default();
    for path in budget.directories(template_dir)? {
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if name.starts_with('.') || name.ends_with(".old") || name.ends_with(".importing") {
            continue;
        }
        for digest in budget.directories(&path)? {
            if digest
                .file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with("sha256:"))
                && regular_file_exists(&digest.join("rootfs.ext4"))?
            {
                targets.templates.push(path_string(path));
                break;
            }
        }
    }
    // Count both resident instances and suspend artifacts; neither proves the
    // absence of the other. Nested enumeration shares the same hard budget,
    // which bounds the walk. Targets aren't capped separately: a suspended
    // sandbox yields two of them, and the measurement deadline bounds probe
    // work, reporting what it skips as unmeasured.
    for sandbox_root in budget.directories(sandbox_cache_dir)? {
        if sandbox_root
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| SandboxId::from_str(n).ok())
            .is_none()
        {
            continue;
        }
        targets.btrfs_snapshots.extend(
            super::snapshots::list_btrfs_snapshot_dirs_in(&sandbox_root, &mut budget)?
                .into_iter()
                .map(path_string),
        );
        let snapshot = sandbox_root.join("snapshot");
        if regular_file_exists(&snapshot.join("vmstate"))?
            && regular_file_exists(&snapshot.join("memory"))?
        {
            targets.snapshotted_sandboxes.push(path_string(snapshot));
        }
        let instance = sandbox_root.join("instance");
        match std::fs::symlink_metadata(&instance) {
            Ok(metadata) if metadata.is_dir() => {
                targets.active_sandboxes.push(path_string(instance))
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    targets.templates.sort();
    targets.snapshotted_sandboxes.sort();
    targets.active_sandboxes.sort();
    targets.btrfs_snapshots.sort();
    Ok(targets)
}

fn regular_file_exists(path: &Path) -> std::io::Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => Ok(metadata.is_file()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

fn path_string(path: PathBuf) -> String {
    path.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests;
