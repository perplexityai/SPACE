use std::fmt;
use std::str::FromStr;

use pplx_space_metrics_emit::{counter, tag};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueGcClass {
    Dead,
    GoneLive,
}

impl QueueGcClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Dead => "dead",
            Self::GoneLive => "gone_live",
        }
    }
}

impl fmt::Display for QueueGcClass {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("queue GC class must be dead or gone_live")]
pub struct QueueGcClassParseError;

impl FromStr for QueueGcClass {
    type Err = QueueGcClassParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "dead" => Ok(Self::Dead),
            "gone_live" => Ok(Self::GoneLive),
            _ => Err(QueueGcClassParseError),
        }
    }
}

/// Which "is this node gone?" detector a queue-GC instance runs. Both share the
/// same store/tick; the tag lets the EC2 (backing-instance) path and the
/// Kubernetes-node-presence path chart and alert separately.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueGcDetector {
    Ec2,
    KubePresence,
}

impl QueueGcDetector {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ec2 => "ec2",
            Self::KubePresence => "kube_presence",
        }
    }
}

impl fmt::Display for QueueGcDetector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Whether a queue-GC tick pruned rows or only observed what it would prune.
/// The observe-first rollout runs `dry_run` before an operator enables writes,
/// so both phases chart separately on the same series.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueGcMode {
    Live,
    DryRun,
}

impl QueueGcMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::DryRun => "dry_run",
        }
    }
}

impl fmt::Display for QueueGcMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueGcResult {
    Success,
    /// The detector could not establish cluster membership this tick (e.g. a
    /// cold or empty node reflector); the tick failed closed and pruned
    /// nothing.
    CacheCold,
    Error,
}

impl QueueGcResult {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::CacheCold => "cache_cold",
            Self::Error => "error",
        }
    }
}

impl fmt::Display for QueueGcResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("queue GC result must be success, cache_cold, or error")]
pub struct QueueGcResultParseError;

impl FromStr for QueueGcResult {
    type Err = QueueGcResultParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "success" => Ok(Self::Success),
            "cache_cold" => Ok(Self::CacheCold),
            "error" => Ok(Self::Error),
            _ => Err(QueueGcResultParseError),
        }
    }
}

/// Queue or trigger records deleted by a queue-GC instance. `class` is the
/// record class (`dead` letters vs `gone_live` refs for a gone node);
/// `detector` identifies the gone-signal (EC2 backing-instance vs k8s node
/// presence); `mode` is `live`/`dry_run`. In dry-run the count is always zero.
pub fn queue_gc_pruned(
    class: QueueGcClass,
    detector: QueueGcDetector,
    mode: QueueGcMode,
    count: u64,
) {
    counter(
        "reconcile.queue.gc.pruned",
        count,
        &[
            tag("class", class.as_str()),
            tag("detector", detector.as_str()),
            tag("mode", mode.as_str()),
        ],
    );
}

/// Queue and trigger records that pass the final gone-node eligibility guards
/// within the per-tick budget. Dry-run and live emit the same unit so operators
/// can compare the preview population with the eventual delete population.
pub fn queue_gc_eligible(detector: QueueGcDetector, mode: QueueGcMode, count: u64) {
    counter(
        "reconcile.queue.gc.eligible",
        count,
        &[
            tag("detector", detector.as_str()),
            tag("mode", mode.as_str()),
        ],
    );
}

/// Nodes classified gone with at least one row passing the final database
/// eligibility guards within the tick budget. Same unit in dry-run and live,
/// so the observe-first series is comparable across the cutover.
pub fn queue_gc_gone_nodes(detector: QueueGcDetector, mode: QueueGcMode, count: u64) {
    counter(
        "reconcile.queue.gc.gone_nodes",
        count,
        &[
            tag("detector", detector.as_str()),
            tag("mode", mode.as_str()),
        ],
    );
}

pub fn queue_gc_tick(result: QueueGcResult, detector: QueueGcDetector) {
    counter(
        "reconcile.queue.gc.tick",
        1,
        &[
            tag("result", result.as_str()),
            tag("detector", detector.as_str()),
        ],
    );
}
