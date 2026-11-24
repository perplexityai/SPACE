//! Distributions of memory charged to resident VM jailer cgroups.

use pplx_space_metrics_emit::{counter, distribution, metric_vocabulary, tag};
use pplx_space_node_runtime_core::cgroup::DEFAULT_SANDBOX_MEMORY_HEADROOM_MIB;
use pplx_space_node_runtime_core::cgroup_memory::CgroupMemoryLimit;

const GIB: u64 = 1024 * 1024 * 1024;
const HEADROOM_BYTES: u64 = DEFAULT_SANDBOX_MEMORY_HEADROOM_MIB * 1024 * 1024;

metric_vocabulary! {
    CgroupLimitBucket {
        UpTo1Gib => "le_1gib_plus_headroom",
        UpTo2Gib => "1_to_2gib_plus_headroom",
        UpTo4Gib => "2_to_4gib_plus_headroom",
        UpTo8Gib => "4_to_8gib_plus_headroom",
        UpTo16Gib => "8_to_16gib_plus_headroom",
        Over16Gib => "gt_16gib_plus_headroom",
        Unlimited => "unlimited",
        Unknown => "unknown",
    }
}

impl CgroupLimitBucket {
    pub(crate) fn from_memory_max(limit: CgroupMemoryLimit) -> Self {
        match limit {
            CgroupMemoryLimit::Unlimited => Self::Unlimited,
            CgroupMemoryLimit::Unknown => Self::Unknown,
            CgroupMemoryLimit::Limited(bytes) => {
                match bytes.saturating_sub(HEADROOM_BYTES).div_ceil(GIB) {
                    0..=1 => Self::UpTo1Gib,
                    2 => Self::UpTo2Gib,
                    3..=4 => Self::UpTo4Gib,
                    5..=8 => Self::UpTo8Gib,
                    9..=16 => Self::UpTo16Gib,
                    _ => Self::Over16Gib,
                }
            }
        }
    }
}

pub(crate) fn sweep_truncated() {
    counter("sandbox.cgroup.memory.sweep_truncated", 1, &[]);
}

metric_vocabulary! {
    AgeBucket {
        Under1m => "lt_1m",
        Under5m => "1_to_5m",
        Under15m => "5_to_15m",
        Under60m => "15_to_60m",
        AtLeast60m => "ge_60m",
        Unknown => "unknown",
    }
}

impl AgeBucket {
    pub(crate) fn from_age_secs(age_secs: Option<i64>) -> Self {
        match age_secs {
            Some(0..=59) => Self::Under1m,
            Some(60..=299) => Self::Under5m,
            Some(300..=899) => Self::Under15m,
            Some(900..=3599) => Self::Under60m,
            Some(3600..) => Self::AtLeast60m,
            _ => Self::Unknown,
        }
    }
}

pub(crate) fn emit(
    current_bytes: u64,
    anon_bytes: Option<u64>,
    cgroup_limit: CgroupLimitBucket,
    age: AgeBucket,
) {
    let tags = &[
        tag("cgroup_limit_bucket", cgroup_limit),
        tag("age_bucket", age),
    ];
    distribution(
        "sandbox.cgroup.memory.current_bytes",
        current_bytes as f64,
        tags,
    );
    if let Some(bytes) = anon_bytes {
        distribution("sandbox.cgroup.memory.anon_bytes", bytes as f64, tags);
    }
}
