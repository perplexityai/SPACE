//! Per-assembly experiment knobs. They do not change authentication or admission.

use super::RangeConcurrency;

#[derive(Clone, Copy, Debug)]
pub(crate) struct RequestTarget(u64);

impl Default for RequestTarget {
    fn default() -> Self {
        Self(8 << 20)
    }
}

impl std::str::FromStr for RequestTarget {
    type Err = TuningError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let bytes = raw.parse().map_err(|_| TuningError::RequestTarget)?;
        if !((1 << 20)..=(32 << 20)).contains(&bytes) {
            return Err(TuningError::RequestTarget);
        }
        Ok(Self(bytes))
    }
}

impl RequestTarget {
    pub(super) fn bytes(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum Scheduling {
    #[default]
    Coupled,
    Pipeline,
    ParallelFrames,
    OverlapReuse,
    Combined,
}

impl Scheduling {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Coupled => "coupled",
            Self::Pipeline => "pipeline",
            Self::ParallelFrames => "parallel-frames",
            Self::OverlapReuse => "overlap-reuse",
            Self::Combined => "combined",
        }
    }

    pub(super) const fn pipeline(self) -> bool {
        matches!(self, Self::Pipeline | Self::Combined)
    }

    pub(super) const fn parallel_frames(self) -> bool {
        matches!(self, Self::ParallelFrames | Self::Combined)
    }

    pub(super) const fn overlap_reuse(self) -> bool {
        matches!(self, Self::OverlapReuse | Self::Combined)
    }
}

impl std::fmt::Display for Scheduling {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.write_str(self.as_str())
    }
}

impl std::str::FromStr for Scheduling {
    type Err = TuningError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "coupled" => Ok(Self::Coupled),
            "pipeline" => Ok(Self::Pipeline),
            "parallel-frames" => Ok(Self::ParallelFrames),
            "overlap-reuse" => Ok(Self::OverlapReuse),
            "combined" => Ok(Self::Combined),
            _ => Err(TuningError::Scheduling),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TuningError {
    #[error("range concurrency: {0}")]
    Concurrency(#[from] super::RangeConcurrencyError),
    #[error("request target must be an integer byte count from 1 MiB through 32 MiB")]
    RequestTarget,
    #[error("scheduling must be coupled, pipeline, parallel-frames, overlap-reuse, or combined")]
    Scheduling,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Tuning {
    pub(super) concurrency: RangeConcurrency,
    pub(super) request_target: RequestTarget,
    pub(super) scheduling: Scheduling,
}

impl Tuning {
    pub(crate) fn from_values(
        concurrency: Option<&str>,
        request_target: Option<&str>,
        scheduling: Option<&str>,
    ) -> Result<Self, TuningError> {
        Ok(Self {
            concurrency: concurrency.map(str::parse).transpose()?.unwrap_or_default(),
            request_target: request_target
                .map(str::parse)
                .transpose()?
                .unwrap_or_default(),
            scheduling: scheduling.map(str::parse).transpose()?.unwrap_or_default(),
        })
    }
}
