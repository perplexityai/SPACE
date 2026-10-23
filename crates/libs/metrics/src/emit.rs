//! The emit core lives in `pplx-space-metrics-emit`; this module keeps the
//! `crate::emit::…` paths the metric modules import from.

pub(crate) use pplx_space_metrics_emit::{
    counter, distribution, elapsed_ms, gauge, tag, timing_ms,
};

#[cfg(any(test, feature = "test-capture"))]
pub(crate) use pplx_space_metrics_emit::test_capture;
