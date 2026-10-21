//! Scale-in ordering for the data plane: the cluster-singleton writer of
//! `controller.kubernetes.io/pod-deletion-cost` on data-plane pods.
//!
//! The writer probes each Ready data-plane pod's internal stream-load report,
//! buckets its connections (attached streams plus in-flight plain requests),
//! and patches the annotation
//! when the bucket changes. These series say whether the ordering signal is
//! current; they do not say whether a scale-in actually honored it.

use crate::emit::{counter, gauge, tag};

metric_vocabulary!(
    /// Outcome of reading one pod's stream-load report.
    ProbeOutcome {
        /// The report was read and parsed.
        Ok => "ok",
        /// The pod IP did not parse, or the request failed, returned a
        /// non-success status, or carried an unparseable body. The pod counts
        /// as `unknown`; a run of these ranks it as busy.
        Error => "error",
        /// The request did not finish within the probe timeout. The pod counts
        /// as `unknown`; a run of these ranks it as busy.
        Timeout => "timeout",
    }
);

metric_vocabulary!(
    /// What the writer did with one probed pod's annotation.
    WriteOutcome {
        /// The patch landed with a new bucket.
        Applied => "applied",
        /// The annotation already carried the desired bucket, or carried none
        /// while the desired bucket is `0` (an absent annotation already ranks
        /// as cost 0).
        Unchanged => "unchanged",
        /// The bucket dropped, but this pod's last write attempt (failed ones
        /// included) started less than the minimum write interval ago; the
        /// lowering waits for a later tick. Raises are never rate-limited.
        RateLimited => "rate_limited",
        /// The apiserver rejected the patch, or it timed out.
        Error => "error",
    }
);

metric_vocabulary!(
    /// The deletion-cost bucket a Ready data-plane pod falls in this tick.
    CostBucket {
        /// No attached streams or in-flight plain requests: first to go on
        /// scale-in.
        Idle => "0",
        /// 1–9 connections (attached streams plus in-flight plain requests).
        Low => "10",
        /// 10–99 connections.
        Medium => "100",
        /// 100 or more connections.
        High => "1000",
        /// The probe failed this tick, so the load is unknown. The pod keeps
        /// its annotation for a short run of failures and is then written the
        /// top bucket (unknown ranks as busy). Alert on this staying non-zero:
        /// the ordering signal for those pods is not being measured.
        Unknown => "unknown",
        /// The pod declares no stream-load listener (no container port named
        /// `internal`), so it isn't probed or ranked. Alert on this being
        /// non-zero too: the writer is on but the data plane's listener isn't.
        NoListener => "no_listener",
    }
);

pub fn probe(outcome: ProbeOutcome) {
    counter(
        "data_plane_deletion_cost.probe",
        1,
        &[tag("outcome", outcome)],
    );
}

/// `count` pods that shared a write outcome this tick, emitted as one packet.
pub fn writes(outcome: WriteOutcome, count: u64) {
    if count == 0 {
        return;
    }
    counter(
        "data_plane_deletion_cost.write",
        count,
        &[tag("outcome", outcome)],
    );
}

/// Ready data-plane pods in `bucket` this tick. Every bucket is emitted on each
/// tick that probes, zeros included, so a bucket that empties reads as zero;
/// the buckets sum to the Ready pods (`no_listener` ones are counted but not
/// probed). Nothing is emitted while the pod
/// reflector is cold or on a replica that isn't the leader, so a silent gauge
/// means the writer isn't running, not that the fleet is empty.
pub fn pods(bucket: CostBucket, count: usize) {
    gauge(
        "data_plane_deletion_cost.pods",
        count as f64,
        &[tag("cost", bucket)],
    );
}
