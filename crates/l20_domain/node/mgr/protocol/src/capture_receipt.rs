//! Disk contract shared by the capture producer and publication recovery.
//! Intent is fsynced before submission. Only the producer may commit it, after
//! capture and confirmed runtime stop. A committed receipt is not a sealed or
//! remotely durable artifact.
pub const INTENT: &str = ".encrypted-capture.json.intent";
pub const COMMITTED: &str = "encrypted-capture.json";
pub const STAGING_PREFIX: &str = ".suspend-publish-";

/// Exclusive producer admission persists even when capture fails mid-write.
pub const CLAIMED: &str = ".encrypted-capture.json.claimed";
/// The admitted producer exited before invoking the runtime capture operation.
pub const REFUSED: &str = ".encrypted-capture.json.refused";
/// The producer finished with an error after capture started. This does not
/// prove the runtime stopped or its Diff base is usable; retain both for recovery.
pub const FAILED: &str = ".encrypted-capture.json.failed";
