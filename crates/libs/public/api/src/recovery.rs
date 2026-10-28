//! Explicit process recovery over command-session WebSockets.
//! Each connection carries one request. Clients retain handles, cursors and
//! mutation sequences; reconnecting never starts or mutates a process implicitly.

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// The data-plane connection must acknowledge this protocol before a client
/// sends an attach frame or recovery request. Header authentication sends its
/// credential in the upgrade request, before acknowledgement. Discovery covers
/// the engine route; this handshake covers the independently deployed data plane.
pub const RECOVERY_WEBSOCKET_PROTOCOL: &str = "space.process-recovery.v1";

/// Create an immutable recovery command session without starting it.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateRecoveryCommandRequest {
    /// Must be 1. Unknown versions are rejected before creating a session.
    pub contract_version: u32,
    pub expected_instance_id: String,
    pub expected_activation_id: String,
    pub process: super::CreateCommandRequest,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessExecution {
    pub instance_id: String,
    pub activation_id: Option<String>,
    pub capabilities: Vec<String>,
    /// Upper bound for retrying a start key, measured from the first attempt.
    pub idempotency_retention_ms: u64,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryStart {
    /// Must be 1. Unknown versions are rejected before effects.
    pub contract_version: u32,
    pub expected_instance_id: String,
    pub expected_activation_id: String,
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "super::schema::process_command")
    )]
    pub command: Vec<String>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryProcessHandle {
    pub instance_id: String,
    pub activation_id: String,
    pub process_id: String,
    pub process_incarnation: String,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(tag = "type", rename_all = "snake_case"))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecoveryClientFrame {
    /// Explicitly start or replay the session's immutable command.
    RecoveryStart,
    RecoveryConnect {
        handle: RecoveryProcessHandle,
        output_cursor: u64,
        resume_after_output_gap: bool,
    },
    RecoveryInput {
        handle: RecoveryProcessHandle,
        sequence: u64,
        data_base64: String,
    },
    RecoveryCloseStdin {
        handle: RecoveryProcessHandle,
        sequence: u64,
    },
    RecoverySignal {
        handle: RecoveryProcessHandle,
        sequence: u64,
        signal: i32,
    },
    RecoveryResize {
        handle: RecoveryProcessHandle,
        cols: u16,
        rows: u16,
    },
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(tag = "type", rename_all = "snake_case"))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecoveryServerFrame {
    RecoveryStarted {
        handle: RecoveryProcessHandle,
        pid: Option<u32>,
    },
    RecoveryConnected {
        output_cursor: u64,
        resume_after_output_gap: bool,
    },
    /// Cursor identifies the next output event after these bytes.
    RecoveryStdout {
        output_cursor: u64,
        data_base64: String,
    },
    RecoveryStderr {
        output_cursor: u64,
        data_base64: String,
    },
    RecoveryOutputGap {
        skipped_events: u64,
        output_cursor: u64,
        output_bytes_before: u64,
    },
    RecoveryExited {
        exit_code: Option<i32>,
        signal: Option<i32>,
        status: super::DataPlaneProcessExitStatus,
    },
    RecoveryAccepted {
        sequence: Option<u64>,
    },
    /// A failure is never permission to retry through a legacy endpoint.
    RecoveryError {
        code: i32,
        reason: Option<String>,
        message: String,
        input_written: Option<u64>,
    },
    /// Successful end of this request. EOF without this or an error is uncertain.
    RecoveryComplete,
}
