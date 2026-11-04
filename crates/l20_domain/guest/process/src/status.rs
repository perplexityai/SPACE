//! Stable guest error metadata shared by native guests and API relays.
/// gRPC metadata key naming why a call failed where its code alone is
/// ambiguous. Hops relay a status unchanged, so the key reaches the API.
pub const ERROR_REASON_METADATA_KEY: &str = "space-error-reason";

/// [`ERROR_REASON_METADATA_KEY`] value of a RESOURCE_EXHAUSTED that is the
/// sandbox's disk or quota being full (ENOSPC, EDQUOT), as opposed to the
/// admission limits that share the code.
pub const GUEST_STORAGE_FULL: &str = "guest-storage-full";

/// [`ERROR_REASON_METADATA_KEY`] value of a NOT_FOUND that is the guest
/// filesystem reporting the requested path (or a component of it) missing,
/// as opposed to the sandbox or its runtime being unknown to a hop.
pub const GUEST_PATH_NOT_FOUND: &str = "guest-path-not-found";

/// [`ERROR_REASON_METADATA_KEY`] value of an OUT_OF_RANGE that ends a
/// process output stream whose cursor is past the output the process has
/// written, as opposed to a message too large for a hop to decode.
pub const PROCESS_OUTPUT_CURSOR_AHEAD: &str = "process-output-cursor-ahead";

/// [`ERROR_REASON_METADATA_KEY`] value of a RESOURCE_EXHAUSTED that ends a
/// process output stream whose cursor is before the oldest output the
/// guest still retains, as opposed to a hop's refusal or a transport reset
/// with the same code. [`PROCESS_OUTPUT_FIRST_SEQUENCE_METADATA_KEY`] says
/// where the retained output starts.
pub const PROCESS_OUTPUT_LAGGED: &str = "process-output-lagged";

/// Metadata key, on a [`PROCESS_OUTPUT_LAGGED`] status, of the cursor of
/// the oldest output the guest still retains, in decimal. Absent when the
/// guest no longer holds the process's output at all.
pub const PROCESS_OUTPUT_FIRST_SEQUENCE_METADATA_KEY: &str = "space-process-output-first-sequence";

/// Message of the guest's [`PROCESS_OUTPUT_LAGGED`] status. Guests older
/// than that reason send it untagged, so it identifies their lag status.
pub const PROCESS_OUTPUT_LAGGED_MESSAGE: &str =
    "process output stream fell behind retained output buffer";

/// [`ERROR_REASON_METADATA_KEY`] value of a request refused before any of
/// its work started: an admission limit, or a guest or registry shutting
/// down. This request did nothing, so the caller may retry it, after the
/// retry hint the status carries and with the same operation identity (an
/// earlier attempt may still have run); hops return it as-is rather than
/// retrying it for the caller.
pub const REQUEST_REFUSED: &str = "request-refused";

/// [`ERROR_REASON_METADATA_KEY`] value of a FAILED_PRECONDITION refusing
/// guest work because the sandbox is not running and the request may not
/// wake it: its wake policy does not allow the request's activity, or the
/// sandbox is deleting or failed. Repeating the request cannot help until
/// the caller wakes or replaces the sandbox.
pub const SANDBOX_NOT_RUNNING: &str = "sandbox-not-running";
