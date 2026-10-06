//! Native `/v1` operation manifest shared by routing and contract generation.

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum NativeHttpMethod {
    Delete,
    Get,
    Patch,
    Post,
    Put,
}

impl NativeHttpMethod {
    pub const fn openapi_name(self) -> &'static str {
        match self {
            Self::Delete => "delete",
            Self::Get => "get",
            Self::Patch => "patch",
            Self::Post => "post",
            Self::Put => "put",
        }
    }
}

/// OpenAPI vendor extension naming an operation's or request field's M1 status.
pub const M1_STATUS_EXTENSION: &str = "x-space-m1-status";
pub const SSE_DATA_SCHEMA_EXTENSION: &str = "x-space-sse-event-data-schema";
pub const MAX_UTF8_BYTES_EXTENSION: &str = "x-space-max-utf8-bytes";
pub const MAX_AGGREGATE_UTF8_BYTES_EXTENSION: &str = "x-space-max-aggregate-utf8-bytes";

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum NativeOperationStatus {
    M1Supported,
    Incubating,
}

impl NativeOperationStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::M1Supported => "m1-supported",
            Self::Incubating => "incubating",
        }
    }

    pub const fn is_m1_supported(self) -> bool {
        matches!(self, Self::M1Supported)
    }
}

/// Which SPACE host router serves an operation.
///
/// Routing parity is asserted per surface, so this has to be declared rather
/// than inferred from the path: a control-plane operation that is never mounted
/// would otherwise stay advertised in the public contract while returning 404.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum NativeSurface {
    ControlPlane,
    DataPlane,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NativeOperation {
    path: &'static str,
    runtime_path: &'static str,
    method: NativeHttpMethod,
    operation_id: &'static str,
    status: NativeOperationStatus,
    surface: NativeSurface,
}

impl NativeOperation {
    const fn new(
        path: &'static str,
        method: NativeHttpMethod,
        operation_id: &'static str,
        status: NativeOperationStatus,
    ) -> Self {
        Self::with_runtime_path(path, path, method, operation_id, status)
    }

    const fn with_runtime_path(
        path: &'static str,
        runtime_path: &'static str,
        method: NativeHttpMethod,
        operation_id: &'static str,
        status: NativeOperationStatus,
    ) -> Self {
        Self {
            path,
            runtime_path,
            method,
            operation_id,
            status,
            surface: NativeSurface::ControlPlane,
        }
    }

    const fn data_plane(
        path: &'static str,
        method: NativeHttpMethod,
        operation_id: &'static str,
        status: NativeOperationStatus,
    ) -> Self {
        Self {
            path,
            runtime_path: path,
            method,
            operation_id,
            status,
            surface: NativeSurface::DataPlane,
        }
    }

    pub const fn path(self) -> &'static str {
        self.path
    }

    pub fn relative_path(self) -> &'static str {
        self.path
            .strip_prefix("/v1")
            .expect("native operation path starts with /v1")
    }

    pub const fn runtime_path(self) -> &'static str {
        self.runtime_path
    }

    pub fn relative_runtime_path(self) -> &'static str {
        self.runtime_path
            .strip_prefix("/v1")
            .expect("native runtime path starts with /v1")
    }

    pub const fn method(self) -> NativeHttpMethod {
        self.method
    }

    pub const fn operation_id(self) -> &'static str {
        self.operation_id
    }

    pub const fn status(self) -> NativeOperationStatus {
        self.status
    }

    pub const fn surface(self) -> NativeSurface {
        self.surface
    }
}

/// The transparent-proxy sub-path mount, which is deliberately not a manifest
/// operation.
///
/// A transparent proxy is not an API operation. The sub-path is opaque and
/// forwarded verbatim to a sandbox-local service, and the method is whatever
/// the caller used, so declaring either would put a shape in the public
/// contract that SPACE does not define and cannot validate. [`PROXY_SESSION`]
/// documents the entry point and its forwarding behaviour; this constant is
/// that operation's transport, and
/// `unclassified_mounts_sit_under_a_declared_operation` keeps it anchored to
/// it rather than letting an arbitrary route be excluded.
pub const PROXY_SESSION_SUBPATH_MOUNT: &str = "/v1/sessions/{session_id}/proxy/{*path}";

const SANDBOX_ACTION_RUNTIME_PATH: &str = "/v1/sandboxes/{sandbox_id}";
const PROCESS_ACTION_RUNTIME_PATH: &str = "/v1/sandboxes/{sandbox_id}/processes/{process_id}";

macro_rules! native_operations {
    ($($name:ident = $value:expr;)+) => {
        $(pub const $name: NativeOperation = $value;)+

        pub const NATIVE_V1_OPERATIONS: &[NativeOperation] = &[$($name),+];
    };
}

use NativeHttpMethod::{Delete, Get, Patch, Post};
use NativeOperationStatus::{Incubating, M1Supported};

native_operations! {
    REGISTER_CREDENTIAL_REFERENCE = NativeOperation::new("/v1/credential-references", Post, "unstableRegisterCredentialReference", Incubating);
    DELETE_CREDENTIAL_REFERENCE = NativeOperation::new("/v1/credential-references/{credential_reference_id}", Delete, "unstableDeleteCredentialReference", Incubating);
    GET_OPERATION = NativeOperation::new("/v1/operations/{operation_id}", Get, "getOperation", M1Supported);
    LIST_OPERATION_EVENTS = NativeOperation::new("/v1/operations/{operation_id}/events", Get, "listOperationEvents", Incubating);
    LIST_SANDBOXES = NativeOperation::new("/v1/sandboxes", Get, "listSandboxes", M1Supported);
    CREATE_SANDBOX = NativeOperation::new("/v1/sandboxes", Post, "createSandbox", M1Supported);
    DELETE_SANDBOX = NativeOperation::new("/v1/sandboxes/{sandbox_id}", Delete, "deleteSandbox", M1Supported);
    GET_SANDBOX = NativeOperation::new("/v1/sandboxes/{sandbox_id}", Get, "getSandbox", M1Supported);
    UPDATE_SANDBOX = NativeOperation::new("/v1/sandboxes/{sandbox_id}", Patch, "updateSandbox", Incubating);
    CREATE_COMMAND = NativeOperation::new("/v1/sandboxes/{sandbox_id}/commands", Post, "createCommand", M1Supported);
    CREATE_FILE_WATCH_SESSION = NativeOperation::new("/v1/sandboxes/{sandbox_id}/file-watch-sessions", Post, "createFileWatchSession", Incubating);
    DISABLE_FILE_CHANGES = NativeOperation::new("/v1/sandboxes/{sandbox_id}/files:changes_disable", Post, "disableFileChanges", Incubating);
    ENABLE_FILE_CHANGES = NativeOperation::new("/v1/sandboxes/{sandbox_id}/files:changes_enable", Post, "enableFileChanges", Incubating);
    LIST_FILES = NativeOperation::new("/v1/sandboxes/{sandbox_id}/files:list", Post, "listFiles", Incubating);
    CREATE_DIRECTORY = NativeOperation::new("/v1/sandboxes/{sandbox_id}/files:mkdir", Post, "createDirectory", Incubating);
    LIST_MODIFIED_FILES = NativeOperation::new("/v1/sandboxes/{sandbox_id}/files:modified", Post, "listModifiedFiles", Incubating);
    READ_FILE = NativeOperation::new("/v1/sandboxes/{sandbox_id}/files:read", Post, "readFile", Incubating);
    REMOVE_FILE = NativeOperation::new("/v1/sandboxes/{sandbox_id}/files:remove", Post, "removeFile", Incubating);
    RENAME_FILE = NativeOperation::new("/v1/sandboxes/{sandbox_id}/files:rename", Post, "renameFile", Incubating);
    STAT_FILE = NativeOperation::new("/v1/sandboxes/{sandbox_id}/files:stat", Post, "statFile", Incubating);
    WRITE_FILE = NativeOperation::new("/v1/sandboxes/{sandbox_id}/files:write", Post, "writeFile", Incubating);
    WRITE_FILE_BATCH = NativeOperation::new("/v1/sandboxes/{sandbox_id}/files:write_batch", Post, "writeFileBatch", Incubating);
    STREAM_SANDBOX_LIFECYCLE_EVENTS = NativeOperation::new("/v1/sandboxes/{sandbox_id}/lifecycle-events", Get, "streamSandboxLifecycleEvents", Incubating);
    STREAM_SANDBOX_METRICS = NativeOperation::new("/v1/sandboxes/{sandbox_id}/metrics-stream", Get, "streamSandboxMetrics", Incubating);
    GET_SANDBOX_NETWORK_POLICY = NativeOperation::new("/v1/sandboxes/{sandbox_id}/network-policy", Get, "getSandboxNetworkPolicy", Incubating);
    CREATE_PORT_SESSION = NativeOperation::new("/v1/sandboxes/{sandbox_id}/port-sessions", Post, "createPortSession", Incubating);
    LIST_PORTS = NativeOperation::new("/v1/sandboxes/{sandbox_id}/ports", Get, "listPorts", Incubating);
    CLOSE_PORT = NativeOperation::new("/v1/sandboxes/{sandbox_id}/ports/{port}/close", Post, "closePort", Incubating);
    OPEN_PORT = NativeOperation::new("/v1/sandboxes/{sandbox_id}/ports/{port}/open", Post, "openPort", Incubating);
    CREATE_PROCESS_SESSION = NativeOperation::new("/v1/sandboxes/{sandbox_id}/process-sessions", Post, "createProcessSession", Incubating);
    LIST_PROCESSES = NativeOperation::new("/v1/sandboxes/{sandbox_id}/processes", Get, "listProcesses", M1Supported);
    GET_PROCESS = NativeOperation::new("/v1/sandboxes/{sandbox_id}/processes/{process_id}", Get, "getProcess", M1Supported);
    KILL_PROCESS = NativeOperation::with_runtime_path("/v1/sandboxes/{sandbox_id}/processes/{process_id}:kill", PROCESS_ACTION_RUNTIME_PATH, Post, "killProcess", M1Supported);
    SIGNAL_PROCESS = NativeOperation::with_runtime_path("/v1/sandboxes/{sandbox_id}/processes/{process_id}:signal", PROCESS_ACTION_RUNTIME_PATH, Post, "signalProcess", M1Supported);
    CREATE_PROTOCOL_SESSION = NativeOperation::new("/v1/sandboxes/{sandbox_id}/protocol-sessions", Post, "createProtocolSession", Incubating);
    LIST_SESSIONS = NativeOperation::new("/v1/sandboxes/{sandbox_id}/sessions", Get, "listSessions", Incubating);
    REVOKE_SESSION = NativeOperation::new("/v1/sandboxes/{sandbox_id}/sessions/{session_id}", Delete, "revokeSession", Incubating);
    GET_SESSION = NativeOperation::new("/v1/sandboxes/{sandbox_id}/sessions/{session_id}", Get, "getSession", Incubating);
    LIST_SNAPSHOTS = NativeOperation::new("/v1/sandboxes/{sandbox_id}/snapshots", Get, "listSnapshots", Incubating);
    GET_SNAPSHOT = NativeOperation::new("/v1/sandboxes/{sandbox_id}/snapshots/{snapshot_id}", Get, "getSnapshot", Incubating);
    GET_SANDBOX_STATS = NativeOperation::new("/v1/sandboxes/{sandbox_id}/stats", Get, "getSandboxStats", Incubating);
    CREATE_TERMINAL_SESSION = NativeOperation::new("/v1/sandboxes/{sandbox_id}/terminal-sessions", Post, "createTerminalSession", Incubating);
    CHECKPOINT_SANDBOX = NativeOperation::with_runtime_path("/v1/sandboxes/{sandbox_id}:checkpoint", SANDBOX_ACTION_RUNTIME_PATH, Post, "checkpointSandbox", Incubating);
    DISABLE_SANDBOX_SNAPSHOTS = NativeOperation::with_runtime_path("/v1/sandboxes/{sandbox_id}:disable_snapshots", SANDBOX_ACTION_RUNTIME_PATH, Post, "disableSandboxSnapshots", Incubating);
    ENABLE_SANDBOX_SNAPSHOTS = NativeOperation::with_runtime_path("/v1/sandboxes/{sandbox_id}:enable_snapshots", SANDBOX_ACTION_RUNTIME_PATH, Post, "enableSandboxSnapshots", Incubating);
    KILL_SANDBOX = NativeOperation::with_runtime_path("/v1/sandboxes/{sandbox_id}:kill", SANDBOX_ACTION_RUNTIME_PATH, Post, "killSandbox", Incubating);
    PAUSE_SANDBOX = NativeOperation::with_runtime_path("/v1/sandboxes/{sandbox_id}:pause", SANDBOX_ACTION_RUNTIME_PATH, Post, "pauseSandbox", M1Supported);
    RESTORE_SANDBOX = NativeOperation::with_runtime_path("/v1/sandboxes/{sandbox_id}:restore", SANDBOX_ACTION_RUNTIME_PATH, Post, "restoreSandbox", Incubating);
    RESUME_SANDBOX = NativeOperation::with_runtime_path("/v1/sandboxes/{sandbox_id}:resume", SANDBOX_ACTION_RUNTIME_PATH, Post, "resumeSandbox", Incubating);
    ROLLBACK_SANDBOX = NativeOperation::with_runtime_path("/v1/sandboxes/{sandbox_id}:rollback", SANDBOX_ACTION_RUNTIME_PATH, Post, "rollbackSandbox", Incubating);
    START_SANDBOX = NativeOperation::with_runtime_path("/v1/sandboxes/{sandbox_id}:start", SANDBOX_ACTION_RUNTIME_PATH, Post, "startSandbox", Incubating);
    STOP_SANDBOX = NativeOperation::with_runtime_path("/v1/sandboxes/{sandbox_id}:stop", SANDBOX_ACTION_RUNTIME_PATH, Post, "stopSandbox", Incubating);
    SUSPEND_SANDBOX = NativeOperation::with_runtime_path("/v1/sandboxes/{sandbox_id}:suspend", SANDBOX_ACTION_RUNTIME_PATH, Post, "suspendSandbox", M1Supported);
    WAKE_SANDBOX = NativeOperation::with_runtime_path("/v1/sandboxes/{sandbox_id}:wake", SANDBOX_ACTION_RUNTIME_PATH, Post, "wakeSandbox", M1Supported);
    SET_SANDBOX_TIMEOUT = NativeOperation::with_runtime_path("/v1/sandboxes/{sandbox_id}:set_timeout", SANDBOX_ACTION_RUNTIME_PATH, Post, "setSandboxTimeout", Incubating);
    ATTACH_SESSION = NativeOperation::data_plane("/v1/sessions/{session_id}/attach", Get, "attachSession", Incubating);
    STREAM_SESSION_EVENTS = NativeOperation::data_plane("/v1/sessions/{session_id}/events", Get, "streamSessionEvents", M1Supported);
    PROXY_SESSION = NativeOperation::data_plane("/v1/sessions/{session_id}/proxy", Get, "proxySession", Incubating);
    LIST_TEMPLATES = NativeOperation::new("/v1/templates", Get, "listTemplates", Incubating);
    GET_TEMPLATE = NativeOperation::new("/v1/templates/{template_name}", Get, "getTemplate", Incubating);
}

#[cfg(test)]
mod tests;
