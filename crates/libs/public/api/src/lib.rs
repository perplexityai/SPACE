//! Native SPACE public API contract vocabulary.
//!
//! This crate is the public boundary layer for `/v1` and the public data
//! plane. It depends on common domain nouns, but common domain crates do not
//! depend on these wire contracts.

use pplx_space_common_core::types::SnapshotId;

use std::{collections::BTreeMap, fmt, str::FromStr};

use pplx_space_common_core::types::{
    AutoLifecyclePolicy, EnvMap, RuntimeKind, SandboxAutoLifecyclePatch, SandboxId, SimpleDuration,
    TemplateReference,
};
pub use pplx_space_common_core::types::{IdempotencyKey, ProcessId};
use pplx_space_net_common::NetworkPolicyV1;
use uuid::Uuid;

#[cfg(feature = "serde")]
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

#[cfg(any(feature = "schema", feature = "serde"))]
mod command;
pub mod routes;
mod sandbox;
#[cfg(feature = "schema")]
mod schema;

pub const PUBLIC_SANDBOX_ID_PREFIX: &str = "sbx_";
pub const PUBLIC_OPERATION_ID_PREFIX: &str = "op_";
pub const PUBLIC_SNAPSHOT_ID_PREFIX: &str = "snap_";
pub const TERMINAL_SESSION_ID_PREFIX: &str = "term_";
pub const PROCESS_SESSION_ID_PREFIX: &str = "proc_";
pub const FILE_WATCH_SESSION_ID_PREFIX: &str = "watch_";
pub const PORT_SESSION_ID_PREFIX: &str = "port_";
pub const PROTOCOL_SESSION_ID_PREFIX: &str = "proto_";
pub const DATA_PLANE_SESSION_EVENTS_PATH_TEMPLATE: &str = routes::STREAM_SESSION_EVENTS.path();

const MAX_PAGE_CURSOR_BYTES: usize = 4096;
const REQUEST_FINGERPRINT_HEX_LEN: usize = 64;

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum PublicIdParseError {
    #[error("public id must start with '{expected}'")]
    MissingPrefix { expected: &'static str },
    #[error("public id after prefix '{prefix}' is not a valid UUID: {source}")]
    InvalidUuid {
        prefix: &'static str,
        #[source]
        source: uuid::Error,
    },
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("unsupported {kind} '{value}'")]
pub struct PublicEnumParseError {
    kind: &'static str,
    value: String,
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("{0}")]
pub struct PublicApiParseError(&'static str);

macro_rules! prefixed_uuid_type {
    ($name:ident, $prefix_const:ident) => {
        #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
        #[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
        #[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct $name(Uuid);

        impl $name {
            pub const PREFIX: &'static str = $prefix_const;

            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }

            pub const fn from_uuid(uuid: Uuid) -> Self {
                Self(uuid)
            }

            pub const fn as_uuid(self) -> Uuid {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_tuple(stringify!($name))
                    .field(&self.to_string())
                    .finish()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "{}{}", Self::PREFIX, self.0)
            }
        }

        impl FromStr for $name {
            type Err = PublicIdParseError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                let Some(uuid) = value.strip_prefix(Self::PREFIX) else {
                    return Err(PublicIdParseError::MissingPrefix {
                        expected: Self::PREFIX,
                    });
                };
                Uuid::parse_str(uuid)
                    .map(Self)
                    .map_err(|source| PublicIdParseError::InvalidUuid {
                        prefix: Self::PREFIX,
                        source,
                    })
            }
        }

        impl TryFrom<String> for $name {
            type Error = PublicIdParseError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::from_str(&value)
            }
        }

        impl From<$name> for Uuid {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.to_string()
            }
        }
    };
}

/// The OpenAPI shape shared by every extensible response vocabulary: a plain
/// string documenting the known values as `examples` instead of a closed
/// `enum`, so generated clients keep working when the vocabulary grows
/// (MVP RFC §7). `$known` is the `&[&str]` of wire values.
macro_rules! extensible_string_schema {
    ($name:ident, $label:literal, $known:expr) => {
        #[cfg(feature = "schema")]
        impl schemars::JsonSchema for $name {
            fn schema_name() -> std::borrow::Cow<'static, str> {
                std::borrow::Cow::Borrowed(stringify!($name))
            }

            fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
                schemars::json_schema!({
                    "type": "string",
                    "description": concat!(
                        "Extensible ", $label,
                        ". Clients must accept values not listed in examples."
                    ),
                    "examples": $known
                })
            }
        }
    };
}

/// A closed vocabulary: the type has exactly the listed variants, `FromStr`
/// rejects anything else, and the OpenAPI schema is a closed `enum`. Use for
/// request-side values.
macro_rules! wire_enum {
    ($name:ident, $label:literal, {$($variant:ident => $wire:literal),+ $(,)?}) => {
        #[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub enum $name {
            $(
                #[cfg_attr(feature = "schema", schemars(rename = $wire))]
                $variant
            ),+
        }

        closed_wire_enum_impls!($name, $label, {$($variant => $wire),+});
    };
}

/// A closed type with an extensible schema: the server dispatches on these
/// values (session kinds select ID prefixes and scopes, actions select
/// operation kinds), so a value this build does not know is rejected rather
/// than carried as `Other`. Clients still see the response-side vocabulary
/// as an open string, and the contract check accepts additions.
macro_rules! wire_enum_open_schema {
    ($name:ident, $label:literal, {$($variant:ident => $wire:literal),+ $(,)?}) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub enum $name {
            $($variant),+
        }

        closed_wire_enum_impls!($name, $label, {$($variant => $wire),+});
        extensible_string_schema!($name, $label, Self::KNOWN_VALUES);
    };
    // Variants after `internal` parse and serialize like the rest but are left
    // out of the published schema: the schema is an extensible string, so a
    // client already accepts a value it doesn't list, and an internal value
    // doesn't change the public contract.
    ($name:ident, $label:literal, {$($variant:ident => $wire:literal),+ $(,)?},
     internal {$($internal:ident => $internal_wire:literal),+ $(,)?}) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub enum $name {
            $($variant,)+
            $($internal),+
        }

        closed_wire_enum_impls!(
            $name, $label, {$($variant => $wire,)+ $($internal => $internal_wire),+}
        );

        impl $name {
            /// [`Self::KNOWN_VALUES`] without the internal ones; what the
            /// schema publishes.
            pub const PUBLIC_VALUES: &'static [&'static str] = &[$($wire),+];
        }

        extensible_string_schema!($name, $label, Self::PUBLIC_VALUES);
    };
}

macro_rules! closed_wire_enum_impls {
    ($name:ident, $label:literal, {$($variant:ident => $wire:literal),+ $(,)?}) => {
        impl $name {
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];
            pub const KNOWN_VALUES: &'static [&'static str] = &[$($wire),+];
            pub const LABEL: &'static str = $label;

            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $wire),+
                }
            }

            /// Strict parse; the same as `FromStr` here, named to match the
            /// open enums so request-side call sites read identically.
            pub fn parse_known(value: &str) -> Result<Self, PublicEnumParseError> {
                value.parse()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl FromStr for $name {
            type Err = PublicEnumParseError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value {
                    $($wire => Ok(Self::$variant),)+
                    other => Err(PublicEnumParseError {
                        kind: $label,
                        value: other.to_string(),
                    }),
                }
            }
        }

        #[cfg(feature = "serde")]
        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(self.as_str())
            }
        }

        #[cfg(feature = "serde")]
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                value.parse().map_err(de::Error::custom)
            }
        }
    };
}

/// A response-side vocabulary that clients must treat as extensible.
///
/// The known variants serialize to their wire strings; any other string
/// deserializes into `Other` and round-trips unchanged, so a value added by a
/// newer server survives an older reader instead of failing the whole
/// response. The OpenAPI schema is `type: string` with the known values as
/// `examples` rather than a closed `enum`, which is what lets generated
/// clients keep working when the vocabulary grows (MVP RFC §7).
///
/// Wire input must not use `FromStr` (infallible): parse request-side values
/// with [`parse_known`](Self::parse_known) so unknown input is still rejected.
macro_rules! open_wire_enum {
    ($name:ident, $label:literal, {$($variant:ident => $wire:literal),+ $(,)?}) => {
        #[derive(Clone, Debug)]
        pub enum $name {
            $($variant),+,
            Other(String),
        }

        impl $name {
            /// Every known variant, in declaration order. Excludes `Other`.
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];
            pub const KNOWN_VALUES: &'static [&'static str] = &[$($wire),+];
            pub const LABEL: &'static str = $label;

            pub fn as_str(&self) -> &str {
                match self {
                    $(Self::$variant => $wire,)+
                    Self::Other(value) => value,
                }
            }

            /// True unless this is an `Other` value this build does not know.
            pub fn is_known(&self) -> bool {
                !matches!(self, Self::Other(_))
            }

            /// Strict parse for request-side input: unknown values are an
            /// error rather than `Other`.
            pub fn parse_known(value: &str) -> Result<Self, PublicEnumParseError> {
                match value {
                    $($wire => Ok(Self::$variant),)+
                    other => Err(PublicEnumParseError {
                        kind: $label,
                        value: other.to_string(),
                    }),
                }
            }

            fn from_wire(value: &str) -> Self {
                match value {
                    $($wire => Self::$variant,)+
                    other => Self::Other(other.to_string()),
                }
            }

            fn from_owned_wire(value: String) -> Self {
                match value.as_str() {
                    $($wire => Self::$variant,)+
                    _ => Self::Other(value),
                }
            }

            fn known_position(&self) -> Option<usize> {
                Self::KNOWN_VALUES
                    .iter()
                    .position(|known| *known == self.as_str())
            }
        }

        impl PartialEq for $name {
            fn eq(&self, other: &Self) -> bool {
                self.as_str() == other.as_str()
            }
        }

        impl Eq for $name {}

        impl PartialOrd for $name {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }

        impl Ord for $name {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                match (self.known_position(), other.known_position()) {
                    (Some(left), Some(right)) => left.cmp(&right),
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => self.as_str().cmp(other.as_str()),
                }
            }
        }

        impl std::hash::Hash for $name {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                std::hash::Hash::hash(self.as_str(), state);
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self::from_owned_wire(value)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl FromStr for $name {
            type Err = std::convert::Infallible;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Ok(Self::from_wire(value))
            }
        }

        #[cfg(feature = "serde")]
        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(self.as_str())
            }
        }

        #[cfg(feature = "serde")]
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Ok(Self::from_owned_wire(value))
            }
        }

        extensible_string_schema!($name, $label, Self::KNOWN_VALUES);
    };
}

prefixed_uuid_type!(PublicSandboxId, PUBLIC_SANDBOX_ID_PREFIX);
prefixed_uuid_type!(OperationId, PUBLIC_OPERATION_ID_PREFIX);
prefixed_uuid_type!(PublicSnapshotId, PUBLIC_SNAPSHOT_ID_PREFIX);
prefixed_uuid_type!(TerminalSessionId, TERMINAL_SESSION_ID_PREFIX);
prefixed_uuid_type!(ProcessSessionId, PROCESS_SESSION_ID_PREFIX);
prefixed_uuid_type!(FileWatchSessionId, FILE_WATCH_SESSION_ID_PREFIX);
prefixed_uuid_type!(PortSessionId, PORT_SESSION_ID_PREFIX);
prefixed_uuid_type!(ProtocolSessionId, PROTOCOL_SESSION_ID_PREFIX);

impl From<SandboxId> for PublicSandboxId {
    fn from(value: SandboxId) -> Self {
        Self::from_uuid(value.as_uuid())
    }
}

impl From<PublicSandboxId> for SandboxId {
    fn from(value: PublicSandboxId) -> Self {
        Self::from_uuid(value.as_uuid())
    }
}

impl From<SnapshotId> for PublicSnapshotId {
    fn from(value: SnapshotId) -> Self {
        Self::from_uuid(value.as_uuid())
    }
}
impl From<PublicSnapshotId> for SnapshotId {
    fn from(value: PublicSnapshotId) -> Self {
        Self::from_uuid(value.as_uuid())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum DataPlaneSessionId {
    Terminal(TerminalSessionId),
    Process(ProcessSessionId),
    FileWatch(FileWatchSessionId),
    Port(PortSessionId),
    Protocol(ProtocolSessionId),
}

impl DataPlaneSessionId {
    pub fn new_for_kind(kind: SessionKind) -> Self {
        match kind {
            SessionKind::Terminal => Self::Terminal(TerminalSessionId::new()),
            SessionKind::Process => Self::Process(ProcessSessionId::new()),
            SessionKind::FileWatch => Self::FileWatch(FileWatchSessionId::new()),
            SessionKind::Port => Self::Port(PortSessionId::new()),
            SessionKind::Protocol => Self::Protocol(ProtocolSessionId::new()),
        }
    }

    pub const fn kind(self) -> SessionKind {
        match self {
            Self::Terminal(_) => SessionKind::Terminal,
            Self::Process(_) => SessionKind::Process,
            Self::FileWatch(_) => SessionKind::FileWatch,
            Self::Port(_) => SessionKind::Port,
            Self::Protocol(_) => SessionKind::Protocol,
        }
    }

    pub const fn as_uuid(self) -> Uuid {
        match self {
            Self::Terminal(id) => id.as_uuid(),
            Self::Process(id) => id.as_uuid(),
            Self::FileWatch(id) => id.as_uuid(),
            Self::Port(id) => id.as_uuid(),
            Self::Protocol(id) => id.as_uuid(),
        }
    }
}

impl fmt::Display for DataPlaneSessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Terminal(id) => id.fmt(formatter),
            Self::Process(id) => id.fmt(formatter),
            Self::FileWatch(id) => id.fmt(formatter),
            Self::Port(id) => id.fmt(formatter),
            Self::Protocol(id) => id.fmt(formatter),
        }
    }
}

/// Renders the canonical data-plane event-stream path for a typed session ID.
pub fn data_plane_session_events_path(session_id: DataPlaneSessionId) -> String {
    DATA_PLANE_SESSION_EVENTS_PATH_TEMPLATE.replace("{session_id}", &session_id.to_string())
}

impl FromStr for DataPlaneSessionId {
    type Err = PublicIdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.starts_with(TerminalSessionId::PREFIX) {
            value.parse().map(Self::Terminal)
        } else if value.starts_with(ProcessSessionId::PREFIX) {
            value.parse().map(Self::Process)
        } else if value.starts_with(FileWatchSessionId::PREFIX) {
            value.parse().map(Self::FileWatch)
        } else if value.starts_with(PortSessionId::PREFIX) {
            value.parse().map(Self::Port)
        } else if value.starts_with(ProtocolSessionId::PREFIX) {
            value.parse().map(Self::Protocol)
        } else {
            Err(PublicIdParseError::MissingPrefix {
                expected: "term_, proc_, watch_, port_, or proto_",
            })
        }
    }
}

#[cfg(feature = "serde")]
impl Serialize for DataPlaneSessionId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for DataPlaneSessionId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(de::Error::custom)
    }
}

open_wire_enum!(OperationState, "operation state", {
    Queued => "queued",
    Running => "running",
    Succeeded => "succeeded",
    Failed => "failed",
    Canceled => "canceled",
    Expired => "expired",
});

wire_enum_open_schema!(OperationKind, "operation kind", {
    CreateSandbox => "create_sandbox",
    ForkSandbox => "fork_sandbox",
    UpdateSandbox => "update_sandbox",
    DeleteSandbox => "delete_sandbox",
    StartSandbox => "start_sandbox",
    StopSandbox => "stop_sandbox",
    PauseSandbox => "pause_sandbox",
    ResumeSandbox => "resume_sandbox",
    SuspendSandbox => "suspend_sandbox",
    RestoreSandbox => "restore_sandbox",
    WakeSandbox => "wake_sandbox",
    KillSandbox => "kill_sandbox",
    CheckpointSandbox => "checkpoint_sandbox",
    EnableSandboxSnapshots => "enable_sandbox_snapshots",
    DisableSandboxSnapshots => "disable_sandbox_snapshots",
    RollbackSandbox => "rollback_sandbox",
    DeleteTemplate => "delete_template",
    CreateSession => "create_session",
}, internal {
    // The durable teardown a failed create queues for the sandbox it already
    // started. A kind of its own so a controller that predates it never claims
    // one: claims select by kind.
    CompensateCreateSandbox => "compensate_create_sandbox",
});

open_wire_enum!(OperationTargetType, "operation target type", {
    Sandbox => "sandbox",
    Session => "session",
    Template => "template",
    Snapshot => "snapshot",
    Volume => "volume",
});

wire_enum!(PublicOperationEventType, "operation event type", {
    Created => "operation.created",
    Claimed => "operation.claimed",
    Completed => "operation.completed",
    Failed => "operation.failed",
    Requeued => "operation.requeued",
    Canceled => "operation.canceled",
    Expired => "operation.expired",
    Error => "operation.error",
});

impl PublicOperationEventType {
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Canceled | Self::Expired
        )
    }
}

wire_enum!(PublicSnapshotType, "snapshot type", {
    Disk => "disk",
    Full => "full",
});

wire_enum!(PublicSnapshotState, "snapshot state", {
    Creating => "creating",
    Ready => "ready",
    Failed => "failed",
});

wire_enum_open_schema!(SandboxLifecycleAction, "sandbox lifecycle action", {
    Start => "start",
    Stop => "stop",
    Pause => "pause",
    Resume => "resume",
    Suspend => "suspend",
    Restore => "restore",
    Wake => "wake",
    Kill => "kill",
    Checkpoint => "checkpoint",
    EnableSnapshots => "enable_snapshots",
    DisableSnapshots => "disable_snapshots",
    Rollback => "rollback",
    Delete => "delete",
});

impl SandboxLifecycleAction {
    pub const fn operation_kind(self) -> OperationKind {
        match self {
            Self::Start => OperationKind::StartSandbox,
            Self::Stop => OperationKind::StopSandbox,
            Self::Pause => OperationKind::PauseSandbox,
            Self::Resume => OperationKind::ResumeSandbox,
            Self::Suspend => OperationKind::SuspendSandbox,
            Self::Restore => OperationKind::RestoreSandbox,
            Self::Wake => OperationKind::WakeSandbox,
            Self::Kill => OperationKind::KillSandbox,
            Self::Checkpoint => OperationKind::CheckpointSandbox,
            Self::EnableSnapshots => OperationKind::EnableSandboxSnapshots,
            Self::DisableSnapshots => OperationKind::DisableSandboxSnapshots,
            Self::Rollback => OperationKind::RollbackSandbox,
            Self::Delete => OperationKind::DeleteSandbox,
        }
    }
}

open_wire_enum!(SandboxLifecycleState, "sandbox lifecycle state", {
    Creating => "creating",
    Stopped => "stopped",
    Starting => "starting",
    Running => "running",
    Pausing => "pausing",
    Paused => "paused",
    Resuming => "resuming",
    Suspending => "suspending",
    Suspended => "suspended",
    Restoring => "restoring",
    Migrating => "migrating",
    Stopping => "stopping",
    Failed => "failed",
    Deleting => "deleting",
    Deleted => "deleted",
});

open_wire_enum!(SandboxFailureCode, "sandbox failure code", {
    RuntimeFailure => "runtime_failure",
});

open_wire_enum!(SandboxIdleAction, "sandbox idle action", {
    Delete => "delete",
    Suspend => "suspend",
});

open_wire_enum!(SandboxConditionType, "sandbox condition type", {
    Ready => "ready",
    NetworkReady => "network_ready",
});

open_wire_enum!(ConditionStatus, "condition status", {
    True => "true",
    False => "false",
    Unknown => "unknown",
});

wire_enum_open_schema!(SessionKind, "session kind", {
    Terminal => "terminal",
    Process => "process",
    FileWatch => "file_watch",
    Port => "port",
    Protocol => "protocol",
});

impl SessionKind {
    pub const fn id_prefix(self) -> &'static str {
        match self {
            Self::Terminal => TerminalSessionId::PREFIX,
            Self::Process => ProcessSessionId::PREFIX,
            Self::FileWatch => FileWatchSessionId::PREFIX,
            Self::Port => PortSessionId::PREFIX,
            Self::Protocol => ProtocolSessionId::PREFIX,
        }
    }

    pub const fn connect_scope(self) -> DataPlaneScope {
        match self {
            Self::Terminal => DataPlaneScope::TerminalConnect,
            Self::Process => DataPlaneScope::ProcessConnect,
            Self::FileWatch => DataPlaneScope::FileWatchConnect,
            Self::Port => DataPlaneScope::PortConnect,
            Self::Protocol => DataPlaneScope::ProtocolConnect,
        }
    }
}

open_wire_enum!(SessionState, "session state", {
    Pending => "pending",
    Attaching => "attaching",
    Open => "open",
    Reconnecting => "reconnecting",
    Closing => "closing",
    Closed => "closed",
    Revoked => "revoked",
    Failed => "failed",
});

wire_enum!(DataPlaneScope, "data-plane scope", {
    TerminalConnect => "terminal:connect",
    ProcessConnect => "process:connect",
    FileWatchConnect => "file_watch:connect",
    PortConnect => "port:connect",
    ProtocolConnect => "protocol:connect",
});

open_wire_enum!(DataPlaneTokenTransport, "data-plane token transport", {
    FirstFrame => "first_frame",
    SecWebSocketProtocol => "sec_websocket_protocol",
    AuthorizationHeader => "authorization_header",
});

wire_enum!(PortSessionProtocol, "port session protocol", {
    Http => "http",
    Tcp => "tcp",
});

wire_enum!(PortExposureProtocol, "port exposure protocol", {
    Tcp => "tcp",
});

wire_enum!(PortExposureVisibility, "port exposure visibility", {
    Private => "private",
    Public => "public",
});

wire_enum!(PortExposureDesiredState, "port exposure desired state", {
    Open => "open",
    Closed => "closed",
});

wire_enum!(PortExposureApplyState, "port exposure apply state", {
    Pending => "pending",
    Applied => "applied",
    Failed => "failed",
});

wire_enum!(ProtocolSessionProtocol, "protocol session protocol", {
    Cdp => "cdp",
});

wire_enum!(SessionTargetProtocol, "session target protocol", {
    Http => "http",
    Tcp => "tcp",
    Cdp => "cdp",
});

impl From<PortSessionProtocol> for SessionTargetProtocol {
    fn from(value: PortSessionProtocol) -> Self {
        match value {
            PortSessionProtocol::Http => Self::Http,
            PortSessionProtocol::Tcp => Self::Tcp,
        }
    }
}

impl From<ProtocolSessionProtocol> for SessionTargetProtocol {
    fn from(value: ProtocolSessionProtocol) -> Self {
        match value {
            ProtocolSessionProtocol::Cdp => Self::Cdp,
        }
    }
}

open_wire_enum!(StreamCloseReason, "stream close reason", {
    ClientClosed => "client_closed",
    BackendClosed => "backend_closed",
    BackendReconnecting => "backend_reconnecting",
    TokenExpired => "token_expired",
    Revoked => "revoked",
    IdleTimeout => "idle_timeout",
    Error => "error",
});

open_wire_enum!(ReconnectReason, "reconnect reason", {
    BackendRestart => "backend_restart",
    SandboxMigration => "sandbox_migration",
    RouteGenerationChanged => "route_generation_changed",
    WorkerUnavailable => "worker_unavailable",
    DataPlaneRestart => "data_plane_restart",
});

open_wire_enum!(FileEventKind, "file event kind", {
    Created => "created",
    Modified => "modified",
    Removed => "removed",
    Renamed => "renamed",
    Chmod => "chmod",
    Unknown => "unknown",
});

wire_enum!(FileType, "file type", {
    File => "file",
    Directory => "directory",
    Symlink => "symlink",
    Other => "other",
});

open_wire_enum!(DataPlaneProcessExitStatus, "data-plane process exit status", {
    Exited => "exited",
    Signaled => "signaled",
    TimedOut => "timed_out",
    Unknown => "unknown",
});

open_wire_enum!(DataPlaneResyncReason, "data-plane resync reason", {
    ProcessStreamReconnected => "process_stream_reconnected",
    FileWatchRestarted => "file_watch_restarted",
    CursorOutOfWindow => "cursor_out_of_window",
    CursorReplayUnavailable => "cursor_replay_unavailable",
});

open_wire_enum!(DataPlaneErrorCode, "data-plane error code", {
    TooManyUnauthenticatedSockets => "too_many_unauthenticated_sockets",
    AttachDeadlineExceeded => "attach_deadline_exceeded",
    AttachRequired => "attach_required",
    AttachFrameInvalid => "attach_frame_invalid",
    InvalidToken => "invalid_token",
    TooManyAttachedStreams => "too_many_attached_streams",
    Unauthenticated => "unauthenticated",
    Forbidden => "forbidden",
    SessionNotFound => "session_not_found",
    SessionNotReady => "session_not_ready",
    RouteResolutionFailed => "route_resolution_failed",
    RouteStateInvalid => "route_state_invalid",
    SessionAlreadyAttached => "session_already_attached",
    BackendStreamError => "backend_stream_error",
    BinaryClientFramesNotSupported => "binary_client_frames_not_supported",
    AlreadyAttached => "already_attached",
    ProcessNotStarted => "process_not_started",
    InvalidResize => "invalid_resize",
    ResizeNotSupported => "resize_not_supported",
    InputNotSupported => "input_not_supported",
    UnsupportedSessionKind => "unsupported_session_kind",
    Internal => "internal",
});

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", serde(tag = "type", rename_all = "snake_case"))]
pub enum DataPlaneClientFrame {
    Attach { token: String },
    Input { seq: u64, data_base64: String },
    Resize { cols: u16, rows: u16 },
    Signal { seq: u64, signal: i32 },
    CloseStdin { seq: u64 },
    Heartbeat { seq: u64 },
    Close,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", serde(tag = "type", rename_all = "snake_case"))]
pub enum DataPlaneServerFrame {
    Attached {
        #[cfg_attr(
            feature = "schema",
            schemars(schema_with = "schema::data_plane_session_id")
        )]
        session_id: DataPlaneSessionId,
        route_generation: u64,
        current_node_id: String,
    },
    ProcessStarted {
        process_id: String,
        pid: Option<u32>,
    },
    Stdout {
        seq: u64,
        data_base64: String,
    },
    Stderr {
        seq: u64,
        data_base64: String,
    },
    InputAccepted {
        seq: u64,
    },
    ProcessExited {
        process_id: String,
        exit_code: i32,
        status: DataPlaneProcessExitStatus,
    },
    FileEvent {
        seq: u64,
        path: String,
        kind: FileEventKind,
    },
    BackendReconnecting {
        reason: ReconnectReason,
        route_generation: u64,
    },
    BackendReconnected {
        route_generation: u64,
        current_node_id: String,
    },
    ResyncRequired {
        reason: DataPlaneResyncReason,
    },
    Error {
        code: DataPlaneErrorCode,
        message: String,
        retryable: bool,
    },
    Closed {
        reason: StreamCloseReason,
    },
    ControlAccepted {
        seq: u64,
    },
    #[cfg_attr(feature = "serde", serde(other))]
    Unknown,
}

open_wire_enum!(PublicErrorCode, "public error code", {
    InvalidRequest => "invalid_request",
    Unauthorized => "unauthorized",
    Forbidden => "forbidden",
    NotFound => "not_found",
    Conflict => "conflict",
    FailedPrecondition => "failed_precondition",
    CapacityUnavailable => "capacity_unavailable",
    RateLimited => "rate_limited",
    IdempotencyConflict => "idempotency_conflict",
    OperationFailed => "operation_failed",
    ServiceUnavailable => "service_unavailable",
    Internal => "internal",
});

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
// Inlined rather than shared: every field is optional on create but always
// serialized on read, so one named component cannot describe both directions
// without weakening the response guarantee.
#[cfg_attr(feature = "schema", schemars(inline))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SandboxResources {
    pub vcpu: Option<u32>,
    pub memory_mib: Option<u32>,
    pub disk_mib: Option<u32>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicTemplateSnapshot {
    pub vcpu: u32,
    pub memory_mib: u32,
    pub snapshot_hash: String,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicTemplateVariant {
    pub reference: String,
    pub content_hash: String,
    pub runtime: Option<RuntimeKind>,
    pub arch: Option<String>,
    pub built_at: Option<String>,
    pub artifact_size_bytes: Option<u64>,
    pub has_base_snapshot: bool,
    pub base_snapshot_size_bytes: Option<u64>,
    pub snapshots: Vec<PublicTemplateSnapshot>,
    pub resources: SandboxResources,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicTemplate {
    pub name: String,
    pub description: Option<String>,
    pub variants: Vec<PublicTemplateVariant>,
    #[cfg_attr(feature = "serde", serde(with = "time::serde::rfc3339"))]
    pub created_at: time::OffsetDateTime,
    #[cfg_attr(feature = "serde", serde(with = "time::serde::rfc3339"))]
    pub updated_at: time::OffsetDateTime,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicSnapshot {
    pub id: PublicSnapshotId,
    pub sandbox_id: PublicSandboxId,
    pub snapshot_type: PublicSnapshotType,
    pub state: PublicSnapshotState,
    pub label: Option<String>,
    pub retention_tier: Option<String>,
    pub size_bytes: Option<u64>,
    pub resources: SandboxResources,
    pub template: Option<String>,
    pub template_digest: Option<String>,
    pub failure_reason: Option<String>,
    #[cfg_attr(feature = "serde", serde(with = "time::serde::rfc3339"))]
    pub created_at: time::OffsetDateTime,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicProcess {
    /// Opaque identity used in process endpoint paths; not the guest's numeric PID.
    #[cfg_attr(feature = "schema", schemars(schema_with = "schema::process_id"))]
    pub process_id: ProcessId,
    /// Numeric PID inside the guest. Do not substitute it for `process_id`.
    pub pid: u32,
    pub command: Vec<String>,
    pub cwd: Option<String>,
    pub user: Option<String>,
    pub tag: Option<String>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessSignalRequest {
    /// Unix signal number from 1 through 64, for example 15 for SIGTERM.
    #[cfg_attr(feature = "schema", schemars(range(min = 1, max = 64)))]
    pub signal: i32,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessMutationResponse {
    /// The runtime accepted the action. This is not proof that the process has
    /// exited; inspect its event stream for the exit result.
    pub ok: bool,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", serde(untagged))]
pub enum PublicSandboxMetadataValue {
    String(String),
    Number(i64),
    Bool(bool),
}

pub type PublicSandboxMetadata = BTreeMap<String, PublicSandboxMetadataValue>;

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq)]
pub struct SetSandboxTimeoutRequest {
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "sandbox::deserialize_timeout")
    )]
    #[cfg_attr(feature = "schema", schemars(with = "String"))]
    pub timeout: SimpleDuration,
}

impl SetSandboxTimeoutRequest {
    pub fn timeout_secs(&self) -> Result<u32, PublicApiParseError> {
        let seconds = sandbox::idle_timeout_secs(self.timeout)?;
        u32::try_from(seconds)
            .map_err(|_| PublicApiParseError("timeout exceeds 4294967295 seconds"))
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq)]
pub struct SetSandboxTimeoutResponse {
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "schema::public_sandbox_id")
    )]
    pub sandbox_id: PublicSandboxId,
    pub idle_timeout: String,
    /// Conservative running deadline estimate, assuming node and API clocks agree within five
    /// seconds, clamped to the observation time. Dormant sandboxes report their stored deadline.
    #[cfg_attr(feature = "serde", serde(with = "time::serde::rfc3339::option"))]
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "schema::optional_date_time")
    )]
    pub expires_at: Option<time::OffsetDateTime>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq)]
pub struct CreateSandboxRequest {
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "schema::template_reference")
    )]
    pub template: TemplateReference,
    // `serde(default)` is redundant for serde, which already maps a missing
    // `Option` field to `None`. It is required for schemars: `schema_with`
    // replaces the field's schema wholesale, which loses the `Option` and would
    // otherwise mark `runtime` required.
    #[cfg_attr(feature = "serde", serde(default))]
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "schema::optional_runtime_kind")
    )]
    pub runtime: Option<RuntimeKind>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub name: Option<String>,
    /// Guest environment for the sandbox.
    ///
    /// Accepts string keys and values without the command endpoint's
    /// environment-name and size limits.
    #[cfg_attr(feature = "serde", serde(default))]
    pub env: BTreeMap<String, String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub metadata: PublicSandboxMetadata,
    #[cfg_attr(feature = "serde", serde(default))]
    pub resources: Option<SandboxResources>,
    #[cfg_attr(feature = "serde", serde(default))]
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "schema::optional_network_policy")
    )]
    pub network_policy: Option<NetworkPolicyV1>,
    #[cfg_attr(feature = "serde", serde(default))]
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "schema::optional_auto_lifecycle_policy")
    )]
    pub auto_lifecycle: Option<AutoLifecyclePolicy>,
    /// Positive compact idle duration, such as `600s` or `1.5m`, resolving to
    /// whole seconds within the supported deadline range. Omit for the default.
    #[cfg_attr(
        feature = "serde",
        serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "sandbox::deserialize_idle_timeout"
        )
    )]
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "schema::optional_incubating_string")
    )]
    pub idle_timeout: Option<SimpleDuration>,
    /// Action after the idle duration: delete or suspend. Defaults to suspend
    /// when idle_timeout is provided. Cannot be combined with auto_lifecycle.
    /// Suspend allows waking on access; delete does not.
    #[cfg_attr(
        feature = "serde",
        serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "sandbox::deserialize_idle_action"
        )
    )]
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "schema::optional_incubating_idle_action_input")
    )]
    pub idle_action: Option<SandboxIdleAction>,
}

impl CreateSandboxRequest {
    pub fn idle_timeout_secs(&self) -> Result<Option<u64>, PublicApiParseError> {
        if (self.idle_timeout.is_some() || self.idle_action.is_some())
            && self.auto_lifecycle.is_some()
        {
            return Err(PublicApiParseError(
                "idle settings cannot be combined with auto_lifecycle",
            ));
        }
        if self.idle_timeout.is_none() && self.idle_action.is_some() {
            return Err(PublicApiParseError("idle_action requires idle_timeout"));
        }
        if self
            .idle_action
            .as_ref()
            .is_some_and(|action| !action.is_known())
        {
            return Err(PublicApiParseError("idle_action must be delete or suspend"));
        }
        self.idle_timeout
            .map(sandbox::idle_timeout_secs)
            .transpose()
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", serde(tag = "mode", rename_all = "snake_case"))]
pub enum SandboxNetworkPolicyPatch {
    Set { policy: NetworkPolicyV1 },
    InheritTemplateDefault,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RollbackSandboxRequest {
    pub snapshot_name: String,
    #[cfg_attr(feature = "serde", serde(default))]
    pub auto_restart: bool,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, PartialEq)]
pub struct UpdateSandboxRequest {
    pub metadata: Option<PublicSandboxMetadata>,
    pub resources: Option<SandboxResources>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub network_policy: Option<SandboxNetworkPolicyPatch>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub auto_lifecycle: Option<SandboxAutoLifecyclePatch>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SandboxCondition {
    pub condition_type: SandboxConditionType,
    pub status: ConditionStatus,
    pub reason: String,
    pub message: String,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq)]
pub struct PublicSandbox {
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "schema::public_sandbox_id")
    )]
    pub id: PublicSandboxId,
    pub name: Option<String>,
    /// Current sandbox lifecycle state, distinct from operation progress and
    /// command exit status. Unknown future response values must be tolerated.
    pub state: SandboxLifecycleState,
    /// Current observed failure classification when available; null is not a
    /// guarantee that the sandbox is ready.
    pub failure_code: Option<SandboxFailureCode>,
    /// Readiness and failure observations with explanatory reasons and messages.
    pub conditions: Vec<SandboxCondition>,
    /// Template reference recorded for this sandbox, when available.
    pub template: Option<String>,
    /// Resolved template content digest, when available.
    pub template_digest: Option<String>,
    /// Stored resource requests, not a measurement of resolved guest capacity.
    /// Null fields mean no explicit value was recorded, not zero capacity.
    pub resources: SandboxResources,
    pub metadata: PublicSandboxMetadata,
    /// Stored idle deadline; null when no automatic lifecycle event is scheduled.
    #[cfg_attr(
        feature = "serde",
        serde(with = "time::serde::rfc3339::option", default)
    )]
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "schema::optional_incubating_date_time")
    )]
    pub expires_at: Option<time::OffsetDateTime>,
    /// Idle duration in seconds, such as `600s`, for a single access-idle stage.
    /// Null when the policy cannot be represented by idle_timeout and idle_action.
    #[cfg_attr(feature = "serde", serde(default))]
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "schema::optional_incubating_string")
    )]
    pub idle_timeout: Option<String>,
    /// Action paired with idle_timeout; null for policies with other shapes.
    #[cfg_attr(feature = "serde", serde(default))]
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "schema::optional_incubating_idle_action")
    )]
    pub idle_action: Option<SandboxIdleAction>,
    #[cfg_attr(feature = "serde", serde(with = "time::serde::rfc3339"))]
    #[cfg_attr(feature = "schema", schemars(schema_with = "schema::date_time"))]
    pub created_at: time::OffsetDateTime,
    #[cfg_attr(feature = "serde", serde(with = "time::serde::rfc3339"))]
    #[cfg_attr(feature = "schema", schemars(schema_with = "schema::date_time"))]
    pub updated_at: time::OffsetDateTime,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, PartialEq)]
pub struct SandboxStatsResponse {
    pub sandbox_id: PublicSandboxId,
    pub cpu_max: String,
    pub memory_max: u64,
    pub memory_current: u64,
    pub pids_max: u64,
    pub pids_current: u64,
    pub cpu_usage_usec: u64,
    pub network_tx_bytes: u64,
    pub network_rx_bytes: u64,
    pub net_gateway_reachable: bool,
    pub tcp_active_connections: u64,
    pub tcp_total_connections: u64,
    pub tcp_denied_connections: u64,
    pub udp_active_mappings: u64,
    pub bytes_forwarded: u64,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortExposure {
    pub port: u16,
    pub protocol: PortExposureProtocol,
    pub visibility: PortExposureVisibility,
    pub desired_state: PortExposureDesiredState,
    pub apply_state: PortExposureApplyState,
    pub last_error: Option<String>,
    pub last_bridge_status: Option<String>,
    pub last_applied_bridge_generation: Option<String>,
    pub last_observed_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListPortExposuresResponse {
    pub exposures: Vec<PortExposure>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenPortExposureRequest {
    pub protocol: Option<PortExposureProtocol>,
    pub visibility: Option<PortExposureVisibility>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClosePortExposureRequest {
    pub protocol: Option<PortExposureProtocol>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortExposureResponse {
    pub exposure: PortExposure,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SandboxOperationResponse {
    /// Poll this operation and require success before relying on the action's
    /// outcome. An HTTP 202 response only accepts the work.
    #[cfg_attr(feature = "schema", schemars(schema_with = "schema::operation_id"))]
    pub operation_id: OperationId,
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "schema::public_sandbox_id")
    )]
    pub sandbox_id: PublicSandboxId,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", serde(tag = "type", rename_all = "snake_case"))]
pub enum PublicOperationResult {
    Sandbox(PublicSandboxOperationResult),
    Template(PublicTemplateOperationResult),
    Session(PublicSessionOperationResult),
    Fork(PublicForkOperationResult),
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq)]
pub struct PublicForkOperationResult {
    pub results: Vec<serde_json::Value>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq)]
pub struct PublicSandboxOperationResult {
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "schema::public_sandbox_id")
    )]
    pub sandbox_id: PublicSandboxId,
    pub state: SandboxLifecycleState,
    pub action: Option<SandboxLifecycleAction>,
    pub controller_response: Option<serde_json::Value>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicTemplateOperationResult {
    pub template: String,
    pub state: String,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicSessionOperationResult {
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "schema::data_plane_session_id")
    )]
    pub session_id: DataPlaneSessionId,
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "schema::public_sandbox_id")
    )]
    pub sandbox_id: PublicSandboxId,
    pub kind: SessionKind,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq)]
pub struct PublicOperationRecord {
    #[cfg_attr(feature = "schema", schemars(schema_with = "schema::operation_id"))]
    pub id: OperationId,
    pub kind: OperationKind,
    /// Progress of asynchronous work: queued/running or a terminal outcome
    /// such as succeeded/failed/canceled/expired. This is neither sandbox state
    /// nor a command's process exit status. Tolerate future response values.
    pub state: OperationState,
    pub target_type: Option<OperationTargetType>,
    pub target_id: Option<String>,
    pub progress_message: String,
    /// Machine-readable failure code for a terminal operation, when present.
    /// The separate terminal_error field supplies additional context; do not
    /// parse human-readable messages as codes.
    pub terminal_error_code: Option<PublicErrorCode>,
    pub terminal_error: Option<serde_json::Value>,
    /// Typed operation result when available. Check `state` for success before
    /// consuming it; null alone does not identify a pending or failed operation.
    pub result: Option<PublicOperationResult>,
    #[cfg_attr(feature = "serde", serde(with = "time::serde::rfc3339"))]
    #[cfg_attr(feature = "schema", schemars(schema_with = "schema::date_time"))]
    pub created_at: time::OffsetDateTime,
    #[cfg_attr(feature = "serde", serde(with = "time::serde::rfc3339::option"))]
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "schema::optional_date_time")
    )]
    pub started_at: Option<time::OffsetDateTime>,
    #[cfg_attr(feature = "serde", serde(with = "time::serde::rfc3339::option"))]
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "schema::optional_date_time")
    )]
    pub completed_at: Option<time::OffsetDateTime>,
    #[cfg_attr(feature = "serde", serde(with = "time::serde::rfc3339"))]
    #[cfg_attr(feature = "schema", schemars(schema_with = "schema::date_time"))]
    pub updated_at: time::OffsetDateTime,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, PartialEq)]
pub struct PublicOperationEvent {
    pub operation_id: OperationId,
    pub sequence: i64,
    pub event_type: PublicOperationEventType,
    pub event: serde_json::Value,
    #[cfg_attr(feature = "serde", serde(with = "time::serde::rfc3339"))]
    pub created_at: time::OffsetDateTime,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TerminalSize {
    #[cfg_attr(feature = "schema", schemars(range(min = 1)))]
    pub cols: u16,
    #[cfg_attr(feature = "schema", schemars(range(min = 1)))]
    pub rows: u16,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateTerminalSessionRequest {
    #[cfg_attr(feature = "serde", serde(default))]
    pub command: Vec<String>,
    pub cwd: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub env: EnvMap,
    pub size: Option<TerminalSize>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateProcessRequest {
    pub command: Vec<String>,
    pub cwd: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub env: EnvMap,
    pub pty: Option<TerminalSize>,
    pub stdin: Option<bool>,
    // Omitted when absent, not serialized as null: this value is serialized
    // into the session's `adapter_state_json`, which the idempotency
    // fingerprint hashes. Emitting a new `"user": null` key would change the
    // fingerprint of a request that is otherwise unchanged, so a retry of a
    // command created before this field existed would conflict instead of
    // replaying.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub user: Option<String>,
    #[cfg_attr(
        feature = "serde",
        serde(default, deserialize_with = "command::deserialize_timeout")
    )]
    pub timeout: Option<SimpleDuration>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateCommandRequest {
    #[cfg_attr(feature = "schema", schemars(schema_with = "schema::process_command"))]
    pub command: Vec<String>,
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "schema::optional_guest_path")
    )]
    #[cfg_attr(feature = "serde", serde(default))]
    pub cwd: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    #[cfg_attr(feature = "schema", schemars(schema_with = "schema::environment_map"))]
    pub env: EnvMap,
    pub pty: Option<TerminalSize>,
    pub stdin: Option<bool>,
    /// Guest user to run as. Defaults to the sandbox's configured user.
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "schema::optional_guest_user")
    )]
    // Omitted when absent rather than serialized as null: this value reaches
    // the session's `adapter_state_json`, which the idempotency fingerprint
    // hashes, so a new `"user": null` key would change the fingerprint of a
    // request that has not otherwise changed.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub user: Option<String>,
    /// Positive duration using SPACE compact syntax, such as `30s`, `5m`, or `1h`.
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "schema::optional_simple_duration")
    )]
    #[cfg_attr(
        feature = "serde",
        serde(default, deserialize_with = "command::deserialize_timeout")
    )]
    pub timeout: Option<SimpleDuration>,
}

impl From<CreateCommandRequest> for CreateProcessRequest {
    fn from(request: CreateCommandRequest) -> Self {
        Self {
            command: request.command,
            cwd: request.cwd,
            env: request.env,
            pty: request.pty,
            stdin: request.stdin,
            user: request.user,
            timeout: request.timeout,
        }
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateFileWatchSessionRequest {
    pub path: String,
    #[cfg_attr(feature = "serde", serde(default))]
    pub recursive: bool,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatePortSessionRequest {
    pub port: u16,
    pub protocol: PortSessionProtocol,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateProtocolSessionRequest {
    pub protocol: ProtocolSessionProtocol,
    pub port: u16,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileStatRequest {
    pub path: String,
    pub user: Option<String>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileListRequest {
    pub path: String,
    pub depth: Option<u32>,
    pub user: Option<String>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileReadRequest {
    pub path: String,
    #[cfg_attr(feature = "serde", serde(default))]
    pub offset: u64,
    pub length: u64,
    pub user: Option<String>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileWriteRequest {
    pub path: String,
    pub data_base64: String,
    #[cfg_attr(feature = "serde", serde(default))]
    pub append: bool,
    pub user: Option<String>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileWriteBatchEntry {
    pub path: String,
    pub data_base64: String,
    #[cfg_attr(feature = "serde", serde(default))]
    pub append: bool,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileWriteBatchRequest {
    pub entries: Vec<FileWriteBatchEntry>,
    pub user: Option<String>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileMkdirRequest {
    pub path: String,
    #[cfg_attr(feature = "serde", serde(default = "default_true"))]
    pub parents: bool,
    pub user: Option<String>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileRemoveRequest {
    pub path: String,
    #[cfg_attr(feature = "serde", serde(default))]
    pub recursive: bool,
    pub user: Option<String>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileRenameRequest {
    pub old_path: String,
    pub new_path: String,
    pub user: Option<String>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileChangeTrackingRequest {
    #[cfg_attr(feature = "serde", serde(default = "default_true"))]
    pub enabled: bool,
    #[cfg_attr(feature = "serde", serde(default))]
    pub roots: Vec<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub include_globs: Vec<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub exclude_globs: Vec<String>,
    pub use_default_excludes: Option<bool>,
    pub user: Option<String>,
}

impl Default for FileChangeTrackingRequest {
    fn default() -> Self {
        Self {
            enabled: true,
            roots: Vec::new(),
            include_globs: Vec::new(),
            exclude_globs: Vec::new(),
            use_default_excludes: None,
            user: None,
        }
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileChangeTrackingResponse {
    pub monitoring_enabled: bool,
    pub observed_since: Option<String>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileModifiedRequest {
    pub window_seconds: Option<u64>,
    pub since: Option<String>,
    pub cursor: Option<String>,
    pub user: Option<String>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileModifiedEntry {
    /// Path relative to `root`.
    pub path: String,
    /// Absolute watched root, absent when an older guest omitted it.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub root: Option<String>,
    pub last_modified_at: Option<String>,
    pub kinds: Vec<FileEventKind>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileModifiedResponse {
    pub files: Vec<FileModifiedEntry>,
    pub observed_since: Option<String>,
    pub retained_since: Option<String>,
    pub cursor: Option<String>,
    pub monitoring_enabled: bool,
    pub overflowed: bool,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileInfo {
    pub path: String,
    pub name: String,
    pub file_type: FileType,
    pub size_bytes: u64,
    pub mode: u32,
    pub modified_at: Option<String>,
    pub owner_uid: u32,
    pub owner_gid: u32,
    pub symlink_target: Option<String>,
    /// Hard links to this inode. `Some(1)` for an ordinary file.
    ///
    /// Reported so a caller can tell an ordinary file from one reachable
    /// by another name. A client that treats extra links as a trust
    /// boundary -- refusing to ingest a tree containing them, say -- needs
    /// the real count and cannot assume `1`.
    ///
    /// `None` means this guest cannot report the count: a pre-`link_count`
    /// guest, or a non-Unix one. It is deliberately not a number, so that
    /// neither a `== 1` nor a `> 1` test can silently accept an unknown as
    /// an ordinary file. Such a caller must reject it.
    #[cfg_attr(feature = "serde", serde(default))]
    pub link_count: Option<u64>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileStatResponse {
    pub entry: FileInfo,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileListResponse {
    pub entries: Vec<FileInfo>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileWriteResponse {
    pub bytes_written: u64,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileWriteBatchResponse {
    pub files_written: u64,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileMutationResponse {
    pub ok: bool,
}

#[cfg(feature = "serde")]
const fn default_true() -> bool {
    true
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataPlaneAttachInfo {
    /// For commands, the WebSocket attachment endpoint. Use `events_url` for
    /// HTTP SSE. Resolve relative URLs against the configured data-plane origin
    /// and validate the destination before sending credentials.
    pub url: String,
    /// HTTP SSE endpoint for this session, when supported. Attaching a command
    /// starts execution; streams are live and do not replay missed output.
    pub events_url: Option<String>,
    /// Session-scoped credential, not a management API token. Keep it secret
    /// and send it only to the verified attachment destination.
    pub token: String,
    /// Attachment-token expiry, separate from session expiry and command timeout.
    #[cfg_attr(feature = "serde", serde(with = "time::serde::rfc3339"))]
    #[cfg_attr(feature = "schema", schemars(schema_with = "schema::date_time"))]
    pub expires_at: time::OffsetDateTime,
    /// Accepted credential transports. Query-string tokens are not supported.
    pub token_transport: Vec<DataPlaneTokenTransport>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicSession {
    pub id: DataPlaneSessionId,
    pub sandbox_id: PublicSandboxId,
    pub kind: SessionKind,
    pub state: SessionState,
    pub reconnect_count: u64,
    pub last_reconnect_reason: Option<ReconnectReason>,
    pub scopes: Vec<DataPlaneScope>,
    pub target_port: Option<u16>,
    pub target_protocol: Option<SessionTargetProtocol>,
    pub target_path: Option<String>,
    pub attach: Option<DataPlaneAttachInfo>,
    #[cfg_attr(feature = "serde", serde(with = "time::serde::rfc3339"))]
    pub created_at: time::OffsetDateTime,
    #[cfg_attr(feature = "serde", serde(with = "time::serde::rfc3339"))]
    pub updated_at: time::OffsetDateTime,
    #[cfg_attr(feature = "serde", serde(with = "time::serde::rfc3339::option"))]
    pub expires_at: Option<time::OffsetDateTime>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicCommand {
    /// Session identity for attachment and event-stream paths.
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "schema::process_session_id")
    )]
    pub session_id: DataPlaneSessionId,
    /// Opaque identity for process endpoint paths, derived from the session
    /// identity. Returned before execution starts; process lookup can return
    /// not_found before attachment and after exit. This is not a numeric PID.
    #[cfg_attr(feature = "schema", schemars(schema_with = "schema::process_id"))]
    pub process_id: ProcessId,
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "schema::public_sandbox_id")
    )]
    pub sandbox_id: PublicSandboxId,
    /// Session state, not proof of command success. Read the process_exited
    /// event and exit result from the command's stream.
    pub state: SessionState,
    /// Attachment destination and credentials when available.
    pub attach: Option<DataPlaneAttachInfo>,
    #[cfg_attr(feature = "serde", serde(with = "time::serde::rfc3339"))]
    #[cfg_attr(feature = "schema", schemars(schema_with = "schema::date_time"))]
    pub created_at: time::OffsetDateTime,
    #[cfg_attr(feature = "serde", serde(with = "time::serde::rfc3339"))]
    #[cfg_attr(feature = "schema", schemars(schema_with = "schema::date_time"))]
    pub updated_at: time::OffsetDateTime,
    #[cfg_attr(feature = "serde", serde(with = "time::serde::rfc3339::option"))]
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "schema::optional_date_time")
    )]
    /// Optional session expiry, distinct from attach.expires_at and the
    /// command's execution timeout. Null means no session expiry is recorded.
    pub expires_at: Option<time::OffsetDateTime>,
}

/// Derives a command session's guest process identity from its session id.
///
/// A process-kind data-plane session runs exactly one guest process, so the
/// session id is that process's identity. Deriving it keeps the control plane
/// able to name the process before it starts, and before
/// `data_plane_sessions.backend_process_id` is bound, without a second
/// identifier to store or reconcile.
pub fn process_id_for_session(session_id: DataPlaneSessionId) -> ProcessId {
    // A session id is a short, NUL-free, prefixed UUID, so it always satisfies
    // `ProcessId`'s bounds.
    ProcessId::parse(session_id.to_string()).expect("a data-plane session id is a valid process id")
}

impl From<PublicSession> for PublicCommand {
    fn from(session: PublicSession) -> Self {
        Self {
            session_id: session.id,
            process_id: process_id_for_session(session.id),
            sandbox_id: session.sandbox_id,
            state: session.state,
            attach: session.attach,
            created_at: session.created_at,
            updated_at: session.updated_at,
            expires_at: session.expires_at,
        }
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicErrorDetail {
    pub key: String,
    pub value: String,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicErrorEnvelope {
    /// Stable programmatic classifier. Branch on this, never on `message`.
    pub code: PublicErrorCode,
    /// Human-readable explanation. Not stable; must not be parsed.
    pub message: String,
    /// Matches the `x-request-id` response header; quote it in bug reports.
    pub request_id: Option<String>,
    /// Authoritative retry decision. When this envelope is present, retry the
    /// request only if this is `true` (honouring `Retry-After` when sent, else
    /// exponential backoff from one second with jitter, at most five attempts),
    /// and only with the same `Idempotency-Key` for mutations. The HTTP status
    /// decides only when no valid envelope was returned.
    pub retryable: bool,
    /// Bounded typed details; empty when the error has none.
    pub details: Vec<PublicErrorDetail>,
}

open_wire_enum!(DataPlaneHttpErrorCode, "data-plane HTTP error code", {
    InvalidRequest => "invalid_request",
    Unauthorized => "unauthorized",
    Forbidden => "forbidden",
    NotFound => "not_found",
    NotReady => "not_ready",
    Conflict => "conflict",
    RateLimited => "rate_limited",
    StreamUnavailable => "stream_unavailable",
    ServiceUnavailable => "service_unavailable",
    UpstreamError => "upstream_error",
    Internal => "internal",
});

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataPlaneHttpError {
    pub code: DataPlaneHttpErrorCode,
    pub message: String,
    pub retryable: bool,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataPlaneHttpErrorEnvelope {
    pub error: DataPlaneHttpError,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schema", schemars(schema_with = "schema::page_cursor"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PageCursor(String);

impl PageCursor {
    pub fn parse(value: impl Into<String>) -> Result<Self, PublicApiParseError> {
        let value = value.into();
        if value.is_empty() {
            return Err(PublicApiParseError("page cursor must not be empty"));
        }
        if value.len() > MAX_PAGE_CURSOR_BYTES {
            return Err(PublicApiParseError("page cursor is too long"));
        }
        if value.contains('\0') {
            return Err(PublicApiParseError("page cursor must not contain NUL"));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PageCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for PageCursor {
    type Err = PublicApiParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for PageCursor {
    type Error = PublicApiParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<PageCursor> for String {
    fn from(value: PageCursor) -> Self {
        value.0
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schema", schemars(rename = "{T}Page"))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicPage<T> {
    pub data: Vec<T>,
    /// Opaque continuation token; pass it unchanged to the same collection's
    /// cursor parameter until null. Cursors for different resources are not
    /// interchangeable.
    pub next_cursor: Option<PageCursor>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RequestFingerprint(String);

impl RequestFingerprint {
    pub fn parse(value: impl Into<String>) -> Result<Self, PublicApiParseError> {
        let value = value.into();
        if value.len() != REQUEST_FINGERPRINT_HEX_LEN {
            return Err(PublicApiParseError(
                "request fingerprint must be a SHA-256 hex digest",
            ));
        }
        if !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(PublicApiParseError(
                "request fingerprint must contain only hex digits",
            ));
        }
        Ok(Self(value.to_ascii_lowercase()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RequestFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for RequestFingerprint {
    type Err = PublicApiParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for RequestFingerprint {
    type Error = PublicApiParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<RequestFingerprint> for String {
    fn from(value: RequestFingerprint) -> Self {
        value.0
    }
}

pub mod credential_references;
#[cfg(test)]
mod tests;
