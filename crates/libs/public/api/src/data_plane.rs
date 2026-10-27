//! Data-plane session identities and stream frames.

use super::*;

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
        /// As on [`DataPlaneHttpError::details`]; omitted when there are none.
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Vec::is_empty")
        )]
        details: Vec<PublicErrorDetail>,
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
