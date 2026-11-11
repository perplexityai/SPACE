use std::{fmt, time::Duration};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeOperationBudget {
    RuntimeMetadata,
    FilesystemMetadata,
    FilesystemMutation,
    FilesystemTransferOpen,
    ProcessMetadata,
    ProcessControl,
    StreamOpen,
}

impl RuntimeOperationBudget {
    pub const fn timeout(self) -> Duration {
        match self {
            Self::RuntimeMetadata => Duration::from_secs(10),
            Self::FilesystemMetadata => Duration::from_secs(10),
            Self::FilesystemMutation => Duration::from_secs(30),
            Self::FilesystemTransferOpen => Duration::from_secs(120),
            Self::ProcessMetadata => Duration::from_secs(10),
            Self::ProcessControl => Duration::from_secs(10),
            Self::StreamOpen => Duration::from_secs(8),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RuntimeMetadata => "runtime_metadata",
            Self::FilesystemMetadata => "filesystem_metadata",
            Self::FilesystemMutation => "filesystem_mutation",
            Self::FilesystemTransferOpen => "filesystem_transfer_open",
            Self::ProcessMetadata => "process_metadata",
            Self::ProcessControl => "process_control",
            Self::StreamOpen => "stream_open",
        }
    }
}

impl fmt::Display for RuntimeOperationBudget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
