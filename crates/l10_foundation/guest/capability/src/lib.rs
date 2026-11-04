//! Platform-neutral guest capability refusal, shared by native guests and relays.
use tonic::{
    Code, Status,
    metadata::{MetadataMap, MetadataValue},
};

const GUEST_CAPABILITY_ERROR_METADATA: &str = "space-guest-capability-error";
const GUEST_CAPABILITY_UNSUPPORTED: &str = "guest_capability_unsupported";
const GUEST_CAPABILITY_METADATA: &str = "space-guest-capability";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GuestCapability {
    PlatformOperations,
    InteractiveProcessControl,
}

impl GuestCapability {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PlatformOperations => "platform_operations",
            Self::InteractiveProcessControl => "interactive_process_control",
        }
    }
}

impl std::fmt::Display for GuestCapability {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for GuestCapability {
    type Err = GuestCapabilityParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(match value {
            "platform_operations" => Self::PlatformOperations,
            "interactive_process_control" => Self::InteractiveProcessControl,
            _ => {
                return Err(GuestCapabilityParseError {
                    value: value.to_owned(),
                });
            }
        })
    }
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
#[error("unsupported guest capability {value:?}")]
pub struct GuestCapabilityParseError {
    value: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GuestCapabilityUnsupported {
    pub capability: GuestCapability,
}

impl GuestCapabilityUnsupported {
    pub fn from_metadata(metadata: &MetadataMap) -> Option<Self> {
        guest_capability_unsupported_from_metadata(metadata)
    }

    pub fn from_status(status: &Status) -> Option<Self> {
        if status.code() != Code::FailedPrecondition {
            return None;
        }
        Self::from_metadata(status.metadata())
    }
}

pub fn guest_capability_unsupported_status(
    capability: GuestCapability,
    message: impl Into<String>,
) -> Status {
    let mut status = Status::failed_precondition(message.into());
    status.metadata_mut().insert(
        GUEST_CAPABILITY_ERROR_METADATA,
        MetadataValue::from_static(GUEST_CAPABILITY_UNSUPPORTED),
    );
    status.metadata_mut().insert(
        GUEST_CAPABILITY_METADATA,
        MetadataValue::from_static(capability.as_str()),
    );
    status
}

fn guest_capability_unsupported_from_metadata(
    metadata: &MetadataMap,
) -> Option<GuestCapabilityUnsupported> {
    let marker = metadata
        .get(GUEST_CAPABILITY_ERROR_METADATA)
        .and_then(|value| value.to_str().ok())?;
    if marker != GUEST_CAPABILITY_UNSUPPORTED {
        return None;
    }
    let capability = metadata
        .get(GUEST_CAPABILITY_METADATA)
        .and_then(|value| value.to_str().ok())?
        .parse()
        .ok()?;
    Some(GuestCapabilityUnsupported { capability })
}
