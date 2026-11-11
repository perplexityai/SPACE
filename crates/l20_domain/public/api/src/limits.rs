//! Bounds on native sandbox create and update input.
//!
//! The handlers call [`validate_create_request`] and
//! [`validate_update_request`] before admitting an operation, and the
//! published `CreateSandboxRequest` schema reads the same constants, so the
//! two cannot disagree about what is accepted.

use std::collections::BTreeMap;
use std::fmt;

use pplx_space_common_core::sandbox_storage::MAX_SANDBOX_ROOTFS_BYTES;
use pplx_space_common_core::types::{
    MAX_ENV_ENTRIES, MAX_ENV_KEY_BYTES, MAX_ENV_TOTAL_BYTES, MAX_ENV_VALUE_BYTES,
};

use crate::{
    CreateSandboxRequest, PublicSandboxMetadata, PublicSandboxMetadataValue, SandboxResources,
    UpdateSandboxRequest,
};

/// Maximum UTF-8 byte length of a sandbox name.
pub const MAX_SANDBOX_NAME_BYTES: usize = 256;
/// Maximum number of metadata entries on one sandbox.
pub const MAX_METADATA_ENTRIES: usize = 64;
/// Maximum UTF-8 byte length of one metadata key.
pub const MAX_METADATA_KEY_BYTES: usize = 128;
/// Maximum UTF-8 byte length of one string metadata value.
pub const MAX_METADATA_VALUE_BYTES: usize = 1024;
/// Maximum combined UTF-8 byte length of all metadata keys and string values.
pub const MAX_METADATA_TOTAL_BYTES: usize = 16 * 1024;

/// Smallest vCPU count a sandbox can request.
pub const MIN_SANDBOX_VCPU: u32 = 1;
/// Largest vCPU count a sandbox can request. Mirrors Firecracker's vCPU
/// limit, which nothing else in SPACE checks before the VM is configured.
pub const MAX_SANDBOX_VCPU: u32 = 32;
/// Smallest guest memory a sandbox can request: the smallest memory a template
/// base snapshot is taken with.
pub const MIN_SANDBOX_MEMORY_MIB: u32 = pplx_space_common_core::types::MIN_BASE_SNAPSHOT_MEMORY_MIB;
/// Largest guest memory a sandbox can request: the node runtime's default
/// guest maximum, `pplx_space_node_runtime_core::cgroup::
/// DEFAULT_SANDBOX_GUEST_MEMORY_MAX_MIB`, which deployments run. That module
/// is host-only, so the value is restated here. A node would otherwise clamp a larger request silently. Raising a
/// node's guest maximum requires raising this too.
pub const MAX_SANDBOX_MEMORY_MIB: u32 = 8192;
/// Smallest writable disk a sandbox can request. The template's own rootfs is
/// the real floor, which the node enforces when it prepares the sandbox.
pub const MIN_SANDBOX_DISK_MIB: u32 = 1;
/// Largest writable disk a sandbox can request.
pub const MAX_SANDBOX_DISK_MIB: u32 = (MAX_SANDBOX_ROOTFS_BYTES / (1024 * 1024)) as u32;

/// Maximum byte length of a native `Idempotency-Key` header, which the API
/// checks with `IdempotencyKey::parse_header`.
pub const MAX_IDEMPOTENCY_KEY_BYTES: usize =
    pplx_space_common_core::types::MAX_IDEMPOTENCY_KEY_HEADER_BYTES;

/// Why an `Idempotency-Key` header value was rejected.
pub use pplx_space_common_core::types::IdempotencyKeyHeaderError as IdempotencyKeyError;

/// Checks a native `Idempotency-Key` header value with
/// `IdempotencyKey::parse_header`: 1 to [`MAX_IDEMPOTENCY_KEY_BYTES`] bytes of
/// printable ASCII, space or tab.
#[deprecated(note = "use `pplx_space_common_core::types::IdempotencyKey::parse_header`")]
pub fn validate_idempotency_key(value: &[u8]) -> Result<&str, IdempotencyKeyError> {
    pplx_space_common_core::types::IdempotencyKey::parse_header(value)?;
    std::str::from_utf8(value).map_err(|_| IdempotencyKeyError::InvalidEncoding)
}

/// A request field these limits cover, named as in the request body.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestField {
    Name,
    Env,
    Metadata,
    Vcpu,
    MemoryMib,
    DiskMib,
}

impl RequestField {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Name => "name",
            Self::Env => "env",
            Self::Metadata => "metadata",
            Self::Vcpu => "resources.vcpu",
            Self::MemoryMib => "resources.memory_mib",
            Self::DiskMib => "resources.disk_mib",
        }
    }
}

impl fmt::Display for RequestField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Why a create or update request was rejected. Messages never echo the
/// rejected value, which may be large.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RequestLimitError {
    #[error("{field} must not contain NUL")]
    ContainsNul { field: RequestField },
    #[error("{field} exceeds {max_bytes} bytes")]
    TooLong {
        field: RequestField,
        max_bytes: usize,
    },
    #[error("{field} has more than {max_entries} entries")]
    TooManyEntries {
        field: RequestField,
        max_entries: usize,
    },
    #[error("a {field} key exceeds {max_bytes} bytes")]
    KeyTooLong {
        field: RequestField,
        max_bytes: usize,
    },
    #[error("a {field} value exceeds {max_bytes} bytes")]
    ValueTooLong {
        field: RequestField,
        max_bytes: usize,
    },
    #[error("{field} keys and values exceed {max_bytes} bytes in total")]
    TotalTooLarge {
        field: RequestField,
        max_bytes: usize,
    },
    #[error("{field} must be between {min} and {max}")]
    OutOfRange {
        field: RequestField,
        min: u32,
        max: u32,
    },
}

impl RequestLimitError {
    pub const fn field(&self) -> RequestField {
        match *self {
            Self::ContainsNul { field }
            | Self::TooLong { field, .. }
            | Self::TooManyEntries { field, .. }
            | Self::KeyTooLong { field, .. }
            | Self::ValueTooLong { field, .. }
            | Self::TotalTooLarge { field, .. }
            | Self::OutOfRange { field, .. } => field,
        }
    }
}

/// Checks a native create request against the limits in this module.
pub fn validate_create_request(request: &CreateSandboxRequest) -> Result<(), RequestLimitError> {
    if let Some(name) = &request.name {
        validate_text(RequestField::Name, name, MAX_SANDBOX_NAME_BYTES)?;
    }
    validate_env(&request.env)?;
    validate_metadata(&request.metadata)?;
    if let Some(resources) = &request.resources {
        validate_resources(resources)?;
    }
    Ok(())
}

/// Checks a native update request against the limits in this module.
pub fn validate_update_request(request: &UpdateSandboxRequest) -> Result<(), RequestLimitError> {
    if let Some(metadata) = &request.metadata {
        validate_metadata(metadata)?;
    }
    if let Some(resources) = &request.resources {
        validate_resources(resources)?;
    }
    Ok(())
}

fn validate_resources(resources: &SandboxResources) -> Result<(), RequestLimitError> {
    for (field, value, min, max) in [
        (
            RequestField::Vcpu,
            resources.vcpu,
            MIN_SANDBOX_VCPU,
            MAX_SANDBOX_VCPU,
        ),
        (
            RequestField::MemoryMib,
            resources.memory_mib,
            MIN_SANDBOX_MEMORY_MIB,
            MAX_SANDBOX_MEMORY_MIB,
        ),
        (
            RequestField::DiskMib,
            resources.disk_mib,
            MIN_SANDBOX_DISK_MIB,
            MAX_SANDBOX_DISK_MIB,
        ),
    ] {
        if value.is_some_and(|value| !(min..=max).contains(&value)) {
            return Err(RequestLimitError::OutOfRange { field, min, max });
        }
    }
    Ok(())
}

fn validate_env(env: &BTreeMap<String, String>) -> Result<(), RequestLimitError> {
    validate_map(
        RequestField::Env,
        env.iter().map(|(key, value)| (key, Some(value.as_str()))),
        MapLimits {
            entries: MAX_ENV_ENTRIES,
            key_bytes: MAX_ENV_KEY_BYTES,
            value_bytes: MAX_ENV_VALUE_BYTES,
            total_bytes: MAX_ENV_TOTAL_BYTES,
        },
    )
}

fn validate_metadata(metadata: &PublicSandboxMetadata) -> Result<(), RequestLimitError> {
    validate_map(
        RequestField::Metadata,
        metadata.iter().map(|(key, value)| {
            let value = match value {
                PublicSandboxMetadataValue::String(value) => Some(value.as_str()),
                PublicSandboxMetadataValue::Number(_) | PublicSandboxMetadataValue::Bool(_) => None,
            };
            (key, value)
        }),
        MapLimits {
            entries: MAX_METADATA_ENTRIES,
            key_bytes: MAX_METADATA_KEY_BYTES,
            value_bytes: MAX_METADATA_VALUE_BYTES,
            total_bytes: MAX_METADATA_TOTAL_BYTES,
        },
    )
}

struct MapLimits {
    entries: usize,
    key_bytes: usize,
    value_bytes: usize,
    total_bytes: usize,
}

/// Checks a string-keyed map whose values are strings or, for metadata,
/// numbers and booleans (`None`), which only count towards the entry limit.
fn validate_map<'a>(
    field: RequestField,
    entries: impl ExactSizeIterator<Item = (&'a String, Option<&'a str>)>,
    limits: MapLimits,
) -> Result<(), RequestLimitError> {
    if entries.len() > limits.entries {
        return Err(RequestLimitError::TooManyEntries {
            field,
            max_entries: limits.entries,
        });
    }
    let mut total_bytes = 0_usize;
    for (key, value) in entries {
        if key.len() > limits.key_bytes {
            return Err(RequestLimitError::KeyTooLong {
                field,
                max_bytes: limits.key_bytes,
            });
        }
        let value = value.unwrap_or_default();
        if value.len() > limits.value_bytes {
            return Err(RequestLimitError::ValueTooLong {
                field,
                max_bytes: limits.value_bytes,
            });
        }
        if key.contains('\0') || value.contains('\0') {
            return Err(RequestLimitError::ContainsNul { field });
        }
        total_bytes += key.len() + value.len();
    }
    if total_bytes > limits.total_bytes {
        return Err(RequestLimitError::TotalTooLarge {
            field,
            max_bytes: limits.total_bytes,
        });
    }
    Ok(())
}

fn validate_text(
    field: RequestField,
    value: &str,
    max_bytes: usize,
) -> Result<(), RequestLimitError> {
    if value.len() > max_bytes {
        return Err(RequestLimitError::TooLong { field, max_bytes });
    }
    if value.contains('\0') {
        return Err(RequestLimitError::ContainsNul { field });
    }
    Ok(())
}

#[cfg(test)]
mod tests;
