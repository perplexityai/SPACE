//! Dependency-light shared primitives for SPACE services.
//!
//! The crate contains dependency-light shared helpers:
//!
//! - [`types`] is the domain type catalog. Prefer it so raw strings, UUIDs,
//!   sizes, paths, and IDs are parsed once at the boundary.
//! - [`host`] and [`guest`] describe platform-specific SPACE runtime contracts.
//! - [`ids`] contains legacy sandbox ID string helpers kept for compatibility.
//!
//! # Examples
//!
//! ```rust
//! # use pplx_space_common_core::types::*;
//! let id = SandboxId::new();
//! let selector = SandboxSelector::parse(id.label().to_string())?;
//!
//! assert!(selector.matches_id(id));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
pub mod sandbox_storage;
pub mod types;

#[cfg(feature = "archive")]
pub mod archive;

pub mod env;
pub mod guest;
pub mod host;
pub use pplx_space_rpc_contracts::{
    artifact_backup_signals, inventory, node_drain_signals, operation_budget,
    port_exposure_signals, restore_ownership, wake_signals,
};
pub use pplx_space_sql_locator::sql;

/// Legacy sandbox ID string helpers.
///
/// New code should prefer [`types::SandboxId`] and [`types::SandboxSelector`].
pub mod ids {
    /// Number of lowercase hex characters in a legacy sandbox display label.
    pub const SANDBOX_LABEL_LEN: usize = 8;

    /// Error returned when a legacy sandbox ID helper receives non-hex input.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::ids::sandbox_label;
    /// assert!(sandbox_label("not a uuid").is_err());
    /// ```
    #[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
    #[error("sandbox id must contain only hex digits and dashes")]
    pub struct InvalidSandboxId;

    fn compact_hex(value: &str) -> Option<String> {
        let mut compact = String::new();
        for c in value.chars() {
            if c == '-' {
                continue;
            }
            if !c.is_ascii_hexdigit() {
                return None;
            }
            compact.push(c.to_ascii_lowercase());
        }
        Some(compact)
    }

    /// Returns the display label for a sandbox ID string.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::ids::sandbox_label;
    /// assert_eq!(
    ///     sandbox_label("019d84bd-280d-7331-99c6-b63377f3a3a2").as_deref(),
    ///     Ok("77f3a3a2")
    /// );
    /// ```
    pub fn sandbox_label(sandbox_id: &str) -> Result<String, InvalidSandboxId> {
        let compact = compact_hex(sandbox_id).ok_or(InvalidSandboxId)?;
        if compact.len() <= SANDBOX_LABEL_LEN {
            Ok(compact)
        } else {
            let reversed_label: String = compact.chars().rev().take(SANDBOX_LABEL_LEN).collect();
            Ok(reversed_label.chars().rev().collect())
        }
    }

    /// Returns a sandbox display label, falling back to the original input.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::ids::sandbox_label_or_id;
    /// assert_eq!(sandbox_label_or_id("bad id"), "bad id");
    /// ```
    pub fn sandbox_label_or_id(sandbox_id: &str) -> String {
        let display_id = sandbox_id.strip_prefix("sbx_").unwrap_or(sandbox_id);
        sandbox_label(display_id).unwrap_or_else(|_| sandbox_id.to_string())
    }

    /// Checks whether a legacy selector matches a sandbox ID string.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::ids::sandbox_id_matches;
    /// let id = "019d84bd-280d-7331-99c6-b63377f3a3a2";
    /// assert!(sandbox_id_matches(id, "77f3"));
    /// assert!(sandbox_id_matches(id, "019d84bd-280d"));
    /// ```
    pub fn sandbox_id_matches(sandbox_id: &str, selector: &str) -> bool {
        if sandbox_id == selector {
            return true;
        }

        let Some(selector) = compact_hex(selector) else {
            return false;
        };
        if selector.is_empty() {
            return false;
        }

        let Some(compact_id) = compact_hex(sandbox_id) else {
            return false;
        };
        if selector.len() <= SANDBOX_LABEL_LEN {
            sandbox_label(sandbox_id).is_ok_and(|label| label.starts_with(&selector))
        } else {
            compact_id.starts_with(&selector)
        }
    }

    #[cfg(test)]
    mod tests;
}

/// Constants shared with spaced host and guest components.
pub mod spaced {
    /// Maximum entries sent in one write batch.
    pub use pplx_space_guest_process::MAX_WRITE_BATCH_ENTRIES;
    /// Largest gRPC message the sandbox manager and the guest filesystem
    /// decode; the node API's worker server enforces the same 4 MiB through
    /// tonic's default. A write batch travels as one message, so it must fit;
    /// streamed writes are split into smaller messages.
    pub const MAX_MESSAGE_BYTES: usize = 4 * 1024 * 1024;

    pub use pplx_space_guest_process::status::*;
}

#[cfg(feature = "kubernetes")]
#[path = "k8s.rs"]
pub mod kubernetes;

#[cfg(test)]
mod runtime_contract_tests;
