//! Sandbox identity, selection, metadata and lifecycle types, the legacy
//! sandbox ID string helpers, the rootfs entitlement and the abandoned-restore
//! retirement switch.

mod restore_retirement;
mod sandbox;
pub mod sandbox_storage;

pub use restore_retirement::*;
pub use sandbox::*;

#[cfg(feature = "sqlx")]
pplx_space_job_id::postgres_uuid!(
    SandboxActivationAttemptId,
    SandboxCleanupEpisodeId,
    SandboxId
);

/// Legacy sandbox ID string helpers.
///
/// New code should prefer [`crate::SandboxId`] and [`crate::SandboxSelector`].
pub mod ids {
    /// Number of lowercase hex characters in a legacy sandbox display label.
    pub const SANDBOX_LABEL_LEN: usize = 8;

    /// Error returned when a legacy sandbox ID helper receives non-hex input.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_sandbox_model::ids::sandbox_label;
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
    /// # use pplx_space_sandbox_model::ids::sandbox_label;
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
    /// # use pplx_space_sandbox_model::ids::sandbox_label_or_id;
    /// assert_eq!(sandbox_label_or_id("bad id"), "bad id");
    /// ```
    pub fn sandbox_label_or_id(sandbox_id: &str) -> String {
        let display_id = sandbox_id.trim_prefix("sbx_");
        sandbox_label(display_id).unwrap_or_else(|_| sandbox_id.to_string())
    }

    /// Checks whether a legacy selector matches a sandbox ID string.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_sandbox_model::ids::sandbox_id_matches;
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
