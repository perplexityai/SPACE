//! Wrappers for values that are safe to carry in memory but unsafe to log.
//!
//! These types make privacy policy visible in signatures. A caller can still
//! explicitly expose the value at a real boundary, but `Debug` output redacts
//! the wrapped data by default.
//!
//! It is its own crate so that code can mark values as PII without depending
//! on `pplx-space-common-core`, which re-exports it from `types::privacy`.

use std::fmt;

/// Marks a value as personally identifiable information.
///
/// Use `Pii<T>` when the raw value may identify a customer, user, org, or
/// workload and should not be emitted through ordinary debug output.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_pii::*;
/// let email = Pii::new("user@example.com");
/// let debug = format!("{email:?}");
/// assert!(debug.starts_with("Pii("));
/// assert!(debug.contains("<redacted>"));
/// assert!(!debug.contains("user@example.com"));
/// ```
#[derive(Clone)]
pub struct Pii<T>(T);

impl<T> Pii<T> {
    /// Wraps a value as PII.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_pii::*;
    /// let value = Pii::new("tenant-a");
    /// assert_eq!(value.expose_pii(), &"tenant-a");
    /// ```
    pub const fn new(value: T) -> Self {
        Self(value)
    }

    /// Borrows the inner value at an explicit PII boundary.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_pii::*;
    /// let value = Pii::new("user@example.com");
    /// assert_eq!(value.expose_pii(), &"user@example.com");
    /// ```
    pub const fn expose_pii(&self) -> &T {
        &self.0
    }

    /// Consumes the wrapper and returns the inner value.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_pii::*;
    /// let value = Pii::new(String::from("tenant-a"));
    /// assert_eq!(value.into_inner(), "tenant-a");
    /// ```
    pub fn into_inner(self) -> T {
        self.0
    }

    /// Transforms the inner value while preserving the PII marker.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_pii::*;
    /// let value = Pii::new("user@example.com").map(str::to_uppercase);
    /// assert_eq!(value.expose_pii(), "USER@EXAMPLE.COM");
    /// ```
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Pii<U> {
        Pii(f(self.0))
    }
}

impl<T> fmt::Debug for Pii<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Pii(<redacted>)")
    }
}

#[cfg(test)]
mod tests;
