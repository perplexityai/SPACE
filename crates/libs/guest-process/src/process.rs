//! Process execution boundary types.
//!
//! These wrappers make process identifiers, commands, users, signals, and
//! idempotency keys explicit in sandbox process APIs.

use std::fmt;
use std::num::NonZeroI32;
use std::str::FromStr;

use uuid::Uuid;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = 64 * 1024;
const MAX_UNIX_SIGNAL_NUMBER: i32 = 64;
pub const MAX_PROCESS_ID_BYTES: usize = 128;
/// Bound on a guest user name, shared by the runtime parser and the
/// published schema so the two cannot disagree about what is accepted.
pub const MAX_GUEST_USER_NAME_BYTES: usize = 64;
pub const MAX_PROCESS_COMMAND_ARGS: usize = 4_096;
pub const MAX_PROCESS_COMMAND_BYTES: usize = 256 * 1024;

/// Default Linux user provisioned in SPACE guest templates.
pub const DEFAULT_GUEST_USER: &str = "user";

/// Error returned when a process-related value fails validation.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_guest_process::*;
/// assert!(ProcessCommand::new(Vec::new()).is_err());
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("{0}")]
pub struct ProcessParseError(&'static str);

/// Identifier for a process tracked by SPACE.
///
/// Generated IDs are UUIDv7 strings, but parsing also accepts bounded external
/// IDs for compatibility with process APIs.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_guest_process::*;
/// let id = ProcessId::new();
/// assert!(!id.as_str().is_empty());
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ProcessId(String);

impl ProcessId {
    /// Creates a fresh process ID.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// let id = ProcessId::new();
    /// assert!(!id.as_str().is_empty());
    /// ```
    pub fn new() -> Self {
        Self(Uuid::now_v7().to_string())
    }

    /// Parses a bounded, non-empty, NUL-free process ID.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// let id = ProcessId::parse("proc-1").unwrap();
    /// assert_eq!(id.as_str(), "proc-1");
    /// ```
    pub fn parse(value: impl Into<String>) -> Result<Self, ProcessParseError> {
        let value = value.into();
        bounded_non_empty(&value, MAX_PROCESS_ID_BYTES, "process id is too long")?;
        Ok(Self(value))
    }

    /// Returns the process ID string.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// let id = ProcessId::parse("proc-1").unwrap();
    /// assert_eq!(id.as_str(), "proc-1");
    /// ```
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for ProcessId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ProcessId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for ProcessId {
    type Err = ProcessParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for ProcessId {
    type Error = ProcessParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<ProcessId> for String {
    fn from(value: ProcessId) -> Self {
        value.0
    }
}

/// Bounded command argv for a guest process.
///
/// `Debug` is redacted because commands can contain paths, inline scripts, or
/// secrets. Use `argv` only at the execution boundary.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_guest_process::*;
/// let command = ProcessCommand::new(vec!["bash".into(), "-lc".into(), "echo ok".into()]).unwrap();
/// assert_eq!(command.argv()[0], "bash");
/// ```
#[derive(Clone, Eq, PartialEq)]
pub struct ProcessCommand(Vec<String>);

impl ProcessCommand {
    /// Builds a command from a non-empty, NUL-free argv vector within the
    /// retained process-metadata limits.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// assert!(ProcessCommand::new(vec!["true".into()]).is_ok());
    /// assert!(ProcessCommand::new(Vec::new()).is_err());
    /// ```
    pub fn new(argv: Vec<String>) -> Result<Self, ProcessParseError> {
        if argv.is_empty() {
            return Err(ProcessParseError("process command argv must not be empty"));
        }
        if argv.len() > MAX_PROCESS_COMMAND_ARGS {
            return Err(ProcessParseError("process command has too many arguments"));
        }
        let command_bytes = argv.iter().fold(0_usize, |total, arg| {
            total.saturating_add(arg.len().saturating_add(1))
        });
        if command_bytes > MAX_PROCESS_COMMAND_BYTES {
            return Err(ProcessParseError("process command is too large"));
        }
        if argv.iter().any(|arg| arg.contains('\0')) {
            return Err(ProcessParseError(
                "process command argv must not contain NUL",
            ));
        }
        Ok(Self(argv))
    }

    /// Returns the validated argv slice.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// let command = ProcessCommand::new(vec!["true".into()]).unwrap();
    /// assert_eq!(command.argv(), &["true".to_string()]);
    /// ```
    pub fn argv(&self) -> &[String] {
        &self.0
    }
}

impl fmt::Debug for ProcessCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ProcessCommand(<redacted>)")
    }
}

/// User name under which a guest operation should run.
///
/// This identifies a guest or runtime user without accepting an unbounded
/// string at operation boundaries.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_guest_process::*;
/// let user = GuestUserName::parse("sandbox").unwrap();
/// assert_eq!(user.as_str(), "sandbox");
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GuestUserName(String);

impl GuestUserName {
    /// Parses a bounded, non-empty, NUL-free guest user name.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// assert!(GuestUserName::parse("sandbox").is_ok());
    /// assert!(GuestUserName::parse("").is_err());
    /// ```
    pub fn parse(value: impl Into<String>) -> Result<Self, ProcessParseError> {
        let value = value.into();
        bounded_non_empty(
            &value,
            MAX_GUEST_USER_NAME_BYTES,
            "process user name is too long",
        )?;
        Ok(Self(value))
    }

    /// Returns the validated guest user name.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// let user = GuestUserName::parse("sandbox").unwrap();
    /// assert_eq!(user.as_str(), "sandbox");
    /// ```
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for GuestUserName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for GuestUserName {
    type Err = ProcessParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for GuestUserName {
    type Error = ProcessParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<GuestUserName> for String {
    fn from(value: GuestUserName) -> Self {
        value.0
    }
}

/// Unix signal number sent to a process.
///
/// Use this instead of raw integers when a process API expects a Unix-style
/// signal value. The catalog accepts the Linux signal number range `1..=64`;
/// runtime-specific adapters can still reject unsupported signals.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_guest_process::*;
/// let signal = ProcessSignal::try_from_i32(15).unwrap();
/// assert_eq!(signal.as_i32(), 15);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ProcessSignal(NonZeroI32);

impl ProcessSignal {
    /// Builds a Unix signal number from an already non-zero integer.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// # use std::num::NonZeroI32;
    /// let signal = ProcessSignal::new(NonZeroI32::new(9).unwrap())?;
    /// assert_eq!(signal.as_i32(), 9);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn new(value: NonZeroI32) -> Result<Self, ProcessParseError> {
        if (1..=MAX_UNIX_SIGNAL_NUMBER).contains(&value.get()) {
            Ok(Self(value))
        } else {
            Err(ProcessParseError(
                "signal must be in the Unix signal range 1..=64",
            ))
        }
    }

    /// Converts an `i32` into a Unix signal number.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// let signal = ProcessSignal::try_from_i32(15).unwrap();
    /// assert_eq!(signal.as_i32(), 15);
    /// assert!(ProcessSignal::try_from_i32(0).is_err());
    /// assert!(ProcessSignal::try_from_i32(-1).is_err());
    /// assert!(ProcessSignal::try_from_i32(65).is_err());
    /// ```
    pub fn try_from_i32(value: i32) -> Result<Self, ProcessParseError> {
        let value = NonZeroI32::new(value).ok_or(ProcessParseError("signal must be non-zero"))?;
        Self::new(value)
    }

    /// Returns the Unix signal used for forceful process termination.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// assert_eq!(ProcessSignal::sigkill().as_i32(), 9);
    /// ```
    pub const fn sigkill() -> Self {
        Self(NonZeroI32::new(9).expect("SIGKILL is non-zero"))
    }

    /// Returns the non-zero signal number.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// # use std::num::NonZeroI32;
    /// let signal = ProcessSignal::try_from_i32(15).unwrap();
    /// assert_eq!(signal.get(), NonZeroI32::new(15).unwrap());
    /// ```
    pub const fn get(self) -> NonZeroI32 {
        self.0
    }

    /// Returns the signal number as a plain integer.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// let signal = ProcessSignal::try_from_i32(15).unwrap();
    /// assert_eq!(signal.as_i32(), 15);
    /// ```
    pub const fn as_i32(self) -> i32 {
        self.0.get()
    }
}

/// Client-provided idempotency key for mutating process APIs.
///
/// Use this to deduplicate retries without treating the key as a process ID or
/// credential. Endpoint-specific code owns compatibility caps; use
/// [`IdempotencyKey::ensure_len_at_most`] at that boundary. The parser still
/// enforces a generous outer cap so accidental unbounded persistence or logging
/// cannot accept arbitrarily large request values.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_guest_process::*;
/// let key = IdempotencyKey::parse("create-process-123").unwrap();
/// assert_eq!(key.as_str(), "create-process-123");
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    /// Parses a non-empty, NUL-free idempotency key within the outer safety cap.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// assert!(IdempotencyKey::parse("request-1").is_ok());
    /// assert!(IdempotencyKey::parse("").is_err());
    /// ```
    pub fn parse(value: impl Into<String>) -> Result<Self, ProcessParseError> {
        let value = value.into();
        non_empty_no_nul(&value)?;
        if value.len() > MAX_IDEMPOTENCY_KEY_BYTES {
            return Err(ProcessParseError("idempotency key is too long"));
        }
        Ok(Self(value))
    }

    /// Returns this key only if it is within an endpoint-owned length cap.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// let key = IdempotencyKey::parse("request-1").unwrap();
    /// assert!(key.clone().ensure_len_at_most(9).is_ok());
    /// assert!(key.ensure_len_at_most(8).is_err());
    /// ```
    pub fn ensure_len_at_most(self, max_len: usize) -> Result<Self, ProcessParseError> {
        if self.0.len() > max_len {
            Err(ProcessParseError("idempotency key is too long"))
        } else {
            Ok(self)
        }
    }

    /// Returns the validated idempotency key string.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// let key = IdempotencyKey::parse("request-1").unwrap();
    /// assert_eq!(key.as_str(), "request-1");
    /// ```
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for IdempotencyKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for IdempotencyKey {
    type Err = ProcessParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for IdempotencyKey {
    type Error = ProcessParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<IdempotencyKey> for String {
    fn from(value: IdempotencyKey) -> Self {
        value.0
    }
}

fn non_empty_no_nul(value: &str) -> Result<(), ProcessParseError> {
    if value.is_empty() {
        return Err(ProcessParseError("value must not be empty"));
    }
    if value.contains('\0') {
        return Err(ProcessParseError("value must not contain NUL"));
    }
    Ok(())
}

fn bounded_non_empty(
    value: &str,
    max_len: usize,
    too_long: &'static str,
) -> Result<(), ProcessParseError> {
    non_empty_no_nul(value)?;
    if value.len() > max_len {
        return Err(ProcessParseError(too_long));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
