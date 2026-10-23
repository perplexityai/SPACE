//! Duration newtypes for sandbox lifecycle timing.
//!
//! These wrappers keep different duration roles from being mixed accidentally:
//! readiness timeouts, retry delays, guest uptime, and snapshot intervals are
//! all positive `std::time::Duration` values with different meanings.
//!
//! # Examples
//!
//! ```rust
//! # use std::time::Duration;
//! # use pplx_space_guest_process::*;
//! let ready = ReadyTimeout::from_secs(30)?;
//! let retry = RetryDelay::new(Duration::from_millis(500))?;
//!
//! assert_eq!(ready.as_duration(), Duration::from_secs(30));
//! assert_eq!(retry.as_duration(), Duration::from_millis(500));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::time::Duration;

pub use pplx_space_durations::{
    DurationParseError, PositiveDuration, SimpleDuration, SimpleDurationParseError,
    format_simple_duration, parse_grpc_timeout, parse_simple_duration, parse_simple_duration_secs,
};

macro_rules! duration_type {
    (
        $(#[$type_meta:meta])*
        $name:ident,
        $example_name:ident,
        $example_secs:literal,
        $description:literal
    ) => {
        $(#[$type_meta])*
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct $name(PositiveDuration);

        impl $name {
            #[doc = concat!(
                "Creates ",
                $description,
                " from a positive `Duration`.\n\n",
                "# Examples\n\n",
                "```rust\n",
                "# use std::time::Duration;\n",
                "# use pplx_space_guest_process::*;\n",
                "let ",
                stringify!($example_name),
                " = ",
                stringify!($name),
                "::new(Duration::from_secs(",
                stringify!($example_secs),
                "))?;\n",
                "assert_eq!(",
                stringify!($example_name),
                ".as_duration(), Duration::from_secs(",
                stringify!($example_secs),
                "));\n",
                "# Ok::<(), Box<dyn std::error::Error>>(())\n",
                "```\n",
            )]
            pub fn new(value: Duration) -> Result<Self, DurationParseError> {
                PositiveDuration::new(value).map(Self).ok_or(DurationParseError)
            }

            #[doc = concat!(
                "Creates ",
                $description,
                " from seconds.\n\n",
                "# Examples\n\n",
                "```rust\n",
                "# use std::time::Duration;\n",
                "# use pplx_space_guest_process::*;\n",
                "let ",
                stringify!($example_name),
                " = ",
                stringify!($name),
                "::from_secs(",
                stringify!($example_secs),
                ")?;\n",
                "assert_eq!(",
                stringify!($example_name),
                ".as_duration(), Duration::from_secs(",
                stringify!($example_secs),
                "));\n",
                "# Ok::<(), Box<dyn std::error::Error>>(())\n",
                "```\n",
            )]
            pub fn from_secs(value: u64) -> Result<Self, DurationParseError> {
                Self::new(Duration::from_secs(value))
            }

            #[doc = concat!(
                "Returns ",
                $description,
                " as a `Duration`.\n\n",
                "# Examples\n\n",
                "```rust\n",
                "# use std::time::Duration;\n",
                "# use pplx_space_guest_process::*;\n",
                "let ",
                stringify!($example_name),
                " = ",
                stringify!($name),
                "::from_secs(",
                stringify!($example_secs),
                ")?;\n",
                "assert_eq!(",
                stringify!($example_name),
                ".as_duration(), Duration::from_secs(",
                stringify!($example_secs),
                "));\n",
                "# Ok::<(), Box<dyn std::error::Error>>(())\n",
                "```\n",
            )]
            pub const fn as_duration(self) -> Duration {
                self.0.get()
            }
        }
    };
}

duration_type!(
    /// Maximum time to wait for a sandbox to report readiness.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use std::time::Duration;
    /// # use pplx_space_guest_process::*;
    /// let timeout = ReadyTimeout::from_secs(30)?;
    /// assert_eq!(timeout.as_duration(), Duration::from_secs(30));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ReadyTimeout,
    timeout,
    30,
    "a ready timeout"
);
duration_type!(
    /// Observed or requested duration for which a guest has been running.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use std::time::Duration;
    /// # use pplx_space_guest_process::*;
    /// let uptime = GuestUptime::from_secs(300)?;
    /// assert_eq!(uptime.as_duration(), Duration::from_secs(300));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    GuestUptime,
    uptime,
    300,
    "a guest uptime"
);
duration_type!(
    /// Delay before retrying an operation.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use std::time::Duration;
    /// # use pplx_space_guest_process::*;
    /// let delay = RetryDelay::from_secs(5)?;
    /// assert_eq!(delay.as_duration(), Duration::from_secs(5));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    RetryDelay,
    delay,
    5,
    "a retry delay"
);
duration_type!(
    /// Interval between snapshot attempts.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use std::time::Duration;
    /// # use pplx_space_guest_process::*;
    /// let interval = SnapshotInterval::from_secs(60)?;
    /// assert_eq!(interval.as_duration(), Duration::from_secs(60));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    SnapshotInterval,
    interval,
    60,
    "a snapshot interval"
);
