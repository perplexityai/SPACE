//! Opt-in virtio memory balloon for Firecracker sandboxes.
//!
//! A template may carry a `balloon` object in its `metadata.json`. Without it
//! a sandbox VM has no balloon device, which is the default. With it, a
//! cold-booted VM gets one balloon device in exactly one [`BalloonMode`]:
//!
//! ```json
//! { "mode": "inflate", "amount_mib": 1024, "deflate_on_oom": true }
//! { "mode": "report", "stats_polling_interval_s": 5 }
//! { "mode": "hint" }
//! ```
//!
//! - `inflate` asks the guest to give `amount_mib` of its memory back to the
//!   host; `deflate_on_oom` lets the guest reclaim it under memory pressure.
//!   Both fields are required and allowed only in this mode.
//! - `report` enables free-page reporting: the guest continually reports free
//!   ranges, which the host releases. Firecracker cannot turn it off after
//!   boot.
//! - `hint` enables free-page hinting, a Firecracker developer-preview feature
//!   with a known race that can corrupt guest memory. The host asks for free
//!   ranges on demand; see the sandbox manager's suspend path for when.
//! - `stats_polling_interval_s` (any mode, default `0` = disabled) enables the
//!   device's statistics.
//!
//! The device is part of the VM's machine state, so snapshots carry it.
//! Changing a template's setting only takes effect for VMs that cold boot
//! from it, including the template's base-snapshot rebuild; sandboxes
//! restored from an existing snapshot keep the device it was taken with.

use std::fmt;
use std::num::NonZeroU32;
use std::str::FromStr;

/// The one balloon feature a template enables.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum BalloonMode {
    #[cfg_attr(feature = "serde", serde(rename = "inflate"))]
    Inflate,
    #[cfg_attr(feature = "serde", serde(rename = "report"))]
    FreePageReporting,
    #[cfg_attr(feature = "serde", serde(rename = "hint"))]
    FreePageHinting,
}

impl BalloonMode {
    pub const ALL: [Self; 3] = [
        Self::Inflate,
        Self::FreePageReporting,
        Self::FreePageHinting,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Inflate => "inflate",
            Self::FreePageReporting => "report",
            Self::FreePageHinting => "hint",
        }
    }
}

impl fmt::Display for BalloonMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("unknown balloon mode {0:?}; expected inflate, report, or hint")]
pub struct BalloonModeParseError(String);

impl FromStr for BalloonMode {
    type Err = BalloonModeParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|mode| mode.as_str() == value)
            .ok_or_else(|| BalloonModeParseError(value.to_string()))
    }
}

/// The balloon device's mode-specific settings.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum BalloonDevice {
    Inflate {
        amount_mib: NonZeroU32,
        deflate_on_oom: bool,
    },
    FreePageReporting,
    FreePageHinting,
}

impl BalloonDevice {
    pub const fn mode(self) -> BalloonMode {
        match self {
            Self::Inflate { .. } => BalloonMode::Inflate,
            Self::FreePageReporting => BalloonMode::FreePageReporting,
            Self::FreePageHinting => BalloonMode::FreePageHinting,
        }
    }
}

/// A template's validated balloon setting.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(try_from = "BalloonConfigWire", into = "BalloonConfigWire")
)]
pub struct BalloonConfig {
    device: BalloonDevice,
    stats_polling_interval_s: u16,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BalloonConfigError {
    #[error("balloon mode inflate requires {field}")]
    MissingField { field: &'static str },
    #[error("balloon {field} is only valid for mode inflate, not {mode}")]
    FieldNotAllowed {
        field: &'static str,
        mode: BalloonMode,
    },
    #[error("balloon amount_mib must be greater than 0")]
    ZeroAmount,
    #[error("balloon amount_mib {amount_mib} must be less than the VM's memory_mib {memory_mib}")]
    AmountNotBelowMemory { amount_mib: u32, memory_mib: u32 },
}

impl BalloonConfig {
    pub const fn new(device: BalloonDevice, stats_polling_interval_s: u16) -> Self {
        Self {
            device,
            stats_polling_interval_s,
        }
    }

    pub const fn device(self) -> BalloonDevice {
        self.device
    }

    pub const fn mode(self) -> BalloonMode {
        self.device.mode()
    }

    /// Seconds between device statistics refreshes; `0` disables them.
    pub const fn stats_polling_interval_s(self) -> u16 {
        self.stats_polling_interval_s
    }

    /// Target balloon size; `0` for the free-page modes.
    pub const fn amount_mib(self) -> u32 {
        match self.device {
            BalloonDevice::Inflate { amount_mib, .. } => amount_mib.get(),
            BalloonDevice::FreePageReporting | BalloonDevice::FreePageHinting => 0,
        }
    }

    pub const fn deflate_on_oom(self) -> bool {
        match self.device {
            BalloonDevice::Inflate { deflate_on_oom, .. } => deflate_on_oom,
            BalloonDevice::FreePageReporting | BalloonDevice::FreePageHinting => false,
        }
    }

    pub const fn free_page_reporting(self) -> bool {
        matches!(self.device, BalloonDevice::FreePageReporting)
    }

    pub const fn free_page_hinting(self) -> bool {
        matches!(self.device, BalloonDevice::FreePageHinting)
    }

    /// Reject an inflation target that would take all of the VM's memory.
    pub fn validate_for_memory_mib(self, memory_mib: u32) -> Result<(), BalloonConfigError> {
        let amount_mib = self.amount_mib();
        if amount_mib >= memory_mib {
            return Err(BalloonConfigError::AmountNotBelowMemory {
                amount_mib,
                memory_mib,
            });
        }
        Ok(())
    }
}

/// `metadata.json` shape; [`BalloonConfig`] is the validated form.
#[cfg(feature = "serde")]
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct BalloonConfigWire {
    mode: BalloonMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    amount_mib: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    deflate_on_oom: Option<bool>,
    #[serde(default, skip_serializing_if = "is_zero")]
    stats_polling_interval_s: u16,
}

#[cfg(feature = "serde")]
fn is_zero(value: &u16) -> bool {
    *value == 0
}

#[cfg(feature = "serde")]
impl TryFrom<BalloonConfigWire> for BalloonConfig {
    type Error = BalloonConfigError;

    fn try_from(wire: BalloonConfigWire) -> Result<Self, Self::Error> {
        let device = match wire.mode {
            BalloonMode::Inflate => {
                let amount_mib = wire.amount_mib.ok_or(BalloonConfigError::MissingField {
                    field: "amount_mib",
                })?;
                let deflate_on_oom =
                    wire.deflate_on_oom
                        .ok_or(BalloonConfigError::MissingField {
                            field: "deflate_on_oom",
                        })?;
                BalloonDevice::Inflate {
                    amount_mib: NonZeroU32::new(amount_mib)
                        .ok_or(BalloonConfigError::ZeroAmount)?,
                    deflate_on_oom,
                }
            }
            BalloonMode::FreePageReporting => {
                wire.reject_inflate_fields()?;
                BalloonDevice::FreePageReporting
            }
            BalloonMode::FreePageHinting => {
                wire.reject_inflate_fields()?;
                BalloonDevice::FreePageHinting
            }
        };
        Ok(Self::new(device, wire.stats_polling_interval_s))
    }
}

#[cfg(feature = "serde")]
impl BalloonConfigWire {
    fn reject_inflate_fields(&self) -> Result<(), BalloonConfigError> {
        let field = if self.amount_mib.is_some() {
            "amount_mib"
        } else if self.deflate_on_oom.is_some() {
            "deflate_on_oom"
        } else {
            return Ok(());
        };
        Err(BalloonConfigError::FieldNotAllowed {
            field,
            mode: self.mode,
        })
    }
}

#[cfg(feature = "serde")]
impl From<BalloonConfig> for BalloonConfigWire {
    fn from(config: BalloonConfig) -> Self {
        let (amount_mib, deflate_on_oom) = match config.device {
            BalloonDevice::Inflate {
                amount_mib,
                deflate_on_oom,
            } => (Some(amount_mib.get()), Some(deflate_on_oom)),
            BalloonDevice::FreePageReporting | BalloonDevice::FreePageHinting => (None, None),
        };
        Self {
            mode: config.mode(),
            amount_mib,
            deflate_on_oom,
            stats_polling_interval_s: config.stats_polling_interval_s,
        }
    }
}

#[cfg(all(test, feature = "serde"))]
mod tests;
