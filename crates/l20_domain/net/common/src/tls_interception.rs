//! Shared TLS interception vocabularies.

/// The durable network-policy choice for TLS interception.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "lowercase"))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TlsInterception {
    #[default]
    Off,
    On,
}

impl TlsInterception {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::On => "on",
        }
    }

    pub const fn is_off(&self) -> bool {
        matches!(self, Self::Off)
    }
}

impl std::fmt::Display for TlsInterception {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("unknown TLS interception policy '{0}'; expected off or on")]
pub struct TlsInterceptionParseError(String);

impl std::str::FromStr for TlsInterception {
    type Err = TlsInterceptionParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "off" => Ok(Self::Off),
            "on" => Ok(Self::On),
            _ => Err(TlsInterceptionParseError(value.to_owned())),
        }
    }
}
