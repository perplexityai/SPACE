//! Versioned logical chunk sizes and authenticated delivery commitments.

/// An explicitly configured namespace for experimental templates. The trailing
/// separator prevents a short name from accidentally matching another name.
#[derive(Clone, Debug)]
pub struct TemplatePrefix(String);

#[derive(Debug, thiserror::Error)]
#[error("chunk experiment prefix must be a nonempty template-name prefix ending in '-'")]
pub struct TemplatePrefixError;

impl std::str::FromStr for TemplatePrefix {
    type Err = TemplatePrefixError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() < 2
            || !value.ends_with('-')
            || super::TemplateName::parse(format!("{value}probe")).is_err()
        {
            return Err(TemplatePrefixError);
        }
        Ok(Self(value.into()))
    }
}

impl TemplatePrefix {
    pub fn contains(&self, template: &str) -> bool {
        template.starts_with(&self.0)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "u64", into = "u64"))]
pub enum ChunkSize {
    Kib512,
    Mib1,
    Mib4,
    Mib16,
    Mib64,
}

impl ChunkSize {
    pub const fn bytes(self) -> u64 {
        match self {
            Self::Kib512 => 512 << 10,
            Self::Mib1 => 1 << 20,
            Self::Mib4 => 4 << 20,
            Self::Mib16 => 16 << 20,
            Self::Mib64 => 64 << 20,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Kib512 => "512KiB",
            Self::Mib1 => "1MiB",
            Self::Mib4 => "4MiB",
            Self::Mib16 => "16MiB",
            Self::Mib64 => "64MiB",
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("chunk size must be 512KiB, 1MiB, 4MiB, 16MiB, or 64MiB")]
pub struct ChunkSizeError;

impl TryFrom<u64> for ChunkSize {
    type Error = ChunkSizeError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            524_288 => Ok(Self::Kib512),
            1_048_576 => Ok(Self::Mib1),
            4_194_304 => Ok(Self::Mib4),
            16_777_216 => Ok(Self::Mib16),
            67_108_864 => Ok(Self::Mib64),
            _ => Err(ChunkSizeError),
        }
    }
}

impl From<ChunkSize> for u64 {
    fn from(value: ChunkSize) -> Self {
        value.bytes()
    }
}

impl std::str::FromStr for ChunkSize {
    type Err = ChunkSizeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "512KiB" => Ok(Self::Kib512),
            "1MiB" => Ok(Self::Mib1),
            "4MiB" => Ok(Self::Mib4),
            "16MiB" => Ok(Self::Mib16),
            "64MiB" => Ok(Self::Mib64),
            _ => Err(ChunkSizeError),
        }
    }
}

impl std::fmt::Display for ChunkSize {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}
