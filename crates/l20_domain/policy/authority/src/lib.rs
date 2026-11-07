//! Authority marker for policy data that can be sourced locally or from the cluster.

use std::fmt;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use pplx_space_net_common::proto::PolicyAuthority as ProtoPolicyAuthority;

/// Authority that owns the currently stored value for policy data.
///
/// `Cluster` means the value should follow cluster reconciliation. `Local`
/// means the node-local value intentionally wins until it is cleared.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum PolicyAuthority {
    #[default]
    Cluster,
    Local,
}

impl PolicyAuthority {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cluster => "cluster",
            Self::Local => "local",
        }
    }

    pub fn as_proto(self) -> ProtoPolicyAuthority {
        self.into()
    }

    pub fn as_proto_i32(self) -> i32 {
        self.as_proto() as i32
    }

    pub fn from_optional_proto(value: i32) -> Result<Option<Self>, PolicyAuthorityParseError> {
        if value == ProtoPolicyAuthority::Unspecified as i32 {
            Ok(None)
        } else {
            Self::try_from(value).map(Some)
        }
    }
}

impl fmt::Display for PolicyAuthority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for PolicyAuthority {
    type Err = PolicyAuthorityParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "cluster" => Ok(Self::Cluster),
            "local" => Ok(Self::Local),
            _ => Err(PolicyAuthorityParseError::InvalidString),
        }
    }
}

impl From<PolicyAuthority> for ProtoPolicyAuthority {
    fn from(authority: PolicyAuthority) -> Self {
        match authority {
            PolicyAuthority::Cluster => Self::Cluster,
            PolicyAuthority::Local => Self::Local,
        }
    }
}

impl TryFrom<ProtoPolicyAuthority> for PolicyAuthority {
    type Error = PolicyAuthorityParseError;

    fn try_from(authority: ProtoPolicyAuthority) -> Result<Self, Self::Error> {
        match authority {
            ProtoPolicyAuthority::Cluster => Ok(Self::Cluster),
            ProtoPolicyAuthority::Local => Ok(Self::Local),
            ProtoPolicyAuthority::Unspecified => Err(PolicyAuthorityParseError::UnspecifiedProto),
        }
    }
}

impl TryFrom<i32> for PolicyAuthority {
    type Error = PolicyAuthorityParseError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        let authority = ProtoPolicyAuthority::try_from(value)
            .map_err(|_| PolicyAuthorityParseError::UnknownProto(value))?;
        Self::try_from(authority)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum PolicyAuthorityParseError {
    #[error("policy authority must be cluster or local")]
    InvalidString,
    #[error("policy authority proto value is unspecified")]
    UnspecifiedProto,
    #[error("policy authority proto value {0} is unknown")]
    UnknownProto(i32),
}
