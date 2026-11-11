use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

const USER_ROLE_ORGANIZATION_ADMIN: &str = "organization_admin";
const USER_ROLE_MEMBER: &str = "member";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UserRole {
    OrganizationAdmin,
    Member,
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("unsupported user role '{0}'")]
pub struct UserRoleParseError(String);

impl UserRole {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OrganizationAdmin => USER_ROLE_ORGANIZATION_ADMIN,
            Self::Member => USER_ROLE_MEMBER,
        }
    }

    pub const fn can_assign_role(self, role: Self) -> bool {
        matches!(
            (self, role),
            (Self::OrganizationAdmin, Self::OrganizationAdmin)
                | (Self::OrganizationAdmin, Self::Member)
        )
    }

    pub const fn can_mutate_role_holder(self, role: Self) -> bool {
        matches!(
            (self, role),
            (Self::OrganizationAdmin, Self::OrganizationAdmin)
                | (Self::OrganizationAdmin, Self::Member)
        )
    }

    pub const fn is_demotion_to(self, role: Self) -> bool {
        matches!((self, role), (Self::OrganizationAdmin, Self::Member))
    }
}

impl fmt::Display for UserRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for UserRole {
    type Err = UserRoleParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            USER_ROLE_ORGANIZATION_ADMIN => Ok(Self::OrganizationAdmin),
            USER_ROLE_MEMBER => Ok(Self::Member),
            other => Err(UserRoleParseError(other.to_string())),
        }
    }
}

impl sqlx::Type<sqlx::Postgres> for UserRole {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <str as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <str as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

impl<'q> sqlx::Encode<'q, sqlx::Postgres> for UserRole {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'q, sqlx::Postgres>>::encode(self.as_str(), buf)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for UserRole {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let raw = <&str as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
        raw.parse::<Self>().map_err(Into::into)
    }
}

#[cfg(test)]
mod tests;
