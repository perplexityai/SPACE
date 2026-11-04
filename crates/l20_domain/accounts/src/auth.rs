//! Authentication and account identity types.
//!
//! These wrappers keep user, org, node, and credential identifiers from being
//! mixed accidentally. They are small boundary types for API and internal auth code.

use std::borrow::Cow;
use std::fmt;
use std::str::FromStr;

use uuid::Uuid;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

const MAX_EMAIL_BYTES: usize = 320;
const MAX_SANDBOX_OWNER_BYTES: usize = 320;

const TEMPLATE_MANAGER_LOCAL_USER_VALUE: &str = "tmplmgr@local";
const BUILDER_LOCAL_USER_VALUE: &str = "builder@local";
const ROOT_LOCAL_USER_VALUE: &str = "root@local";
const DEPLOY_LOCAL_USER_VALUE: &str = "deploy@local";
const DEV_LOCAL_USER_VALUE: &str = "dev@local";
const ORPHANED_LOCAL_USER_VALUE: &str = "orphaned@local";

/// Error returned when an auth-related value fails validation.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_accounts::*;
/// assert!(Email::parse("not-an-email").is_err());
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("{0}")]
pub struct AuthParseError(&'static str);

impl AuthParseError {
    pub(crate) const fn new(message: &'static str) -> Self {
        Self(message)
    }
}

/// Error returned when an auth/account UUID cannot be parsed.
///
/// # Examples
///
/// ```rust
/// # use std::str::FromStr;
/// # use pplx_space_accounts::UserId;
/// assert!(UserId::from_str("not-a-uuid").is_err());
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum AuthIdParseError {
    #[error("invalid auth UUID: {0}")]
    InvalidUuid(#[from] uuid::Error),
}

/// The owner principal stored on a sandbox-manager sandbox row.
///
/// This is the node-local sandbox owner, not the credential lookup identity.
/// Platform users are represented by `user:<uuid>` while local service and
/// operator owners use bounded local labels such as `root@local`.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SandboxOwner(Cow<'static, str>);

/// Local pseudo-user used by template-manager for sandboxes it owns internally.
pub static TEMPLATE_MANAGER_SANDBOX_OWNER: SandboxOwner =
    SandboxOwner::new_static(TEMPLATE_MANAGER_LOCAL_USER_VALUE);
/// Local pseudo-user used by builder for build sandboxes.
pub static BUILDER_SANDBOX_OWNER: SandboxOwner = SandboxOwner::new_static(BUILDER_LOCAL_USER_VALUE);
/// Local pseudo-user used by root-owned local sandboxes.
pub static ROOT_SANDBOX_OWNER: SandboxOwner = SandboxOwner::new_static(ROOT_LOCAL_USER_VALUE);
/// Local pseudo-user used by deploy-owned local sandboxes.
pub static DEPLOY_SANDBOX_OWNER: SandboxOwner = SandboxOwner::new_static(DEPLOY_LOCAL_USER_VALUE);
/// Local pseudo-user used by developer-owned local sandboxes.
pub static DEV_SANDBOX_OWNER: SandboxOwner = SandboxOwner::new_static(DEV_LOCAL_USER_VALUE);
/// Local pseudo-user used for node-local artifacts recovered without metadata.
pub static ORPHANED_SANDBOX_OWNER: SandboxOwner =
    SandboxOwner::new_static(ORPHANED_LOCAL_USER_VALUE);

impl SandboxOwner {
    /// Creates a sandbox owner from a static local pseudo-user label.
    ///
    /// The constructor rejects empty, oversized, or non-visible-ASCII labels in
    /// const contexts.
    #[expect(
        clippy::indexing_slicing,
        reason = "const fn cannot use slice::get on this toolchain; loop bounds keep indexing in range"
    )]
    pub const fn new_static(value: &'static str) -> Self {
        assert!(!value.is_empty(), "sandbox owner is invalid");
        assert!(
            value.len() <= MAX_SANDBOX_OWNER_BYTES,
            "sandbox owner is invalid"
        );

        let bytes = value.as_bytes();
        let mut index = 0;
        while index < bytes.len() {
            assert!(
                is_static_sandbox_owner_byte(bytes[index]),
                "sandbox owner is invalid"
            );
            index += 1;
        }

        Self(Cow::Borrowed(value))
    }

    /// Parses a bounded, displayable sandbox owner label.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_accounts::*;
    /// let user = SandboxOwner::parse("tmplmgr@local").unwrap();
    /// assert_eq!(user.kind(), SandboxOwnerKind::TemplateManager);
    /// ```
    pub fn parse(value: impl Into<String>) -> Result<Self, AuthParseError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_SANDBOX_OWNER_BYTES
            || value.chars().any(invalid_sandbox_owner_char)
        {
            return Err(AuthParseError("sandbox owner is invalid"));
        }
        Ok(Self(Cow::Owned(value)))
    }

    /// Builds the owner label for an API-authenticated platform user.
    pub fn platform_user(user_id: UserId) -> Self {
        Self(Cow::Owned(format!("user:{user_id}")))
    }

    /// Builds the local pseudo-user label for a numeric Unix uid.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_accounts::SandboxOwner;
    /// assert_eq!(SandboxOwner::uid_local(1000).as_str(), "uid-1000@local");
    /// ```
    pub fn uid_local(uid: u32) -> Self {
        Self(Cow::Owned(format!("uid-{uid}@local")))
    }

    /// Returns the validated sandbox owner string.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_accounts::*;
    /// let user = SandboxOwner::parse("builder@local").unwrap();
    /// assert_eq!(user.as_str(), "builder@local");
    /// ```
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }

    /// Whether this owner is an API-authenticated platform user
    /// (`user:<uuid>`) — the only owner shape the control plane mints for
    /// cluster-managed sandboxes. Local pseudo-users (`root@local`,
    /// `tmplmgr@local`, `orphaned@local`, …) and ad-hoc operator labels
    /// return `false`. Destructive reconcilers use this as positive
    /// evidence of cluster provenance: a row the cluster never owned must
    /// never be torn down on the cluster's behalf.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_accounts::*;
    /// assert!(SandboxOwner::platform_user(UserId::new()).is_platform_user());
    /// assert!(!SandboxOwner::parse("root@local").unwrap().is_platform_user());
    /// assert!(!SandboxOwner::parse("someone@example.com").unwrap().is_platform_user());
    /// ```
    pub fn is_platform_user(&self) -> bool {
        matches!(self.kind(), SandboxOwnerKind::PlatformUser(_))
    }

    /// Classifies reserved local pseudo-users and platform users.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_accounts::*;
    /// let user_id = UserId::new();
    /// let owner = SandboxOwner::platform_user(user_id);
    /// assert_eq!(owner.kind(), SandboxOwnerKind::PlatformUser(user_id));
    /// ```
    pub fn kind(&self) -> SandboxOwnerKind {
        if self == &TEMPLATE_MANAGER_SANDBOX_OWNER {
            SandboxOwnerKind::TemplateManager
        } else if self == &BUILDER_SANDBOX_OWNER {
            SandboxOwnerKind::Builder
        } else if self == &ROOT_SANDBOX_OWNER {
            SandboxOwnerKind::Root
        } else if self == &DEPLOY_SANDBOX_OWNER {
            SandboxOwnerKind::Deploy
        } else if self == &DEV_SANDBOX_OWNER {
            SandboxOwnerKind::Dev
        } else if self == &ORPHANED_SANDBOX_OWNER {
            SandboxOwnerKind::Orphaned
        } else if let Some(user_id) = self
            .as_str()
            .strip_prefix("user:")
            .and_then(|value| UserId::from_str(value).ok())
        {
            SandboxOwnerKind::PlatformUser(user_id)
        } else {
            SandboxOwnerKind::Local(self.clone())
        }
    }
}

impl fmt::Debug for SandboxOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SandboxOwner").field(&self.0).finish()
    }
}

impl fmt::Display for SandboxOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}

impl FromStr for SandboxOwner {
    type Err = AuthParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for SandboxOwner {
    type Error = AuthParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl TryFrom<&str> for SandboxOwner {
    type Error = AuthParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<SandboxOwner> for String {
    fn from(value: SandboxOwner) -> Self {
        value.0.into_owned()
    }
}

impl AsRef<str> for SandboxOwner {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// Classification of a sandbox owner label.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SandboxOwnerKind {
    TemplateManager,
    Builder,
    Root,
    Deploy,
    Dev,
    Orphaned,
    PlatformUser(UserId),
    Local(SandboxOwner),
}

/// Owner kinds accepted for operator-created node-local sandboxes.
pub const LOCAL_OPERATOR_SANDBOX_OWNER_KINDS: &[SandboxOwnerKind] = &[
    SandboxOwnerKind::Root,
    SandboxOwnerKind::Deploy,
    SandboxOwnerKind::Dev,
];

impl SandboxOwnerKind {
    /// Returns the static owner value for reserved local owner kinds.
    pub fn reserved_owner(&self) -> Option<&'static SandboxOwner> {
        match self {
            SandboxOwnerKind::TemplateManager => Some(&TEMPLATE_MANAGER_SANDBOX_OWNER),
            SandboxOwnerKind::Builder => Some(&BUILDER_SANDBOX_OWNER),
            SandboxOwnerKind::Root => Some(&ROOT_SANDBOX_OWNER),
            SandboxOwnerKind::Deploy => Some(&DEPLOY_SANDBOX_OWNER),
            SandboxOwnerKind::Dev => Some(&DEV_SANDBOX_OWNER),
            SandboxOwnerKind::Orphaned => Some(&ORPHANED_SANDBOX_OWNER),
            SandboxOwnerKind::PlatformUser(_) | SandboxOwnerKind::Local(_) => None,
        }
    }

    /// Returns the static owner label for reserved local owner kinds.
    pub fn reserved_owner_label(&self) -> Option<&'static str> {
        self.reserved_owner().map(SandboxOwner::as_str)
    }
}

pub fn format_reserved_sandbox_owner_kind_labels(kinds: &[SandboxOwnerKind]) -> String {
    let labels = kinds
        .iter()
        .filter_map(SandboxOwnerKind::reserved_owner_label)
        .collect::<Vec<_>>();
    match labels.as_slice() {
        [] => String::new(),
        [only] => (*only).to_string(),
        [first, second] => format!("{first} or {second}"),
        _ => {
            let (last, rest) = labels.split_last().expect("non-empty labels");
            format!("{}, or {last}", rest.join(", "))
        }
    }
}

macro_rules! uuid_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
        #[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
        #[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct $name(Uuid);

        impl $name {
            #[doc = concat!(
                "Creates a fresh `",
                stringify!($name),
                "` using a UUIDv7 value.\n\n",
                "# Examples\n\n",
                "```rust\n",
                "# use pplx_space_accounts::*;\n",
                "let id = ",
                stringify!($name),
                "::new();\n",
                "assert!(!id.to_string().is_empty());\n",
                "```"
            )]
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }

            #[doc = concat!(
                "Wraps an existing UUID as a `",
                stringify!($name),
                "`.\n\n",
                "# Examples\n\n",
                "```rust\n",
                "# use pplx_space_accounts::*;\n",
                "let uuid = uuid::Uuid::nil();\n",
                "let id = ",
                stringify!($name),
                "::from_uuid(uuid);\n",
                "assert_eq!(id.as_uuid(), uuid);\n",
                "```"
            )]
            pub const fn from_uuid(value: Uuid) -> Self {
                Self(value)
            }

            #[doc = concat!(
                "Returns the underlying UUID.\n\n",
                "# Examples\n\n",
                "```rust\n",
                "# use pplx_space_accounts::*;\n",
                "let uuid = uuid::Uuid::nil();\n",
                "let id = ",
                stringify!($name),
                "::from_uuid(uuid);\n",
                "assert_eq!(id.as_uuid(), uuid);\n",
                "```"
            )]
            pub const fn as_uuid(self) -> Uuid {
                self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_tuple(stringify!($name))
                    .field(&self.to_string())
                    .finish()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl FromStr for $name {
            type Err = AuthIdParseError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(value).map(Self).map_err(Into::into)
            }
        }

        impl TryFrom<String> for $name {
            type Error = AuthIdParseError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::from_str(&value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.to_string()
            }
        }

        #[cfg(feature = "sqlx")]
        impl sqlx::postgres::PgHasArrayType for $name {
            fn array_type_info() -> sqlx::postgres::PgTypeInfo {
                <Uuid as sqlx::postgres::PgHasArrayType>::array_type_info()
            }
        }

        #[cfg(feature = "sqlx")]
        impl sqlx::Type<sqlx::Postgres> for $name {
            fn type_info() -> sqlx::postgres::PgTypeInfo {
                <Uuid as sqlx::Type<sqlx::Postgres>>::type_info()
            }

            fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
                <Uuid as sqlx::Type<sqlx::Postgres>>::compatible(ty)
            }
        }

        #[cfg(feature = "sqlx")]
        impl<'q> sqlx::Encode<'q, sqlx::Postgres> for $name {
            fn encode_by_ref(
                &self,
                buf: &mut sqlx::postgres::PgArgumentBuffer,
            ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
                <Uuid as sqlx::Encode<'q, sqlx::Postgres>>::encode_by_ref(&self.0, buf)
            }
        }

        #[cfg(feature = "sqlx")]
        impl<'r> sqlx::Decode<'r, sqlx::Postgres> for $name {
            fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
                Ok(Self(<Uuid as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?))
            }
        }
    };
}

uuid_id!(CredentialReferenceId);

uuid_id!(
    /// Authorization for one system template build's publication.
    SystemPublicationGrantId
);

uuid_id!(
    /// Authorization for one tenant template build's publication.
    TenantPublicationGrantId
);

uuid_id!(
    /// Identity of a persisted background job, retained across retries and recovery.
    JobId
);

uuid_id!(
    /// Stable identifier for a platform user.
    ///
    /// Use this instead of a raw UUID when a value denotes a user.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_accounts::*;
    /// let id = UserId::new();
    /// assert_eq!(UserId::from_uuid(id.as_uuid()), id);
    /// ```
    UserId
);
uuid_id!(
    /// Stable identifier for an organization.
    ///
    /// Keeping org IDs distinct from user IDs prevents swapped arguments in API
    /// and authorization code.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_accounts::*;
    /// let id = OrgId::new();
    /// assert_eq!(OrgId::from_uuid(id.as_uuid()), id);
    /// ```
    OrgId
);

uuid_id!(
    /// Stable identifier for a SPACE node.
    ///
    /// Use this for persisted node identity and node-scoped auth decisions.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_accounts::*;
    /// let id = NodeId::new();
    /// assert_eq!(NodeId::from_uuid(id.as_uuid()), id);
    /// ```
    NodeId
);
uuid_id!(
    /// Stable identifier for an API key record.
    ///
    /// This identifies the key object, not the secret key material presented by
    /// a caller.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_accounts::*;
    /// let id = ApiKeyId::new();
    /// assert_eq!(ApiKeyId::from_uuid(id.as_uuid()), id);
    /// ```
    ApiKeyId
);

/// Email address value used at explicit authenticated-user boundaries.
///
/// `Debug` redacts the address, so callers must use `expose` when they
/// intentionally need the raw string.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_accounts::*;
/// let email = Email::parse("user@example.com").unwrap();
/// assert_eq!(format!("{email:?}"), "Email(<redacted>)");
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Email(String);

impl Email {
    /// Parses a bounded email string with a non-empty local and domain part.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_accounts::*;
    /// let email = Email::parse("user@example.com").unwrap();
    /// assert_eq!(email.expose(), "user@example.com");
    /// ```
    pub fn parse(value: impl Into<String>) -> Result<Self, AuthParseError> {
        let value = value.into();
        let Some((local, domain)) = value.split_once('@') else {
            return Err(AuthParseError("email is invalid"));
        };
        if value.len() > MAX_EMAIL_BYTES
            || local.is_empty()
            || domain.is_empty()
            || domain.contains('@')
            || value.chars().any(invalid_email_char)
        {
            return Err(AuthParseError("email is invalid"));
        }

        Ok(Self(format!(
            "{}@{}",
            local.to_ascii_lowercase(),
            domain.to_ascii_lowercase()
        )))
    }

    /// Exposes the raw email string at an intentional PII boundary.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_accounts::*;
    /// let email = Email::parse("user@example.com").unwrap();
    /// assert_eq!(email.expose(), "user@example.com");
    /// ```
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Email {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Email(<redacted>)")
    }
}

impl FromStr for Email {
    type Err = AuthParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for Email {
    type Error = AuthParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<Email> for String {
    fn from(value: Email) -> Self {
        value.0
    }
}

/// URL- and DNS-friendly organization slug.
///
/// Use this for owner-facing org identifiers where the slug has already been
/// validated separately from `OrgId`.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_accounts::*;
/// let slug = OrgSlug::parse("acme-prod").unwrap();
/// assert_eq!(slug.as_str(), "acme-prod");
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct OrgSlug(String);

impl OrgSlug {
    /// Parses a lowercase ASCII slug.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_accounts::*;
    /// assert!(OrgSlug::parse("acme-prod").is_ok());
    /// assert!(OrgSlug::parse("Acme").is_err());
    /// ```
    pub fn parse(value: impl Into<String>) -> Result<Self, AuthParseError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value
                .chars()
                .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-')
        {
            return Err(AuthParseError("org slug is invalid"));
        }
        Ok(Self(value))
    }

    /// Returns the validated slug string.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_accounts::*;
    /// let slug = OrgSlug::parse("acme-prod").unwrap();
    /// assert_eq!(slug.as_str(), "acme-prod");
    /// ```
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for OrgSlug {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl TryFrom<String> for OrgSlug {
    type Error = AuthParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<OrgSlug> for String {
    fn from(value: OrgSlug) -> Self {
        value.0
    }
}

/// Authorization role assigned to a platform user.
///
/// Use this at policy boundaries instead of stringly typed role names.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_accounts::*;
/// let role = UserRole::OrgAdmin;
/// assert!(matches!(role, UserRole::OrgAdmin));
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum UserRole {
    DeployerAdmin,
    OrgAdmin,
    Member,
}

/// Non-secret key used to identify a credential value.
///
/// This is safe to display as a key name; the associated credential value is
/// held by the credential storage layer.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_accounts::*;
/// let key = CredentialKey::parse("github-token").unwrap();
/// assert_eq!(key.as_str(), "github-token");
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct CredentialKey(String);

impl CredentialKey {
    /// Parses a bounded, NUL-free credential key.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_accounts::*;
    /// assert!(CredentialKey::parse("github-token").is_ok());
    /// assert!(CredentialKey::parse("").is_err());
    /// ```
    pub fn parse(value: impl Into<String>) -> Result<Self, AuthParseError> {
        let value = value.into();
        if value.is_empty() || value.len() > 256 || value.contains('\0') {
            return Err(AuthParseError("credential key is invalid"));
        }
        Ok(Self(value))
    }

    /// Returns the validated credential key string.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_accounts::*;
    /// let key = CredentialKey::parse("github-token").unwrap();
    /// assert_eq!(key.as_str(), "github-token");
    /// ```
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CredentialKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl TryFrom<String> for CredentialKey {
    type Error = AuthParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<CredentialKey> for String {
    fn from(value: CredentialKey) -> Self {
        value.0
    }
}

fn invalid_email_char(ch: char) -> bool {
    ch.is_control() || ch.is_whitespace() || is_bidi_control(ch)
}

fn invalid_sandbox_owner_char(ch: char) -> bool {
    ch.is_control() || ch.is_whitespace() || is_bidi_control(ch)
}

const fn is_static_sandbox_owner_byte(byte: u8) -> bool {
    matches!(byte, b'!'..=b'~')
}

fn is_bidi_control(ch: char) -> bool {
    matches!(ch, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
}

#[cfg(test)]
mod tests;
