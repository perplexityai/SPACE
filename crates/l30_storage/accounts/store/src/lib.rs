//! Owning store for accounts state: `organizations`, `users`, `api_keys`,
//! console `sessions`, and the singleton `clusters` row naming the managing
//! organization.
//!
//! Each statement is a free function that returns `sqlx::Error` unchanged, so
//! callers keep classifying unique and foreign-key violations themselves.
//! Most take any executor (`impl PgExecutor`), so they run in the caller's
//! transaction when there is one; the locking reads below take
//! `&mut PgConnection` instead. The row types, and the `users.role` and
//! `api_keys.allowed_surfaces` column types, are the API's account models.
//!
//! Locking reads take the caller's open transaction as `&mut PgConnection`
//! and are named for the lock they take (`lock_…`, `share_lock_…`); the lock
//! lasts until that transaction ends. Statements that compare with the
//! database clock keep `NOW()` in their SQL, so they decide on the
//! transaction's start time in whatever executor the caller passes. Guarded
//! writes, whose `WHERE` re-checks the row's state, are named for their
//! transition and return their outcome (whether they wrote, or an enum such
//! as `ApiKeyRevocation` that also says why not); matching no row is an
//! outcome, not an error. Callers keep their transactions and decide what to
//! do with each outcome.
//!
//! Statements on these tables that have not moved here yet stay with their
//! callers.

pub mod api_keys;
pub mod clusters;
pub mod organizations;
mod role;
pub mod sessions;
mod surface;
pub mod users;

pub use api_keys::{ApiKey, ApiKeyAuth, ApiKeyRevocation, ApiKeyRow, CreateApiKey};
pub use clusters::Cluster;
pub use organizations::{
    FederatedOrganization, NewOrganization, Organization, OrganizationBranding, OrganizationOwner,
    OrganizationPatch,
};
pub use role::{UserRole, UserRoleParseError};
pub use sessions::NewSession;
pub use surface::{ApiSurface, ApiSurfaceParseError};
pub use users::{
    FederatedUser, FederatedUserRecord, NewUser, User, UserProfilePatch, UserWithOrganization,
};

#[cfg(test)]
mod tests;
