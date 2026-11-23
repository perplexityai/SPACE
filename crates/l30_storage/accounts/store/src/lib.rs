//! Owning store for accounts state: `organizations`, `users`, `api_keys`,
//! console `sessions`, and the singleton `clusters` row naming the managing
//! organization.
//!
//! Each statement is an executor-generic free function, so it runs in the
//! caller's transaction when there is one, and returns `sqlx::Error`
//! unchanged, so callers keep classifying unique and foreign-key violations
//! themselves. The row types, and the `users.role` and
//! `api_keys.allowed_surfaces` column types, are the API's account models.
//!
//! Some statements on these tables stay with their callers, which keep their
//! locks and transactions: guarded writes whose `WHERE` re-checks the row's
//! state, locking reads (`FOR UPDATE`, `FOR SHARE`), and reads that compare
//! with the database clock. Others have not moved here yet.

pub mod api_keys;
pub mod clusters;
pub mod organizations;
mod role;
pub mod sessions;
mod surface;
pub mod users;

pub use api_keys::{ApiKey, ApiKeyAuth, ApiKeyRow, CreateApiKey};
pub use clusters::Cluster;
pub use organizations::{FederatedOrganization, NewOrganization, Organization, OrganizationPatch};
pub use role::{UserRole, UserRoleParseError};
pub use sessions::NewSession;
pub use surface::{ApiSurface, ApiSurfaceParseError};
pub use users::{FederatedUser, FederatedUserRecord, NewUser, User, UserProfilePatch};

#[cfg(test)]
mod tests;
