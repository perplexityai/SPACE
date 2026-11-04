//! Account and tenancy identity types: user, org, node, job, API key,
//! credential and publication-grant IDs, sandbox owners, emails, org slugs,
//! user roles and credential keys (`auth`), and the federated-identity
//! upstream IDs and sandbox owner source (`federated_identity`). They carry
//! only their own validation rules and storage codecs.
//!
//! They are their own crate so that code can name them without depending on
//! `pplx-space-common-core`. `pplx-space-common-core` re-exports them from
//! `types::auth` and `types::federated_identity`.

pub mod auth;
pub mod federated_identity;
#[cfg(feature = "rusqlite")]
mod sqlite;

pub use auth::*;
pub use federated_identity::*;
