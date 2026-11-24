//! Account and tenancy identity types: user, org, node, API key, credential
//! and publication-grant IDs, sandbox owners, emails, org slugs, user roles
//! and credential keys (`auth`); the federated-identity upstream IDs and
//! sandbox owner source (`federated_identity`); and the ID of the client whose
//! activation created a sandbox's runtime (`client_id`). They carry only their
//! own validation rules and storage codecs. `auth` also re-exports the job ID
//! from `pplx-space-job-id` and the ID parse error from `pplx-space-uuid-id`.

pub mod auth;
pub mod client_id;
pub mod federated_identity;

pub use auth::*;
pub use client_id::*;
pub use federated_identity::*;
