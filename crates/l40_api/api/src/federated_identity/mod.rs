pub mod config;
pub mod grants;
mod identity;
pub mod ingress;
mod store;

pub(crate) use identity::{
    FederatedOwnerRecord, MaterializeOwnerError, ResolvePrincipalError, materialize_owner,
    resolve_principal,
};

pub use config::*;
pub use grants::*;
pub use ingress::*;

#[cfg(test)]
mod schema_tests;
