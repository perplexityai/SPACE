//! Node publication: validation is separated from transactional persistence.

pub(crate) mod diagnostics;
mod errors;
pub(crate) mod outcome;
pub(crate) mod store;
pub(crate) mod validate;
