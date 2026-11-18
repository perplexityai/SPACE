//! Subject identity for credential exchange and resource scope.
//!
//! These types are their own crate so that code can name them without
//! depending on `pplx-space-common-core`.

mod subject;

pub use subject::{SubjectId, SubjectIdParseError, SubjectKind};
