use pplx_space_byte_size::SizeParseError;
use pplx_space_content_digest::DigestParseError;
use pplx_space_execution_spec::{ComputeParseError, DiskIoLimitsError};
use pplx_space_postgres_pool::DbError;
use pplx_space_template_model::TemplateParseError;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    #[error("invalid pool slot state: {0}")]
    InvalidSlotState(String),
    #[error("invalid pool slot transition from {from} to {to}")]
    InvalidSlotTransition {
        from: &'static str,
        to: &'static str,
    },
    #[error("pool slot {slot_id} is not warming")]
    SlotNotWarming { slot_id: Uuid },
    #[error(
        "encryptable pool configs cannot be created yet: twin rows are \
         blocked until the legacy release/shape index is dropped and \
         warm-pool members carry the encryptable member shape"
    )]
    EncryptableConfigFenced,
    #[error("invalid pool resource field {field}: {value}")]
    InvalidResource { field: &'static str, value: i32 },
    #[error("invalid pool disk I/O field {field}: {value}")]
    InvalidDiskIo { field: &'static str, value: i64 },
    #[error(transparent)]
    DiskIoLimits(#[from] DiskIoLimitsError),
    #[error(transparent)]
    TemplateName(#[from] TemplateParseError),
    #[error(transparent)]
    ContentHash(#[from] DigestParseError),
    #[error(transparent)]
    Compute(#[from] ComputeParseError),
    #[error(transparent)]
    Size(#[from] SizeParseError),
    #[error(transparent)]
    Sql(#[from] DbError),
}

/// Lets a verb's `?` turn the driver's error into this one, losslessly,
/// through [`DbError`]'s own conversion.
impl From<sqlx::Error> for PoolError {
    fn from(error: sqlx::Error) -> Self {
        Self::Sql(DbError::from(error))
    }
}

pub type Result<T> = std::result::Result<T, PoolError>;
