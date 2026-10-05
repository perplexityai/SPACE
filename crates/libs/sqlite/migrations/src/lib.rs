#[derive(Debug, thiserror::Error)]
#[error("SQLite migration compatibility check failed: {0}")]
pub struct MigrationCompatibilityError(
    #[source] Box<dyn std::error::Error + Send + Sync + 'static>,
);

impl MigrationCompatibilityError {
    pub fn new(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self(Box::new(source))
    }
}
