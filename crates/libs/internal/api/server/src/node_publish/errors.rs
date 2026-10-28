use tonic::Status;

use super::store::ApplyPublishError;
use crate::published_state::ApplySandboxClaimsError;

// Keep transport mapping at the RPC boundary. Legacy operations retain their
// existing status unchanged until they acquire operation-specific error types.
impl From<ApplyPublishError> for Status {
    fn from(error: ApplyPublishError) -> Self {
        match error {
            ApplyPublishError::Legacy(error) => error,
            ApplyPublishError::Sandboxes(error) => error.into(),
            error @ (ApplyPublishError::Begin(_) | ApplyPublishError::Commit(_)) => {
                Self::internal(error.to_string())
            }
        }
    }
}

impl From<ApplySandboxClaimsError> for Status {
    fn from(error: ApplySandboxClaimsError) -> Self {
        match error {
            ApplySandboxClaimsError::Legacy(error) => error,
            error => Self::internal(error.to_string()),
        }
    }
}
