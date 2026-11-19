#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error(transparent)]
    Transport(#[from] crate::UdsConnectError),
}
