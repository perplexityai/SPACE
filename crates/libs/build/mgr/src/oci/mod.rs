mod assembly;
pub mod bundle;
mod containerd;
mod guest_seed;
mod registry;
mod storage;

pub(crate) use assembly::assemble_from_mounted_image;
pub use assembly::{
    OciImageAssemblyError, OciImageAssemblyRequest, RootfsAssemblyError, assemble_from_image,
};
pub use containerd::{
    OciBuildError, OciCommandFailure, OciFileOperation, OciMountRequest, OciOperation, OciRootfs,
    mount,
};
pub use guest_seed::GuestSeedError;
pub(crate) use guest_seed::GuestSeeds;
#[cfg(feature = "aws-ecr")]
pub use registry::EcrConfigurationError;
pub use storage::OciStorageError;

#[cfg(feature = "aws-ecr")]
pub fn validate_registry_configuration() -> Result<(), EcrConfigurationError> {
    registry::validate_configuration()
}
