use pplx_space_common_core::types::{
    ArtifactComponentMetadata, GuestPlatform, RuntimeIdentity, StateFormatIdentity,
};
use pplx_space_execution_common::proto;
use tonic::Status;

#[derive(Debug)]
struct StateFormatCapability {
    identity: StateFormatIdentity,
    can_create: bool,
    can_load: bool,
}

#[derive(Debug)]
struct RuntimeInstallation {
    identity: RuntimeIdentity,
    guests: Vec<GuestPlatform>,
    state_formats: Vec<StateFormatCapability>,
}

#[derive(Debug)]
struct ComponentInstallation {
    component: ArtifactComponentMetadata,
    default_for_new: bool,
    legacy_default: bool,
}

#[derive(Debug)]
pub(crate) struct ValidatedExecutionInventory {
    runtimes: Vec<RuntimeInstallation>,
    components: Vec<ComponentInstallation>,
}

impl ValidatedExecutionInventory {
    pub(crate) fn try_from_proto(
        inventory: &proto::NodeExecutionInventory,
    ) -> Result<Self, Status> {
        let runtimes = inventory
            .runtimes
            .iter()
            .map(RuntimeInstallation::try_from_proto)
            .collect::<Result<_, _>>()?;
        let components = inventory
            .components
            .iter()
            .map(ComponentInstallation::try_from_proto)
            .collect::<Result<_, _>>()?;
        Ok(Self {
            runtimes,
            components,
        })
    }

    pub(crate) fn into_legacy_rows(self) -> (Vec<serde_json::Value>, Vec<serde_json::Value>) {
        (
            self.runtimes
                .into_iter()
                .map(RuntimeInstallation::into_json)
                .collect(),
            self.components
                .into_iter()
                .map(ComponentInstallation::into_json)
                .collect(),
        )
    }
}

impl RuntimeInstallation {
    fn try_from_proto(installation: &proto::RuntimeInstallation) -> Result<Self, Status> {
        let identity = installation
            .runtime
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("runtime installation is missing identity"))
            .and_then(|runtime| {
                RuntimeIdentity::try_from(runtime)
                    .map_err(|error| Status::invalid_argument(error.to_string()))
            })?;
        let guests = installation
            .supported_guests
            .iter()
            .map(|guest| {
                GuestPlatform::try_from(guest)
                    .map_err(|error| Status::invalid_argument(error.to_string()))
            })
            .collect::<Result<_, _>>()?;
        let state_formats = installation
            .state_formats
            .iter()
            .map(StateFormatCapability::try_from_proto)
            .collect::<Result<_, _>>()?;
        Ok(Self {
            identity,
            guests,
            state_formats,
        })
    }

    fn into_json(self) -> serde_json::Value {
        let guests = self
            .guests
            .into_iter()
            .map(|guest| {
                serde_json::json!({
                    "guest_os": guest.os.as_str(),
                    "guest_arch": guest.architecture.as_str(),
                })
            })
            .collect::<Vec<_>>();
        let state_formats = self
            .state_formats
            .into_iter()
            .map(|capability| {
                serde_json::json!({
                    "state_format_name": capability.identity.format.as_str(),
                    "state_format_version": capability.identity.version.as_str(),
                    "can_create": capability.can_create,
                    "can_load": capability.can_load,
                })
            })
            .collect::<Vec<_>>();
        serde_json::json!({
            "runtime": self.identity.kind.alias(),
            "runtime_version": self.identity.version.as_str(),
            "guests": guests,
            "state_formats": state_formats,
        })
    }
}

impl StateFormatCapability {
    fn try_from_proto(capability: &proto::StateFormatCapability) -> Result<Self, Status> {
        let identity = capability.identity.as_ref().ok_or_else(|| {
            Status::invalid_argument("state format capability is missing identity")
        })?;
        let identity = StateFormatIdentity::try_from(identity)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        Ok(Self {
            identity,
            can_create: capability.can_create,
            can_load: capability.can_load,
        })
    }
}

impl ComponentInstallation {
    fn try_from_proto(installation: &proto::ArtifactComponentInstallation) -> Result<Self, Status> {
        let component = installation.component.as_ref().ok_or_else(|| {
            Status::invalid_argument("artifact component installation is missing component")
        })?;
        let component = ArtifactComponentMetadata::try_from(component)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        Ok(Self {
            component,
            default_for_new: installation.default_for_new,
            legacy_default: installation.legacy_default,
        })
    }

    fn into_json(self) -> serde_json::Value {
        let component = self.component;
        serde_json::json!({
            "kind": component.kind.as_str(),
            "role_key": component.role.as_ref().map_or("", |role| role.as_str()),
            "version_key": component.version.as_ref().map(ToString::to_string).unwrap_or_default(),
            "digest_key": component.digest.as_ref().map(ToString::to_string).unwrap_or_default(),
            "format_key": component.format.as_ref().map(ToString::to_string).unwrap_or_default(),
            "version": component.version.as_ref().map(ToString::to_string),
            "digest": component.digest.as_ref().map(ToString::to_string),
            "format": component.format.as_ref().map(ToString::to_string),
            "default_for_new": self.default_for_new,
            "legacy_default": self.legacy_default,
        })
    }
}

#[cfg(test)]
mod tests;
