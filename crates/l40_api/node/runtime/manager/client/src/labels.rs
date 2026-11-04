//! Short names for the protocol's enum values.
//!
//! A prost enum field is an `i32`, and its generated name carries the enum's
//! prefix (`EXECUTION_ARTIFACT_KIND_RUNTIME`). JSON output and the CLI show the
//! suffix in kebab case (`runtime`), which matches the runtime-core
//! vocabulary's `as_str`. A value this build does not know renders as
//! `unknown-<n>` rather than failing, so a newer server stays readable.

/// A generated protocol enum with a common value prefix.
pub trait ProtoEnumLabel: TryFrom<i32> {
    const PREFIX: &'static str;
    fn proto_name(&self) -> &'static str;
}

macro_rules! proto_enum_labels {
    ($($ty:path => $prefix:literal, $serializer:ident;)+) => {
        $(
            impl ProtoEnumLabel for $ty {
                const PREFIX: &'static str = $prefix;

                fn proto_name(&self) -> &'static str {
                    self.as_str_name()
                }
            }
        )+

        /// `serialize_with` targets named by the build script.
        pub(crate) mod serialize {
            pub(crate) fn runtime_identities<S: serde::Serializer>(
                values: &[crate::execution::RuntimeIdentity], serializer: S,
            ) -> Result<S::Ok, S::Error> {
                #[derive(serde::Serialize)]
                struct Identity<'a> { kind: String, version: &'a str }
                use serde::Serialize as _;
                values.iter().map(|value| Identity {
                    kind: super::label::<crate::execution::RuntimeKind>(value.kind),
                    version: &value.version,
                }).collect::<Vec<_>>().serialize(serializer)
            }
            $(
                pub(crate) fn $serializer<S: serde::Serializer>(
                    value: &i32,
                    serializer: S,
                ) -> Result<S::Ok, S::Error> {
                    serializer.serialize_str(&super::label::<$ty>(*value))
                }
            )+
        }
    };
}

proto_enum_labels! {
    crate::proto::ExecutionArtifactKind => "EXECUTION_ARTIFACT_KIND_", execution_artifact_kind;
    crate::proto::ArtifactLocation => "ARTIFACT_LOCATION_", artifact_location;
    crate::proto::ArtifactProvenance => "ARTIFACT_PROVENANCE_", artifact_provenance;
    crate::proto::ExecutionArtifactVersionSource => "EXECUTION_ARTIFACT_VERSION_SOURCE_", execution_artifact_version_source;
    crate::proto::ExecutionArtifactStatus => "EXECUTION_ARTIFACT_STATUS_", execution_artifact_status;
    crate::proto::ArtifactManagement => "ARTIFACT_MANAGEMENT_", artifact_management;
    crate::proto::ArtifactProvider => "ARTIFACT_PROVIDER_", artifact_provider;
    crate::proto::ScanRootPresence => "SCAN_ROOT_PRESENCE_", scan_root_presence;
    crate::proto::ScanIssueKind => "SCAN_ISSUE_KIND_", scan_issue_kind;
    crate::proto::ProtectedPathRole => "PROTECTED_PATH_ROLE_", protected_path_role;
    crate::proto::PathProtection => "PATH_PROTECTION_", path_protection;
    crate::proto::ArtifactPresence => "ARTIFACT_PRESENCE_", artifact_presence;
    crate::proto::ArtifactAction => "ARTIFACT_ACTION_", artifact_action;
    crate::proto::OperationState => "OPERATION_STATE_", operation_state;
    crate::proto::OperationOutcome => "OPERATION_OUTCOME_", operation_outcome;
    crate::execution::RuntimeKind => "RUNTIME_KIND_", runtime_kind;
    crate::execution::ArtifactComponentKind => "ARTIFACT_COMPONENT_KIND_", artifact_component_kind;
}

/// The short kebab-case name of `value` as an `E`.
pub fn label<E: ProtoEnumLabel>(value: i32) -> String {
    match E::try_from(value) {
        Ok(known) => {
            let name = known.proto_name();
            name.strip_prefix(E::PREFIX)
                .unwrap_or(name)
                .to_ascii_lowercase()
                .replace('_', "-")
        }
        Err(_) => format!("unknown-{value}"),
    }
}

#[cfg(test)]
mod tests;
