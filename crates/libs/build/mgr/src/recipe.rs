use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;

use oci_spec::distribution::{ParseError as OciReferenceParseError, Reference as OciReference};
use pplx_space_build_mgr_client::proto as build_proto;
use pplx_space_common_core::types::{
    ArtifactVersion, ArtifactVersionParseError, Digest, DigestParseError, EnvKey, EnvMap,
    EnvParseError, TemplateReference, TemplateReferenceParseError, TemplateRuntime,
    TemplateRuntimeError, TemplateSelector,
};
use serde::Deserialize;

const MIB: u64 = 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum RecipeError {
    #[error("cannot read recipe {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("cannot parse recipe {path}: {source}")]
    Parse {
        path: String,
        #[source]
        source: toml::de::Error,
    },
    #[error("cannot adapt recipe {path}: {source}")]
    Adapt {
        path: String,
        #[source]
        source: RecipeAdapterError,
    },
    #[error("invalid recipe {path}: {source}")]
    Invalid {
        path: String,
        #[source]
        source: RecipeProtoError,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum RecipeAdapterError {
    #[error("recipe metadata.base is required when building from a template-backed rootfs")]
    MissingBase,
    #[error("recipe metadata.image is required when building from an OCI image")]
    MissingImage,
    #[error("[source] is required when building from scratch")]
    MissingSource,
    #[error("[source] is only supported when building from scratch")]
    UnexpectedSource,
    #[error("[guest_kernel] is only supported when building from scratch")]
    UnexpectedGuestKernel,
    #[error("recipe metadata.image is only supported when building from an OCI image")]
    UnexpectedImage,
    #[error("--base is only supported for recipes that build from a template")]
    BaseOverrideForScratch,
    #[error("unsupported scratch source type: {value}")]
    UnknownSourceType { value: String },
    #[error("copy step {index} needs a payload from source {source_path}")]
    MissingCopyPayload { index: usize, source_path: String },
    #[error("memory_mib {value} overflows memory_bytes")]
    MemoryOverflow { value: u32 },
    #[error("disk_size_mb {value} overflows disk_bytes")]
    DiskOverflow { value: u32 },
}

#[derive(Debug, thiserror::Error)]
pub enum RecipeProtoError {
    #[error("runtime declaration must be the only runtime step, last, with no env_refs")]
    RuntimePlacement,
    #[error("invalid runtime declaration: {0}")]
    Runtime(#[from] TemplateRuntimeError),
    #[error("recipe step {index} {field}: {source}")]
    Environment {
        index: usize,
        field: &'static str,
        #[source]
        source: EnvParseError,
    },
    #[error("recipe metadata is required")]
    MissingMetadata,
    #[error("recipe build_config balloon mode is required")]
    MissingBalloonMode,
    #[error("recipe build_config balloon stats_polling_interval_s {value} exceeds 65535")]
    BalloonStatsIntervalOutOfRange { value: u32 },
    #[error("recipe build_config balloon inflate amount_mib must be greater than 0")]
    ZeroBalloonAmount,
    #[error("recipe origin is required")]
    MissingOrigin,
    #[error("recipe origin kind is required")]
    MissingOriginKind,
    #[error("scratch recipe source is required")]
    MissingScratchSource,
    #[error("scratch recipe source kind is required")]
    MissingScratchSourceKind,
    #[error("scratch recipe source {field} {value:?} is invalid; expected {}", field.expected())]
    InvalidScratchSource {
        field: ScratchSourceField,
        value: String,
    },
    #[error("pinned scratch recipe guest kernel is required")]
    MissingScratchGuestKernel,
    #[error("OCI image reference {value:?} is invalid: {source}")]
    InvalidOciImageReference {
        value: String,
        #[source]
        source: OciReferenceParseError,
    },
    #[error("recipe step {index} is missing a step body")]
    MissingStep { index: usize },
    #[error("recipe step {index} copy payload is required")]
    MissingCopyPayload { index: usize },
    #[error("recipe step {index} package manager is unspecified")]
    MissingPackageManager { index: usize },
    #[error("recipe step {index} has unknown package manager value {value}")]
    UnknownPackageManager { index: usize, value: i32 },
    #[error("template origin base is required")]
    EmptyBase,
    #[error("template origin base {value:?} is invalid: {source}")]
    InvalidBase {
        value: String,
        #[source]
        source: TemplateReferenceParseError,
    },
    #[error("template origin base {value:?} must use a tag or full artifact digest")]
    BaseDigestPrefix { value: String },
    #[error("recipe step {index} run command is required")]
    EmptyRunCommand { index: usize },
    #[error("recipe step {index} copy dest is required")]
    EmptyCopyDest { index: usize },
    #[error("recipe step {index} copy dest must be an absolute guest path")]
    RelativeCopyDest { index: usize },
    #[error("recipe step {index} copy blob sha256 is required")]
    EmptyBlobSha256 { index: usize },
    #[error("recipe step {index} copy blob sha256 is invalid")]
    InvalidBlobSha256 { index: usize },
    #[error("recipe build_config.vcpu {value} exceeds u8::MAX")]
    VcpuOutOfRange { value: u32 },
    #[error("recipe build_config.memory_bytes {value} exceeds u32 MiB range")]
    MemoryOutOfRange { value: u64 },
    #[error("recipe build_config.disk_bytes {value} exceeds u32 MiB range")]
    DiskOutOfRange { value: u64 },
    #[error("scratch guest kernel version {value:?} is invalid: {source}")]
    InvalidGuestKernelVersion {
        value: String,
        #[source]
        source: ArtifactVersionParseError,
    },
    #[error("scratch guest kernel digest {value:?} is invalid: {source}")]
    InvalidGuestKernelDigest {
        value: String,
        #[source]
        source: DigestParseError,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Recipe {
    pub metadata: RecipeMetadata,
    pub build_config: BuildConfig,
    pub origin: Origin,
    pub steps: Vec<NamedStep>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecipeMetadata {
    pub description: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BuildConfig {
    pub vcpu: Option<u8>,
    pub memory_mib: Option<u32>,
    pub disk_size_mb: Option<u32>,
    /// Recorded in the built template's metadata for its sandboxes'
    /// cold boots; the build VM itself boots with its base's setting.
    pub balloon: Option<pplx_space_common_core::types::BalloonConfig>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Origin {
    FromTemplate {
        base: TemplateReference,
        base_raw: String,
    },
    FromOciImage {
        base: TemplateReference,
        base_raw: String,
        reference: OciReference,
    },
    FromScratch {
        source: SourceConfig,
        guest_kernel: Option<GuestKernelPin>,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct GuestKernelPin {
    pub version: ArtifactVersion,
    pub digest: Digest,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceConfig {
    pub release: String,
    pub version: String,
    pub date: String,
}

/// Wrapper that adds an optional human-readable name to any step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamedStep {
    /// User-supplied step name, retained as recipe data but omitted from diagnostics.
    pub display_name: Option<String>,
    /// Guest names resolved from the operator's SPACE_BUILD_EXPORT_<NAME> values.
    pub env_refs: Vec<EnvKey>,
    pub step: Step,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    SetRuntime(TemplateRuntime),
    ClearRuntime,
    PackageInstall {
        manager: PackageManager,
        packages: Vec<String>,
        extra_args: Vec<String>,
    },
    Run {
        command: String,
        user: Option<String>,
    },
    Env {
        vars: EnvMap,
    },
    Copy {
        payload: CopyPayload,
        dest: String,
        source_path: Option<String>,
    },
    PlaywrightInstall {
        browsers: Vec<String>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PackageManager {
    Apt,
    Pip,
    Npm,
    NpmGlobal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CopyPayload {
    InlineBytes(Vec<u8>),
    BlobSha256(String),
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TomlRecipe {
    pub metadata: TomlRecipeMetadata,
    #[serde(default)]
    pub source: Option<TomlSourceConfig>,
    #[serde(default)]
    pub guest_kernel: Option<GuestKernelPin>,
    #[serde(default)]
    pub steps: Vec<TomlNamedStep>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct TomlNamedStep {
    pub name: Option<String>,
    #[serde(default)]
    pub env_refs: Vec<String>,
    #[serde(flatten)]
    pub step: TomlStep,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct TomlRecipeMetadata {
    pub name: String,
    pub description: Option<String>,
    #[serde(default, rename = "from")]
    pub build_from: TomlBuildFrom,
    pub base: Option<String>,
    pub image: Option<String>,
    pub vcpu: Option<u8>,
    pub memory_mib: Option<u32>,
    pub disk_size_mb: Option<u32>,
    /// Same shape as template `metadata.json` `balloon`.
    pub balloon: Option<pplx_space_common_core::types::BalloonConfig>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum TomlBuildFrom {
    #[default]
    Template,
    Scratch,
    OciImage,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct TomlSourceConfig {
    #[serde(rename = "type")]
    pub source_type: String,
    pub release: String,
    pub version: String,
    pub date: String,
}

fn default_user() -> Option<String> {
    Some("root".into())
}

fn default_browsers() -> Vec<String> {
    vec!["chromium".into()]
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum TomlStep {
    SetRuntime {
        declaration: TemplateRuntime,
    },
    ClearRuntime,
    AptInstall {
        packages: Vec<String>,
    },
    PipInstall {
        packages: Vec<String>,
        #[serde(default)]
        extra_args: Vec<String>,
    },
    NpmInstall {
        packages: Vec<String>,
    },
    NpmInstallGlobal {
        packages: Vec<String>,
    },
    Run {
        command: String,
        #[serde(default = "default_user")]
        user: Option<String>,
    },
    Env {
        /// Literal key-value pairs. TOML callers resolve placeholders here.
        vars: HashMap<String, String>,
    },
    Copy {
        source: String,
        dest: String,
    },
    PlaywrightInstall {
        #[serde(default = "default_browsers")]
        browsers: Vec<String>,
    },
}

impl TomlRecipe {
    pub fn from_file(path: &str) -> Result<Self, RecipeError> {
        let content = std::fs::read_to_string(path).map_err(|source| RecipeError::Read {
            path: path.to_string(),
            source,
        })?;
        toml::from_str(&content).map_err(|source| RecipeError::Parse {
            path: path.to_string(),
            source,
        })
    }

    pub fn into_proto<F>(
        self,
        path: &str,
        base_override: Option<&str>,
        mut copy_payload: F,
    ) -> Result<build_proto::Recipe, RecipeError>
    where
        F: FnMut(usize, &str) -> Result<build_proto::recipe::copy::Payload, RecipeAdapterError>,
    {
        self.try_into_proto(base_override, &mut copy_payload)
            .map_err(|source| RecipeError::Adapt {
                path: path.to_string(),
                source,
            })
    }

    fn try_into_proto<F>(
        self,
        base_override: Option<&str>,
        copy_payload: &mut F,
    ) -> Result<build_proto::Recipe, RecipeAdapterError>
    where
        F: FnMut(usize, &str) -> Result<build_proto::recipe::copy::Payload, RecipeAdapterError>,
    {
        let origin = match self.metadata.build_from {
            TomlBuildFrom::Template => {
                if self.source.is_some() {
                    return Err(RecipeAdapterError::UnexpectedSource);
                }
                if self.guest_kernel.is_some() {
                    return Err(RecipeAdapterError::UnexpectedGuestKernel);
                }
                if self.metadata.image.is_some() {
                    return Err(RecipeAdapterError::UnexpectedImage);
                }
                let base = base_override
                    .map(str::to_string)
                    .or(self.metadata.base)
                    .ok_or(RecipeAdapterError::MissingBase)?;
                build_proto::recipe::Origin {
                    kind: Some(build_proto::recipe::origin::Kind::FromTemplate(
                        build_proto::recipe::origin::FromTemplate { base },
                    )),
                }
            }
            TomlBuildFrom::Scratch => {
                if base_override.is_some() {
                    return Err(RecipeAdapterError::BaseOverrideForScratch);
                }
                if self.metadata.image.is_some() {
                    return Err(RecipeAdapterError::UnexpectedImage);
                }
                let source = self.source.ok_or(RecipeAdapterError::MissingSource)?;
                let source = Some(source.try_into()?);
                let kind = match self.guest_kernel {
                    Some(guest_kernel) => build_proto::recipe::origin::Kind::PinnedFromScratch(
                        build_proto::recipe::origin::PinnedFromScratch {
                            source,
                            guest_kernel: Some(guest_kernel.into()),
                        },
                    ),
                    None => build_proto::recipe::origin::Kind::FromScratch(
                        build_proto::recipe::origin::FromScratch { source },
                    ),
                };
                build_proto::recipe::Origin { kind: Some(kind) }
            }
            TomlBuildFrom::OciImage => {
                if self.source.is_some() {
                    return Err(RecipeAdapterError::UnexpectedSource);
                }
                if self.guest_kernel.is_some() {
                    return Err(RecipeAdapterError::UnexpectedGuestKernel);
                }
                let base = base_override
                    .map(str::to_string)
                    .or(self.metadata.base)
                    .ok_or(RecipeAdapterError::MissingBase)?;
                let reference = self
                    .metadata
                    .image
                    .ok_or(RecipeAdapterError::MissingImage)?;
                build_proto::recipe::Origin {
                    kind: Some(build_proto::recipe::origin::Kind::FromOciImage(
                        build_proto::recipe::origin::FromOciImage { base, reference },
                    )),
                }
            }
        };

        Ok(build_proto::Recipe {
            metadata: Some(build_proto::recipe::Metadata {
                description: self.metadata.description,
            }),
            build_config: Some(build_proto::recipe::BuildConfig {
                vcpu: self.metadata.vcpu.map(u32::from),
                memory_bytes: self
                    .metadata
                    .memory_mib
                    .map(mib_to_bytes)
                    .transpose()
                    .map_err(|_overflow| RecipeAdapterError::MemoryOverflow {
                        value: self.metadata.memory_mib.unwrap_or_default(),
                    })?,
                disk_bytes: self
                    .metadata
                    .disk_size_mb
                    .map(mib_to_bytes)
                    .transpose()
                    .map_err(|_overflow| RecipeAdapterError::DiskOverflow {
                        value: self.metadata.disk_size_mb.unwrap_or_default(),
                    })?,
                balloon: self.metadata.balloon.map(balloon_proto),
            }),
            origin: Some(origin),
            steps: self
                .steps
                .into_iter()
                .enumerate()
                .map(|(index, step)| step.into_proto(index + 1, copy_payload))
                .collect::<Result<_, _>>()?,
        })
    }
}

/// Stand-in blob digest for copy steps when a recipe is checked without uploading its sources.
const CHECK_BLOB_SHA256: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// A recipe file that passed [`check_recipe_file`], reduced to plain values.
///
/// This is a supported interface for code outside this crate that keeps its own recipes, such as
/// downstream template repositories and their tests. Prefer it over the TOML and proto adapter
/// types, which change with the build pipeline. Changes to this type or to
/// [`check_recipe_file`] are API changes.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct CheckedRecipe {
    /// Base template reference as written, for template and OCI-image recipes. `None` for
    /// scratch recipes.
    pub base: Option<String>,
    /// Steps in recipe order.
    pub steps: Vec<CheckedStep>,
}

/// One step of a [`CheckedRecipe`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CheckedStep {
    Run {
        command: String,
        user: Option<String>,
    },
    Copy {
        /// Source path as written in the recipe.
        source: String,
        /// Absolute guest destination.
        dest: String,
    },
    /// Any other step, named by its recipe `type`, such as `apt-install`.
    Other { kind: &'static str },
}

/// Checks a recipe file the way a build does and returns its steps.
///
/// The file is parsed, adapted to the build request, and validated as the build manager validates
/// a received request, so `Ok` means a build would accept the recipe. Copy sources are not read or
/// uploaded; each copy step gets a stand-in payload.
pub fn check_recipe_file(path: &Path) -> Result<CheckedRecipe, RecipeError> {
    let display_path = path.display().to_string();
    let content = std::fs::read_to_string(path).map_err(|source| RecipeError::Read {
        path: display_path.clone(),
        source,
    })?;
    let toml_recipe: TomlRecipe =
        toml::from_str(&content).map_err(|source| RecipeError::Parse {
            path: display_path.clone(),
            source,
        })?;
    let base = match toml_recipe.metadata.build_from {
        TomlBuildFrom::Template | TomlBuildFrom::OciImage => toml_recipe.metadata.base.clone(),
        TomlBuildFrom::Scratch => None,
    };
    let steps = toml_recipe
        .steps
        .iter()
        .map(|named| match &named.step {
            TomlStep::SetRuntime { .. } => CheckedStep::Other {
                kind: "set-runtime",
            },
            TomlStep::ClearRuntime => CheckedStep::Other {
                kind: "clear-runtime",
            },
            TomlStep::Run { command, user } => CheckedStep::Run {
                command: command.clone(),
                user: user.clone(),
            },
            TomlStep::Copy { source, dest } => CheckedStep::Copy {
                source: source.clone(),
                dest: dest.clone(),
            },
            TomlStep::AptInstall { .. } => CheckedStep::Other {
                kind: "apt-install",
            },
            TomlStep::PipInstall { .. } => CheckedStep::Other {
                kind: "pip-install",
            },
            TomlStep::NpmInstall { .. } => CheckedStep::Other {
                kind: "npm-install",
            },
            TomlStep::NpmInstallGlobal { .. } => CheckedStep::Other {
                kind: "npm-install-global",
            },
            TomlStep::Env { .. } => CheckedStep::Other { kind: "env" },
            TomlStep::PlaywrightInstall { .. } => CheckedStep::Other {
                kind: "playwright-install",
            },
        })
        .collect();
    // Report the file as written, but only after it passes every check a build applies.
    let proto = toml_recipe.into_proto(&display_path, None, |_index, _source| {
        Ok(build_proto::recipe::copy::Payload::BlobSha256(
            CHECK_BLOB_SHA256.to_string(),
        ))
    })?;
    Recipe::try_from(proto).map_err(|source| RecipeError::Invalid {
        path: display_path,
        source,
    })?;
    Ok(CheckedRecipe { base, steps })
}

impl TomlNamedStep {
    fn into_proto<F>(
        self,
        index: usize,
        copy_payload: &mut F,
    ) -> Result<build_proto::recipe::Step, RecipeAdapterError>
    where
        F: FnMut(usize, &str) -> Result<build_proto::recipe::copy::Payload, RecipeAdapterError>,
    {
        Ok(build_proto::recipe::Step {
            display_name: self.name,
            env_refs: self.env_refs,
            body: Some(match self.step {
                TomlStep::SetRuntime { declaration } => {
                    build_proto::recipe::step::Body::SetRuntime(
                        pplx_space_build_mgr_client::runtime::encode(&declaration),
                    )
                }
                TomlStep::ClearRuntime => build_proto::recipe::step::Body::ClearRuntime(
                    build_proto::recipe::ClearRuntime {},
                ),
                TomlStep::AptInstall { packages } => {
                    build_proto::recipe::step::Body::PackageInstall(
                        build_proto::recipe::PackageInstall {
                            manager: build_proto::recipe::PackageManager::Apt as i32,
                            packages,
                            extra_args: vec![],
                        },
                    )
                }
                TomlStep::PipInstall {
                    packages,
                    extra_args,
                } => build_proto::recipe::step::Body::PackageInstall(
                    build_proto::recipe::PackageInstall {
                        manager: build_proto::recipe::PackageManager::Pip as i32,
                        packages,
                        extra_args,
                    },
                ),
                TomlStep::NpmInstall { packages } => {
                    build_proto::recipe::step::Body::PackageInstall(
                        build_proto::recipe::PackageInstall {
                            manager: build_proto::recipe::PackageManager::Npm as i32,
                            packages,
                            extra_args: vec![],
                        },
                    )
                }
                TomlStep::NpmInstallGlobal { packages } => {
                    build_proto::recipe::step::Body::PackageInstall(
                        build_proto::recipe::PackageInstall {
                            manager: build_proto::recipe::PackageManager::NpmGlobal as i32,
                            packages,
                            extra_args: vec![],
                        },
                    )
                }
                TomlStep::Run { command, user } => {
                    build_proto::recipe::step::Body::Run(build_proto::recipe::Run { command, user })
                }
                TomlStep::Env { vars } => {
                    build_proto::recipe::step::Body::Env(build_proto::recipe::Env {
                        vars: vars
                            .into_iter()
                            .map(|(key, value)| (key, resolve_host_env_refs(&value)))
                            .collect(),
                    })
                }
                TomlStep::Copy { source, dest } => {
                    build_proto::recipe::step::Body::Copy(build_proto::recipe::Copy {
                        payload: Some(copy_payload(index, &source)?),
                        dest,
                        source_path: None,
                    })
                }
                TomlStep::PlaywrightInstall { browsers } => {
                    build_proto::recipe::step::Body::PlaywrightInstall(
                        build_proto::recipe::PlaywrightInstall { browsers },
                    )
                }
            }),
        })
    }
}

impl TryFrom<TomlSourceConfig> for build_proto::recipe::Source {
    type Error = RecipeAdapterError;

    fn try_from(source: TomlSourceConfig) -> Result<Self, Self::Error> {
        let kind = match source.source_type.as_str() {
            "ubuntu-minimal-cloud-squashfs" => Some(
                build_proto::recipe::source::Kind::UbuntuMinimalCloudSquashfs(
                    build_proto::recipe::source::UbuntuMinimalCloudSquashfs {
                        release: source.release,
                        version: source.version,
                        date: source.date,
                    },
                ),
            ),
            _ => {
                return Err(RecipeAdapterError::UnknownSourceType {
                    value: source.source_type,
                });
            }
        };
        Ok(Self { kind })
    }
}

impl From<GuestKernelPin> for build_proto::recipe::origin::GuestKernel {
    fn from(guest_kernel: GuestKernelPin) -> Self {
        Self {
            version: guest_kernel.version.to_string(),
            digest: guest_kernel.digest.to_string(),
        }
    }
}

impl TryFrom<build_proto::Recipe> for Recipe {
    type Error = RecipeProtoError;

    fn try_from(recipe: build_proto::Recipe) -> Result<Self, Self::Error> {
        for (index, step) in recipe.steps.iter().enumerate() {
            if matches!(
                step.body,
                Some(
                    build_proto::recipe::step::Body::SetRuntime(_)
                        | build_proto::recipe::step::Body::ClearRuntime(_)
                )
            ) && (index + 1 != recipe.steps.len() || !step.env_refs.is_empty())
            {
                return Err(RecipeProtoError::RuntimePlacement);
            }
        }
        let metadata = recipe.metadata.ok_or(RecipeProtoError::MissingMetadata)?;
        let build_config = recipe.build_config.unwrap_or_default();
        Ok(Self {
            metadata: RecipeMetadata {
                description: empty_to_none(metadata.description),
            },
            build_config: BuildConfig::try_from(build_config)?,
            origin: recipe
                .origin
                .ok_or(RecipeProtoError::MissingOrigin)?
                .try_into()?,
            steps: recipe
                .steps
                .into_iter()
                .enumerate()
                .map(|(index, step)| step.try_into_named_step(index + 1))
                .collect::<Result<_, _>>()?,
        })
    }
}

impl TryFrom<build_proto::recipe::BuildConfig> for BuildConfig {
    type Error = RecipeProtoError;

    fn try_from(config: build_proto::recipe::BuildConfig) -> Result<Self, Self::Error> {
        let vcpu = match config.vcpu {
            Some(value) => Some(
                value
                    .try_into()
                    .map_err(|_| RecipeProtoError::VcpuOutOfRange { value })?,
            ),
            None => None,
        };
        Ok(Self {
            vcpu,
            memory_mib: config
                .memory_bytes
                .map(bytes_to_mib_u32)
                .transpose()
                .map_err(|_| RecipeProtoError::MemoryOutOfRange {
                    value: config.memory_bytes.unwrap_or_default(),
                })?,
            disk_size_mb: config
                .disk_bytes
                .map(bytes_to_mib_u32)
                .transpose()
                .map_err(|_| RecipeProtoError::DiskOutOfRange {
                    value: config.disk_bytes.unwrap_or_default(),
                })?,
            balloon: config.balloon.map(balloon_from_proto).transpose()?,
        })
    }
}

fn balloon_proto(
    balloon: pplx_space_common_core::types::BalloonConfig,
) -> build_proto::recipe::Balloon {
    use build_proto::recipe::balloon::{FreePageHinting, FreePageReporting, Inflate, Mode};
    use pplx_space_common_core::types::BalloonDevice;
    let mode = match balloon.device() {
        BalloonDevice::Inflate {
            amount_mib,
            deflate_on_oom,
        } => Mode::Inflate(Inflate {
            amount_mib: amount_mib.get(),
            deflate_on_oom,
        }),
        BalloonDevice::FreePageReporting => Mode::FreePageReporting(FreePageReporting {}),
        BalloonDevice::FreePageHinting => Mode::FreePageHinting(FreePageHinting {}),
    };
    build_proto::recipe::Balloon {
        mode: Some(mode),
        stats_polling_interval_s: u32::from(balloon.stats_polling_interval_s()),
    }
}

fn balloon_from_proto(
    balloon: build_proto::recipe::Balloon,
) -> Result<pplx_space_common_core::types::BalloonConfig, RecipeProtoError> {
    use build_proto::recipe::balloon::Mode;
    use pplx_space_common_core::types::{BalloonConfig, BalloonDevice};
    let device = match balloon.mode.ok_or(RecipeProtoError::MissingBalloonMode)? {
        Mode::Inflate(inflate) => BalloonDevice::Inflate {
            amount_mib: std::num::NonZeroU32::new(inflate.amount_mib)
                .ok_or(RecipeProtoError::ZeroBalloonAmount)?,
            deflate_on_oom: inflate.deflate_on_oom,
        },
        Mode::FreePageReporting(_) => BalloonDevice::FreePageReporting,
        Mode::FreePageHinting(_) => BalloonDevice::FreePageHinting,
    };
    let stats_polling_interval_s =
        u16::try_from(balloon.stats_polling_interval_s).map_err(|_| {
            RecipeProtoError::BalloonStatsIntervalOutOfRange {
                value: balloon.stats_polling_interval_s,
            }
        })?;
    Ok(BalloonConfig::new(device, stats_polling_interval_s))
}

impl TryFrom<build_proto::recipe::Origin> for Origin {
    type Error = RecipeProtoError;

    fn try_from(origin: build_proto::recipe::Origin) -> Result<Self, Self::Error> {
        match origin.kind.ok_or(RecipeProtoError::MissingOriginKind)? {
            build_proto::recipe::origin::Kind::FromTemplate(from_template) => {
                let (base, base_raw) = parse_template_base(from_template.base)?;
                Ok(Origin::FromTemplate { base, base_raw })
            }
            build_proto::recipe::origin::Kind::FromScratch(from_scratch) => {
                let source = from_scratch
                    .source
                    .ok_or(RecipeProtoError::MissingScratchSource)?
                    .try_into()?;
                Ok(Origin::FromScratch {
                    source,
                    guest_kernel: None,
                })
            }
            build_proto::recipe::origin::Kind::PinnedFromScratch(from_scratch) => {
                let source = from_scratch
                    .source
                    .ok_or(RecipeProtoError::MissingScratchSource)?
                    .try_into()?;
                let guest_kernel = from_scratch
                    .guest_kernel
                    .ok_or(RecipeProtoError::MissingScratchGuestKernel)?
                    .try_into()?;
                Ok(Origin::FromScratch {
                    source,
                    guest_kernel: Some(guest_kernel),
                })
            }
            build_proto::recipe::origin::Kind::FromOciImage(from_oci_image) => {
                let (base, base_raw) = parse_template_base(from_oci_image.base)?;
                let value = from_oci_image.reference;
                let reference =
                    value
                        .parse()
                        .map_err(|source| RecipeProtoError::InvalidOciImageReference {
                            value,
                            source,
                        })?;
                Ok(Origin::FromOciImage {
                    base,
                    base_raw,
                    reference,
                })
            }
        }
    }
}

impl TryFrom<build_proto::recipe::origin::GuestKernel> for GuestKernelPin {
    type Error = RecipeProtoError;

    fn try_from(
        guest_kernel: build_proto::recipe::origin::GuestKernel,
    ) -> Result<Self, Self::Error> {
        let version = ArtifactVersion::parse(&guest_kernel.version).map_err(|source| {
            RecipeProtoError::InvalidGuestKernelVersion {
                value: guest_kernel.version,
                source,
            }
        })?;
        let digest = Digest::from_str(&guest_kernel.digest).map_err(|source| {
            RecipeProtoError::InvalidGuestKernelDigest {
                value: guest_kernel.digest,
                source,
            }
        })?;
        Ok(Self { version, digest })
    }
}

fn parse_template_base(base_raw: String) -> Result<(TemplateReference, String), RecipeProtoError> {
    if base_raw.is_empty() {
        return Err(RecipeProtoError::EmptyBase);
    }
    let base =
        TemplateReference::from_str(&base_raw).map_err(|source| RecipeProtoError::InvalidBase {
            value: base_raw.clone(),
            source,
        })?;
    if matches!(base.selector(), TemplateSelector::ArtifactDigestPrefix(_)) {
        // Prefix selectors need registry candidates to resolve uniquely. Recipe parsing
        // has no template-manager lookup, and template-manager requests accept only
        // tags or exact hashes, so build bases must be unambiguous here.
        return Err(RecipeProtoError::BaseDigestPrefix { value: base_raw });
    }
    Ok((base, base_raw))
}

impl TryFrom<build_proto::recipe::Source> for SourceConfig {
    type Error = RecipeProtoError;

    fn try_from(source: build_proto::recipe::Source) -> Result<Self, Self::Error> {
        match source
            .kind
            .ok_or(RecipeProtoError::MissingScratchSourceKind)?
        {
            build_proto::recipe::source::Kind::UbuntuMinimalCloudSquashfs(source) => Ok(Self {
                release: ScratchSourceField::Release.validate(source.release)?,
                version: ScratchSourceField::Version.validate(source.version)?,
                date: ScratchSourceField::Date.validate(source.date)?,
            }),
        }
    }
}

/// Ubuntu Minimal Cloud selectors become URL path segments and values staged
/// for a privileged build container, so only the published shapes are
/// accepted: `minimal/releases/<release>/<date>/ubuntu-<version>-...`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScratchSourceField {
    Release,
    Version,
    Date,
}

impl ScratchSourceField {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Release => "release",
            Self::Version => "version",
            Self::Date => "date",
        }
    }

    fn expected(self) -> &'static str {
        match self {
            Self::Release => "a lowercase Ubuntu codename such as \"resolute\"",
            Self::Version => "an Ubuntu version such as \"26.04\"",
            Self::Date => "\"release\" or \"release-YYYYMMDD[.N]\"",
        }
    }

    fn accepts(self, value: &str) -> bool {
        match self {
            Self::Release => {
                (1..=32).contains(&value.len()) && value.bytes().all(|b| b.is_ascii_lowercase())
            }
            Self::Version => {
                let mut parts = value.split('.');
                let (Some(year), Some(month), point, None) =
                    (parts.next(), parts.next(), parts.next(), parts.next())
                else {
                    return false;
                };
                is_ascii_digits(year, 2..=2)
                    && is_ascii_digits(month, 2..=2)
                    && point.is_none_or(|point| is_ascii_digits(point, 1..=2))
            }
            Self::Date => match value.strip_prefix("release-") {
                None => value == "release",
                Some(stamp) => match stamp.split_once('.') {
                    None => is_ascii_digits(stamp, 8..=8),
                    Some((day, serial)) => {
                        is_ascii_digits(day, 8..=8) && is_ascii_digits(serial, 1..=3)
                    }
                },
            },
        }
    }

    fn validate(self, value: String) -> Result<String, RecipeProtoError> {
        if self.accepts(&value) {
            Ok(value)
        } else {
            Err(RecipeProtoError::InvalidScratchSource { field: self, value })
        }
    }
}

impl std::fmt::Display for ScratchSourceField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

fn is_ascii_digits(value: &str, len: std::ops::RangeInclusive<usize>) -> bool {
    len.contains(&value.len()) && value.bytes().all(|b| b.is_ascii_digit())
}

trait ProtoStepExt {
    fn try_into_named_step(self, index: usize) -> Result<NamedStep, RecipeProtoError>;
}

impl ProtoStepExt for build_proto::recipe::Step {
    fn try_into_named_step(self, index: usize) -> Result<NamedStep, RecipeProtoError> {
        let env_refs = self
            .env_refs
            .into_iter()
            .map(EnvKey::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|source| RecipeProtoError::Environment {
                index,
                field: "env_refs",
                source,
            })?;
        let step = self
            .body
            .ok_or(RecipeProtoError::MissingStep { index })?
            .try_into_step(index)?;
        Ok(NamedStep {
            display_name: empty_to_none(self.display_name),
            env_refs,
            step,
        })
    }
}

trait ProtoStepBodyExt {
    fn try_into_step(self, index: usize) -> Result<Step, RecipeProtoError>;
}

impl ProtoStepBodyExt for build_proto::recipe::step::Body {
    fn try_into_step(self, index: usize) -> Result<Step, RecipeProtoError> {
        match self {
            build_proto::recipe::step::Body::SetRuntime(value) => Ok(Step::SetRuntime(
                pplx_space_build_mgr_client::runtime::decode(value)?,
            )),
            build_proto::recipe::step::Body::ClearRuntime(_) => Ok(Step::ClearRuntime),
            build_proto::recipe::step::Body::PackageInstall(step) => {
                let manager = match build_proto::recipe::PackageManager::try_from(step.manager) {
                    Ok(build_proto::recipe::PackageManager::Unspecified) => {
                        return Err(RecipeProtoError::MissingPackageManager { index });
                    }
                    Ok(build_proto::recipe::PackageManager::Apt) => PackageManager::Apt,
                    Ok(build_proto::recipe::PackageManager::Pip) => PackageManager::Pip,
                    Ok(build_proto::recipe::PackageManager::Npm) => PackageManager::Npm,
                    Ok(build_proto::recipe::PackageManager::NpmGlobal) => PackageManager::NpmGlobal,
                    Err(_) => {
                        return Err(RecipeProtoError::UnknownPackageManager {
                            index,
                            value: step.manager,
                        });
                    }
                };
                Ok(Step::PackageInstall {
                    manager,
                    packages: step.packages,
                    extra_args: step.extra_args,
                })
            }
            build_proto::recipe::step::Body::Run(step) => {
                if step.command.is_empty() {
                    return Err(RecipeProtoError::EmptyRunCommand { index });
                }
                Ok(Step::Run {
                    command: step.command,
                    user: empty_to_none(step.user),
                })
            }
            build_proto::recipe::step::Body::Env(step) => {
                let vars = EnvMap::try_from(step.vars).map_err(|source| {
                    RecipeProtoError::Environment {
                        index,
                        field: "env",
                        source,
                    }
                })?;
                Ok(Step::Env { vars })
            }
            build_proto::recipe::step::Body::Copy(step) => {
                if step.dest.is_empty() {
                    return Err(RecipeProtoError::EmptyCopyDest { index });
                }
                if !step.dest.starts_with('/') {
                    return Err(RecipeProtoError::RelativeCopyDest { index });
                }
                let payload = match step
                    .payload
                    .ok_or(RecipeProtoError::MissingCopyPayload { index })?
                {
                    build_proto::recipe::copy::Payload::InlineBytes(bytes) => {
                        CopyPayload::InlineBytes(bytes)
                    }
                    build_proto::recipe::copy::Payload::BlobSha256(sha) => {
                        if sha.is_empty() {
                            return Err(RecipeProtoError::EmptyBlobSha256 { index });
                        }
                        crate::blob::blob_path(&sha).map_err(|_invalid_sha| {
                            RecipeProtoError::InvalidBlobSha256 { index }
                        })?;
                        CopyPayload::BlobSha256(sha)
                    }
                };
                Ok(Step::Copy {
                    payload,
                    dest: step.dest,
                    source_path: empty_to_none(step.source_path),
                })
            }
            build_proto::recipe::step::Body::PlaywrightInstall(step) => {
                Ok(Step::PlaywrightInstall {
                    browsers: step.browsers,
                })
            }
        }
    }
}

fn mib_to_bytes(value: u32) -> Result<u64, ()> {
    u64::from(value).checked_mul(MIB).ok_or(())
}

fn bytes_to_mib_u32(value: u64) -> Result<u32, ()> {
    value.div_ceil(MIB).try_into().map_err(|_| ())
}

fn empty_to_none(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.is_empty())
}

fn resolve_host_env_refs(value: &str) -> String {
    resolve_env_refs_with(value, |name| std::env::var(name).unwrap_or_default())
}

fn resolve_env_refs_with<F>(value: &str, mut lookup: F) -> String
where
    F: FnMut(&str) -> String,
{
    let mut result = String::with_capacity(value.len());
    let mut rest = value;
    while let Some(pos) = rest.find('$') {
        let (before, after) = rest.split_at(pos);
        result.push_str(before);
        let Some(after_dollar) = after.strip_prefix('$') else {
            break;
        };
        let end = after_dollar
            .char_indices()
            .find(|(_, c)| !c.is_ascii_alphanumeric() && *c != '_')
            .map(|(index, _)| index)
            .unwrap_or(after_dollar.len());
        let (var_name, after_var) = after_dollar.split_at(end);
        if var_name.is_empty() {
            result.push('$');
            rest = after_dollar;
            continue;
        }
        result.push_str(&lookup(var_name));
        rest = after_var;
    }
    result.push_str(rest);
    result
}

impl std::fmt::Display for NamedStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Names, like commands, can contain secrets. Logs, spans and build
        // errors all use Display, so they share the same input-free description.
        write!(f, "{}", self.step)
    }
}

impl std::fmt::Display for Step {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Step::PackageInstall {
                manager, packages, ..
            } => write!(f, "{manager} ({} packages)", packages.len()),
            Step::SetRuntime(_) => write!(f, "set runtime"),
            Step::ClearRuntime => write!(f, "clear runtime"),
            Step::Run { .. } => write!(f, "run"),
            Step::Env { vars } => write!(f, "env ({} vars)", vars.as_map().len()),
            Step::Copy { .. } => write!(f, "copy payload"),
            Step::PlaywrightInstall { browsers } => {
                write!(f, "playwright-install ({} browsers)", browsers.len())
            }
        }
    }
}

impl std::fmt::Display for PackageManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PackageManager::Apt => write!(f, "apt-install"),
            PackageManager::Pip => write!(f, "pip-install"),
            PackageManager::Npm => write!(f, "npm-install"),
            PackageManager::NpmGlobal => write!(f, "npm-install-global"),
        }
    }
}

#[cfg(test)]
mod tests;
