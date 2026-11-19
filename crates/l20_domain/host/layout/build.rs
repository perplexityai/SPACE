use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::string::FromUtf8Error;

const IDENTITY_VARS: &[&str] = &[
    "SPACE_GROUP_SPECS",
    "SPACE_USER_SPECS",
    "SPACE_SANDBOX_MANAGER_GROUPS",
    "SPACE_TEMPLATE_MANAGER_GROUPS",
    "SPACE_CREDENTIAL_MANAGER_GROUPS",
    "SPACE_NODECTL_GROUPS",
    "SPACE_NODEAPI_WORKER_GROUPS",
    "SPACE_NODEAPI_BUILDER_GROUPS",
    "SPACE_NODEAPI_CLUSTER_GROUPS",
    "SPACE_BUILD_MANAGER_GROUPS",
    "SPACE_KUBERNETES_NODE_CONTROLLER_GROUPS",
];

#[derive(Clone)]
struct Group {
    const_name: String,
    name: String,
    gid: u32,
}

#[derive(Clone)]
struct User {
    const_name: String,
    name: String,
    uid: u32,
    group_const: String,
}

#[derive(Debug)]
enum BuildScriptError {
    MissingEnv(&'static str),
    LocateProjectUtf8(FromUtf8Error),
    WorkspaceManifestMissingParent(PathBuf),
    WorkspaceRootNotFound(PathBuf),
    SourceIdentitiesIo {
        path: PathBuf,
        source: io::Error,
    },
    SourceIdentitiesFailed {
        path: PathBuf,
        stderr: String,
    },
    IdentityOutputUtf8(FromUtf8Error),
    IdentityOutputMissingSeparator(String),
    MissingIdentityVar(&'static str),
    InvalidGroupSpec(String),
    InvalidGroupGid {
        name: String,
        source: std::num::ParseIntError,
    },
    InvalidUserSpec(String),
    InvalidUserUid {
        name: String,
        source: std::num::ParseIntError,
    },
    UnknownPrimaryGroup {
        user: String,
        group: String,
    },
    UnknownServiceGroup {
        var_name: &'static str,
        group: String,
    },
    WriteGenerated {
        path: PathBuf,
        source: io::Error,
    },
}

impl fmt::Display for BuildScriptError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingEnv(name) => write!(f, "missing required environment variable {name}"),
            Self::LocateProjectUtf8(source) => {
                write!(f, "cargo locate-project printed non-utf8 output: {source}")
            }
            Self::WorkspaceManifestMissingParent(path) => write!(
                f,
                "workspace manifest path has no parent directory: {}",
                path.display()
            ),
            Self::WorkspaceRootNotFound(start) => write!(
                f,
                "failed to locate workspace root from {} containing infra/space/identity/node-identities.sh",
                start.display()
            ),
            Self::SourceIdentitiesIo { path, source } => {
                write!(f, "failed to run bash for {}: {source}", path.display())
            }
            Self::SourceIdentitiesFailed { path, stderr } => {
                write!(f, "failed to source {}: {stderr}", path.display())
            }
            Self::IdentityOutputUtf8(source) => {
                write!(f, "identity output contained non-utf8 bytes: {source}")
            }
            Self::IdentityOutputMissingSeparator(entry) => {
                write!(f, "identity output entry is not KEY=VALUE: {entry}")
            }
            Self::MissingIdentityVar(name) => {
                write!(f, "identity script did not emit required variable {name}")
            }
            Self::InvalidGroupSpec(line) => {
                write!(f, "group spec should be name:gid, got {line}")
            }
            Self::InvalidGroupGid { name, source } => {
                write!(f, "group {name} has invalid numeric gid: {source}")
            }
            Self::InvalidUserSpec(line) => {
                write!(
                    f,
                    "user spec should be alias:name:uid:primary_group, got {line}"
                )
            }
            Self::InvalidUserUid { name, source } => {
                write!(f, "user {name} has invalid numeric uid: {source}")
            }
            Self::UnknownPrimaryGroup { user, group } => {
                write!(f, "user {user} references unknown primary group {group}")
            }
            Self::UnknownServiceGroup { var_name, group } => {
                write!(f, "{var_name} references unknown group {group}")
            }
            Self::WriteGenerated { path, source } => {
                write!(f, "failed to write {}: {source}", path.display())
            }
        }
    }
}

impl Error for BuildScriptError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::LocateProjectUtf8(source) | Self::IdentityOutputUtf8(source) => Some(source),
            Self::SourceIdentitiesIo { source, .. } | Self::WriteGenerated { source, .. } => {
                Some(source)
            }
            Self::InvalidGroupGid { source, .. } | Self::InvalidUserUid { source, .. } => {
                Some(source)
            }
            Self::MissingEnv(_)
            | Self::WorkspaceManifestMissingParent(_)
            | Self::WorkspaceRootNotFound(_)
            | Self::SourceIdentitiesFailed { .. }
            | Self::IdentityOutputMissingSeparator(_)
            | Self::MissingIdentityVar(_)
            | Self::InvalidGroupSpec(_)
            | Self::InvalidUserSpec(_)
            | Self::UnknownPrimaryGroup { .. }
            | Self::UnknownServiceGroup { .. } => None,
        }
    }
}

fn main() -> Result<(), BuildScriptError> {
    let manifest_dir = PathBuf::from(
        env::var_os("CARGO_MANIFEST_DIR")
            .ok_or(BuildScriptError::MissingEnv("CARGO_MANIFEST_DIR"))?,
    );
    let repo_root = workspace_root(&manifest_dir)?;
    let identities_path = repo_root.join("infra/space/identity/node-identities.sh");

    println!("cargo:rerun-if-changed={}", identities_path.display());
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=CARGO_WORKSPACE_DIR");

    let raw = source_shell_identities(&identities_path)?;
    let groups = parse_groups(raw_value(&raw, "SPACE_GROUP_SPECS")?)?;
    let group_by_name: BTreeMap<String, Group> = groups
        .iter()
        .cloned()
        .map(|group| (group.name.clone(), group))
        .collect();

    let users = parse_users(raw_value(&raw, "SPACE_USER_SPECS")?, &group_by_name)?;

    let output = render_rust(&raw, &groups, &group_by_name, &users)?;
    let out_path =
        PathBuf::from(env::var_os("OUT_DIR").ok_or(BuildScriptError::MissingEnv("OUT_DIR"))?)
            .join("linux_identities.rs");
    fs::write(&out_path, output).map_err(|source| BuildScriptError::WriteGenerated {
        path: out_path,
        source,
    })?;
    Ok(())
}

fn workspace_root(manifest_dir: &Path) -> Result<PathBuf, BuildScriptError> {
    if let Some(workspace_dir) = env::var_os("CARGO_WORKSPACE_DIR") {
        return Ok(PathBuf::from(workspace_dir));
    }

    let cargo = env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    match Command::new(cargo)
        .arg("locate-project")
        .arg("--workspace")
        .arg("--message-format")
        .arg("plain")
        .current_dir(manifest_dir)
        .output()
    {
        Ok(output) if output.status.success() => {
            let manifest =
                String::from_utf8(output.stdout).map_err(BuildScriptError::LocateProjectUtf8)?;
            let manifest = manifest.trim();
            if !manifest.is_empty() {
                let manifest = PathBuf::from(manifest);
                return manifest
                    .parent()
                    .map(Path::to_path_buf)
                    .ok_or(BuildScriptError::WorkspaceManifestMissingParent(manifest));
            }
        }
        Ok(output) => {
            eprintln!(
                "warning: cargo locate-project --workspace failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Err(error) => {
            eprintln!("warning: failed to run cargo locate-project --workspace: {error}");
        }
    }

    manifest_dir
        .ancestors()
        .find(|dir| {
            dir.join("infra/space/identity/node-identities.sh")
                .is_file()
        })
        .map(Path::to_path_buf)
        .ok_or_else(|| BuildScriptError::WorkspaceRootNotFound(manifest_dir.to_path_buf()))
}

fn source_shell_identities(path: &Path) -> Result<BTreeMap<String, String>, BuildScriptError> {
    let script = r#"
set -euo pipefail
. "$1"
shift
for name in "$@"; do
  printf '%s=%s\0' "$name" "${!name}"
done
"#;
    let output = Command::new("bash")
        .arg("-c")
        .arg(script)
        .arg("bash")
        .arg(path)
        .args(IDENTITY_VARS)
        .output()
        .map_err(|source| BuildScriptError::SourceIdentitiesIo {
            path: path.to_path_buf(),
            source,
        })?;

    if !output.status.success() {
        return Err(BuildScriptError::SourceIdentitiesFailed {
            path: path.to_path_buf(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        });
    }

    let mut values = BTreeMap::new();
    for entry in output.stdout.split(|byte| *byte == b'\0') {
        if entry.is_empty() {
            continue;
        }
        let entry =
            String::from_utf8(entry.to_vec()).map_err(BuildScriptError::IdentityOutputUtf8)?;
        let (key, value) = entry
            .split_once('=')
            .ok_or_else(|| BuildScriptError::IdentityOutputMissingSeparator(entry.clone()))?;
        values.insert(key.to_string(), value.to_string());
    }
    Ok(values)
}

fn raw_value<'a>(
    raw: &'a BTreeMap<String, String>,
    name: &'static str,
) -> Result<&'a str, BuildScriptError> {
    raw.get(name)
        .map(String::as_str)
        .ok_or(BuildScriptError::MissingIdentityVar(name))
}

fn parse_groups(specs: &str) -> Result<Vec<Group>, BuildScriptError> {
    specs
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            (!line.is_empty()).then_some(line)
        })
        .map(|line| {
            let (name, gid) = line
                .split_once(':')
                .ok_or_else(|| BuildScriptError::InvalidGroupSpec(line.to_string()))?;
            Ok(Group {
                const_name: const_name_for_identity(name),
                name: name.to_string(),
                gid: gid
                    .parse()
                    .map_err(|source| BuildScriptError::InvalidGroupGid {
                        name: name.to_string(),
                        source,
                    })?,
            })
        })
        .collect()
}

fn parse_users(
    specs: &str,
    group_by_name: &BTreeMap<String, Group>,
) -> Result<Vec<User>, BuildScriptError> {
    specs
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            (!line.is_empty()).then_some(line)
        })
        .map(|line| {
            let parts = line.split(':').collect::<Vec<_>>();
            if parts.len() != 4 {
                return Err(BuildScriptError::InvalidUserSpec(line.to_string()));
            }
            let group_name = parts
                .get(3)
                .ok_or_else(|| BuildScriptError::InvalidUserSpec(line.to_string()))?;
            let user_name = parts
                .get(1)
                .ok_or_else(|| BuildScriptError::InvalidUserSpec(line.to_string()))?;
            let uid = parts
                .get(2)
                .ok_or_else(|| BuildScriptError::InvalidUserSpec(line.to_string()))?
                .parse()
                .map_err(|source| BuildScriptError::InvalidUserUid {
                    name: (*user_name).to_string(),
                    source,
                })?;
            let group_const = group_by_name
                .get(*group_name)
                .ok_or_else(|| BuildScriptError::UnknownPrimaryGroup {
                    user: (*user_name).to_string(),
                    group: (*group_name).to_string(),
                })?
                .const_name
                .clone();
            Ok(User {
                const_name: parts
                    .first()
                    .ok_or_else(|| BuildScriptError::InvalidUserSpec(line.to_string()))?
                    .to_string(),
                name: (*user_name).to_string(),
                uid,
                group_const,
            })
        })
        .collect()
}

fn const_name_for_identity(name: &str) -> String {
    name.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
        .replace("KUBERNETES", "K8S")
}

fn rust_string(value: &str) -> String {
    format!("{value:?}")
}

fn render_rust(
    raw: &BTreeMap<String, String>,
    groups: &[Group],
    group_by_name: &BTreeMap<String, Group>,
    users: &[User],
) -> Result<String, BuildScriptError> {
    let mut out = String::new();
    out.push_str("// @generated by crates/l20_domain/host/layout/build.rs from infra/space/identity/node-identities.sh\n");
    out.push_str("#[derive(Clone, Copy, Debug, Eq, PartialEq)]\n");
    out.push_str("pub struct LinuxGroup {\n");
    out.push_str("    pub name: &'static str,\n");
    out.push_str("    pub gid: u32,\n");
    out.push_str("}\n\n");
    out.push_str("#[derive(Clone, Copy, Debug, Eq, PartialEq)]\n");
    out.push_str("pub struct LinuxUser {\n");
    out.push_str("    pub name: &'static str,\n");
    out.push_str("    pub uid: u32,\n");
    out.push_str("    pub primary_group: LinuxGroup,\n");
    out.push_str("}\n\n");

    out.push_str("pub mod group_names {\n");
    for group in groups {
        out.push_str(&format!(
            "    pub const {}: &str = {};\n",
            group.const_name,
            rust_string(&group.name)
        ));
    }
    out.push_str("}\n\n");

    out.push_str("pub mod gids {\n");
    for group in groups {
        out.push_str(&format!(
            "    pub const {}: u32 = {};\n",
            group.const_name, group.gid
        ));
    }
    out.push_str("}\n\n");

    out.push_str("pub mod groups {\n");
    out.push_str("    use super::{gids, group_names, LinuxGroup};\n");
    for group in groups {
        out.push_str(&format!(
            "    pub const {}: LinuxGroup = LinuxGroup {{ name: group_names::{}, gid: gids::{} }};\n",
            group.const_name, group.const_name, group.const_name
        ));
    }
    out.push_str("}\n\n");

    out.push_str("pub mod user_names {\n");
    for user in users {
        out.push_str(&format!(
            "    pub const {}: &str = {};\n",
            user.const_name,
            rust_string(&user.name)
        ));
    }
    out.push_str("}\n\n");

    out.push_str("pub mod uids {\n");
    for user in users {
        out.push_str(&format!(
            "    pub const {}: u32 = {};\n",
            user.const_name, user.uid
        ));
    }
    out.push_str("}\n\n");

    out.push_str("pub mod users {\n");
    out.push_str("    use super::{groups, uids, user_names, LinuxUser};\n");
    for user in users {
        out.push_str(&format!(
            "    pub const {}: LinuxUser = LinuxUser {{ name: user_names::{}, uid: uids::{}, primary_group: groups::{} }};\n",
            user.const_name, user.const_name, user.const_name, user.group_const
        ));
    }
    out.push_str("}\n\n");

    out.push_str("pub mod service_groups {\n");
    out.push_str("    use super::{groups, LinuxGroup};\n");
    for (const_name, var_name) in [
        ("SANDBOX_MANAGER", "SPACE_SANDBOX_MANAGER_GROUPS"),
        ("TEMPLATE_MANAGER", "SPACE_TEMPLATE_MANAGER_GROUPS"),
        ("CREDENTIAL_MANAGER", "SPACE_CREDENTIAL_MANAGER_GROUPS"),
        ("NODECTL", "SPACE_NODECTL_GROUPS"),
        ("NODEAPI_WORKER", "SPACE_NODEAPI_WORKER_GROUPS"),
        ("NODEAPI_BUILDER", "SPACE_NODEAPI_BUILDER_GROUPS"),
        ("NODEAPI_CLUSTER", "SPACE_NODEAPI_CLUSTER_GROUPS"),
        ("BUILD_MANAGER", "SPACE_BUILD_MANAGER_GROUPS"),
        (
            "K8S_NODE_CONTROLLER",
            "SPACE_KUBERNETES_NODE_CONTROLLER_GROUPS",
        ),
    ] {
        let group_consts = raw_value(raw, var_name)?
            .split_whitespace()
            .map(|name| {
                group_by_name
                    .get(name)
                    .map(|group| format!("groups::{}", group.const_name))
                    .ok_or_else(|| BuildScriptError::UnknownServiceGroup {
                        var_name,
                        group: name.to_string(),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?
            .join(", ");
        out.push_str(&format!(
            "    pub const {const_name}: &[LinuxGroup] = &[{group_consts}];\n"
        ));
    }
    out.push_str("}\n\n");

    let all_groups = groups
        .iter()
        .map(|group| format!("groups::{}", group.const_name))
        .collect::<Vec<_>>()
        .join(", ");
    out.push_str(&format!(
        "pub const ALL_GROUPS: &[LinuxGroup] = &[{all_groups}];\n"
    ));
    let all_users = users
        .iter()
        .map(|user| format!("users::{}", user.const_name))
        .collect::<Vec<_>>()
        .join(", ");
    out.push_str(&format!(
        "pub const ALL_USERS: &[LinuxUser] = &[{all_users}];\n"
    ));

    Ok(out)
}
