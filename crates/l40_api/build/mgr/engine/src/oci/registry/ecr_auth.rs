use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};

use serde::Serialize;

use super::RegistryError;

const DEFAULT_ECR_AUTH_DIR: &str = "/run/space-ecr-auth";

pub(in crate::oci) struct EcrPullAuth {
    registry: String,
    authorization_token: String,
}

pub(in crate::oci) struct EcrAuthDirectory(tempfile::TempDir);

#[derive(Serialize)]
struct HostsFile<'a> {
    server: String,
    capabilities: [&'static str; 2],
    header: HostsHeader<'a>,
}

#[derive(Serialize)]
struct HostsHeader<'a> {
    #[serde(rename = "Authorization")]
    authorization: &'a str,
}

impl EcrPullAuth {
    #[cfg(any(feature = "aws-ecr", test))]
    pub(in crate::oci) fn new(registry: String, authorization_token: String) -> Self {
        Self {
            registry,
            authorization_token,
        }
    }

    pub(in crate::oci) fn materialize(
        &self,
        root: &Path,
    ) -> Result<EcrAuthDirectory, RegistryError> {
        let directory = tempfile::Builder::new()
            .prefix("space-ecr-auth-")
            .tempdir_in(root)
            .map_err(|source| RegistryError::EcrAuthDirectory {
                path: root.to_path_buf(),
                source,
            })?;
        let registry_dir = directory.path().join(&self.registry);
        fs::create_dir(&registry_dir).map_err(|source| RegistryError::EcrAuthDirectory {
            path: registry_dir.clone(),
            source,
        })?;
        let hosts_path = registry_dir.join("hosts.toml");
        let authorization = format!("Basic {}", self.authorization_token);
        let contents = toml::to_string(&HostsFile {
            server: format!("https://{}", self.registry),
            capabilities: ["pull", "resolve"],
            header: HostsHeader {
                authorization: &authorization,
            },
        })
        .map_err(|source| RegistryError::EcrAuthSerialization { source })?;
        let mut hosts_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&hosts_path)
            .map_err(|source| RegistryError::EcrAuthFile {
                path: hosts_path.clone(),
                source,
            })?;
        hosts_file
            .write_all(contents.as_bytes())
            .map_err(|source| RegistryError::EcrAuthFile {
                path: hosts_path,
                source,
            })?;
        Ok(EcrAuthDirectory(directory))
    }
}

impl EcrAuthDirectory {
    pub(in crate::oci) fn path(&self) -> &Path {
        self.0.path()
    }
}

pub(in crate::oci) fn ecr_auth_root() -> PathBuf {
    PathBuf::from(DEFAULT_ECR_AUTH_DIR)
}
