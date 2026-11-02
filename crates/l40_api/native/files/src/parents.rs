//! `parents` writes: create a write's missing directories only once the guest
//! reports its path missing.

use pplx_space_common_core::types::SandboxId;
use pplx_space_guest_process::{GuestUserName, IdempotencyKey};
use pplx_space_sbx_mgr_protocol::proto::SpacedMkdirRequest;

use crate::{failure::FileCallFailure, worker::spaced_mkdir_request};

/// A `parents` write: write, and if the guest reports the path missing,
/// create the missing directories through the mkdir `files:mkdir` issues and
/// write again. Directories are created only after that answer, never up
/// front: guest mkdir refuses a path through a symlinked directory that a
/// plain write follows, so an eager mkdir would fail writes that succeed
/// today, and costs a request on every write that does not need it.
pub async fn write_creating_parents<'a, T, E, Write, WriteFut, Mkdir, MkdirFut>(
    idempotency_key: &IdempotencyKey,
    paths: impl IntoIterator<Item = &'a str>,
    user: Option<&GuestUserName>,
    mut write: Write,
    mut mkdir: Mkdir,
    sandbox_id: SandboxId,
) -> Result<T, E>
where
    E: FileCallFailure,
    Write: FnMut(&IdempotencyKey) -> WriteFut,
    WriteFut: std::future::Future<Output = Result<T, E>>,
    Mkdir: FnMut(SpacedMkdirRequest) -> MkdirFut,
    MkdirFut: std::future::Future<Output = Result<(), E>>,
{
    let error = match write(idempotency_key).await {
        Err(error) if error.is_guest_path_not_found() => error,
        result => return result,
    };
    let directories = deepest_parent_directories(paths);
    let Some(retry_key) = parents_retry_key(idempotency_key).filter(|_| !directories.is_empty())
    else {
        return Err(error);
    };
    for directory in directories {
        mkdir(spaced_mkdir_request(sandbox_id, directory, true, user)).await?;
    }
    write(&retry_key).await
}

/// The key for writing again once the missing directories exist. The guest
/// settled the first key with its not-found outcome, so the retry needs its
/// own; deriving it keeps a client's retry of the whole request idempotent.
/// `None` only when the client's key leaves no room for the suffix.
fn parents_retry_key(key: &IdempotencyKey) -> Option<IdempotencyKey> {
    IdempotencyKey::parse(format!("{}:parents", key.as_str())).ok()
}

/// The directories to create for `paths`, deepest first in path order: one
/// that is an ancestor of another is created by the deeper mkdir.
pub fn deepest_parent_directories<'a>(paths: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    let mut directories: Vec<String> = Vec::new();
    for directory in paths.into_iter().filter_map(write_parent_directory) {
        if !directories.contains(&directory) {
            directories.push(directory);
        }
    }
    let covered = |candidate: &String| {
        directories.iter().any(|other| {
            other
                .strip_prefix(candidate.as_str())
                .is_some_and(|rest| rest.starts_with('/'))
        })
    };
    directories
        .iter()
        .filter(|candidate| !covered(candidate))
        .cloned()
        .collect()
}

/// The directory a write of `path` lands in, resolved lexically so a `..`
/// cannot create a directory the write never uses. `None` when the path names
/// no file, or its directory is the root or the user's home (a bare relative
/// name), which always exist. A relative result stays relative, so the guest
/// resolves it against the same home as the write.
pub fn write_parent_directory(path: &str) -> Option<String> {
    use std::path::{Component, Path};
    let path = Path::new(path);
    let absolute = path.is_absolute();
    let mut parts: Vec<&str> = Vec::new();
    let mut names_file = false;
    for component in path.components() {
        names_file = false;
        match component {
            Component::Normal(part) => {
                parts.push(part.to_str()?);
                names_file = true;
            }
            Component::ParentDir => match parts.last() {
                Some(&last) if last != ".." => {
                    parts.pop();
                }
                _ if !absolute => parts.push(".."),
                _ => {}
            },
            Component::RootDir | Component::CurDir | Component::Prefix(_) => {}
        }
    }
    if !names_file {
        return None;
    }
    parts.pop();
    if parts.is_empty() {
        return None;
    }
    let directory = parts.join("/");
    Some(if absolute {
        format!("/{directory}")
    } else {
        directory
    })
}
