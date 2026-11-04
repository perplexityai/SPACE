use std::collections::BTreeSet;
use std::fs;
use std::io::Read as _;
use std::path::{Component, Path, PathBuf};

use object::Object as _;
use object::read::elf::ProgramHeader as _;

use super::{
    GuestBundleError, GuestBundleOperation, OPTIONAL_PATHS, REQUIRED_PATHS,
    normalize_relative_path, path_arg, privileged_command,
};

const DEPENDENCY_SCAN_ROOTS: &[&str] = &[
    "bin",
    "sbin",
    "usr/bin",
    "usr/lib",
    "usr/libexec",
    "usr/local/bin",
    "usr/local/sbin",
    "usr/sbin",
];

pub(super) fn bundle_entries(
    seed_root: &Path,
    as_root: bool,
) -> Result<Vec<PathBuf>, GuestBundleError> {
    let seed_root = fs::canonicalize(seed_root).map_err(|source| GuestBundleError::Io {
        operation: GuestBundleOperation::Inspect,
        path: seed_root.to_path_buf(),
        source,
    })?;
    let mut entries = BTreeSet::new();
    entries.extend(REQUIRED_PATHS.iter().map(PathBuf::from));
    entries.extend(
        OPTIONAL_PATHS
            .iter()
            .map(PathBuf::from)
            .filter(|path| seed_root.join(path).exists() || seed_root.join(path).is_symlink()),
    );
    // Client configuration belongs to the image's package manager, not the seed.
    entries.remove(Path::new("etc/ssh"));
    let ssh = seed_root.join("etc/ssh");
    for entry in read_dir(&ssh)? {
        let entry = dir_entry(entry, &ssh)?;
        if !matches!(
            entry.file_name().to_str(),
            Some("ssh_config" | "ssh_config.d")
        ) {
            entries.insert(relative_to(&seed_root, &entry.path())?);
        }
    }
    add_auth_modules(&seed_root, &mut entries)?;
    add_symlink_targets(&seed_root, &mut entries)?;

    let elf_paths = elf_paths(&seed_root, &entries)?;
    for path in elf_paths {
        let dependencies = dynamic_dependencies(&seed_root, &path, as_root)?;
        add_dynamic_dependency_paths(&seed_root, &mut entries, &dependencies)?;
    }

    Ok(remove_descendants(entries))
}

pub(super) fn add_dynamic_dependency_paths(
    seed_root: &Path,
    entries: &mut BTreeSet<PathBuf>,
    dependencies: &[PathBuf],
) -> Result<(), GuestBundleError> {
    let seed_root = fs::canonicalize(seed_root).map_err(|source| GuestBundleError::Io {
        operation: GuestBundleOperation::Inspect,
        path: seed_root.to_path_buf(),
        source,
    })?;
    entries.extend(dependencies.iter().cloned());
    add_symlink_targets(&seed_root, entries)
}

fn add_symlink_targets(
    seed_root: &Path,
    entries: &mut BTreeSet<PathBuf>,
) -> Result<(), GuestBundleError> {
    let mut pending: Vec<_> = entries.iter().cloned().collect();
    while let Some(entry) = pending.pop() {
        let path = seed_root.join(&entry);
        if !fs::symlink_metadata(&path)
            .map_err(|source| GuestBundleError::Io {
                operation: GuestBundleOperation::Inspect,
                path: path.clone(),
                source,
            })?
            .file_type()
            .is_symlink()
        {
            continue;
        }

        let target = fs::read_link(&path).map_err(|source| GuestBundleError::Io {
            operation: GuestBundleOperation::Inspect,
            path: path.clone(),
            source,
        })?;
        let target = if target.is_absolute() {
            target
                .strip_prefix("/")
                .map_err(|_| GuestBundleError::UnsafeDependency {
                    path: target.display().to_string(),
                })?
                .to_path_buf()
        } else {
            entry.parent().unwrap_or(Path::new("")).join(target)
        };
        let target = normalize_relative_path(&target)?;
        if entries.insert(target.clone()) {
            pending.push(target);
        }
    }
    Ok(())
}

fn add_auth_modules(
    seed_root: &Path,
    entries: &mut BTreeSet<PathBuf>,
) -> Result<(), GuestBundleError> {
    let lib = seed_root.join("usr/lib");
    for architecture in read_dir(&lib)? {
        let architecture = dir_entry(architecture, &lib)?;
        let path = architecture.path();
        if !path.is_dir() {
            continue;
        }
        let security = path.join("security");
        if security.is_dir() {
            entries.insert(relative_to(seed_root, &security)?);
        }
        for entry in read_dir(&path)? {
            let entry = dir_entry(entry, &path)?;
            let name = entry.file_name();
            if name.to_string_lossy().starts_with("libnss_") {
                entries.insert(relative_to(seed_root, &entry.path())?);
            }
        }
    }
    Ok(())
}

fn elf_paths(
    seed_root: &Path,
    entries: &BTreeSet<PathBuf>,
) -> Result<Vec<PathBuf>, GuestBundleError> {
    let mut files = Vec::new();
    for entry in entries.iter().filter(|entry| dependency_scan_entry(entry)) {
        visit_files(seed_root, &seed_root.join(entry), &mut files)?;
    }
    files.sort();
    files.dedup();
    Ok(files)
}

fn dependency_scan_entry(path: &Path) -> bool {
    DEPENDENCY_SCAN_ROOTS
        .iter()
        .any(|root| path.starts_with(root))
}

fn visit_files(
    seed_root: &Path,
    path: &Path,
    files: &mut Vec<PathBuf>,
) -> Result<(), GuestBundleError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| GuestBundleError::Io {
        operation: GuestBundleOperation::Inspect,
        path: path.to_path_buf(),
        source,
    })?;
    if metadata.file_type().is_symlink() {
        return Ok(());
    }
    if metadata.is_dir() {
        for entry in read_dir(path)? {
            let entry = dir_entry(entry, path)?;
            visit_files(seed_root, &entry.path(), files)?;
        }
        return Ok(());
    }
    if metadata.is_file() && is_elf(path)? {
        files.push(relative_to(seed_root, path)?);
    }
    Ok(())
}

fn is_elf(path: &Path) -> Result<bool, GuestBundleError> {
    let mut file = fs::File::open(path).map_err(|source| GuestBundleError::Io {
        operation: GuestBundleOperation::Inspect,
        path: path.to_path_buf(),
        source,
    })?;
    let mut magic = [0; 4];
    Ok(file.read_exact(&mut magic).is_ok() && magic == *b"\x7fELF")
}

fn dynamic_dependencies(
    seed_root: &Path,
    binary: &Path,
    as_root: bool,
) -> Result<Vec<PathBuf>, GuestBundleError> {
    if !elf_needs_dependency_scan(&seed_root.join(binary))? {
        return Ok(Vec::new());
    }
    let mut command = privileged_command("chroot", as_root);
    command.arg(seed_root).args([
        "/usr/bin/ldd",
        path_arg(&Path::new("/").join(binary)).as_str(),
    ]);
    let output = command.output().map_err(|source| GuestBundleError::Io {
        operation: GuestBundleOperation::Inspect,
        path: seed_root.join(binary),
        source,
    })?;
    if !output.status.success() {
        return Err(GuestBundleError::Command {
            operation: GuestBundleOperation::Inspect,
            code: output.status.code(),
        });
    }

    parse_dynamic_dependencies(&String::from_utf8_lossy(&output.stdout))
}

pub(super) fn parse_dynamic_dependencies(output: &str) -> Result<Vec<PathBuf>, GuestBundleError> {
    let mut dependencies = Vec::new();
    for line in output.lines() {
        let fields: Vec<_> = line.split_whitespace().collect();
        let Some(first) = fields.first() else {
            continue;
        };
        let value = if let Some(index) = fields.iter().position(|field| *field == "=>") {
            let Some(value) = fields.get(index + 1) else {
                continue;
            };
            if *value == "not" && fields.get(index + 2) == Some(&"found") {
                return Err(GuestBundleError::MissingDependency {
                    dependency: (*first).to_string(),
                });
            }
            *value
        } else {
            *first
        };
        if value.starts_with('/') {
            dependencies.push(safe_relative_path(value)?);
        }
    }
    Ok(dependencies)
}

fn elf_needs_dependency_scan(path: &Path) -> Result<bool, GuestBundleError> {
    let bytes = fs::read(path).map_err(|source| GuestBundleError::Io {
        operation: GuestBundleOperation::Inspect,
        path: path.to_path_buf(),
        source,
    })?;
    let file =
        object::File::parse(bytes.as_slice()).map_err(|source| GuestBundleError::InvalidElf {
            path: path.to_path_buf(),
            source,
        })?;
    let is_shared_object = file.kind() == object::ObjectKind::Dynamic;
    let has_interpreter = match file {
        object::File::Elf32(elf) => elf
            .elf_program_headers()
            .iter()
            .any(|header| header.p_type(elf.endian()) == object::elf::PT_INTERP),
        object::File::Elf64(elf) => elf
            .elf_program_headers()
            .iter()
            .any(|header| header.p_type(elf.endian()) == object::elf::PT_INTERP),
        _ => {
            return Err(GuestBundleError::UnsafeDependency {
                path: path.display().to_string(),
            });
        }
    };
    Ok(is_shared_object || has_interpreter)
}

fn safe_relative_path(path: &str) -> Result<PathBuf, GuestBundleError> {
    let path = Path::new(path);
    if !path.is_absolute()
        || path == Path::new("/")
        || path
            .components()
            .any(|component| component == Component::ParentDir)
    {
        return Err(GuestBundleError::UnsafeDependency {
            path: path.display().to_string(),
        });
    }
    path.strip_prefix("/")
        .map(Path::to_path_buf)
        .map_err(|_| GuestBundleError::UnsafeDependency {
            path: path.display().to_string(),
        })
}

fn remove_descendants(entries: BTreeSet<PathBuf>) -> Vec<PathBuf> {
    let mut entries: Vec<_> = entries.into_iter().collect();
    entries.sort_by_key(|path| path.components().count());
    let mut kept = Vec::<PathBuf>::new();
    for entry in entries {
        if !kept.iter().any(|parent| entry.starts_with(parent)) {
            kept.push(entry);
        }
    }
    kept
}

fn read_dir(path: &Path) -> Result<fs::ReadDir, GuestBundleError> {
    fs::read_dir(path).map_err(|source| GuestBundleError::Io {
        operation: GuestBundleOperation::Inspect,
        path: path.to_path_buf(),
        source,
    })
}

fn dir_entry(
    entry: Result<fs::DirEntry, std::io::Error>,
    parent: &Path,
) -> Result<fs::DirEntry, GuestBundleError> {
    entry.map_err(|source| GuestBundleError::Io {
        operation: GuestBundleOperation::Inspect,
        path: parent.to_path_buf(),
        source,
    })
}

fn relative_to(root: &Path, path: &Path) -> Result<PathBuf, GuestBundleError> {
    path.strip_prefix(root)
        .map(Path::to_path_buf)
        .map_err(|_| GuestBundleError::UnsafeDependency {
            path: path.display().to_string(),
        })
}
