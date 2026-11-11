use std::os::unix::fs::{PermissionsExt as _, symlink};

use super::*;

/// A tree rooted at a tempdir the test user owns, audited as if that user
/// were root, since tests cannot create root-owned directories.
struct Tree {
    _dir: tempfile::TempDir,
    /// The tempdir resolved, as the audit resolves linked roots (a macOS
    /// tempdir sits behind `/var`, a link).
    root: PathBuf,
}

impl Tree {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        Self {
            root: std::fs::canonicalize(dir.path()).unwrap(),
            _dir: dir,
        }
    }

    fn path(&self, relative: &str) -> PathBuf {
        self.root.join(relative)
    }

    /// Create `relative` and every missing parent, then set its mode.
    fn dir(&self, relative: &str, mode: u32) -> PathBuf {
        let path = self.path(relative);
        std::fs::create_dir_all(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
        path
    }

    fn identity(&self) -> AuditIdentity {
        let metadata = std::fs::metadata(&self.root).unwrap();
        AuditIdentity {
            trusted_uid: metadata.uid(),
            service_uid: metadata.uid(),
            service_gid: metadata.gid(),
            audit_from: self.root.clone(),
        }
    }

    fn audit(&self, path: &Path, role: PathRole) -> Protection {
        audit(path, role, &self.identity()).protection
    }
}

fn problems(protection: Protection) -> Vec<String> {
    match protection {
        Protection::Unprotected(problems) => problems,
        other => panic!("expected unprotected, got {other:?}"),
    }
}

#[test]
fn an_install_root_is_roots_alone() {
    let tree = Tree::new();
    tree.dir("vendor", 0o755);
    // Read-only to everyone but root.
    let store = tree.dir("vendor/firecracker", 0o755);
    assert_eq!(
        tree.audit(&store, PathRole::InstallRoot),
        Protection::Protected
    );

    // The runtime manager's, as earlier layouts had it: what is in the store
    // runs as root, so no other account may own it.
    let mut identity = tree.identity();
    identity.trusted_uid += 1;
    let found = problems(audit(&store, PathRole::InstallRoot, &identity).protection);
    assert!(
        found.iter().any(|problem| problem.contains("not root")),
        "{found:?}"
    );
}

#[test]
fn an_install_root_writable_by_a_group_or_everyone_is_unprotected() {
    let tree = Tree::new();
    tree.dir("vendor", 0o755);
    // Group-writable, sticky or not, as the runtime manager's group once
    // was allowed.
    for mode in [0o775, 0o1775] {
        let root = tree.dir("vendor/firecracker", mode);
        let found = problems(tree.audit(&root, PathRole::InstallRoot));
        assert!(
            found
                .iter()
                .any(|problem| problem.contains("only root may write")),
            "{mode:o}: {found:?}"
        );
    }

    let open = tree.dir("vendor/kernels", 0o757);
    let found = problems(tree.audit(&open, PathRole::InstallRoot));
    assert!(
        found
            .iter()
            .any(|problem| problem.contains("writable by every account")),
        "{found:?}"
    );
}

#[test]
fn a_linked_install_root_is_audited_where_it_resolves() {
    let tree = Tree::new();
    tree.dir("vendor/runtime-manager/kernels", 0o755);
    std::fs::set_permissions(
        tree.path("vendor/runtime-manager"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    symlink("runtime-manager/kernels", tree.path("vendor/kernels")).unwrap();
    assert_eq!(
        tree.audit(&tree.path("vendor/kernels"), PathRole::InstallRoot),
        Protection::Protected
    );

    std::fs::set_permissions(
        tree.path("vendor/runtime-manager/kernels"),
        std::fs::Permissions::from_mode(0o757),
    )
    .unwrap();
    let found = problems(tree.audit(&tree.path("vendor/kernels"), PathRole::InstallRoot));
    assert!(
        found.iter().any(|problem| problem
            .contains("runtime-manager/kernels is writable by every account")),
        "{found:?}"
    );

    symlink("runtime-manager/missing", tree.path("vendor/firecracker")).unwrap();
    let found = problems(tree.audit(&tree.path("vendor/firecracker"), PathRole::InstallRoot));
    assert!(
        found
            .iter()
            .any(|problem| problem.contains("cannot resolve")),
        "{found:?}"
    );
}

#[test]
fn a_cache_kernel_root_under_sticky_shared_parents_is_protected() {
    let tree = Tree::new();
    tree.dir("cache", 0o1775);
    tree.dir("cache/firecracker", 0o1775);
    let kernels = tree.dir("cache/firecracker/kernels", 0o755);

    assert_eq!(
        tree.audit(&kernels, PathRole::RootManagedCache),
        Protection::Protected
    );
}

#[test]
fn a_group_writable_parent_without_the_sticky_bit_exposes_its_children() {
    let tree = Tree::new();
    // Today's cache skeleton: 2775 without the sticky bit.
    tree.dir("cache", 0o775);
    tree.dir("cache/firecracker", 0o775);
    let kernels = tree.dir("cache/firecracker/kernels", 0o755);

    let found = problems(tree.audit(&kernels, PathRole::RootManagedCache));
    assert_eq!(
        found
            .iter()
            .filter(|problem| problem.contains("without the sticky bit"))
            .count(),
        2,
        "both shared parents are reported: {found:?}"
    );
}

#[test]
fn a_group_writable_cache_kernel_root_is_unprotected() {
    let tree = Tree::new();
    tree.dir("cache", 0o1775);
    tree.dir("cache/firecracker", 0o1775);
    let kernels = tree.dir("cache/firecracker/kernels", 0o775);

    let found = problems(tree.audit(&kernels, PathRole::RootManagedCache));
    assert!(
        found
            .iter()
            .any(|problem| problem.contains("writable by accounts")),
        "{found:?}"
    );
}

#[test]
fn state_must_be_private_to_the_runtime_manager() {
    let tree = Tree::new();
    tree.dir("vendor", 0o755);
    tree.dir("vendor/runtime-manager", 0o755);
    let state = tree.dir("vendor/runtime-manager/state", 0o700);
    assert_eq!(
        tree.audit(&state, PathRole::StateDirectory),
        Protection::Protected
    );
    // Clients traverse it to reach the socket beside the store.
    std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o750)).unwrap();
    let database = state.join("runtime-manager.db");
    std::fs::write(&database, b"").unwrap();
    std::fs::set_permissions(&database, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(
        tree.audit(&state, PathRole::StateDirectory),
        Protection::Protected
    );

    std::fs::set_permissions(&database, std::fs::Permissions::from_mode(0o640)).unwrap();
    let found = problems(tree.audit(&state, PathRole::StateDirectory));
    assert!(
        found.iter().any(|problem| problem.contains("private")),
        "{found:?}"
    );
    std::fs::set_permissions(&database, std::fs::Permissions::from_mode(0o600)).unwrap();
    std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o770)).unwrap();
    let found = problems(tree.audit(&state, PathRole::StateDirectory));
    assert!(
        found
            .iter()
            .any(|problem| problem.contains("writable by accounts other")),
        "{found:?}"
    );
}

#[test]
fn state_owned_by_another_account_is_unprotected() {
    let tree = Tree::new();
    tree.dir("vendor", 0o755);
    let state = tree.dir("vendor/state", 0o700);
    let mut identity = tree.identity();
    identity.service_uid += 1;

    let found = problems(audit(&state, PathRole::StateDirectory, &identity).protection);
    assert!(
        found
            .iter()
            .any(|problem| problem.contains("not the runtime manager's uid")),
        "{found:?}"
    );
}

#[test]
fn ancestors_owned_by_an_untrusted_account_are_reported() {
    let tree = Tree::new();
    tree.dir("vendor", 0o755);
    let root = tree.dir("vendor/firecracker", 0o1775);
    let mut identity = tree.identity();
    identity.trusted_uid += 1;

    let found = problems(audit(&root, PathRole::InstallRoot, &identity).protection);
    assert!(
        found
            .iter()
            .any(|problem| problem.contains("so its owner can replace")),
        "{found:?}"
    );
}

#[test]
fn a_symlinked_ancestor_or_target_is_unprotected() {
    let tree = Tree::new();
    tree.dir("real/kernels", 0o755);
    symlink(tree.path("real"), tree.path("link")).unwrap();
    let found = problems(tree.audit(&tree.path("link/kernels"), PathRole::RootManagedCache));
    assert!(
        found.iter().any(|problem| problem.contains("is a symlink")),
        "{found:?}"
    );

    symlink(tree.path("real/kernels"), tree.path("kernels")).unwrap();
    let found = problems(tree.audit(&tree.path("kernels"), PathRole::RootManagedCache));
    assert!(
        found.iter().any(|problem| problem.contains("is a symlink")),
        "{found:?}"
    );
}

#[test]
fn a_missing_path_is_absent_and_a_file_is_not_a_directory() {
    let tree = Tree::new();
    assert_eq!(
        tree.audit(&tree.path("vendor/firecracker"), PathRole::InstallRoot),
        Protection::Absent
    );

    std::fs::write(tree.path("state"), b"").unwrap();
    let found = problems(tree.audit(&tree.path("state"), PathRole::StateDirectory));
    assert!(
        found
            .iter()
            .any(|problem| problem.contains("not a directory")),
        "{found:?}"
    );
}

#[test]
fn a_kernel_image_another_account_can_write_is_unprotected() {
    let tree = Tree::new();
    tree.dir("cache", 0o1775);
    tree.dir("cache/firecracker", 0o1775);
    let assets = tree.dir("cache/firecracker/assets", 0o755);
    tree.dir("cache/firecracker/assets/v1.15.1/kernel", 0o755);
    let image = tree.path("cache/firecracker/assets/v1.15.1/kernel/vmlinux.bin");
    std::fs::write(&image, b"kernel").unwrap();
    std::fs::set_permissions(&image, std::fs::Permissions::from_mode(0o644)).unwrap();
    symlink("v1.15.1", tree.path("cache/firecracker/assets/default")).unwrap();
    assert_eq!(
        tree.audit(&assets, PathRole::RootManagedCache),
        Protection::Protected,
        "a root-owned 0644 image and the version alias are fine"
    );

    std::fs::set_permissions(&image, std::fs::Permissions::from_mode(0o664)).unwrap();
    let found = problems(tree.audit(&assets, PathRole::RootManagedCache));
    assert!(
        found
            .iter()
            .any(|problem| problem.contains("vmlinux.bin has mode 664")),
        "{found:?}"
    );

    // Jail setup used to chown the shared kernel inode to a sandbox uid.
    std::fs::set_permissions(&image, std::fs::Permissions::from_mode(0o644)).unwrap();
    let mut identity = tree.identity();
    identity.trusted_uid += 1;
    identity.service_uid += 1;
    let found = problems(audit(&assets, PathRole::RootManagedCache, &identity).protection);
    assert!(
        found
            .iter()
            .any(|problem| problem.contains("which can rewrite it")),
        "{found:?}"
    );
}

#[test]
fn install_root_entries_must_be_roots() {
    let tree = Tree::new();
    tree.dir("vendor", 0o755);
    let root = tree.dir("vendor/firecracker", 0o755);
    tree.dir("vendor/firecracker/v1.15.1/bin", 0o755);
    std::fs::write(
        tree.path("vendor/firecracker/v1.15.1/bin/firecracker"),
        b"fc",
    )
    .unwrap();
    assert_eq!(
        tree.audit(&root, PathRole::InstallRoot),
        Protection::Protected
    );
    let mut identity = tree.identity();
    identity.trusted_uid += 1; // the test user plays the runtime manager here
    let report = audit(&root, PathRole::InstallRoot, &identity).protection;
    let found = problems(report);
    assert!(
        found
            .iter()
            .any(|problem| problem.contains("which can rewrite it")),
        "entries another account owns are not allowed: {found:?}"
    );
}

#[test]
fn content_problems_are_capped() {
    let tree = Tree::new();
    tree.dir("cache", 0o755);
    let kernels = tree.dir("cache/kernels", 0o755);
    for index in 0..20 {
        tree.dir(&format!("cache/kernels/6.1.{index}"), 0o775);
    }
    let found = problems(tree.audit(&kernels, PathRole::RootManagedCache));
    assert_eq!(found.len(), MAX_REPORTED_CONTENT_PROBLEMS + 1, "{found:?}");
    assert!(
        found.last().unwrap().starts_with("and 12 more under"),
        "{found:?}"
    );
}
