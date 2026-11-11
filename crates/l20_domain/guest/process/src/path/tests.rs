use super::{
    GuestPath, MAX_GUEST_PATH_BYTES, MAX_RELATIVE_GLOB_PATTERN_BYTES,
    MAX_SAFE_PATH_COMPONENT_BYTES, RelativeGlobPattern, RelativeGuestPath, SafePathComponent,
    conventional_linux_guest_home, normalize_linux_guest_path,
};
use crate::GuestUserName;

#[test]
fn guest_paths_reject_controls_and_excessive_length() {
    GuestPath::parse("/workspace").unwrap();
    GuestPath::parse("/work\nspace").unwrap_err();
    GuestPath::parse("x".repeat(MAX_GUEST_PATH_BYTES + 1)).unwrap_err();

    RelativeGuestPath::parse("src/main.rs").unwrap();
    RelativeGuestPath::parse("src/\rmain.rs").unwrap_err();
    RelativeGuestPath::parse("x".repeat(MAX_GUEST_PATH_BYTES + 1)).unwrap_err();
}

#[test]
fn linux_guest_path_normalization_roots_relative_paths_at_home() {
    let user = GuestUserName::parse("user").unwrap();
    let home = conventional_linux_guest_home(&user).unwrap();
    assert_eq!(
        normalize_linux_guest_path("", &home).unwrap().expose(),
        "/home/user"
    );
    assert_eq!(
        normalize_linux_guest_path(".", &home).unwrap().expose(),
        "/home/user"
    );
    assert_eq!(
        normalize_linux_guest_path("src/../main.py", &home)
            .unwrap()
            .expose(),
        "/home/user/main.py"
    );
    assert_eq!(
        normalize_linux_guest_path("../../etc/shadow", &home)
            .unwrap()
            .expose(),
        "/etc/shadow"
    );
}

#[test]
fn linux_guest_path_normalization_preserves_absolute_paths() {
    let user = GuestUserName::parse("root").unwrap();
    let home = conventional_linux_guest_home(&user).unwrap();
    assert_eq!(home.expose(), "/root");
    assert_eq!(
        normalize_linux_guest_path("/tmp/file", &home)
            .unwrap()
            .expose(),
        "/tmp/file"
    );
    assert_eq!(
        normalize_linux_guest_path("workspace/file", &home)
            .unwrap()
            .expose(),
        "/root/workspace/file"
    );
}

#[test]
fn linux_guest_path_normalization_rejects_non_linux_shapes() {
    let user = GuestUserName::parse("user").unwrap();
    let home = conventional_linux_guest_home(&user).unwrap();
    normalize_linux_guest_path("/tmp/\nfile", &home).unwrap_err();
    normalize_linux_guest_path("C:\\Users\\user\\file.txt", &home).unwrap_err();
    normalize_linux_guest_path("C:/Users/user/file.txt", &home).unwrap_err();
    normalize_linux_guest_path("\\\\server\\share\\file.txt", &home).unwrap_err();
    normalize_linux_guest_path("//server/share/file.txt", &home).unwrap_err();
}

#[test]
fn relative_glob_pattern_rejects_parent_traversal() {
    RelativeGlobPattern::parse("src/**/*.rs").unwrap();
    RelativeGlobPattern::parse("../secret/**").unwrap_err();
    RelativeGlobPattern::parse("../../../etc/**").unwrap_err();
    RelativeGlobPattern::parse("src/../secret/**").unwrap_err();
    RelativeGlobPattern::parse("src/**\n*").unwrap_err();
    RelativeGlobPattern::parse("x".repeat(MAX_RELATIVE_GLOB_PATTERN_BYTES + 1)).unwrap_err();
}

#[test]
fn safe_path_component_rejects_oversized_components() {
    SafePathComponent::parse("a".repeat(MAX_SAFE_PATH_COMPONENT_BYTES)).unwrap();
    SafePathComponent::parse("a".repeat(MAX_SAFE_PATH_COMPONENT_BYTES + 1)).unwrap_err();
}
