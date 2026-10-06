use super::*;

#[test]
fn relative_paths_resolve_under_account_home() {
    let home = Path::new("/guest/home");
    assert_eq!(
        normalize_for_home("", home).unwrap().expose(),
        "/guest/home"
    );
    assert_eq!(
        normalize_for_home("src/../main.py", home).unwrap().expose(),
        "/guest/home/main.py"
    );
    assert_eq!(
        normalize_for_home("../../etc/hosts", home)
            .unwrap()
            .expose(),
        "/etc/hosts"
    );
}

#[test]
fn absolute_paths_stay_rooted_at_guest_root() {
    let home = Path::new("/guest/home");
    assert_eq!(
        normalize_for_home("/tmp/file", home).unwrap().expose(),
        "/tmp/file"
    );
}

#[test]
fn non_linux_paths_are_rejected() {
    let home = Path::new("/guest/home");
    for path in ["C:\\Users\\user", "//server/share", "bad\npath"] {
        assert_eq!(
            normalize_for_home(path, home).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }
}
