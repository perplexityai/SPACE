use super::*;

#[cfg(unix)]
#[test]
fn path_to_string_rejects_non_utf8_paths() {
    use std::os::unix::ffi::OsStrExt;

    let path = Path::new(std::ffi::OsStr::from_bytes(b"\xff"));
    let error = path_to_string(path).expect_err("non-utf8 path rejected");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
}
