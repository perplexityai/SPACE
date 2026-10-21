use super::UnixSocketPath;

#[test]
fn unix_socket_path_rejects_platform_invalid_paths() {
    UnixSocketPath::parse("/tmp/space.sock").unwrap();
    UnixSocketPath::parse("space.sock").unwrap_err();
    UnixSocketPath::parse("/tmp/space\0sock").unwrap_err();
    UnixSocketPath::parse(format!("/tmp/{}.sock", "a".repeat(120))).unwrap_err();
}
