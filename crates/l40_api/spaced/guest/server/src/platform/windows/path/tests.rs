use super::*;
#[test]
fn local_paths_refuse_windows_namespace_and_alias_escapes() {
    let home = Path::new(r"C:\Users\user");
    for path in [
        r"C:\work\a.txt",
        r"work\a.txt",
        ".",
        r"..\other",
        r"D:/data/file",
    ] {
        assert!(LocalPath::parse(path, home).is_ok(), "{path}");
    }
    for path in [
        r"\\server\share",
        r"\\?\C:\work",
        r"\\.\pipe\x",
        r"C:relative",
        r"\rooted",
        r"C:\x:stream",
        r"C:\NUL.txt",
        r"C:\dir.\file",
        r"C:\COM1",
        r"C:\COM¹.txt",
        r"C:\a*",
        "C:\\bad\0x",
    ] {
        assert!(LocalPath::parse(path, home).is_err(), "{path}");
    }
}
