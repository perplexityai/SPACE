#![cfg(target_os = "linux")]

use std::ffi::OsString;
use std::fs;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::process::Command;

use pplx_space_local_node_mgr::snapshot::extract_ext4_to_dir;

#[test]
#[ignore = "requires root to repair read-only inline files"]
fn inline_rootfs_extraction_preserves_file_contents_and_names() {
    let fixture = tempfile::tempdir().unwrap();
    let source = fixture.path().join("source");
    fs::create_dir_all(source.join("nested")).unwrap();
    let sizes = [0, 1, 14, 59, 60, 61, 100, 160, 200, 4096];
    for size in sizes {
        let content: Vec<_> = (0..size).map(|index| (index % 251) as u8).collect();
        fs::write(source.join("nested").join(size.to_string()), content).unwrap();
    }
    let names = [
        b"line\nbreak".as_slice(),
        b"quoted\"name",
        b"back\\slash",
        b"non-utf8-\xff",
    ];
    for name in names {
        fs::write(
            source.join(OsString::from_vec(name.to_vec())),
            b"small payload",
        )
        .unwrap();
    }
    fs::set_permissions(source.join("nested/14"), fs::Permissions::from_mode(0o444)).unwrap();
    symlink("nested/14", source.join("link")).unwrap();
    fs::hard_link(source.join("nested/14"), source.join("hardlink")).unwrap();

    for features in ["^inline_data", "inline_data"] {
        let image = fixture.path().join(format!("{features}.ext4"));
        fs::File::create(&image)
            .unwrap()
            .set_len(64 * 1024 * 1024)
            .unwrap();
        let status = Command::new("mke2fs")
            .args(["-q", "-t", "ext4", "-O", features, "-d"])
            .arg(&source)
            .arg("-F")
            .arg(&image)
            .status()
            .unwrap();
        assert!(status.success());
        let extracted = fixture.path().join(features);

        extract_ext4_to_dir(&image, &extracted).unwrap();

        for size in sizes {
            let name = format!("nested/{size}");
            assert_eq!(
                fs::read(extracted.join(&name)).unwrap(),
                fs::read(source.join(name)).unwrap()
            );
        }
        for name in names {
            assert_eq!(
                fs::read(extracted.join(OsString::from_vec(name.to_vec()))).unwrap(),
                b"small payload"
            );
        }
        assert_eq!(
            fs::read_link(extracted.join("link")).unwrap(),
            std::path::Path::new("nested/14")
        );
        assert_eq!(
            fs::metadata(extracted.join("nested/14"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o444
        );
        assert_eq!(
            fs::read(extracted.join("hardlink")).unwrap(),
            fs::read(source.join("hardlink")).unwrap()
        );
    }
}
