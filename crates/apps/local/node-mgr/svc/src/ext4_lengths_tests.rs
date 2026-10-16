use std::fs;
use std::os::unix::fs::symlink;
use std::process::Command;

use super::restore_file_lengths;

#[test]
#[cfg(target_os = "linux")]
fn correction_rejects_replaced_entries_and_nonzero_padding() {
    for invalid in [
        "file symlink",
        "directory symlink",
        "hardlink",
        "nonzero padding",
    ] {
        let fixture = tempfile::tempdir().unwrap();
        let source = fixture.path().join("source");
        fs::create_dir_all(source.join("nested")).unwrap();
        fs::write(source.join("nested/file"), b"small payload").unwrap();
        let image = fixture.path().join("rootfs.ext4");
        fs::File::create(&image)
            .unwrap()
            .set_len(64 * 1024 * 1024)
            .unwrap();
        assert!(
            Command::new("mke2fs")
                .args(["-q", "-t", "ext4", "-O", "inline_data", "-d"])
                .arg(&source)
                .arg("-F")
                .arg(&image)
                .status()
                .unwrap()
                .success()
        );
        let extracted = fixture.path().join("extracted");
        fs::create_dir_all(extracted.join("nested")).unwrap();
        let target = extracted.join("nested/file");
        let external = fixture.path().join("external");
        fs::create_dir(&external).unwrap();
        let external_file = external.join("file");
        fs::write(&external_file, b"external file must remain intact").unwrap();
        match invalid {
            "file symlink" => symlink(&external_file, &target).unwrap(),
            "directory symlink" => {
                fs::remove_dir(extracted.join("nested")).unwrap();
                symlink(&external, extracted.join("nested")).unwrap();
            }
            "hardlink" => fs::hard_link(&external_file, &target).unwrap(),
            "nonzero padding" => fs::write(&target, b"small payload trailing data").unwrap(),
            _ => unreachable!(),
        }
        fs::create_dir(extracted.join("lost+found")).unwrap();

        let result = restore_file_lengths(&image, &extracted);

        assert!(result.is_err(), "{invalid}");
        assert_eq!(
            fs::read(&external_file).unwrap(),
            b"external file must remain intact"
        );
    }
}
