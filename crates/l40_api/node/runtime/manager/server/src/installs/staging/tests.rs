use super::*;

/// The first bytes of an x86_64 ELF `vmlinux` and of an arm64 Linux `Image`
/// (a PE stub with the `ARM\x64` magic at byte 56), as Firecracker publishes
/// them.
fn header(architecture: Arch) -> Vec<u8> {
    let mut header = vec![0u8; 4096];
    match architecture {
        Arch::X86_64 => {
            header[..4].copy_from_slice(b"\x7fELF");
            header[4] = 2;
            header[5] = 1;
            header[6] = 1;
            header[16..18].copy_from_slice(&2u16.to_le_bytes());
            header[18..20].copy_from_slice(&62u16.to_le_bytes());
            header[20..24].copy_from_slice(&1u32.to_le_bytes());
            header[52..54].copy_from_slice(&64u16.to_le_bytes());
        }
        Arch::Aarch64 => {
            header[..2].copy_from_slice(b"MZ");
            header[56..60].copy_from_slice(b"ARMd");
        }
    }
    header
}

#[test]
fn each_architecture_accepts_only_its_own_boot_image_format() {
    let dir = tempfile::tempdir().unwrap();
    let write = |name: &str, bytes: &[u8]| {
        let path = dir.path().join(name);
        std::fs::write(&path, bytes).unwrap();
        path
    };
    let x86 = write("x86", &header(Arch::X86_64));
    let arm = write("arm", &header(Arch::Aarch64));
    let short = write("short", b"MZ");
    let empty = write("zeros", &[0u8; 4096]);

    assert!(is_boot_image(&x86, Arch::X86_64).unwrap());
    assert!(is_boot_image(&arm, Arch::Aarch64).unwrap());
    // An arm64 node does not boot an ELF kernel, nor an x86_64 node an Image.
    assert!(!is_boot_image(&x86, Arch::Aarch64).unwrap());
    assert!(!is_boot_image(&arm, Arch::X86_64).unwrap());
    for architecture in [Arch::X86_64, Arch::Aarch64] {
        assert!(!is_boot_image(&short, architecture).unwrap());
        assert!(!is_boot_image(&empty, architecture).unwrap());
    }
    is_boot_image(&dir.path().join("missing"), Arch::Aarch64).unwrap_err();
}
