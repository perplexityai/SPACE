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
fn each_runtime_and_architecture_accepts_only_its_boot_image_formats() {
    let dir = tempfile::tempdir().unwrap();
    let mut bzimage = vec![0; 4096];
    bzimage[0x1fe..0x200].copy_from_slice(&0xaa55u16.to_le_bytes());
    bzimage[0x202..0x206].copy_from_slice(b"HdrS");
    bzimage[0x206..0x208].copy_from_slice(&0x020cu16.to_le_bytes());
    bzimage[0x211] = 1;
    bzimage[0x236] = 1;
    let mut x86_32 = bzimage.clone();
    x86_32[0x236] = 0;
    let mut old_protocol = bzimage.clone();
    old_protocol[0x206..0x208].copy_from_slice(&0x020bu16.to_le_bytes());
    let mut low_image = bzimage.clone();
    low_image[0x211] = 0;
    let mut no_payload = bzimage.clone();
    no_payload.truncate(2560);
    for (name, bytes, allowed) in [
        ("elf", header(Arch::X86_64), [true, true, false, false]),
        ("arm", header(Arch::Aarch64), [false, false, true, true]),
        ("bzimage", bzimage, [false, true, false, false]),
        ("32-bit", x86_32, [false; 4]),
        ("old", old_protocol, [false; 4]),
        ("low", low_image, [false; 4]),
        ("no-payload", no_payload, [false; 4]),
        ("short", b"MZ".to_vec(), [false; 4]),
        ("zeros", vec![0; 4096], [false; 4]),
    ] {
        let path = dir.path().join(name);
        std::fs::write(&path, bytes).unwrap();
        for ((arch, runtime), expected) in [
            (Arch::X86_64, RuntimeKind::Firecracker),
            (Arch::X86_64, RuntimeKind::CloudHypervisor),
            (Arch::Aarch64, RuntimeKind::Firecracker),
            (Arch::Aarch64, RuntimeKind::CloudHypervisor),
        ]
        .into_iter()
        .zip(allowed)
        {
            assert_eq!(
                is_boot_image(&path, arch, runtime).unwrap(),
                expected,
                "{name} {runtime} {arch}"
            );
        }
    }
    is_boot_image(
        &dir.path().join("missing"),
        Arch::Aarch64,
        RuntimeKind::CloudHypervisor,
    )
    .unwrap_err();
}
