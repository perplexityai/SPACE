use super::*;

#[test]
fn role_sizes_parse_with_bytesize_parser() {
    assert_eq!(
        MemorySize::from_str("4 GiB").unwrap().as_bytes(),
        4_294_967_296
    );
    assert_eq!(
        DiskSize::from_str("4 GB").unwrap().as_bytes(),
        4_000_000_000
    );
}

#[test]
fn role_sizes_reject_zero() {
    assert_eq!(MemorySize::b(0), Err(SizeParseError::Zero));
    assert_eq!(DiskSize::b(0), Err(SizeParseError::Zero));
    assert_eq!(MemorySize::try_from(0), Err(SizeParseError::Zero));
    assert_eq!(ByteSize::b(0).as_u64(), 0);
}

#[test]
fn role_sizes_support_policy_caps_without_global_maximum() {
    let memory = MemorySize::gib(4).unwrap();
    assert_eq!(
        memory.ensure_at_most(MemorySize::gib(2).unwrap()),
        Err(SizeParseError::ExceedsPolicy)
    );
    assert_eq!(
        memory.ensure_at_most(MemorySize::gib(8).unwrap()),
        Ok(memory)
    );

    let disk = DiskSize::gib(20).unwrap();
    assert_eq!(
        disk.ensure_at_most(DiskSize::gib(10).unwrap()),
        Err(SizeParseError::ExceedsPolicy)
    );
    assert_eq!(disk.ensure_at_most(DiskSize::gib(20).unwrap()), Ok(disk));
}
