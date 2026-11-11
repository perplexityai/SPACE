use super::*;

const MAPPER: &str = "sbxvol-01a0a702-518c-7211-a938-72bfa87751b0-snapshot";

fn device(block: Option<(u32, u32)>) -> BusyDevice {
    BusyDevice {
        mapper: MAPPER.to_string(),
        block,
        ..BusyDevice::default()
    }
}

fn matching_points<'a>(
    mountinfo: &'a str,
    device: &BusyDevice,
    superblock: Option<(u32, u32)>,
) -> Vec<&'a str> {
    mountinfo
        .lines()
        .filter_map(parse_mountinfo_line)
        .filter(|mount| is_volume_mount(mount, device, superblock))
        .map(|mount| mount.point)
        .collect()
}

/// A namespace that copied the volume mount privately keeps it after
/// node-manager's lazy unmount. Its mountinfo may spell the source by mapper
/// name, by `/dev/dm-N`, or (after a rename) by neither; the btrfs superblock
/// number still identifies it, but only when the caller has shown it current.
#[test]
fn volume_mounts_match_by_source_and_by_a_current_superblock() {
    let mountinfo = format!(
        "\
        900 880 0:851 / /space/cache/sandboxes/a/snapshot-vol rw - btrfs /dev/mapper/{MAPPER} rw\n\
        901 880 0:851 / /jail/firecracker/a/root/encsnap rw - btrfs /dev/dm-5 rw\n\
        902 880 0:851 / /tmp/renamed rw - btrfs /dev/mapper/{MAPPER}-leak-1 rw\n\
        903 880 0:852 / /space/cache/sandboxes/b/snapshot-vol rw - btrfs /dev/dm-6 rw\n\
        904 880 259:1 / /space/cache rw - btrfs /dev/nvme1n1 rw\n"
    );
    let device = device(Some((253, 5)));

    assert_eq!(
        matching_points(&mountinfo, &device, Some((0, 851))),
        [
            "/space/cache/sandboxes/a/snapshot-vol",
            "/jail/firecracker/a/root/encsnap",
            "/tmp/renamed",
        ]
    );
    // Without a current superblock, a renamed source no longer matches: its
    // number may belong to an unrelated filesystem.
    assert_eq!(
        matching_points(&mountinfo, &device, None),
        [
            "/space/cache/sandboxes/a/snapshot-vol",
            "/jail/firecracker/a/root/encsnap",
        ]
    );

    let line = parse_mountinfo_line(mountinfo.lines().next().unwrap()).unwrap();
    assert_eq!((line.id, line.numbers), (900, Some((0, 851))));
}

#[test]
fn parses_dmsetup_info_open_count_and_numbers() {
    let info = "\
Name:              sbxvol-x-snapshot
State:             ACTIVE
Read Ahead:        256
Tables present:    LIVE
Open count:        1
Event number:      0
Major, minor:      253, 5
Number of targets: 1
UUID: CRYPT-LUKS2-0123-sbxvol-x-snapshot
";
    assert_eq!(parse_open_count(info), Some(1));
    assert_eq!(parse_dmsetup_device_numbers(info), Some((253, 5)));
    assert_eq!(parse_open_count("Device does not exist."), None);
}

/// `/proc/<pid>/maps` prints the superblock device in hex.
#[test]
fn parses_maps_device_in_hex() {
    let line = "7f0000000000-7f0200000000 rw-p 00000000 00:38 257 /encsnap/memory";
    assert_eq!(parse_maps_line(line), Some(((0, 0x38), "/encsnap/memory")));
    let anonymous = "7ffd1000-7ffd2000 rw-p 00000000 00:00 0 ";
    assert_eq!(parse_maps_line(anonymous), Some(((0, 0), "")));
}

#[test]
fn parses_fdinfo_mount_id() {
    let fdinfo = "pos:\t0\nflags:\t02100000\nmnt_id:\t1234\nino:\t257\n";
    assert_eq!(parse_fdinfo_mount_id(fdinfo), Some(1234));
    assert_eq!(parse_fdinfo_mount_id("pos:\t0\n"), None);
}

/// Command lines and file names come from other processes; a newline or an
/// escape sequence in one must not reach the log as a control character.
#[test]
fn printable_escapes_control_characters() {
    assert_eq!(
        printable("sh -c 'a\nb'\u{1b}[2J\tend"),
        "sh -c 'a\\nb'\\u{1b}[2J\\tend"
    );
    assert_eq!(printable("/encsnap/memory"), "/encsnap/memory");
}
