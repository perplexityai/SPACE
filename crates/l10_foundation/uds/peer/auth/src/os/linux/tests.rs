use super::*;

const SAMPLE_STATUS: &str = "\
Name:\tspace-sbxmgr\n\
State:\tS (sleeping)\n\
Tgid:\t12345\n\
Pid:\t12345\n\
PPid:\t1\n\
Uid:\t1001\t1001\t1001\t1001\n\
Gid:\t1001\t1001\t1001\t1001\n\
CapInh:\t0000000000000000\n\
CapPrm:\t00000000a80425fb\n\
CapEff:\t00000000a80425fb\n\
CapBnd:\t000001ffffffffff\n\
CapAmb:\t0000000000000000\n";

#[test]
fn parses_proc_status_uid_and_capeff() {
    let parsed = parse_proc_status(SAMPLE_STATUS).expect("status parses");
    assert_eq!(parsed.effective_uid, 1001);
    assert_eq!(parsed.cap_effective, 0x0000_0000_a804_25fb);
}

#[test]
fn parses_proc_status_with_split_uids() {
    // setresuid()-style: real != effective. We keep the effective uid,
    // which is what SO_PEERCRED is compared against.
    let s = "Uid:\t0\t1001\t0\t0\nCapEff:\t0000000000000000\n";
    let parsed = parse_proc_status(s).expect("status parses");
    assert_eq!(parsed.effective_uid, 1001);
    assert_eq!(parsed.cap_effective, 0);
}

#[test]
fn rejects_proc_status_with_single_uid_column() {
    // A `Uid:` line missing the effective column is malformed; we must not
    // silently fall back to the real uid.
    let s = "Uid:\t1001\nCapEff:\t0000000000000000\n";
    assert!(parse_proc_status(s).is_none());
}

#[test]
fn rejects_proc_status_missing_capeff() {
    let s = "Uid:\t1001\t1001\t1001\t1001\n";
    assert!(parse_proc_status(s).is_none());
}

#[test]
fn rejects_proc_status_missing_uid() {
    let s = "CapEff:\t0000000000000000\n";
    assert!(parse_proc_status(s).is_none());
}
