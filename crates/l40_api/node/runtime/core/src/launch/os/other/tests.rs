use super::{
    ProcessHandle, process_state_from_status, validate_process_pid, validate_raw_process_pid,
};

#[test]
fn reject_process_group_pids() {
    validate_process_pid(0).unwrap_err();
    validate_raw_process_pid(0).unwrap_err();
    validate_raw_process_pid(-1).unwrap_err();
}

#[test]
fn reject_own_process_pids() {
    let pid = std::process::id();
    validate_process_pid(pid).unwrap_err();
    validate_raw_process_pid(pid as i32).unwrap_err();
}

#[test]
fn accept_positive_process_pids() {
    let pid = if std::process::id() == 1 { 2 } else { 1 };
    assert_eq!(validate_process_pid(pid).unwrap(), pid as libc::pid_t);
    assert_eq!(
        validate_raw_process_pid(pid as i32).unwrap(),
        pid as libc::pid_t
    );
}

#[test]
fn process_handle_rejects_process_group_pid() {
    let Err(_) = ProcessHandle::open(0) else {
        panic!("a process-group pid must be rejected");
    };
}

#[test]
fn parses_proc_status_state() {
    let status = "Name:\tfirecracker\nState:\tZ (zombie)\nPid:\t123\n";
    assert_eq!(process_state_from_status(status), Some('Z'));

    let status = "Name:\tfirecracker\nState:\tS (sleeping)\nPid:\t123\n";
    assert_eq!(process_state_from_status(status), Some('S'));

    assert_eq!(process_state_from_status("Name:\tfirecracker\n"), None);
}
