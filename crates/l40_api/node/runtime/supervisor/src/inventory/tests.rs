use super::*;

#[test]
fn live_runtime_pid_removes_stale_symlink() -> Result<(), Box<dyn std::error::Error>> {
    let sandbox_id = SandboxId::new();
    let tempdir = tempfile::tempdir()?;
    let pid_link = tempdir.path().join("fc.pid");
    unix_fs::symlink("/proc/999999999", &pid_link)?;

    let pid_link = pid_link.to_string_lossy();
    assert_eq!(live_runtime_pid_at(&sandbox_id, &pid_link)?, None);
    assert!(!std::path::Path::new(pid_link.as_ref()).exists());

    Ok(())
}
