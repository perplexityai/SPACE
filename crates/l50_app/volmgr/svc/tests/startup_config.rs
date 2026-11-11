use std::path::PathBuf;
use std::process::Command;

fn space_volmgr_svc_path() -> PathBuf {
    std::env::var_os("NEXTEST_BIN_EXE_space-volmgr-svc")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_space-volmgr-svc")))
}

#[test]
fn duplicate_pull_priority_bands_return_a_configuration_error() {
    let tempdir = std::env::temp_dir().join(format!(
        "space-volmgr-startup-config-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&tempdir);
    std::fs::create_dir(&tempdir).unwrap();
    let socket = tempdir.join("volmgr.sock");
    let db = tempdir.join("volmgr.db");
    let staging = tempdir.join("staging");
    let mut command = Command::new(space_volmgr_svc_path());
    if cfg!(target_os = "macos") {
        command.env("SSL_CERT_FILE", "/etc/ssl/cert.pem");
    }
    let output = command
        .args([
            "--socket",
            socket.to_str().unwrap(),
            "--db",
            db.to_str().unwrap(),
            "--staging-dir",
            staging.to_str().unwrap(),
            "--pull-priority-band",
            "100:4:8",
            "--pull-priority-band",
            "100:2:4",
        ])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    std::fs::remove_dir_all(tempdir).unwrap();

    assert!(!output.status.success());
    assert!(
        stderr.contains("duplicate pull priority band minimum 100"),
        "unexpected stderr: {stderr}"
    );
    assert!(
        !stderr.contains("panicked at"),
        "unexpected panic: {stderr}"
    );
}
