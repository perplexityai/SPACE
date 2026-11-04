use std::path::PathBuf;
use std::process::Command;

fn gateway_binary() -> PathBuf {
    std::env::var_os("NEXTEST_BIN_EXE_space-netgtw-svc")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_space-netgtw-svc")))
}

#[test]
fn retired_tls_interception_mode_is_rejected() {
    let output = Command::new(gateway_binary())
        .args(["--tls-interception-mode", "off", "--version"])
        .output()
        .expect("run space-netgtw-svc");
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(!output.status.success(), "retired flag was accepted");
    assert!(
        stderr.contains("unexpected argument '--tls-interception-mode'"),
        "unexpected stderr: {stderr}"
    );
}
