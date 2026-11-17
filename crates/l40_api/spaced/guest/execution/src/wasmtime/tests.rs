use super::*;

fn profile() -> WasiProcessProfile {
    WasiProcessProfile::new(
        "/tools/wasmtime".into(),
        "/tools/python.wasm".into(),
        "/sandboxes/one/workspace".into(),
        "/tools/python".into(),
        128 * 1024 * 1024,
    )
    .unwrap()
}

#[test]
fn rejects_native_execution_pty_and_other_sandbox_cwd() {
    for (argv, cwd, pty) in [
        (vec!["/bin/sh".into()], "/sandboxes/one/workspace", false),
        (vec!["python3".into()], "/sandboxes/two/workspace", false),
        (vec!["python3".into()], "/sandboxes/one/workspace", true),
    ] {
        profile()
            .command(&argv, Path::new(cwd), std::iter::empty(), pty)
            .unwrap_err();
    }
}

#[test]
fn guest_environment_never_becomes_native_engine_environment() {
    let command = profile()
        .command(
            &["python3".into(), "-c".into(), "print(42)".into()],
            Path::new("/sandboxes/one/workspace"),
            [("LD_PRELOAD".into(), "/workspace/injected.so".into())].into_iter(),
            false,
        )
        .unwrap();
    let native = command.as_std();
    assert_eq!(native.get_program(), "/tools/wasmtime");
    assert_eq!(native.get_envs().count(), 0);
    let arguments: Vec<_> = native.get_args().map(|a| a.to_str().unwrap()).collect();
    assert!(
        arguments
            .windows(2)
            .any(|a| a == ["--env", "LD_PRELOAD=/workspace/injected.so"])
    );
}
