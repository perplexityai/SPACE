use super::*;

#[test]
fn output_is_preserved_up_to_the_limit_and_overflow_is_reported() {
    for length in [5, 1_048_577] {
        let (tx, rx) = std::sync::mpsc::sync_channel(2);
        let captured = capture_output(std::io::Cursor::new(vec![b'x'; length]), tx)
            .expect("collector")
            .join()
            .expect("thread")
            .expect("read");
        assert_eq!(captured.len(), length.min(1_048_576));
        assert_eq!(rx.try_recv().is_ok(), length > 1_048_576);
    }
}

#[test]
fn file_paths_reject_escape_and_symlink() {
    let dir = tempfile::tempdir().expect("fixture");
    guest_path(dir.path(), "/etc/passwd").expect_err("outside writable preopens");
    guest_path(dir.path(), "/tmp/../etc/passwd").expect_err("path traversal");
    guest_path(dir.path(), "tmp/a").expect_err("relative path");
    fs::create_dir(dir.path().join("tmp")).expect("fixture");
    std::os::unix::fs::symlink("/etc", dir.path().join("tmp/link")).expect("fixture");
    guest_path(dir.path(), "/tmp/link/passwd").expect_err("symlink escape");
    assert_eq!(
        guest_path(dir.path(), "/tmp/answer.txt").expect("safe"),
        dir.path().join("tmp/answer.txt")
    );
}

#[test]
fn lifecycle_round_trip_does_not_require_kvm_or_launch_a_process() {
    let root = tempfile::tempdir().expect("fixture");
    let tools = tempfile::tempdir().expect("fixture");
    let request = |value: serde_json::Value| {
        let mut output = Vec::new();
        run_request(
            WasmConfig {
                root: root.path().to_path_buf(),
                tools: tools.path().to_path_buf(),
                module: tools.path().join("module.wasm"),
                preopens: BTreeMap::new(),
            },
            serde_json::to_vec(&value).expect("request").as_slice(),
            &mut output,
        )
        .expect("request succeeds");
        serde_json::from_slice::<serde_json::Value>(&output).expect("response")
    };
    let created = request(serde_json::json!({"operation": "create"}));
    let id = created["sandbox_id"].as_str().expect("id");
    assert_eq!(created["runtime"], "wasmtime-wasi");
    request(serde_json::json!({
        "operation":"upload", "sandbox_id": id, "path":"/tmp/answer.txt", "content":"5050\n"
    }));
    let downloaded = request(serde_json::json!({
        "operation":"download", "sandbox_id":id, "path":"/tmp/answer.txt"
    }));
    assert_eq!(downloaded["content"], "5050\n");
    request(serde_json::json!({"operation":"delete", "sandbox_id":id}));
    assert!(!root.path().join(id).exists());
}

#[test]
fn request_protocol_rejects_traversal_ids_and_unknown_fields() {
    let root = tempfile::tempdir().expect("fixture");
    for request in [
        r#"{"operation":"delete","sandbox_id":"../../etc"}"#,
        r#"{"operation":"create","network":true}"#,
    ] {
        assert!(
            run_request(
                WasmConfig {
                    root: root.path().to_path_buf(),
                    tools: root.path().to_path_buf(),
                    module: root.path().join("module.wasm"),
                    preopens: BTreeMap::new(),
                },
                request.as_bytes(),
                Vec::new(),
            )
            .is_err()
        );
    }
}
