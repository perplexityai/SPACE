use super::*;

#[test]
fn legacy_launch_spec_keeps_the_vm_exec_contract() {
    // Persist the pre-discriminator wire contract independently of the current
    // serializer, so rollback requests cannot silently select an embedded guest.
    let config: Config = serde_json::from_str(
        r#"{
            "root": "/fixture/root",
            "registration_directory": "/fixture/registration",
            "network_namespace": "/fixture/network",
            "executable": "/fixture/cloud-hypervisor",
            "executable_source": {
                "sha256": "sha256:0000000000000000000000000000000000000000000000000000000000000000",
                "verity": "sha256:0000000000000000000000000000000000000000000000000000000000000000"
            },
            "uid": 10001,
            "gid": 10001,
            "cgroup": "/fixture/cgroup",
            "output": "/fixture/root/console.log",
            "arguments": ["--seccomp", "true"]
        }"#,
    )
    .unwrap();
    assert_eq!(config.runtime, RuntimeKind::CloudHypervisor);
    assert!(matches!(config.entrypoint, Entrypoint::Runtime));
    assert_eq!(config.arguments, ["--seccomp", "true"]);
}
