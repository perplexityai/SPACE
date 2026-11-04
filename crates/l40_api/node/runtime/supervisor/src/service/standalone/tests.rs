use super::*;
use pplx_space_common_core::types::Arch;

#[test]
fn standalone_configuration_refuses_unconfined_locators() {
    let environments = CompiledEnvironment::for_architecture(Arch::X86_64).unwrap();
    let environment = environments
        .iter()
        .find(|e| e.release().release.kind == RuntimeKind::CloudHypervisor)
        .unwrap();
    let sandbox = SandboxId::new();
    let release = environment.release();
    let request = LaunchRuntimeRequest {
        sandbox_id: sandbox.to_string(),
        workspace_root: "/synthetic/jails".into(),
        network_namespace: "/synthetic/netns".into(),
        console_path: format!("/synthetic/jails/cloud-hypervisor/{sandbox}/root/console.log"),
        uid: 10001,
        gid: 10001,
        ..Default::default()
    };
    for directory in release_directory_names(&release.release, release.digest()) {
        let mut valid = request.clone();
        valid.runtime_binary = format!("/synthetic/releases/{directory}/bin/cloud-hypervisor");
        let config = standalone_config_for_environment(&valid, sandbox, environment).unwrap();
        assert_eq!(config.root.join("console.log"), config.output);
        assert_eq!(config.executable, Path::new(&valid.runtime_binary));

        enum Field {
            Runtime,
            Workspace,
            Network,
            Console,
        }
        for (field, value) in [
            (Field::Runtime, "/synthetic/unpinned/bin/cloud-hypervisor"),
            (Field::Runtime, "relative/bin/cloud-hypervisor"),
            (Field::Workspace, "relative"),
            (Field::Workspace, "/synthetic/../jails"),
            (Field::Network, "relative"),
            (Field::Network, "/synthetic/../netns"),
            (Field::Console, "/synthetic/another-sandbox/console.log"),
        ] {
            let mut invalid = valid.clone();
            match field {
                Field::Runtime => invalid.runtime_binary = value.into(),
                Field::Workspace => invalid.workspace_root = value.into(),
                Field::Network => invalid.network_namespace = value.into(),
                Field::Console => invalid.console_path = value.into(),
            }
            match standalone_config_for_environment(&invalid, sandbox, environment) {
                Ok(_) => panic!("accepted unconfined locator {value}"),
                Err(error) => assert_eq!(error.code(), tonic::Code::InvalidArgument),
            }
        }
        let firecracker = environments
            .iter()
            .find(|e| e.release().release.kind == RuntimeKind::Firecracker)
            .unwrap();
        match standalone_config_for_environment(&valid, sandbox, firecracker) {
            Ok(_) => panic!("accepted a different runtime's contract"),
            Err(error) => assert_eq!(error.code(), tonic::Code::FailedPrecondition),
        }
    }
}
