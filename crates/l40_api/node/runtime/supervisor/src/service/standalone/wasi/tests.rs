use super::*;
use pplx_space_content_digest::Digest;
use pplx_space_cpu_arch::Arch;
use pplx_space_execution_spec::BootArtifact;

#[test]
fn boot_contract_requires_both_typed_catalog_inputs_without_extras() {
    let pin = |kind| {
        PinnedBootArtifact::new(
            ComponentIdentity {
                kind,
                version: "fixture-v1".parse().unwrap(),
                digest: Digest::sha256([42; 32]),
            },
            Arch::X86_64,
            64,
        )
        .unwrap()
    };
    let module = pin(ArtifactComponentKind::WasmModule);
    let library = pin(ArtifactComponentKind::GuestTools);
    let artifact = |role: &str, pin: &PinnedBootArtifact, format: &str| {
        BootArtifact::new(
            role.parse().unwrap(),
            pin.identity().clone(),
            format.parse().unwrap(),
        )
    };
    let inputs = vec![
        artifact("module", &module, "wasip1-command"),
        artifact("standard-library", &library, "cpython315-zip"),
    ];
    let catalog = |identity: &ComponentIdentity| {
        [&module, &library]
            .into_iter()
            .find(|pin| pin.identity() == identity)
            .cloned()
    };
    let boot = |contract: &str, artifacts| BootProfile::ArtifactsV1 {
        contract: contract.parse().unwrap(),
        artifacts,
    };
    for artifacts in [inputs.clone(), inputs.iter().rev().cloned().collect()] {
        assert_eq!(
            boot_inputs(&boot("wasi-python-v1", artifacts), catalog).unwrap(),
            (module.clone(), library.clone()),
        );
    }
    for invalid in [
        boot("unqualified-python-v2", inputs.clone()),
        boot("wasi-python-v1", vec![]),
        boot("wasi-python-v1", vec![inputs[0].clone()]),
        boot("wasi-python-v1", vec![inputs[0].clone(), inputs[0].clone()]),
        boot(
            "wasi-python-v1",
            vec![
                inputs[0].clone(),
                artifact("standard-library", &library, "arbitrary-native-directory"),
            ],
        ),
        boot(
            "wasi-python-v1",
            vec![
                artifact("module", &library, "wasip1-command"),
                inputs[1].clone(),
            ],
        ),
        boot(
            "wasi-python-v1",
            vec![inputs[0].clone(), inputs[1].clone(), inputs[1].clone()],
        ),
    ] {
        assert_eq!(
            boot_inputs(&invalid, catalog).unwrap_err().code(),
            tonic::Code::FailedPrecondition,
        );
    }
    let valid = boot("wasi-python-v1", inputs);
    for replacement in [None, Some(library)] {
        assert_eq!(
            boot_inputs(&valid, |_| replacement.clone())
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition,
        );
    }
}
