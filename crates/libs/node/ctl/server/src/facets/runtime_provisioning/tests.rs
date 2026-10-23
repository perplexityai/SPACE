use std::sync::Mutex;

use super::*;
use pplx_space_common_core::types::HostOperatingSystem;

fn catalog() -> &'static [CompiledEnvironment] {
    CompiledEnvironment::cached_for_architecture(Arch::X86_64).unwrap()
}

fn host() -> HostPlatform {
    HostPlatform {
        os: HostOperatingSystem::Linux,
        architecture: Arch::X86_64,
    }
}

fn recipe(names: &[&str]) -> RuntimeProvisioningRecipe {
    RuntimeProvisioningRecipe {
        required_environments: names
            .iter()
            .map(|name| {
                catalog()
                    .iter()
                    .find(|entry| entry.name() == *name)
                    .unwrap()
                    .definition()
                    .reference()
                    .to_string()
            })
            .collect(),
    }
}

#[test]
fn recipe_validation_is_complete_and_deduplicates_shared_dependencies() {
    let plan = InstallationPlan::new(
        recipe(&["e0", "fc-1.17.0-linux-6.1.155"]),
        host(),
        catalog(),
    )
    .unwrap();
    // The two complete environments share one immutable kernel.
    assert_eq!(plan.artifacts.len(), 3);
    let duplicate = InstallationPlan::new(recipe(&["e0", "e0"]), host(), catalog())
        .err()
        .unwrap();
    assert!(matches!(duplicate, ProvisioningError::Duplicate(_)));
    let mut unknown = recipe(&["e0"]);
    unknown
        .required_environments
        .push(format!("sha256:{}", "f".repeat(64)));
    assert!(matches!(
        InstallationPlan::new(unknown, host(), catalog()),
        Err(ProvisioningError::Unknown(_))
    ));
    let foreign_host = HostPlatform {
        os: HostOperatingSystem::Macos,
        ..host()
    };
    assert!(matches!(
        InstallationPlan::new(recipe(&["e0"]), foreign_host, catalog()),
        Err(ProvisioningError::Unknown(_))
    ));
    let malformed = RuntimeProvisioningRecipe {
        required_environments: vec!["e0".into()],
    };
    assert!(matches!(
        InstallationPlan::new(malformed, host(), catalog()),
        Err(ProvisioningError::Reference(_))
    ));
    let oversized = RuntimeProvisioningRecipe {
        required_environments: vec!["invalid".into(); MAX_ENVIRONMENTS + 1],
    };
    assert!(matches!(
        InstallationPlan::new(oversized, host(), catalog()),
        Err(ProvisioningError::TooLarge)
    ));
}

#[test]
fn missing_policy_and_old_servers_do_not_become_empty_recipes() {
    for response in [
        Ok(GetNodeRuntimeDesiredStateResponse { recipe: None }),
        Err(Status::unimplemented("older cluster")),
    ] {
        assert_eq!(recipe_from_response(response).unwrap(), None);
    }
    let empty = recipe_from_response(Ok(GetNodeRuntimeDesiredStateResponse {
        recipe: Some(recipe(&[])),
    }))
    .unwrap();
    assert!(empty.unwrap().required_environments.is_empty());
    for code in [
        Code::Unavailable,
        Code::PermissionDenied,
        Code::InvalidArgument,
    ] {
        assert!(matches!(
            recipe_from_response(Err(Status::new(code, "synthetic"))),
            Err(ProvisioningError::Fetch(_))
        ));
    }
}

fn observation(identity: &runtime::ArtifactIdentity) -> runtime::ExecutionArtifact {
    let mut entry = runtime::ExecutionArtifact {
        management: runtime::ArtifactManagement::SpaceManaged as i32,
        status: runtime::ExecutionArtifactStatus::Valid as i32,
        ..Default::default()
    };
    match identity.artifact.as_ref().unwrap() {
        runtime::artifact_identity::Artifact::Runtime(runtime) => {
            entry.kind = runtime::ExecutionArtifactKind::Runtime as i32;
            entry.runtime_kind = runtime.kind;
            entry.version = Some(runtime.version.clone());
            entry.digest = Some(runtime.digest.clone());
        }
        runtime::artifact_identity::Artifact::Component(component) => {
            entry.kind = runtime::ExecutionArtifactKind::Component as i32;
            entry.component_kind = component.kind;
            entry.version = Some(component.version.clone());
            entry.digest = Some(component.digest.clone());
        }
    }
    entry
}

#[derive(Default)]
struct FakeManager {
    inventory: Mutex<runtime::ListExecutionArtifactsResponse>,
    submitted: Mutex<Vec<runtime::ArtifactIdentity>>,
    fail: bool,
}

impl ArtifactManager for FakeManager {
    async fn inventory(&self) -> Result<runtime::ListExecutionArtifactsResponse, Status> {
        Ok(self.inventory.lock().unwrap().clone())
    }
    async fn install(&self, identity: runtime::ArtifactIdentity) -> Result<(), Status> {
        self.submitted.lock().unwrap().push(identity);
        if self.fail {
            Err(Status::unavailable("synthetic submission failure"))
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn ticks_use_fresh_inventory_and_never_confuse_submission_with_completion() {
    let plan = InstallationPlan::new(recipe(&["e0"]), host(), catalog()).unwrap();
    let manager = FakeManager::default();
    manager.inventory.lock().unwrap().complete = true;
    for _ in 0..2 {
        assert!(matches!(
            reconcile(&plan, &manager).await,
            Err(ProvisioningError::Pending {
                missing: 2,
                failed: 0
            })
        ));
    }
    // Active request deduplication belongs to rtmgr's existing worker tests.
    assert_eq!(manager.submitted.lock().unwrap().len(), 4);
    manager
        .inventory
        .lock()
        .unwrap()
        .artifacts
        .push(observation(&plan.artifacts[0]));
    assert!(matches!(
        reconcile(&plan, &manager).await,
        Err(ProvisioningError::Pending {
            missing: 1,
            failed: 0
        })
    ));
    manager
        .inventory
        .lock()
        .unwrap()
        .artifacts
        .push(observation(&plan.artifacts[1]));
    reconcile(&plan, &manager).await.unwrap();
    assert_eq!(manager.submitted.lock().unwrap().len(), 5);
    // Removing intent neither deletes installed bytes nor issues more work.
    reconcile(
        &InstallationPlan::new(recipe(&[]), host(), catalog()).unwrap(),
        &manager,
    )
    .await
    .unwrap();
    assert_eq!(manager.submitted.lock().unwrap().len(), 5);
    assert_eq!(manager.inventory.lock().unwrap().artifacts.len(), 2);
}

#[tokio::test]
async fn incomplete_inventory_blocks_mutation_and_submission_failures_do_not_skip_siblings() {
    let plan = InstallationPlan::new(recipe(&["e0"]), host(), catalog()).unwrap();
    let manager = FakeManager {
        fail: true,
        ..Default::default()
    };
    assert!(matches!(
        reconcile(&plan, &manager).await,
        Err(ProvisioningError::IncompleteInventory)
    ));
    assert!(manager.submitted.lock().unwrap().is_empty());
    manager.inventory.lock().unwrap().complete = true;
    assert!(matches!(
        reconcile(&plan, &manager).await,
        Err(ProvisioningError::Pending {
            missing: 2,
            failed: 2
        })
    ));
    assert_eq!(manager.submitted.lock().unwrap().len(), 2);
}

#[test]
fn installed_evidence_requires_exact_managed_bytes_and_a_valid_status() {
    let plan = InstallationPlan::new(recipe(&["e0"]), host(), catalog()).unwrap();
    for identity in &plan.artifacts {
        let valid = observation(identity);
        assert!(installed(identity, &valid));
        let cases = [
            runtime::ExecutionArtifact {
                management: runtime::ArtifactManagement::External as i32,
                ..valid.clone()
            },
            runtime::ExecutionArtifact {
                status: runtime::ExecutionArtifactStatus::Unreadable as i32,
                ..valid.clone()
            },
            runtime::ExecutionArtifact {
                digest: None,
                ..valid.clone()
            },
            runtime::ExecutionArtifact {
                digest: Some(format!("sha256:{}", "f".repeat(64))),
                ..valid.clone()
            },
            runtime::ExecutionArtifact {
                version: Some("different".into()),
                ..valid
            },
        ];
        for invalid in cases {
            assert!(!installed(identity, &invalid));
        }
    }
}
