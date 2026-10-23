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
    let partial = InstallationPlan::new(unknown, host(), catalog()).unwrap();
    assert_eq!(partial.artifacts.len(), 2);
    assert_eq!(partial.unsupported.len(), 1);
    let foreign_host = HostPlatform {
        os: HostOperatingSystem::Macos,
        ..host()
    };
    let foreign = InstallationPlan::new(recipe(&["e0"]), foreign_host, catalog()).unwrap();
    assert!(foreign.artifacts.is_empty());
    assert_eq!(foreign.unsupported.len(), 1);
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
    submission_error: Mutex<Option<Code>>,
    jobs: Mutex<Vec<runtime::Operation>>,
    poll_error: Mutex<Option<Code>>,
}

impl ArtifactManager for FakeManager {
    async fn inventory(&self) -> Result<runtime::ListExecutionArtifactsResponse, Status> {
        Ok(self.inventory.lock().unwrap().clone())
    }
    async fn install(
        &self,
        identity: runtime::ArtifactIdentity,
    ) -> Result<runtime::Operation, Status> {
        self.submitted.lock().unwrap().push(identity.clone());
        if let Some(code) = *self.submission_error.lock().unwrap() {
            return Err(Status::new(code, "synthetic submission failure"));
        }
        let mut jobs = self.jobs.lock().unwrap();
        let operation = runtime::Operation {
            identity: Some(identity),
            operation_id: format!("job-{}", jobs.len()),
            action: runtime::ArtifactAction::Install as i32,
            state: runtime::OperationState::Queued as i32,
            ..Default::default()
        };
        jobs.push(operation.clone());
        Ok(operation)
    }
    async fn operation(&self, id: String) -> Result<runtime::Operation, Status> {
        if let Some(code) = *self.poll_error.lock().unwrap() {
            return Err(Status::new(code, "synthetic polling error"));
        }
        self.jobs
            .lock()
            .unwrap()
            .iter()
            .find(|job| job.operation_id == id)
            .cloned()
            .ok_or_else(|| Status::not_found("job missing"))
    }
}

async fn tick(
    plan: &InstallationPlan,
    manager: &FakeManager,
    state: &Provisioning,
) -> Result<Progress, ProvisioningError> {
    reconcile(
        plan,
        manager,
        state,
        tokio::time::Instant::now() + RECONCILE_BUDGET,
    )
    .await
}

#[tokio::test]
async fn ticks_use_fresh_inventory_and_never_confuse_submission_with_completion() {
    let plan = InstallationPlan::new(recipe(&["e0"]), host(), catalog()).unwrap();
    let manager = FakeManager::default();
    let state = Provisioning::default();
    // An unrelated scan failure does not prevent idempotent installation.
    manager.inventory.lock().unwrap().complete = false;
    for _ in 0..2 {
        assert_eq!(
            tick(&plan, &manager, &state).await.unwrap(),
            Progress::Installing { missing: 2 }
        );
    }
    assert_eq!(manager.submitted.lock().unwrap().len(), 2);
    // A succeeded job is not evidence that the inventory contains good bytes.
    for job in manager.jobs.lock().unwrap().iter_mut() {
        job.state = runtime::OperationState::Succeeded as i32;
    }
    assert_eq!(
        tick(&plan, &manager, &state).await.unwrap(),
        Progress::Installing { missing: 2 }
    );
    manager
        .inventory
        .lock()
        .unwrap()
        .artifacts
        .push(observation(&plan.artifacts[0]));
    assert!(matches!(
        tick(&plan, &manager, &state).await,
        Err(ProvisioningError::Blocked {
            missing: 1,
            failed: 1,
            ..
        })
    ));
    manager
        .inventory
        .lock()
        .unwrap()
        .artifacts
        .push(observation(&plan.artifacts[1]));
    assert_eq!(
        tick(&plan, &manager, &state).await.unwrap(),
        Progress::Converged
    );
    // Positive required evidence suffices even with unrelated scan issues.
    assert!(!manager.inventory.lock().unwrap().complete);
    assert_eq!(manager.submitted.lock().unwrap().len(), 2);
    tick(
        &InstallationPlan::new(recipe(&[]), host(), catalog()).unwrap(),
        &manager,
        &state,
    )
    .await
    .unwrap();
    assert_eq!(manager.inventory.lock().unwrap().artifacts.len(), 2);
}

#[tokio::test(start_paused = true)]
async fn terminal_failures_are_observed_and_retries_wait_for_backoff() {
    let plan = InstallationPlan::new(recipe(&["e0"]), host(), catalog()).unwrap();
    // Exercise both immediate submission failures and asynchronous job failures.
    for immediate in [false, true] {
        let manager = FakeManager {
            submission_error: Mutex::new(immediate.then_some(Code::FailedPrecondition)),
            ..Default::default()
        };
        let state = Provisioning::default();
        let first = tick(&plan, &manager, &state).await;
        if immediate {
            assert!(matches!(
                first,
                Err(ProvisioningError::Blocked { failed: 2, .. })
            ));
        } else {
            assert_eq!(first.unwrap(), Progress::Installing { missing: 2 });
            for job in manager.jobs.lock().unwrap().iter_mut() {
                job.state = runtime::OperationState::Failed as i32;
                job.error = Some("synthetic permanent publication failure".into());
            }
        }
        for _ in 0..2 {
            assert!(matches!(
                tick(&plan, &manager, &state).await,
                Err(ProvisioningError::Blocked { failed: 2, .. })
            ));
        }
        assert_eq!(manager.submitted.lock().unwrap().len(), 2);
        tokio::time::advance(operations::RETRY_BACKOFF).await;
        let retry = tick(&plan, &manager, &state).await;
        if immediate {
            assert!(matches!(
                retry,
                Err(ProvisioningError::Blocked { failed: 2, .. })
            ));
        } else {
            assert_eq!(retry.unwrap(), Progress::Installing { missing: 2 });
        }
        assert_eq!(manager.submitted.lock().unwrap().len(), 4);
    }
}

#[tokio::test]
async fn unsupported_entries_do_not_block_known_dependencies_or_claim_convergence() {
    let mut requested = recipe(&["e0"]);
    requested
        .required_environments
        .push(format!("sha256:{}", "f".repeat(64)));
    let plan = InstallationPlan::new(requested, host(), catalog()).unwrap();
    let manager = FakeManager::default();
    let state = Provisioning::default();
    for installed in [false, true] {
        if installed {
            manager.inventory.lock().unwrap().artifacts =
                plan.artifacts.iter().map(observation).collect();
        }
        let Err(ProvisioningError::Blocked {
            failed,
            unsupported,
            ..
        }) = tick(&plan, &manager, &state).await
        else {
            panic!("unsupported environment must remain visible");
        };
        assert_eq!(failed, 0);
        assert_eq!(unsupported, plan.unsupported);
    }
    assert_eq!(manager.submitted.lock().unwrap().len(), 2);
}

#[tokio::test(start_paused = true)]
async fn timeouts_identify_the_stage_without_extending_the_shared_deadline() {
    let deadline = tokio::time::Instant::now() + RECONCILE_BUDGET;
    let result = within::<()>(deadline, Stage::Inventory, std::future::pending()).await;
    assert!(matches!(
        result,
        Err(ProvisioningError::Timeout {
            stage: Stage::Inventory
        })
    ));
    assert_eq!(tokio::time::Instant::now(), deadline);
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

#[tokio::test(start_paused = true)]
async fn polling_preserves_active_jobs_and_retries_missing_jobs_after_backoff() {
    let plan = InstallationPlan::new(recipe(&["e0"]), host(), catalog()).unwrap();
    let manager = FakeManager::default();
    let state = Provisioning::default();
    tick(&plan, &manager, &state).await.unwrap();
    *manager.poll_error.lock().unwrap() = Some(Code::Unavailable);
    assert!(matches!(
        tick(&plan, &manager, &state).await,
        Err(ProvisioningError::Blocked { failed: 2, .. })
    ));
    *manager.poll_error.lock().unwrap() = None;
    tick(&plan, &manager, &state).await.unwrap();
    assert_eq!(manager.submitted.lock().unwrap().len(), 2);
    *manager.poll_error.lock().unwrap() = Some(Code::NotFound);
    assert!(matches!(
        tick(&plan, &manager, &state).await,
        Err(ProvisioningError::Blocked { failed: 2, .. })
    ));
    *manager.poll_error.lock().unwrap() = None;
    assert!(matches!(
        tick(&plan, &manager, &state).await,
        Err(ProvisioningError::Blocked { failed: 2, .. })
    ));
    tokio::time::advance(operations::RETRY_BACKOFF).await;
    tick(&plan, &manager, &state).await.unwrap();
    assert_eq!(manager.submitted.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn operation_identity_and_action_are_checked_on_every_poll() {
    enum Corruption {
        Identity,
        Action,
        EmptyId,
    }
    for corruption in [
        Corruption::Identity,
        Corruption::Action,
        Corruption::EmptyId,
    ] {
        let plan = InstallationPlan::new(recipe(&["e0"]), host(), catalog()).unwrap();
        let manager = FakeManager::default();
        let state = Provisioning::default();
        tick(&plan, &manager, &state).await.unwrap();
        {
            let mut jobs = manager.jobs.lock().unwrap();
            match corruption {
                Corruption::Identity => jobs[0].identity = jobs[1].identity.clone(),
                Corruption::Action => jobs[0].action = runtime::ArtifactAction::Uninstall as i32,
                Corruption::EmptyId => jobs[0].operation_id.clear(),
            }
        }
        assert!(matches!(
            tick(&plan, &manager, &state).await,
            Err(ProvisioningError::Blocked { failed: 1, .. })
        ));
        assert_eq!(manager.submitted.lock().unwrap().len(), 2);
    }
}

#[tokio::test(start_paused = true)]
async fn transient_submission_failures_retry_without_terminal_job_backoff() {
    for code in [Code::Unavailable, Code::DeadlineExceeded, Code::Cancelled] {
        let plan = InstallationPlan::new(recipe(&["e0"]), host(), catalog()).unwrap();
        let manager = FakeManager {
            submission_error: Mutex::new(Some(code)),
            ..Default::default()
        };
        let state = Provisioning::default();
        assert!(matches!(
            tick(&plan, &manager, &state).await,
            Err(ProvisioningError::Blocked { failed: 2, .. })
        ));
        // The next pass can immediately recover. No terminal failure was
        // observed, and a lost reply rejoins active jobs in rtmgr.
        *manager.submission_error.lock().unwrap() = None;
        assert_eq!(
            tick(&plan, &manager, &state).await.unwrap(),
            Progress::Installing { missing: 2 }
        );
        assert_eq!(manager.submitted.lock().unwrap().len(), 4);
        tick(&plan, &manager, &state).await.unwrap();
        assert_eq!(manager.submitted.lock().unwrap().len(), 4);
    }
}
