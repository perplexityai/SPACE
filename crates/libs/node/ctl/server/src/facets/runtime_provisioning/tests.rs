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
fn missing_recipe_and_old_servers_do_not_become_empty_recipes() {
    for response in [
        Ok(GetNodeRuntimeDesiredStateResponse {
            recipe: None,
            execution_policy: None,
        }),
        Err(Status::unimplemented("older cluster")),
    ] {
        assert_eq!(desired_from_response(response).unwrap(), (None, None));
    }
    let (empty, _) = desired_from_response(Ok(GetNodeRuntimeDesiredStateResponse {
        recipe: Some(recipe(&[])),
        execution_policy: None,
    }))
    .unwrap();
    assert!(empty.unwrap().required_environments.is_empty());
    for code in [
        Code::Unavailable,
        Code::PermissionDenied,
        Code::InvalidArgument,
    ] {
        assert!(matches!(
            desired_from_response(Err(Status::new(code, "synthetic"))),
            Err(ProvisioningError::Fetch(_))
        ));
    }
}

fn desired_policy() -> RuntimeEnvironmentDesiredPolicy {
    let [e0, e1] = ["e0", "fc-1.17.0-linux-6.1.155"]
        .map(|name| recipe(&[name]).required_environments.pop().unwrap());
    RuntimeEnvironmentDesiredPolicy {
        generation: 12,
        default_environment_ref: Some(e1.clone()),
        executing_environment_refs: vec![e0, e1],
        legacy_unbound_allowed: Some(false),
    }
}

#[derive(Default)]
struct FakePolicyApplier {
    requests: Mutex<Vec<node_manager::ApplyExecutionPolicyRequest>>,
    reply: Mutex<Option<Result<node_manager::ApplyExecutionPolicyResponse, Status>>>,
}

impl PolicyApplier for FakePolicyApplier {
    async fn apply(
        &self,
        request: node_manager::ApplyExecutionPolicyRequest,
    ) -> Result<node_manager::ApplyExecutionPolicyResponse, Status> {
        self.requests.lock().unwrap().push(request);
        self.reply
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| Ok(Default::default()))
    }
}

#[tokio::test]
async fn every_response_policy_reaches_node_manager_in_full() {
    let manager = FakePolicyApplier::default();
    let served = desired_policy();
    // The same generation twice is applied twice: generations restart at 0
    // when the schema is reapplied, so none is ever skipped.
    for _ in 0..2 {
        apply_policy(&manager, Some(served.clone())).await.unwrap();
    }
    // The bootstrap seed is generation 0 with nothing set; it's still a policy,
    // so the node publishes that it applied it.
    let bootstrap = RuntimeEnvironmentDesiredPolicy {
        generation: 0,
        default_environment_ref: None,
        executing_environment_refs: vec![],
        legacy_unbound_allowed: None,
    };
    apply_policy(&manager, Some(bootstrap)).await.unwrap();
    // An absent policy is forwarded too: node-manager returns to its startup
    // settings rather than keeping the last cluster policy.
    apply_policy(&manager, None).await.unwrap();
    let requests = manager.requests.lock().unwrap();
    let expected = node_manager::ClusterExecutionPolicy {
        generation: served.generation,
        default_environment_reference: served.default_environment_ref.clone(),
        executing_environment_references: served.executing_environment_refs.clone(),
        legacy_unbound_allowed: served.legacy_unbound_allowed,
    };
    assert_eq!(requests.len(), 4);
    assert_eq!(requests[0].policy.as_ref(), Some(&expected));
    assert_eq!(requests[1].policy.as_ref(), Some(&expected));
    assert_eq!(
        requests[2].policy,
        Some(node_manager::ClusterExecutionPolicy::default())
    );
    assert_eq!(requests[3].policy, None);
}

#[tokio::test]
async fn policy_application_failures_are_visible_except_for_older_node_managers() {
    let manager = FakePolicyApplier::default();
    for outcome in [
        node_manager::ClusterDefaultOutcome::Unusable,
        node_manager::ClusterDefaultOutcome::NotAllowed,
    ] {
        // A refused default is reported, not a failed pass.
        *manager.reply.lock().unwrap() = Some(Ok(node_manager::ApplyExecutionPolicyResponse {
            enforced: None,
            default_outcome: outcome as i32,
        }));
        apply_policy(&manager, Some(desired_policy()))
            .await
            .unwrap();
    }
    *manager.reply.lock().unwrap() = Some(Err(Status::unimplemented("older node-manager")));
    apply_policy(&manager, Some(desired_policy()))
        .await
        .unwrap();
    for code in [
        Code::Unavailable,
        Code::InvalidArgument,
        Code::PermissionDenied,
    ] {
        *manager.reply.lock().unwrap() = Some(Err(Status::new(code, "synthetic")));
        assert!(matches!(
            apply_policy(&manager, Some(desired_policy())).await,
            Err(ProvisioningError::ApplyPolicy(error)) if error.code() == code
        ));
    }
}

struct StalledPolicyApplier;

impl PolicyApplier for StalledPolicyApplier {
    async fn apply(
        &self,
        _request: node_manager::ApplyExecutionPolicyRequest,
    ) -> Result<node_manager::ApplyExecutionPolicyResponse, Status> {
        std::future::pending().await
    }
}

#[tokio::test(start_paused = true)]
async fn stalled_policy_application_times_out_within_its_budget() {
    let started = tokio::time::Instant::now();
    assert!(matches!(
        apply_policy(&StalledPolicyApplier, Some(desired_policy())).await,
        Err(ProvisioningError::PolicyTimeout)
    ));
    assert_eq!(started.elapsed(), POLICY_BUDGET);
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
    /// A job ID whose poll replies with an empty operation ID.
    blank_polled_id: Mutex<Option<String>>,
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
        let mut operation = self
            .jobs
            .lock()
            .unwrap()
            .iter()
            .find(|job| job.operation_id == id)
            .cloned()
            .ok_or_else(|| Status::not_found("job missing"))?;
        if self.blank_polled_id.lock().unwrap().as_deref() == Some(id.as_str()) {
            operation.operation_id.clear();
        }
        Ok(operation)
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

#[tokio::test(start_paused = true)]
async fn an_absent_recipe_keeps_backoffs_and_an_empty_one_clears_them() {
    let plan = InstallationPlan::new(recipe(&["e0"]), host(), catalog()).unwrap();
    let deadline = || tokio::time::Instant::now() + RECONCILE_BUDGET;
    for (assigned, retried) in [(None, false), (Some(recipe(&[])), true)] {
        let manager = FakeManager {
            submission_error: Mutex::new(Some(Code::FailedPrecondition)),
            ..Default::default()
        };
        let state = Provisioning::default();
        assert!(matches!(
            tick(&plan, &manager, &state).await,
            Err(ProvisioningError::Blocked { failed: 2, .. })
        ));
        install(assigned, &state, deadline()).await.unwrap();
        assert!(matches!(
            tick(&plan, &manager, &state).await,
            Err(ProvisioningError::Blocked { failed: 2, .. })
        ));
        let submissions = if retried { 4 } else { 2 };
        assert_eq!(manager.submitted.lock().unwrap().len(), submissions);
    }
}

#[tokio::test(start_paused = true)]
async fn a_job_that_fails_while_the_recipe_is_absent_still_backs_off() {
    let plan = InstallationPlan::new(recipe(&["e0"]), host(), catalog()).unwrap();
    let manager = FakeManager::default();
    let state = Provisioning::default();
    assert_eq!(
        tick(&plan, &manager, &state).await.unwrap(),
        Progress::Installing { missing: 2 }
    );
    install(None, &state, tokio::time::Instant::now() + RECONCILE_BUDGET)
        .await
        .unwrap();
    for job in manager.jobs.lock().unwrap().iter_mut() {
        job.state = runtime::OperationState::Failed as i32;
        job.error = Some("synthetic failure while no recipe was assigned".into());
    }
    // The failure is observed on the kept job, then held for the backoff.
    for _ in 0..2 {
        assert!(matches!(
            tick(&plan, &manager, &state).await,
            Err(ProvisioningError::Blocked { failed: 2, .. })
        ));
    }
    assert_eq!(manager.submitted.lock().unwrap().len(), 2);
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
async fn polling_preserves_active_jobs_and_resubmits_expired_ones() {
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
    // An expired job observed no failure, so it's resubmitted at once.
    *manager.poll_error.lock().unwrap() = Some(Code::NotFound);
    assert_eq!(
        tick(&plan, &manager, &state).await.unwrap(),
        Progress::Installing { missing: 2 }
    );
    assert_eq!(manager.submitted.lock().unwrap().len(), 4);
    *manager.poll_error.lock().unwrap() = None;
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
                Corruption::EmptyId => {
                    *manager.blank_polled_id.lock().unwrap() = Some(jobs[0].operation_id.clone());
                }
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
