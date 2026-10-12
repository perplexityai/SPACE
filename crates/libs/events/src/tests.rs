use std::collections::BTreeSet;

use super::*;

#[test]
fn sandbox_event_type_names_are_unique() {
    let mut names = BTreeSet::new();

    for event_type in SANDBOX_EVENT_TYPES {
        let inserted = names.insert(event_type.as_str());

        assert!(inserted, "duplicate event type name: {event_type}");
    }
}

#[test]
fn sandbox_event_source_names_are_unique() {
    let mut names = BTreeSet::new();

    for source in SANDBOX_EVENT_SOURCES {
        let inserted = names.insert(source.as_str());

        assert!(inserted, "duplicate event source name: {source}");
    }
}

#[test]
fn sandbox_event_types_derive_a_phase() {
    for event_type in SANDBOX_EVENT_TYPES {
        assert!(
            !event_type.phase().as_str().is_empty(),
            "missing phase for event type: {event_type}"
        );
    }
}

#[test]
fn verify_before_destroy_sweep_outcomes_have_domain_names_and_phases() {
    // The sweep's non-destructive outcomes: converge a stale row to the
    // running state its live VM already reached, revert a dead-but-backed-
    // up row to `suspended`, or defer when destruction can't be proven
    // safe. Names are domain-level (no component identity) and phases
    // reflect that converge/revert complete a transition while a defer
    // leaves the row queued for a later tick.
    assert_eq!(
        SandboxEventType::LifecycleConverged.as_str(),
        "lifecycle_converged"
    );
    assert_eq!(
        SandboxEventType::LifecycleConverged.phase(),
        SandboxEventPhase::Completed
    );
    assert_eq!(
        SandboxEventType::LifecycleRevertedToSuspended.as_str(),
        "lifecycle_reverted_to_suspended"
    );
    assert_eq!(
        SandboxEventType::LifecycleRevertedToSuspended.phase(),
        SandboxEventPhase::Completed
    );
    assert_eq!(
        SandboxEventType::LifecycleTransitionRolledBack.as_str(),
        "lifecycle_transition_rolled_back"
    );
    assert_eq!(
        SandboxEventType::LifecycleTransitionRolledBack.phase(),
        SandboxEventPhase::Completed
    );
    assert_eq!(SandboxEventType::SweepDeferred.as_str(), "sweep_deferred");
    assert_eq!(
        SandboxEventType::SweepDeferred.phase(),
        SandboxEventPhase::Queued
    );
}

#[test]
fn sandbox_failed_and_quarantined_have_domain_names_and_failed_phase() {
    assert_eq!(SandboxEventType::SandboxFailed.as_str(), "sandbox_failed");
    assert_eq!(
        SandboxEventType::SandboxFailed.phase(),
        SandboxEventPhase::Failed
    );
    assert_eq!(
        SandboxEventType::SandboxQuarantined.as_str(),
        "sandbox_quarantined"
    );
    assert_eq!(
        SandboxEventType::SandboxQuarantined.phase(),
        SandboxEventPhase::Failed
    );
}

#[test]
fn suspend_superseded_has_domain_name_and_completed_phase() {
    assert_eq!(
        SandboxEventType::SuspendSuperseded.as_str(),
        "suspend_superseded"
    );
    assert_eq!(
        SandboxEventType::SuspendSuperseded.phase(),
        SandboxEventPhase::Completed
    );
}

#[test]
fn delete_superseded_has_domain_name_and_completed_phase() {
    assert_eq!(
        SandboxEventType::DeleteSuperseded.as_str(),
        "delete_superseded"
    );
    assert_eq!(
        SandboxEventType::DeleteSuperseded.phase(),
        SandboxEventPhase::Completed
    );
}

#[test]
fn rootfs_stream_reclaim_has_domain_name_and_completed_phase() {
    // Reclaiming the uploaded rootfs diff stream is a completed
    // cleanup outcome tied to a durable backup handoff — name is
    // domain-level (the artifact concept, no component identity).
    assert_eq!(
        SandboxEventType::RootfsStreamReclaimed.as_str(),
        "rootfs_stream_reclaimed"
    );
    assert_eq!(
        SandboxEventType::RootfsStreamReclaimed.phase(),
        SandboxEventPhase::Completed
    );
}

#[test]
fn snapshot_pruned_has_domain_name_and_completed_phase() {
    // Pruning an old durable snapshot under the keep-newest-N
    // retention policy is a completed durable-artifact deletion;
    // the name is domain-level (no component identity), the
    // reconciler identity lives in `source`.
    assert_eq!(SandboxEventType::SnapshotPruned.as_str(), "snapshot_pruned");
    assert_eq!(
        SandboxEventType::SnapshotPruned.phase(),
        SandboxEventPhase::Completed
    );
}

#[test]
fn fork_events_have_domain_names_and_phases() {
    assert_eq!(SandboxEventType::ForkRequested.as_str(), "fork_requested");
    assert_eq!(
        SandboxEventType::ForkRequested.phase(),
        SandboxEventPhase::Requested
    );
    assert_eq!(
        SandboxEventType::ForkArtifactReady.phase(),
        SandboxEventPhase::Completed
    );
    assert_eq!(
        SandboxEventType::ForkTargetActivated.phase(),
        SandboxEventPhase::Completed
    );
    assert_eq!(
        SandboxEventType::ForkCompleted.phase(),
        SandboxEventPhase::Completed
    );
    assert_eq!(
        SandboxEventType::ForkFailed.phase(),
        SandboxEventPhase::Failed
    );
}

#[test]
fn net_gateway_cycle_events_have_domain_names_and_phases() {
    // Cycling the sandbox's egress gateway in place is a logical
    // network-path transition; the name stays domain-level (the gateway
    // concept, not the sidecar binary), the component identity lives in
    // `source` and the operator causality in `trigger`.
    assert_eq!(
        SandboxEventType::NetGatewayCycleRequested.as_str(),
        "net_gateway_cycle_requested"
    );
    assert_eq!(
        SandboxEventType::NetGatewayCycleRequested.phase(),
        SandboxEventPhase::Requested
    );
    assert_eq!(
        SandboxEventType::NetGatewayCycleCompleted.as_str(),
        "net_gateway_cycle_completed"
    );
    assert_eq!(
        SandboxEventType::NetGatewayCycleCompleted.phase(),
        SandboxEventPhase::Completed
    );
    assert_eq!(
        SandboxEventType::NetGatewayCycleFailed.as_str(),
        "net_gateway_cycle_failed"
    );
    assert_eq!(
        SandboxEventType::NetGatewayCycleFailed.phase(),
        SandboxEventPhase::Failed
    );
}

#[test]
fn sandbox_event_details_store_suspend_after_wake_delta() {
    let details = SandboxEventDetails::default().ms_since_wake("312");

    assert_eq!(details.ms_since_wake.as_deref(), Some("312"));
}

#[test]
fn sandbox_event_details_store_create_template_resolution_context() {
    let details = SandboxEventDetails::default()
        .requested_template("app-runtime:a1925")
        .template_id("app-runtime")
        .template_content_hash("sha256:abc")
        .claim_id("019e")
        .pool_slot_id("slot-id")
        .disk_mib("102400")
        .runtime("fc");

    assert_eq!(
        details.requested_template.as_deref(),
        Some("app-runtime:a1925")
    );
    assert_eq!(details.template_id.as_deref(), Some("app-runtime"));
    assert_eq!(details.template_content_hash.as_deref(), Some("sha256:abc"));
    assert_eq!(details.claim_id.as_deref(), Some("019e"));
    assert_eq!(details.pool_slot_id.as_deref(), Some("slot-id"));
    assert_eq!(details.disk_mib.as_deref(), Some("102400"));
    assert_eq!(details.runtime.as_deref(), Some("fc"));
}

#[test]
fn sandbox_event_details_store_network_policy_refresh_readiness() {
    let details = SandboxEventDetails::default()
        .network_state("ready")
        .installed_network_policy_ref("cluster:sandbox:s:generation:1:sha256:abc")
        .interception_readiness("dependencies_not_ready");

    assert_eq!(details.network_state.as_deref(), Some("ready"));
    assert_eq!(
        details.installed_network_policy_ref.as_deref(),
        Some("cluster:sandbox:s:generation:1:sha256:abc")
    );
    assert_eq!(
        details.interception_readiness.as_deref(),
        Some("dependencies_not_ready")
    );
}

#[test]
fn sandbox_event_details_store_owner_provenance() {
    let details = SandboxEventDetails::default()
        .owner_source("federated_identity")
        .upstream_issuer("example-issuer")
        .upstream_org_id("org-uuid")
        .upstream_subject_id("123");

    assert_eq!(details.owner_source.as_deref(), Some("federated_identity"));
    assert_eq!(details.upstream_issuer.as_deref(), Some("example-issuer"));
    assert_eq!(details.upstream_org_id.as_deref(), Some("org-uuid"));
    assert_eq!(details.upstream_subject_id.as_deref(), Some("123"));
}

#[test]
fn sandbox_event_details_store_restore_provenance() {
    let details = SandboxEventDetails::default()
        .bool_field(SandboxEventBoolField::RestoreStorageUriPresent, true)
        .restored_snapshot_id("snapshot-id")
        .restored_snapshot_local_artifact_id("artifact-from-s3")
        .local_artifact_id("artifact-local")
        .restore_provenance(SandboxRestoreProvenance::RemoteLocalMismatch)
        .error_kind("btrfs_receive");

    assert_eq!(details.restore_storage_uri_present, Some(true));
    assert_eq!(details.restored_snapshot_id.as_deref(), Some("snapshot-id"));
    assert_eq!(
        details.restored_snapshot_local_artifact_id.as_deref(),
        Some("artifact-from-s3")
    );
    assert_eq!(details.local_artifact_id.as_deref(), Some("artifact-local"));
    assert_eq!(
        details.restore_provenance.as_deref(),
        Some("remote_local_mismatch")
    );
    assert_eq!(details.error_kind.as_deref(), Some("btrfs_receive"));
}

#[test]
fn sandbox_event_details_project_execution_metadata() {
    use pplx_space_common_core::types::{
        Arch, ArtifactComponentKind, ArtifactComponentMetadata, ArtifactComponentRole,
        ArtifactFormat, ArtifactVersion, Digest, ExecutionMetadata, FIRECRACKER_STATE_FORMAT,
        GuestOperatingSystem, GuestPlatform, RuntimeIdentity, RuntimeKind, RuntimeVersion,
        StateFormatIdentity, StateFormatName, StateFormatVersion,
    };

    let execution = ExecutionMetadata::new(
        RuntimeIdentity {
            kind: RuntimeKind::Firecracker,
            version: RuntimeVersion::parse("1.15.1").unwrap(),
        },
        GuestPlatform {
            os: GuestOperatingSystem::Linux,
            architecture: Arch::X86_64,
        },
        Some(StateFormatIdentity {
            format: StateFormatName::parse(FIRECRACKER_STATE_FORMAT).unwrap(),
            version: StateFormatVersion::parse("9.0.0").unwrap(),
        }),
        vec![ArtifactComponentMetadata {
            kind: ArtifactComponentKind::Kernel,
            role: Some(ArtifactComponentRole::parse("boot").unwrap()),
            version: Some(ArtifactVersion::parse("vmlinux-6.10.14").unwrap()),
            digest: Some(Digest::sha256([0x42; 32])),
            format: Some(ArtifactFormat::parse("vmlinux").unwrap()),
        }],
    )
    .unwrap();

    let details = SandboxEventDetails::default().execution_metadata(&execution);

    assert_eq!(details.runtime.as_deref(), Some("fc"));
    assert_eq!(details.runtime_version.as_deref(), Some("1.15.1"));
    assert_eq!(details.guest_os.as_deref(), Some("linux"));
    assert_eq!(details.guest_arch.as_deref(), Some("x86_64"));
    assert_eq!(
        details.state_format_name.as_deref(),
        Some(FIRECRACKER_STATE_FORMAT)
    );
    assert_eq!(details.state_format_version.as_deref(), Some("9.0.0"));
    assert_eq!(details.kernel_version.as_deref(), Some("vmlinux-6.10.14"));
    assert_eq!(
        details.kernel_digest.as_deref(),
        Some("sha256:4242424242424242424242424242424242424242424242424242424242424242")
    );
}

/// A detail reaches the event stream only if `emit_with` lists it. Setting it
/// on the builder and reading it back exercises the setter and stops there, so
/// capture what a subscriber is actually handed.
#[test]
fn network_policy_refresh_readiness_reaches_the_emitted_event() {
    use pplx_space_common_core::types::SandboxId;
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::layer::SubscriberExt;

    type Fields = Arc<Mutex<Vec<(String, String)>>>;

    struct Capture(Fields);

    impl tracing::field::Visit for Capture {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0
                .lock()
                .unwrap()
                .push((field.name().to_string(), format!("{value:?}")));
        }
    }

    struct CaptureLayer(Fields);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CaptureLayer {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            event.record(&mut Capture(Arc::clone(&self.0)));
        }
    }

    let fields: Fields = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(CaptureLayer(Arc::clone(&fields)));
    tracing::subscriber::with_default(subscriber, || {
        SandboxEvent::new(
            SandboxId::new(),
            SandboxEventType::NetworkPolicyRefreshCompleted,
            SandboxEventSource::SbxManager,
        )
        .emit_with(
            SandboxEventDetails::default()
                .network_state("ready")
                .interception_readiness("dependencies_not_ready"),
        );
    });

    let captured = fields.lock().unwrap();
    let readiness = captured
        .iter()
        .find(|(name, _)| name == "interception_readiness")
        .unwrap_or_else(|| panic!("no interception_readiness field in {captured:?}"));
    assert!(
        readiness.1.contains("dependencies_not_ready"),
        "interception_readiness carried {:?}",
        readiness.1
    );
}
