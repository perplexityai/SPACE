//! Lookups and pulls of a release bound to one environment: a twin, or the
//! release itself.

use pplx_space_content_digest::Digest;
use pplx_space_node_api_client::proto::{FindTemplateReleaseResponse, TemplateRelease};

use super::*;

const NAME: &str = "base";

fn hash() -> String {
    format!("sha256:{}", "a".repeat(64))
}

fn environment(byte: u8) -> EnvironmentReference {
    EnvironmentReference::new(Digest::sha256([byte; 32]))
}

fn response(served: Option<String>) -> FindTemplateReleaseResponse {
    FindTemplateReleaseResponse {
        release: Some(TemplateRelease {
            template_name: NAME.into(),
            runtime: DEFAULT_RUNTIME.into(),
            hash: hash(),
            environment: served,
            ..Default::default()
        }),
        environment_applied: true,
        ..Default::default()
    }
}

fn lookup(environment: Option<EnvironmentReference>) -> ReleaseLookup<'static> {
    ReleaseLookup {
        name: NAME,
        req_hash: "",
        req_tag: "latest",
        environment,
    }
}

#[test]
fn a_selected_lookup_takes_only_a_release_bound_to_its_environment() {
    let requested = environment(1);
    let release = parse_release_response(
        lookup(Some(requested)),
        None,
        false,
        response(Some(requested.to_string())),
    )
    .expect("a release bound to the requested environment");
    assert_eq!(release.environment, Some(requested));

    // An unbound release, or one bound elsewhere, isn't the one asked for.
    for served in [None, Some(environment(2).to_string())] {
        assert!(matches!(
            parse_release_response(lookup(Some(requested)), None, false, response(served)),
            Err(FindReleaseError::NotFound)
        ));
    }

    // The initial environment's builds share the legacy partition with
    // unbound ones, so its selector also takes an unbound release, as the
    // cluster's does.
    let initial = pplx_space_node_runtime_core::catalog::initial_environment_reference();
    for served in [None, Some(initial.to_string())] {
        parse_release_response(lookup(Some(initial)), None, false, response(served))
            .expect("an unbound or initial-environment release serves the initial environment");
    }
    assert!(matches!(
        parse_release_response(
            lookup(Some(initial)),
            None,
            false,
            response(Some(requested.to_string()))
        ),
        Err(FindReleaseError::NotFound)
    ));
}

/// A lookup naming no environment takes the release it's served, keeping
/// its binding. One that doesn't parse is an invalid response, whether or
/// not the lookup named an environment: unread, it would look unbound. A
/// lookup naming one keeps the answer's org-specific verdict with it.
#[test]
fn a_lookup_without_an_environment_takes_any_binding_but_a_malformed_one() {
    for served in [None, Some(environment(2))] {
        let release = parse_release_response(
            lookup(None),
            None,
            false,
            response(served.map(|environment| environment.to_string())),
        )
        .expect("an environment-less lookup takes the release it's served");
        assert_eq!(release.environment, served);
    }
    assert!(matches!(
        parse_release_response(
            lookup(None),
            None,
            false,
            response(Some("not-a-reference".into()))
        ),
        Err(FindReleaseError::InvalidResponse)
    ));
    // A lookup naming an environment keeps the answer's visibility verdict,
    // which doesn't depend on the binding.
    for (visibility, org_specific) in [
        (TemplateVisibility::Generic, false),
        (TemplateVisibility::OrgSpecific, true),
    ] {
        let mut malformed = response(Some("not-a-reference".into()));
        malformed.template_visibility = visibility.into();
        let parsed = parse_release_response(lookup(Some(environment(1))), None, false, malformed);
        assert!(
            matches!(parsed, Err(FindReleaseError::InvalidBinding { org_specific: o }) if o == org_specific),
            "{visibility:?}"
        );
    }
}

#[test]
fn a_selected_latest_list_keeps_only_entries_bound_to_its_environment() {
    let requested = environment(1);
    let requested_wire = requested.to_string();
    let other = environment(2).to_string();
    assert!(latest_entry_serves(None, None));
    assert!(latest_entry_serves(None, Some(&other)));
    assert!(latest_entry_serves(Some(requested), Some(&requested_wire)));
    assert!(!latest_entry_serves(Some(requested), None));
    assert!(!latest_entry_serves(Some(requested), Some(&other)));
    assert!(!latest_entry_serves(
        Some(requested),
        Some("not-a-reference")
    ));
    let initial = pplx_space_node_runtime_core::catalog::initial_environment_reference();
    assert!(latest_entry_serves(Some(initial), None));
    assert!(!latest_entry_serves(Some(initial), Some(&other)));
    assert!(!latest_entry_serves(Some(initial), Some("not-a-reference")));
}

/// A latest release the cluster spells bare is the `sha256:` build the pull
/// stores, so prefetch pins and checks the build it warms. A hash that
/// doesn't parse, or an entry with no release, is skipped by a lookup naming
/// no environment, and makes one naming an environment an error, so prefetch
/// keeps that environment's pins.
#[test]
fn latest_releases_carry_the_canonical_hash_spelling() {
    use pplx_space_node_api_client::proto::{
        ListLatestTemplateReleasesEntry, ListLatestTemplateReleasesResponse,
    };
    let twin = environment(1);
    let canonical = hash();
    let bare = canonical.trim_start_matches("sha256:");
    let entry = |hash: &str| ListLatestTemplateReleasesEntry {
        release: Some(TemplateRelease {
            hash: hash.to_string(),
            environment: Some(twin.to_string()),
            ..Default::default()
        }),
        template_visibility: TemplateVisibility::Generic as i32,
        ..Default::default()
    };
    let response = |entries| ListLatestTemplateReleasesResponse {
        entries,
        template_visibility: TemplateVisibility::Generic as i32,
        environment_applied: true,
    };
    let hashes = |outcome| {
        let LatestReleasesOutcome::Resolved { releases, .. } = outcome else {
            panic!("a generic answer with releases resolves");
        };
        releases
            .into_iter()
            .map(|release: LatestRelease| release.content_hash)
            .collect::<Vec<_>>()
    };
    assert_eq!(
        hashes(latest_releases_from(
            NAME,
            Some(twin),
            response(vec![entry(bare), entry(&canonical)])
        )),
        vec![canonical.clone(), canonical.clone()]
    );
    let malformed = || {
        vec![
            entry(&canonical),
            entry("not-a-hash"),
            entry(""),
            ListLatestTemplateReleasesEntry {
                release: None,
                ..entry(&canonical)
            },
        ]
    };
    assert_eq!(
        hashes(latest_releases_from(NAME, None, response(malformed()))),
        vec![canonical.clone()]
    );
    for bad in malformed().into_iter().skip(1) {
        assert!(matches!(
            latest_releases_from(NAME, Some(twin), response(vec![entry(&canonical), bad])),
            LatestReleasesOutcome::Error
        ));
    }
}

/// A selected latest list holding an entry whose binding doesn't parse is an
/// error, as a tag lookup's is, so prefetch keeps the template's pins rather
/// than reading nothing bound there. An empty list still resolves empty, and
/// an environment-less lookup takes any binding.
#[test]
fn a_selected_latest_list_with_a_malformed_binding_is_an_error() {
    use pplx_space_node_api_client::proto::{
        ListLatestTemplateReleasesEntry, ListLatestTemplateReleasesResponse,
    };
    let response = |served: &[&str]| ListLatestTemplateReleasesResponse {
        entries: served
            .iter()
            .map(|served| ListLatestTemplateReleasesEntry {
                release: Some(TemplateRelease {
                    hash: hash(),
                    environment: Some(served.to_string()),
                    ..Default::default()
                }),
                template_visibility: TemplateVisibility::Generic as i32,
                ..Default::default()
            })
            .collect(),
        template_visibility: TemplateVisibility::Generic as i32,
        environment_applied: true,
    };
    let twin = environment(1).to_string();
    for served in [
        &["not-a-reference"][..],
        &[twin.as_str(), "not-a-reference"],
    ] {
        assert!(
            matches!(
                latest_releases_from(NAME, Some(environment(1)), response(served)),
                LatestReleasesOutcome::Error
            ),
            "{served:?}"
        );
    }
    assert!(matches!(
        latest_releases_from(NAME, Some(environment(1)), response(&[])),
        LatestReleasesOutcome::Resolved { releases, .. } if releases.is_empty()
    ));
    assert!(matches!(
        latest_releases_from(NAME, None, response(&["not-a-reference"])),
        LatestReleasesOutcome::Resolved { releases, .. } if releases.len() == 1
    ));
}

#[test]
fn pulls_of_one_digest_in_two_partitions_run_apart_and_anything_else_waits() {
    let twin = TemplateCachePartition::Environment(environment(1));
    let other = TemplateCachePartition::Environment(environment(2));
    let key = |partition| PullKey::in_partition(NAME, hash(), partition);

    assert!(!key(Some(twin)).is_held_by(&key(Some(TemplateCachePartition::Legacy))));
    assert!(!key(Some(twin)).is_held_by(&key(Some(other))));
    assert!(key(Some(twin)).is_held_by(&key(Some(twin))));
    // A pull whose manifest picks its partition, or a claim, holds and is
    // held by every partition's.
    assert!(key(Some(twin)).is_held_by(&key(None)));
    assert!(key(None).is_held_by(&key(Some(twin))));
    assert!(key(Some(twin)).is_held_by(&PullKey::template(NAME)));
    assert!(PullKey::template(NAME).is_held_by(&key(Some(twin))));
    assert!(!key(Some(twin)).is_held_by(&PullKey::in_partition(
        NAME,
        format!("sha256:{}", "b".repeat(64)),
        Some(twin)
    )));
    assert!(!key(Some(twin)).is_held_by(&PullKey::in_partition("other", hash(), Some(twin))));
}

#[test]
fn a_twin_pull_starts_beside_its_original_s_and_a_digest_claim_waits_for_both() {
    let twin = TemplateCachePartition::Environment(environment(1));
    let partitions = [Some(TemplateCachePartition::Legacy), Some(twin)];
    // Release the two flights in both orders: whichever is left still holds
    // the claim off, and once both go it succeeds.
    for order in [[0, 1], [1, 0]] {
        let puller = VolumePuller::new();
        // Each partition's own flight is kept beside its guard: the guard
        // takes one `Arc` handle and the test keeps a clone, so it can check
        // which flight a later pull joins.
        let (flights, mut guards): (Vec<_>, Vec<Option<PullFlightGuard>>) = partitions
            .iter()
            .map(|partition| {
                let key = PullKey::in_partition(NAME, hash(), *partition);
                let (flight, started) = puller.start_or_join_flight(key.clone());
                assert!(
                    started,
                    "another partition's pull doesn't hold {partition:?}'s"
                );
                let guard =
                    PullFlightGuard::new(key, Arc::clone(&flight), Arc::clone(&puller.in_flight));
                (flight, Some(guard))
            })
            .unzip();
        let (joined, started) =
            puller.start_or_join_flight(PullKey::in_partition(NAME, hash(), Some(twin)));
        assert!(!started);
        assert!(
            Arc::ptr_eq(&joined, &flights[1]),
            "the same partition joins its own pull, not the other partition's"
        );
        assert!(puller.try_claim_digest(NAME, &hash()).is_none());
        guards[order[0]] = None;
        assert!(
            puller.is_claimed(NAME, &hash()),
            "{:?}'s pull holds the claim",
            partitions[order[1]]
        );
        assert!(puller.try_claim_digest(NAME, &hash()).is_none());
        guards[order[1]] = None;
        assert!(puller.try_claim_digest(NAME, &hash()).is_some());
    }
}

/// The `metadata.json` a pull reads is the served release's own: a lookup
/// naming an environment reads the environment's key only when the cluster
/// served a release bound to it, and the shared one for an unbound release
/// it served for an environment cached in the legacy partition. A lookup
/// naming none reads the shared key, as before twins, whatever was served.
#[test]
fn the_served_release_picks_its_metadata_key() {
    let initial = pplx_space_node_runtime_core::catalog::initial_environment_reference();
    let spec = |environment| PullSpec {
        name: NAME,
        req_hash: "",
        req_tag: "",
        trigger: PullTrigger::PrefetchTag,
        required: None,
        environment,
    };
    let served = |requested, served| PullTarget::of(&spec(requested), served).served_environment;
    assert_eq!(
        served(Some(environment(1)), Some(environment(1))),
        Some(environment(1))
    );
    assert_eq!(served(Some(initial), None), None);
    assert_eq!(served(Some(initial), Some(initial)), Some(initial));
    assert_eq!(served(None, Some(environment(1))), None);
}

/// A twin shares its original's manifest and `metadata.json`, bound to the
/// original's environment, and carries its own binding in its environment's
/// metadata. The pull rebinds only its local manifest: the shared artifacts
/// with the twin's execution, once the twin's environment takes the shared
/// build as a cold-boot source. The rebound manifest then normalizes against
/// the twin's metadata, where the shared one is an execution mismatch. A twin
/// metadata with no binding, an environment the node catalog doesn't know, or
/// one that can't cold-boot the shared build is refused.
#[test]
fn a_twin_s_manifest_takes_its_own_metadata_s_binding_once_it_can_cold_boot() {
    use pplx_space_execution_catalog::catalog::CompiledEnvironment;
    use pplx_space_template_manifest::{TemplateManifest, TemplateMetadata};
    let catalog =
        CompiledEnvironment::cached_for_architecture(pplx_space_cpu_arch::Arch::X86_64).unwrap();
    let execution = |select: &dyn Fn(&CompiledEnvironment) -> bool| {
        catalog
            .iter()
            .find(|entry| select(entry))
            .unwrap()
            .definition()
            .execution_requirements()
            .unwrap()
    };
    let original = execution(&|entry| entry.is_initial());
    // A campaign's twin binds an upgrade target the catalog certifies as
    // able to cold-boot the original's builds.
    let target = *catalog
        .iter()
        .find(|entry| entry.is_initial())
        .unwrap()
        .upgrade_targets()
        .first()
        .expect("the initial environment has a certified upgrade target");
    let twin = execution(&|entry| entry.definition().reference() == target);
    let mut manifest = TemplateManifest {
        runtime: "fc".to_string(),
        arch: "x86_64".to_string(),
        ..Default::default()
    };
    manifest.files.insert(
        "rootfs.ext4".to_string(),
        pplx_space_template_manifest::TemplateManifestFile {
            digest: hash(),
            size: Some(1024),
            compression: None,
        },
    );
    manifest.set_execution_metadata(original.clone());
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let metadata_of = |execution: pplx_space_execution_spec::ExecutionMetadata| {
        let mut metadata = TemplateMetadata::default();
        metadata.set_execution_metadata(execution);
        metadata
    };
    let shared = serde_json::to_vec(&metadata_of(original.clone())).unwrap();
    let twin_metadata = metadata_of(twin.clone());
    let twin_bytes = serde_json::to_vec(&twin_metadata).unwrap();

    // Read as published, the original's binding wins.
    assert_eq!(
        release_partition(&manifest_bytes, &twin_bytes).unwrap(),
        pplx_space_node_runtime_core::catalog::template_cache_partition(original.environment())
    );
    let bound = bind_manifest_to_metadata(&manifest_bytes, &shared, &twin_bytes).unwrap();
    assert_eq!(
        release_partition(&bound, &twin_bytes).unwrap(),
        pplx_space_node_runtime_core::catalog::template_cache_partition(twin.environment())
    );
    let bound: TemplateManifest = serde_json::from_slice(&bound).unwrap();
    assert_eq!(bound.files, manifest.files, "the shared artifacts stay");
    let normalize = |manifest: TemplateManifest| {
        crate::service::normalize_template_manifest(
            manifest,
            NAME,
            &hash(),
            &twin_metadata,
            1,
            None,
        )
    };
    normalize(bound).unwrap();
    assert!(matches!(
        normalize(manifest.clone()),
        Err(crate::service::NormalizeTemplateManifestError::ExecutionMismatch)
    ));

    let refused = |twin_metadata: &TemplateMetadata| {
        bind_manifest_to_metadata(
            &manifest_bytes,
            &shared,
            &serde_json::to_vec(twin_metadata).unwrap(),
        )
    };
    assert_eq!(
        refused(&TemplateMetadata::default()),
        Err(PullFailure::ExecutionMetadataInvalid),
        "no binding"
    );
    assert_eq!(
        refused(&metadata_of(twin.clone().with_environment(environment(9)))),
        Err(PullFailure::ExecutionMetadataInvalid),
        "an environment the catalog doesn't know"
    );
    assert_eq!(
        refused(&metadata_of(
            execution(&|entry| entry.name() == "fc-1.17.0-linux-6.18.44").with_environment(target)
        )),
        Err(PullFailure::ExecutionMetadataInvalid),
        "an execution that doesn't match the environment it names"
    );
    for other in ["fc-1.17.0-linux-6.18.44", "ch-54.0-linux-7.2.8"] {
        assert_eq!(
            refused(&metadata_of(execution(&|entry| entry.name() == other))),
            Err(PullFailure::ExecutionMetadataInvalid),
            "{other} can't cold-boot the original's build"
        );
    }

    // An unbound original, published before execution metadata, is the
    // initial environment's: its certified upgrade target's twin rebinds.
    let unbound_manifest = serde_json::to_vec(&TemplateManifest {
        runtime: "fc".to_string(),
        arch: "x86_64".to_string(),
        ..Default::default()
    })
    .unwrap();
    let unbound_shared = serde_json::to_vec(&TemplateMetadata::default()).unwrap();
    let bound = bind_manifest_to_metadata(&unbound_manifest, &unbound_shared, &twin_bytes).unwrap();
    assert_eq!(
        release_partition(&bound, &twin_bytes).unwrap(),
        pplx_space_node_runtime_core::catalog::template_cache_partition(twin.environment())
    );
    // Its source is the initial environment's own execution, not the twin's:
    // an environment that can't cold-boot the initial environment's builds is
    // refused for it too.
    assert_eq!(
        bind_manifest_to_metadata(
            &unbound_manifest,
            &unbound_shared,
            &serde_json::to_vec(&metadata_of(execution(&|entry| {
                entry.name() == "fc-1.17.0-linux-6.18.44"
            })))
            .unwrap(),
        ),
        Err(PullFailure::ExecutionMetadataInvalid)
    );
    // One that records no architecture is the node's, as its row is, and
    // the `amd64` spelling is x86_64's.
    for arch in ["", "amd64"] {
        let manifest = serde_json::to_vec(&TemplateManifest {
            runtime: "fc".to_string(),
            arch: arch.to_string(),
            ..Default::default()
        })
        .unwrap();
        if arch.is_empty() && std::env::consts::ARCH != "x86_64" {
            continue;
        }
        bind_manifest_to_metadata(&manifest, &unbound_shared, &twin_bytes)
            .unwrap_or_else(|failure| panic!("arch {arch:?}: {failure:?}"));
    }
    // A template without execution metadata gives its build an unbound
    // execution with no boot components, which the catalog reads as the
    // initial environment's; in the manifest or only in the shared metadata,
    // its certified upgrade target's twin rebinds, and an environment that
    // can't cold-boot the initial environment's builds is still refused.
    let componentless = pplx_space_execution_spec::ExecutionMetadata::initial_firecracker(
        pplx_space_cpu_arch::Arch::X86_64,
        vec![],
    );
    assert!(
        CompiledEnvironment::of_execution(catalog, &componentless)
            .is_some_and(|entry| entry.is_initial())
    );
    let mut componentless_manifest = TemplateManifest {
        runtime: "fc".to_string(),
        arch: "x86_64".to_string(),
        ..Default::default()
    };
    componentless_manifest.set_execution_metadata(componentless.clone());
    let componentless_manifest = serde_json::to_vec(&componentless_manifest).unwrap();
    let componentless_shared = serde_json::to_vec(&metadata_of(componentless)).unwrap();
    let unrunnable = serde_json::to_vec(&metadata_of(execution(&|entry| {
        entry.name() == "fc-1.17.0-linux-6.18.44"
    })))
    .unwrap();
    for (case, manifest, shared) in [
        ("in the manifest", &componentless_manifest, &unbound_shared),
        (
            "in the shared metadata",
            &unbound_manifest,
            &componentless_shared,
        ),
    ] {
        let bound = bind_manifest_to_metadata(manifest, shared, &twin_bytes)
            .unwrap_or_else(|failure| panic!("{case}: {failure:?}"));
        assert_eq!(
            release_partition(&bound, &twin_bytes).unwrap(),
            pplx_space_node_runtime_core::catalog::template_cache_partition(twin.environment()),
            "{case}"
        );
        assert_eq!(
            bind_manifest_to_metadata(manifest, shared, &unrunnable),
            Err(PullFailure::ExecutionMetadataInvalid),
            "{case}"
        );
    }

    // A manifest with no execution takes its binding from the shared
    // metadata, and unreadable shared metadata is refused.
    bind_manifest_to_metadata(&unbound_manifest, &shared, &twin_bytes).unwrap();
    assert_eq!(
        bind_manifest_to_metadata(&unbound_manifest, b"not json", &twin_bytes),
        Err(PullFailure::ExecutionMetadataInvalid)
    );
    // Shared metadata whose execution fails its schema (a schema version
    // with no execution) is refused, though the manifest's execution is
    // valid and the rebind would replace the metadata; so is a manifest
    // whose own execution fails its schema.
    let mut missing_execution = metadata_of(original.clone());
    missing_execution.execution = None;
    assert!(missing_execution.schema_version.is_some());
    missing_execution.execution_metadata().unwrap_err();
    assert_eq!(
        bind_manifest_to_metadata(
            &manifest_bytes,
            &serde_json::to_vec(&missing_execution).unwrap(),
            &twin_bytes,
        ),
        Err(PullFailure::ExecutionMetadataInvalid)
    );
    let mut legacy_manifest = manifest.clone();
    legacy_manifest.schema_version = None;
    legacy_manifest.execution_metadata().unwrap_err();
    assert_eq!(
        bind_manifest_to_metadata(
            &serde_json::to_vec(&legacy_manifest).unwrap(),
            &shared,
            &twin_bytes,
        ),
        Err(PullFailure::ManifestMalformed)
    );
    // An empty shared `metadata.json` is no metadata, as the other readers
    // take it: the manifest's binding, or the initial environment's, is the
    // source.
    for manifest in [&manifest_bytes, &unbound_manifest] {
        let bound = bind_manifest_to_metadata(manifest, b"", &twin_bytes).unwrap();
        assert_eq!(
            release_partition(&bound, &twin_bytes).unwrap(),
            pplx_space_node_runtime_core::catalog::template_cache_partition(twin.environment())
        );
    }

    // Shared files that disagree on the execution are refused, as the
    // original's pull refuses them, though the twin could cold-boot the
    // manifest's: here the shared metadata names the twin's execution.
    assert_eq!(
        bind_manifest_to_metadata(
            &manifest_bytes,
            &serde_json::to_vec(&metadata_of(twin.clone())).unwrap(),
            &twin_bytes,
        ),
        Err(PullFailure::ExecutionMetadataInvalid)
    );
    // So are ones that agree on the execution but not on the guest image,
    // as the original's pull refuses them (`GuestImageMismatch`).
    let mut other_guest_image = metadata_of(original.clone());
    // The manifest records no guest image; the shared metadata records the
    // default one, valid for the same execution.
    other_guest_image.set_guest_image(Some(
        pplx_space_execution_spec::guest_image::GuestImageContract::default(),
    ));
    other_guest_image.execution_metadata().unwrap();
    assert_ne!(manifest.guest_image, other_guest_image.guest_image);
    assert!(matches!(
        crate::service::normalize_template_manifest(
            manifest.clone(),
            NAME,
            &hash(),
            &other_guest_image,
            1,
            None,
        ),
        Err(crate::service::NormalizeTemplateManifestError::GuestImageMismatch)
    ));
    assert_eq!(
        bind_manifest_to_metadata(
            &manifest_bytes,
            &serde_json::to_vec(&other_guest_image).unwrap(),
            &twin_bytes,
        ),
        Err(PullFailure::ExecutionMetadataInvalid)
    );
}

/// A digest whose release the cluster binds to no environment can still have
/// a build bound to a non-initial one by its manifest, which its holders
/// report, so a lookup requiring that environment names it. The cluster's
/// selector doesn't match it there, so the lookup falls back to the digest's
/// unbound release with no binding, and the pull's partition check decides.
/// A release bound to another environment, a tag lookup, an environment-less
/// one and any other error don't fall back.
#[tokio::test]
async fn a_required_digest_falls_back_to_its_unbound_release() {
    use std::cell::RefCell;
    let required = environment(1);
    let release = |environment: Option<EnvironmentReference>| ResolvedRelease {
        hash: hash(),
        storage_uris: vec!["s3://bucket/base".to_string()],
        visibility: TemplateVisibility::Generic,
        environment,
    };
    // The environments asked for, and the answer without one.
    let run = async |environment: Option<EnvironmentReference>,
                     req_hash: &str,
                     bound: Result<ResolvedRelease, FindReleaseError>,
                     unbound: Result<ResolvedRelease, FindReleaseError>| {
        let asked = RefCell::new(Vec::new());
        let (bound, unbound) = (RefCell::new(Some(bound)), RefCell::new(Some(unbound)));
        let found = find_bound_release_or_unbound_digest(environment, req_hash, |selector| {
            asked.borrow_mut().push(selector);
            let answer = match selector {
                Some(_) => bound.borrow_mut().take().unwrap(),
                None => unbound.borrow_mut().take().unwrap(),
            };
            std::future::ready(answer)
        })
        .await;
        (found, asked.into_inner())
    };

    let (found, asked) = run(
        Some(required),
        &hash(),
        Err(FindReleaseError::NotFound),
        Ok(release(None)),
    )
    .await;
    assert!(matches!(
        found,
        Ok(ResolvedRelease {
            environment: None,
            ..
        })
    ));
    assert_eq!(asked, vec![Some(required), None]);

    let (found, asked) = run(
        Some(required),
        &hash(),
        Err(FindReleaseError::NotFound),
        Ok(release(Some(environment(2)))),
    )
    .await;
    assert!(
        matches!(found, Err(FindReleaseError::NotFound)),
        "bound elsewhere"
    );
    assert_eq!(asked, vec![Some(required), None]);

    let (found, asked) = run(
        Some(required),
        "",
        Err(FindReleaseError::NotFound),
        Ok(release(None)),
    )
    .await;
    assert!(
        matches!(found, Err(FindReleaseError::NotFound)),
        "a tag lookup"
    );
    assert_eq!(asked, vec![Some(required)]);

    let (found, asked) = run(
        Some(required),
        &hash(),
        Err(FindReleaseError::Unavailable),
        Ok(release(None)),
    )
    .await;
    assert!(
        matches!(found, Err(FindReleaseError::Unavailable)),
        "another error"
    );
    assert_eq!(asked, vec![Some(required)]);

    let (found, asked) = run(
        Some(required),
        &hash(),
        Ok(release(Some(required))),
        Ok(release(None)),
    )
    .await;
    assert!(
        matches!(found, Ok(ResolvedRelease { environment: Some(served), .. }) if served == required)
    );
    assert_eq!(asked, vec![Some(required)]);

    // An environment-less lookup asks once, and its miss is the answer.
    let (found, asked) = run(
        None,
        &hash(),
        Ok(release(None)),
        Err(FindReleaseError::NotFound),
    )
    .await;
    assert!(matches!(found, Err(FindReleaseError::NotFound)));
    assert_eq!(asked, vec![None]);
}

/// A twin bound to the required environment with no storage location falls
/// back to the digest's unbound release when that one has a location: a
/// release registered before releases recorded their binding can hold the
/// only copy, and the pull's partition check makes its manifest prove the
/// environment. Otherwise the twin stays the answer, so a node that already
/// holds its build still takes it from its cache.
#[tokio::test]
async fn a_location_less_twin_falls_back_to_its_unbound_release_with_a_location() {
    use std::cell::RefCell;
    let required = environment(1);
    let release = |environment: Option<EnvironmentReference>, located: bool| ResolvedRelease {
        hash: hash(),
        storage_uris: if located {
            vec!["s3://bucket/base".to_string()]
        } else {
            Vec::new()
        },
        visibility: TemplateVisibility::Generic,
        environment,
    };
    let run = async |bound: ResolvedRelease, unbound: Result<ResolvedRelease, FindReleaseError>| {
        let asked = RefCell::new(Vec::new());
        let (bound, unbound) = (RefCell::new(Some(Ok(bound))), RefCell::new(Some(unbound)));
        let found = find_bound_release_or_unbound_digest(Some(required), &hash(), |selector| {
            asked.borrow_mut().push(selector);
            std::future::ready(match selector {
                Some(_) => bound.borrow_mut().take().unwrap(),
                None => unbound.borrow_mut().take().unwrap(),
            })
        })
        .await;
        (
            found.map(|found| (found.environment, found.storage_uris.len())),
            asked.into_inner(),
        )
    };

    let (found, asked) = run(release(Some(required), false), Ok(release(None, true))).await;
    assert!(matches!(found, Ok((None, 1))), "the located original");
    assert_eq!(asked, vec![Some(required), None]);

    for (unbound, why) in [
        (
            Ok(release(None, false)),
            "an original with no location either",
        ),
        (
            Ok(release(Some(environment(2)), true)),
            "a release bound elsewhere",
        ),
        (Err(FindReleaseError::NotFound), "no unbound release"),
        (Err(FindReleaseError::Unavailable), "a failed fallback"),
    ] {
        let (found, asked) = run(release(Some(required), false), unbound).await;
        assert!(
            matches!(found, Ok((Some(served), 0)) if served == required),
            "the twin stays the answer after {why}"
        );
        assert_eq!(asked, vec![Some(required), None], "{why}");
    }

    // A located twin, and an unbound release the lookup served, ask once.
    for bound in [release(Some(required), true), release(None, false)] {
        let served = bound.environment;
        let (found, asked) = run(bound, Ok(release(None, true))).await;
        assert!(matches!(found, Ok((environment, _)) if environment == served));
        assert_eq!(asked, vec![Some(required)]);
    }
}

/// A pull's own lookup, which `pull_inner` runs when no authorized lookup
/// resolved the release first (a create or a pool's environment write by
/// digest with no caller org), takes the same fallback for an on-demand
/// pull, so such a create still finds a digest's unbound release. Prefetch
/// asks only for the release bound to the environment.
#[tokio::test]
async fn an_on_demand_pull_falls_back_to_the_unbound_release_and_prefetch_does_not() {
    use std::cell::RefCell;
    let required = environment(1);
    for (trigger, expected) in [
        (PullTrigger::OnDemand, vec![Some(required), None]),
        (PullTrigger::PrefetchLatestRelease, vec![Some(required)]),
        (PullTrigger::PrefetchTag, vec![Some(required)]),
    ] {
        let asked = RefCell::new(Vec::new());
        let found = find_pull_release(trigger, Some(required), &hash(), |selector| {
            asked.borrow_mut().push(selector);
            std::future::ready(match selector {
                Some(_) => Err(FindReleaseError::NotFound),
                None => Ok(ResolvedRelease {
                    hash: hash(),
                    storage_uris: vec!["s3://bucket/base".to_string()],
                    visibility: TemplateVisibility::Generic,
                    environment: None,
                }),
            })
        })
        .await;
        assert_eq!(asked.into_inner(), expected, "{trigger:?}");
        assert_eq!(
            found.is_ok(),
            trigger == PullTrigger::OnDemand,
            "{trigger:?}"
        );
    }
}

/// A cluster API that predates twins ignores the environment selector and
/// leaves `environment_applied` false. Nothing in such an answer is used, so
/// every lookup naming an environment reads it as `SelectorIgnored`, even
/// with a binding this node can't parse: prefetch as unknown, and an
/// on-demand pull asks again without a selector.
#[test]
fn an_unacknowledged_selector_answer_is_never_used() {
    let requested = environment(1);
    let unacknowledged = |served: Option<String>| FindTemplateReleaseResponse {
        environment_applied: false,
        ..response(served)
    };
    let initial = pplx_space_node_runtime_core::catalog::initial_environment_reference();
    for (requested, served) in [
        (requested, None),
        (requested, Some(requested.to_string())),
        (requested, Some(environment(2).to_string())),
        (requested, Some("not-a-reference".into())),
        // The initial environment's builds sit in the legacy partition with
        // unbound ones, and an ignored answer is still unused for it.
        (initial, None),
        (initial, Some(initial.to_string())),
    ] {
        let mut response = unacknowledged(served.clone());
        response.storage_locations = vec![Default::default()];
        response.storage_locations[0].connection_string = "s3://bucket/base".into();
        assert!(
            matches!(
                parse_release_response(lookup(Some(requested)), None, false, response),
                Err(FindReleaseError::SelectorIgnored {
                    org_specific: false
                })
            ),
            "({requested:?}, {served:?})"
        );
    }
    // A lookup naming no environment needs no acknowledgement.
    parse_release_response(lookup(None), None, false, unacknowledged(None))
        .expect("an environment-less lookup takes any answer");
}

/// When the cluster ignores the selector, the on-demand lookup drops its
/// answer and asks again without one, by digest or by tag. It takes that
/// answer when its release is unbound, so the pull's partition check decides
/// as before twins, or bound to the environment; one bound elsewhere is
/// `NotFound`, and the second lookup's own error is the answer.
#[tokio::test]
async fn an_ignored_selector_asks_again_without_one() {
    use std::cell::RefCell;
    let required = environment(1);
    let release = |environment: Option<EnvironmentReference>| ResolvedRelease {
        hash: hash(),
        storage_uris: vec!["s3://bucket/base".to_string()],
        visibility: TemplateVisibility::Generic,
        environment,
    };
    for req_hash in [hash(), String::new()] {
        for (unselected, expected) in [
            (Ok(release(None)), Ok(None)),
            (Ok(release(Some(required))), Ok(Some(required))),
            (Ok(release(Some(environment(2)))), Err("not found")),
            (Err(FindReleaseError::NotFound), Err("not found")),
            (Err(FindReleaseError::Unavailable), Err("unavailable")),
        ] {
            let asked = RefCell::new(Vec::new());
            let unselected = RefCell::new(Some(unselected));
            let found =
                find_bound_release_or_unbound_digest(Some(required), &req_hash, |selector| {
                    asked.borrow_mut().push(selector);
                    std::future::ready(match selector {
                        Some(_) => Err(FindReleaseError::SelectorIgnored {
                            org_specific: false,
                        }),
                        None => unselected.borrow_mut().take().unwrap(),
                    })
                })
                .await;
            let found = match found {
                Ok(release) => Ok(release.environment),
                Err(FindReleaseError::NotFound) => Err("not found"),
                Err(FindReleaseError::Unavailable) => Err("unavailable"),
                Err(other) => panic!("{other}"),
            };
            assert_eq!(found, expected, "{req_hash:?}");
            assert_eq!(asked.into_inner(), vec![Some(required), None]);
        }
    }
}

/// A pull reads a release's own `environments/<environment>/metadata.json`
/// only for a twin: one served bound to an environment its shared files
/// don't bind. An original bound to the served environment, or an unbound
/// release served for the initial environment, reads only the shared files,
/// so a release without the key is never asked for it.
#[test]
fn only_a_twin_reads_its_environment_s_metadata_key() {
    use pplx_space_execution_spec::ExecutionMetadata;
    let files = |environment: Option<EnvironmentReference>| {
        let mut manifest = pplx_space_template_manifest::TemplateManifest {
            runtime: "fc".to_string(),
            arch: "x86_64".to_string(),
            ..Default::default()
        };
        let mut metadata = pplx_space_template_manifest::TemplateMetadata::default();
        if let Some(environment) = environment {
            let execution =
                ExecutionMetadata::initial_firecracker(pplx_space_cpu_arch::Arch::X86_64, vec![])
                    .with_environment(environment);
            manifest.set_execution_metadata(execution.clone());
            metadata.set_execution_metadata(execution);
        }
        (
            serde_json::to_vec(&manifest).unwrap(),
            serde_json::to_vec(&metadata).unwrap(),
        )
    };
    let (original, twin) = (environment(1), environment(2));
    let (manifest, metadata) = files(Some(original));
    assert!(!is_twin_of_shared_files(&manifest, &metadata, original));
    assert!(is_twin_of_shared_files(&manifest, &metadata, twin));
    let initial = pplx_space_node_runtime_core::catalog::initial_environment_reference();
    let (manifest, metadata) = files(None);
    assert!(!is_twin_of_shared_files(&manifest, &metadata, initial));
    assert!(is_twin_of_shared_files(&manifest, &metadata, twin));
    // Shared files that don't parse aren't a twin's: the pull refuses them
    // as malformed rather than read an environment key that may not exist.
    assert!(!is_twin_of_shared_files(b"not json", &metadata, twin));
}

/// A twin's prefetch pull waits, before it takes a pull permit, for a pull
/// or claim of its digest that holds its partition, and not for another
/// partition's pull.
#[tokio::test]
async fn a_twin_pull_waits_for_its_digest_s_other_flights_before_a_permit() {
    let puller = VolumePuller::new();
    let twin = TemplateCachePartition::Environment(environment(1));
    let other = TemplateCachePartition::Environment(environment(2));
    let wait = |puller: VolumePuller| {
        tokio::spawn(async move {
            puller
                .wait_for_digest_flights(NAME, &hash(), Some(twin))
                .await
        })
    };

    // Nothing holds it: no wait.
    tokio::time::timeout(std::time::Duration::from_secs(1), wait(puller.clone()))
        .await
        .expect("nothing holds the twin")
        .unwrap();

    // Another partition's pull doesn't hold it.
    let (_other, started) =
        puller.start_or_join_flight(PullKey::in_partition(NAME, hash(), Some(other)));
    assert!(started);
    tokio::time::timeout(std::time::Duration::from_secs(1), wait(puller.clone()))
        .await
        .expect("another partition's pull doesn't hold a twin's")
        .unwrap();

    // A pull of its own partition holds it, and so does an environment-less
    // pull of the digest, which holds every partition.
    for holder in [Some(twin), None] {
        let puller = VolumePuller::new();
        let (_flight, started) =
            puller.start_or_join_flight(PullKey::in_partition(NAME, hash(), holder));
        assert!(started);
        let digest = hash();
        let mut waiting = std::pin::pin!(puller.wait_for_digest_flights(NAME, &digest, Some(twin)));
        assert!(
            futures_util::FutureExt::now_or_never(waiting.as_mut()).is_none(),
            "a pull in {holder:?} holds the twin"
        );
    }

    // A claim of the digest, holding every partition as an
    // environment-less pull does, holds it.
    let puller = VolumePuller::new();
    let claim = puller.try_claim_digest(NAME, &hash()).unwrap();
    let digest = hash();
    let mut waiting = std::pin::pin!(puller.wait_for_digest_flights(NAME, &digest, Some(twin)));
    assert!(
        futures_util::FutureExt::now_or_never(waiting.as_mut()).is_none(),
        "the twin waits for the digest's claim"
    );
    drop(claim);
    tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
        .await
        .expect("the wait ends once the claim goes");
}

/// An org-specific verdict stands over a malformed entry in a selected latest
/// list, whether the response gives it or an entry does, even one after the
/// malformed entry: prefetch flushes the template's pins on it rather than
/// keeping them as it does for an `Error`.
#[test]
fn an_org_specific_verdict_stands_over_a_malformed_latest_entry() {
    use pplx_space_node_api_client::proto::{
        ListLatestTemplateReleasesEntry, ListLatestTemplateReleasesResponse,
    };
    let entry = |release: Option<TemplateRelease>, visibility: TemplateVisibility| {
        ListLatestTemplateReleasesEntry {
            release,
            template_visibility: visibility as i32,
            ..Default::default()
        }
    };
    let bound = |hash: String, served: &str| TemplateRelease {
        hash,
        environment: Some(served.to_string()),
        ..Default::default()
    };
    let twin = environment(1).to_string();
    let malformed = [
        ("no release", None),
        (
            "a hash that doesn't parse",
            Some(bound("nope".into(), &twin)),
        ),
        (
            "a binding that doesn't parse",
            Some(bound(hash(), "not-a-reference")),
        ),
    ];
    let generic = TemplateVisibility::Generic;
    let org = TemplateVisibility::OrgSpecific;
    for (case, release) in malformed {
        for (verdict, response_visibility, later_entry, expect_org) in [
            ("none", generic, generic, false),
            ("the response's", org, generic, true),
            ("a later entry's", generic, org, true),
        ] {
            let resp = ListLatestTemplateReleasesResponse {
                entries: vec![
                    entry(release.clone(), generic),
                    entry(Some(bound(hash(), &twin)), later_entry),
                ],
                template_visibility: response_visibility as i32,
                environment_applied: true,
            };
            let outcome = latest_releases_from(NAME, Some(environment(1)), resp);
            if expect_org {
                assert!(
                    matches!(
                        outcome,
                        LatestReleasesOutcome::Resolved { visibility: TemplateVisibility::OrgSpecific, ref releases }
                            if releases.is_empty()
                    ),
                    "{case}, {verdict} verdict"
                );
            } else {
                assert!(
                    matches!(outcome, LatestReleasesOutcome::Error),
                    "{case}, {verdict} verdict"
                );
            }
        }
    }
}

/// An answer whose selector the cluster ignored still keeps its visibility
/// verdict, which doesn't depend on the selector: a tag lookup's and a
/// latest list's `SelectorIgnored` say whether the template is org-specific,
/// from the response or, for the list, any entry.
#[test]
fn an_ignored_selector_keeps_the_answer_s_org_specific_verdict() {
    use pplx_space_node_api_client::proto::{
        ListLatestTemplateReleasesEntry, ListLatestTemplateReleasesResponse,
    };
    for (visibility, org_specific) in [
        (TemplateVisibility::Generic, false),
        (TemplateVisibility::Unspecified, false),
        (TemplateVisibility::OrgSpecific, true),
    ] {
        let tag = FindTemplateReleaseResponse {
            environment_applied: false,
            template_visibility: visibility as i32,
            ..response(Some(environment(1).to_string()))
        };
        assert!(
            matches!(
                parse_release_response(lookup(Some(environment(1))), None, false, tag),
                Err(FindReleaseError::SelectorIgnored { org_specific: got }) if got == org_specific
            ),
            "tag lookup, {visibility:?}"
        );
        let list = |response_visibility: TemplateVisibility,
                    entry_visibility: TemplateVisibility| {
            ListLatestTemplateReleasesResponse {
                entries: vec![ListLatestTemplateReleasesEntry {
                    release: Some(TemplateRelease {
                        hash: hash(),
                        ..Default::default()
                    }),
                    template_visibility: entry_visibility as i32,
                    ..Default::default()
                }],
                template_visibility: response_visibility as i32,
                environment_applied: false,
            }
        };
        for (whose, resp) in [
            ("response", list(visibility, TemplateVisibility::Generic)),
            ("entry", list(TemplateVisibility::Generic, visibility)),
        ] {
            assert!(
                matches!(
                    latest_releases_from(NAME, Some(environment(1)), resp),
                    LatestReleasesOutcome::SelectorIgnored { org_specific: got } if got == org_specific
                ),
                "latest list, {visibility:?} on the {whose}"
            );
        }
    }
}

/// The partition probe refuses shared files the full pull would refuse,
/// before a partition miss or the twin check reads them leniently: metadata
/// JSON that doesn't parse, or whose execution fails its schema, as a bound
/// schema version with no execution does, and a manifest and metadata whose
/// executions both pass their schemas but disagree, and documents the
/// original's own pull refuses: a guest image the two disagree on, with a
/// bound manifest or a legacy one, or one in the metadata. Empty metadata, an unbound
/// document and agreeing bound files read.
#[test]
fn the_probe_refuses_shared_files_the_full_pull_would_refuse() {
    use pplx_space_execution_spec::ExecutionMetadata;
    use pplx_space_template_manifest::{TemplateManifest, TemplateMetadata};
    let unbound = || TemplateManifest {
        runtime: "fc".to_string(),
        arch: "x86_64".to_string(),
        ..Default::default()
    };
    let bound_without_execution = serde_json::to_vec(&serde_json::json!({
        "schema_version": pplx_space_template_manifest::BOUND_TEMPLATE_METADATA_SCHEMA_VERSION,
    }))
    .unwrap();
    let execution = |environment: EnvironmentReference| {
        ExecutionMetadata::initial_firecracker(pplx_space_cpu_arch::Arch::X86_64, vec![])
            .with_environment(environment)
    };
    // The initial environment's own requirements, which the catalog resolves,
    // as a release whose own pull passes carries.
    let initial = || {
        pplx_space_execution_catalog::catalog::CompiledEnvironment::cached_for_architecture(
            pplx_space_cpu_arch::Arch::X86_64,
        )
        .unwrap()
        .iter()
        .find(|entry| entry.is_initial())
        .unwrap()
        .definition()
        .execution_requirements()
        .unwrap()
    };
    // A bound document with or without a guest image contract.
    let image = |with_image: bool| {
        with_image.then(pplx_space_execution_spec::guest_image::GuestImageContract::default)
    };
    let bound_manifest = |execution: ExecutionMetadata, with_image: bool| {
        let mut manifest = unbound();
        manifest.set_execution_metadata(execution);
        manifest.set_guest_image(image(with_image));
        serde_json::to_vec(&manifest).unwrap()
    };
    let bound_metadata = |execution: ExecutionMetadata, with_image: bool| {
        let mut metadata = TemplateMetadata::default();
        metadata.set_execution_metadata(execution);
        metadata.set_guest_image(image(with_image));
        serde_json::to_vec(&metadata).unwrap()
    };
    let unbound = serde_json::to_vec(&unbound()).unwrap();
    let invalid = Err(PullFailure::ExecutionMetadataInvalid);
    let cases = [
        ("empty metadata", unbound.clone(), Vec::new(), Ok(())),
        ("unbound metadata", unbound.clone(), b"{}".to_vec(), Ok(())),
        (
            "metadata that doesn't parse",
            unbound.clone(),
            b"{not json".to_vec(),
            invalid,
        ),
        (
            "a bound schema with no execution",
            unbound.clone(),
            bound_without_execution,
            invalid,
        ),
        (
            "agreeing bound files",
            bound_manifest(initial(), false),
            bound_metadata(initial(), false),
            Ok(()),
        ),
        (
            "a metadata guest image, which the original's own pull refuses",
            bound_manifest(initial(), true),
            bound_metadata(initial(), true),
            invalid,
        ),
        (
            "a metadata guest image a legacy manifest lacks",
            unbound.clone(),
            bound_metadata(initial(), true),
            invalid,
        ),
        (
            "two valid executions that disagree",
            bound_manifest(execution(environment(1)), false),
            bound_metadata(execution(environment(2)), false),
            invalid,
        ),
        (
            "a bound manifest's guest image the metadata disagrees with",
            bound_manifest(initial(), true),
            bound_metadata(initial(), false),
            invalid,
        ),
        (
            "a manifest that doesn't parse",
            b"{not json".to_vec(),
            Vec::new(),
            Err(PullFailure::ManifestMalformed),
        ),
    ];
    for (case, manifest, metadata, expected) in cases {
        assert_eq!(
            read_shared_documents(&manifest, &metadata).map(drop),
            expected,
            "{case}"
        );
    }
}

/// A scoped tag lookup's malformed binding keeps an org-specific verdict for
/// prefetch, which flushes the template's pins on it, and is otherwise an
/// error.
#[test]
fn a_malformed_binding_keeps_only_an_org_specific_verdict_for_prefetch() {
    assert!(matches!(
        tag_resolve_outcome(Err(FindReleaseError::InvalidBinding { org_specific: true })),
        TagResolveOutcome::InvalidOrgSpecific
    ));
    assert!(matches!(
        tag_resolve_outcome(Err(FindReleaseError::InvalidBinding {
            org_specific: false
        })),
        TagResolveOutcome::Error
    ));
}
