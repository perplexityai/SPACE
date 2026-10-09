use super::{
    GuestKernelPin, Origin, PackageManager, Recipe, RecipeAdapterError, RecipeProtoError, Step,
    TomlBuildFrom, TomlRecipe, TomlStep,
};
use pplx_space_build_mgr_client::proto::recipe::copy;
use pplx_space_build_mgr_client::proto::{Recipe as ProtoRecipe, recipe};
use prost::{Message, Oneof};
use rstest::{fixture, rstest};
use std::path::Path;

#[test]
fn step_descriptions_do_not_disclose_recipe_inputs() {
    let secret = "BUILD_SECRET_CANARY";
    let cases = [
        (
            Step::Run {
                command: format!("echo '{secret}' >> /etc/environment"),
                user: Some(secret.into()),
            },
            "run",
        ),
        (
            Step::Env {
                vars: pplx_space_common_core::types::EnvMap::try_from(
                    std::collections::BTreeMap::from([(secret.into(), secret.into())]),
                )
                .unwrap(),
            },
            "env (1 vars)",
        ),
        (
            Step::PackageInstall {
                manager: PackageManager::Pip,
                packages: vec![secret.into()],
                extra_args: vec![secret.into()],
            },
            "pip-install (1 packages)",
        ),
        (
            Step::Copy {
                payload: super::CopyPayload::InlineBytes(secret.as_bytes().to_vec()),
                dest: secret.into(),
                source_path: Some(secret.into()),
            },
            "copy payload",
        ),
        (
            Step::PlaywrightInstall {
                browsers: vec![secret.into()],
            },
            "playwright-install (1 browsers)",
        ),
    ];
    for (step, expected) in cases {
        assert_eq!(step.to_string(), expected);
        let named = super::NamedStep {
            display_name: Some(secret.into()),
            env_refs: vec![pplx_space_common_core::types::EnvKey::parse(secret).unwrap()],
            step,
        };
        assert_eq!(named.to_string(), expected);
    }
}

/// The BPF guest kernel the shared seed pins; see `templates/firecracker/recipes/seed.toml`.
const SEED_KERNEL_VERSION: &str = "vmlinux-6.1.155-bpf-modsig";
const SEED_KERNEL_DIGEST: &str =
    "sha256:15ea4431a450eedbfcb4b4d3d85dd8e941b829a0606db6cd0e0167530421f933";

const KERNEL_VERSION: &str = "vmlinux-6.1.200-custom";
const KERNEL_DIGEST: &str =
    "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

#[fixture]
fn oci_python_smoke_recipe() -> TomlRecipe {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../..")
        .join("templates/firecracker/recipes/oci-python-smoke.toml");
    TomlRecipe::from_file(path.to_str().expect("UTF-8 recipe path"))
        .expect("OCI Python smoke recipe should parse")
}

// rustfmt resolves out-of-line modules regardless of `cfg`, so it must not look for this file
// in a checkout that lacks it; run-rust-checks.sh formats the file directly when present.

#[test]
fn build_from_defaults_to_template() {
    let recipe: TomlRecipe = toml::from_str(
        r#"
[metadata]
name = "code-runtime"
base = "seed"
"#,
    )
    .expect("recipe should parse");

    assert_eq!(recipe.metadata.build_from, TomlBuildFrom::Template);
}

#[test]
fn base_override_supplies_missing_template_base() {
    let recipe: TomlRecipe = toml::from_str(
        r#"
[metadata]
name = "code-runtime"
"#,
    )
    .expect("recipe should parse");

    let proto = recipe
        .try_into_proto(Some("seed-from-cli"), &mut |_index, _source| {
            unreachable!("no copy steps")
        })
        .expect("base override should supply template base");

    assert!(matches!(
        proto.origin.and_then(|origin| origin.kind),
        Some(recipe::origin::Kind::FromTemplate(from_template))
            if from_template.base == "seed-from-cli"
    ));
}

#[test]
fn unknown_scratch_source_type_is_reported_by_adapter() {
    let recipe: TomlRecipe = toml::from_str(
        r#"
[metadata]
name = "seed"
from = "scratch"

[source]
type = "mystery-rootfs"
release = "questing"
version = "25.10"
date = "release"
"#,
    )
    .expect("recipe should parse");

    let error = recipe
        .try_into_proto(None, &mut |_index, _source| unreachable!("no copy steps"))
        .expect_err("unknown source type should be rejected");
    assert!(matches!(
        error,
        RecipeAdapterError::UnknownSourceType { value } if value == "mystery-rootfs"
    ));
}

#[test]
fn scratch_recipe_requires_complete_source() {
    let err = toml::from_str::<TomlRecipe>(
        r#"
[metadata]
name = "seed"
from = "scratch"

[source]
release = "questing"
"#,
    );

    let message = match err {
        Ok(_) => panic!("recipe should reject incomplete scratch source"),
        Err(err) => err.to_string(),
    };
    assert!(message.contains("missing field"));
}

/// The shared seed pins the BPF guest kernel, so the Falcon sensor can attach
/// CO-RE instead of falling back to Reduced Functionality Mode. Pinning it here
/// rather than in a Falcon-specific seed keeps one lineage, which is the agreed
/// architecture, and makes staging a hard prerequisite: a node without this
/// exact kernel in its runtime inventory cannot cold-boot anything built from
/// this seed. Every cluster's worker and builder values must carry the matching
/// `additionalGuestKernel` entry before this pin changes.
#[test]
fn ordinary_seed_recipe_pins_the_bpf_guest_kernel() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../..")
        .join("templates/firecracker/recipes/seed.toml");
    let recipe = TomlRecipe::from_file(path.to_str().expect("UTF-8 recipe path"))
        .expect("seed recipe should parse")
        .try_into_proto(None, &mut |_index, _source| {
            unreachable!("seed has no copy steps")
        })
        .expect("seed recipe should adapt");
    let recipe = Recipe::try_from(recipe).expect("seed proto should convert");

    assert!(matches!(
        recipe.origin,
        Origin::FromScratch {
            guest_kernel: Some(GuestKernelPin { version, digest }),
            ..
        } if version.as_str() == SEED_KERNEL_VERSION && digest.to_string() == SEED_KERNEL_DIGEST
    ));
}

#[test]
fn scratch_guest_kernel_round_trips_from_toml() {
    let recipe: TomlRecipe = toml::from_str(&format!(
        r#"
[metadata]
name = "scratch-modsig-kernel"
from = "scratch"

[source]
type = "ubuntu-minimal-cloud-squashfs"
release = "resolute"
version = "26.04"
date = "release"

[guest_kernel]
version = "{KERNEL_VERSION}"
digest = "{KERNEL_DIGEST}"
"#
    ))
    .expect("pinned scratch recipe should parse");
    let recipe = recipe
        .try_into_proto(None, &mut |_index, _source| {
            unreachable!("recipe has no copy steps")
        })
        .expect("pinned scratch recipe should adapt");
    let recipe = Recipe::try_from(recipe).expect("pinned scratch proto should convert");

    assert!(matches!(
        recipe.origin,
        Origin::FromScratch {
            guest_kernel: Some(GuestKernelPin { version, digest }),
            ..
        } if version.as_str() == KERNEL_VERSION && digest.to_string() == KERNEL_DIGEST
    ));
}

#[rstest]
#[case(format!("version = \"{KERNEL_VERSION}\""))]
#[case(format!("digest = \"{KERNEL_DIGEST}\""))]
fn scratch_guest_kernel_requires_both_toml_fields(#[case] guest_kernel: String) {
    let recipe = format!(
        r#"
[metadata]
name = "scratch-modsig-kernel"
from = "scratch"

[source]
type = "ubuntu-minimal-cloud-squashfs"
release = "resolute"
version = "26.04"
date = "release"

[guest_kernel]
{guest_kernel}
"#
    );

    toml::from_str::<TomlRecipe>(&recipe).unwrap_err();
}

#[test]
fn scratch_recipe_rejects_misspelled_guest_kernel_table() {
    let recipe = format!(
        r#"
[metadata]
name = "scratch-modsig-kernel"
from = "scratch"

[source]
type = "ubuntu-minimal-cloud-squashfs"
release = "resolute"
version = "26.04"
date = "release"

[guest-kernel]
version = "{KERNEL_VERSION}"
digest = "{KERNEL_DIGEST}"
"#
    );

    toml::from_str::<TomlRecipe>(&recipe).unwrap_err();
}

#[test]
fn scratch_guest_kernel_rejects_partial_and_malformed_pins() {
    let mut missing_kernel = scratch_proto_with_kernel(KERNEL_VERSION, KERNEL_DIGEST);
    let Some(recipe::origin::Kind::PinnedFromScratch(from_scratch)) = missing_kernel
        .origin
        .as_mut()
        .and_then(|origin| origin.kind.as_mut())
    else {
        panic!("pinned scratch origin should be set");
    };
    from_scratch.guest_kernel = None;
    assert!(matches!(
        Recipe::try_from(missing_kernel),
        Err(RecipeProtoError::MissingScratchGuestKernel)
    ));

    let cases = [
        ("", KERNEL_DIGEST, true),
        ("bad/version", KERNEL_DIGEST, true),
        (KERNEL_VERSION, "", false),
        (KERNEL_VERSION, "sha256:not-a-full-digest", false),
    ];

    for (version, digest, version_error) in cases {
        let error = Recipe::try_from(scratch_proto_with_kernel(version, digest))
            .expect_err("invalid kernel pin should be rejected");
        assert_eq!(
            matches!(&error, RecipeProtoError::InvalidGuestKernelVersion { .. }),
            version_error,
            "unexpected validation error for version={version:?}, digest={digest:?}"
        );
        assert_eq!(
            matches!(&error, RecipeProtoError::InvalidGuestKernelDigest { .. }),
            !version_error,
            "unexpected validation error for version={version:?}, digest={digest:?}"
        );
    }
}

#[test]
fn pinned_scratch_guest_kernel_fails_closed_through_legacy_proxy() {
    let mut recipe = scratch_proto_with_kernel(KERNEL_VERSION, KERNEL_DIGEST);
    let origin = recipe.origin.take().expect("scratch origin should be set");
    recipe.origin = Some(legacy_origin_round_trip(origin));

    let error = Recipe::try_from(recipe)
        .expect_err("a legacy proxy must not turn a pinned build into an unpinned build");

    assert!(matches!(error, RecipeProtoError::MissingOriginKind));
}

/// Scratch source selectors reach a URL and a privileged build container, so
/// the shared conversion accepts only Ubuntu Minimal Cloud release names.
#[rstest]
#[case::current_seed("resolute", "26.04", "release-20260908", None)]
#[case::moving_release("resolute", "26.04", "release", None)]
#[case::older_lts_point_release("noble", "24.04.1", "release-20240423.1", None)]
#[case::empty_release("", "26.04", "release", Some("release"))]
#[case::uppercase_release("Resolute", "26.04", "release", Some("release"))]
#[case::release_path("resolute/../x", "26.04", "release", Some("release"))]
#[case::release_quote("resolute\"; id; echo \"", "26.04", "release", Some("release"))]
#[case::release_newline("resolute\nid", "26.04", "release", Some("release"))]
#[case::version_substitution("resolute", "$(id)", "release", Some("version"))]
#[case::version_trailing_space("resolute", "26.04 ", "release", Some("version"))]
#[case::version_one_part("resolute", "26", "release", Some("version"))]
#[case::version_four_parts("resolute", "26.04.1.2", "release", Some("version"))]
#[case::date_backticks("resolute", "26.04", "`id`", Some("date"))]
#[case::date_short("resolute", "26.04", "release-2026090", Some("date"))]
#[case::date_traversal("resolute", "26.04", "release/../../x", Some("date"))]
#[case::date_empty_serial("resolute", "26.04", "release-20260908.", Some("date"))]
#[case::date_unprefixed("resolute", "26.04", "20260908", Some("date"))]
fn scratch_source_selectors_are_validated(
    #[case] release: &str,
    #[case] version: &str,
    #[case] date: &str,
    #[case] invalid_field: Option<&str>,
) {
    let mut recipe = scratch_proto_with_kernel(KERNEL_VERSION, KERNEL_DIGEST);
    let Some(recipe::origin::Kind::PinnedFromScratch(recipe::origin::PinnedFromScratch {
        source:
            Some(recipe::Source {
                kind: Some(recipe::source::Kind::UbuntuMinimalCloudSquashfs(source)),
            }),
        ..
    })) = recipe
        .origin
        .as_mut()
        .and_then(|origin| origin.kind.as_mut())
    else {
        panic!("pinned scratch source should be set");
    };
    *source = recipe::source::UbuntuMinimalCloudSquashfs {
        release: release.to_string(),
        version: version.to_string(),
        date: date.to_string(),
    };

    let result = Recipe::try_from(recipe);

    match invalid_field {
        None => {
            let recipe = result.expect("supported scratch source should convert");
            assert!(matches!(
                recipe.origin,
                Origin::FromScratch { source, .. }
                    if source.release == release && source.version == version && source.date == date
            ));
        }
        Some(field) => {
            let error = result.expect_err("malformed scratch source should be rejected");
            assert!(
                matches!(&error, RecipeProtoError::InvalidScratchSource { field: actual, .. }
                    if actual.as_str() == field),
                "unexpected error for {field}: {error:?}"
            );
        }
    }
}

fn scratch_proto_with_kernel(version: &str, digest: &str) -> ProtoRecipe {
    ProtoRecipe {
        metadata: Some(recipe::Metadata { description: None }),
        build_config: Some(recipe::BuildConfig::default()),
        origin: Some(recipe::Origin {
            kind: Some(recipe::origin::Kind::PinnedFromScratch(
                recipe::origin::PinnedFromScratch {
                    source: Some(recipe::Source {
                        kind: Some(recipe::source::Kind::UbuntuMinimalCloudSquashfs(
                            recipe::source::UbuntuMinimalCloudSquashfs {
                                release: "resolute".to_string(),
                                version: "26.04".to_string(),
                                date: "release".to_string(),
                            },
                        )),
                    }),
                    guest_kernel: Some(recipe::origin::GuestKernel {
                        version: version.to_string(),
                        digest: digest.to_string(),
                    }),
                },
            )),
        }),
        steps: vec![],
    }
}

fn legacy_origin_round_trip(origin: recipe::Origin) -> recipe::Origin {
    let legacy = LegacyOrigin::decode(origin.encode_to_vec().as_slice())
        .expect("current origin should decode with the legacy schema");
    recipe::Origin::decode(legacy.encode_to_vec().as_slice())
        .expect("legacy origin should decode with the current schema")
}

#[derive(Clone, PartialEq, Message)]
struct LegacyOrigin {
    #[prost(oneof = "legacy_origin::Kind", tags = "1, 2, 3")]
    kind: Option<legacy_origin::Kind>,
}

mod legacy_origin {
    use super::*;

    #[derive(Clone, PartialEq, Oneof)]
    pub enum Kind {
        #[prost(bytes, tag = "1")]
        Template(Vec<u8>),
        #[prost(message, tag = "2")]
        Scratch(LegacyFromScratch),
        #[prost(bytes, tag = "3")]
        OciImage(Vec<u8>),
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct LegacyFromScratch {
        #[prost(bytes = "vec", optional, tag = "1")]
        source: Option<Vec<u8>>,
    }
}

#[rstest]
fn oci_python_smoke_recipe_adapts_to_seed_overlay(oci_python_smoke_recipe: TomlRecipe) {
    let proto = oci_python_smoke_recipe
        .try_into_proto(None, &mut |_index, _source| {
            unreachable!("smoke recipe has no copy steps")
        })
        .expect("OCI Python smoke recipe should adapt");

    assert!(matches!(
        proto.origin.and_then(|origin| origin.kind),
        Some(recipe::origin::Kind::FromOciImage(from_oci_image))
            if from_oci_image.base == "seed"
                && from_oci_image.reference
                    == "docker.io/library/python:3.12-slim-bookworm"
    ));
}

#[test]
fn bundled_firecracker_recipes_parse_and_adapt_to_proto() {
    for (path, recipe) in bundled_recipes() {
        let proto = recipe
            .try_into_proto(None, &mut |_index, _source| {
                Ok(copy::Payload::BlobSha256(
                    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".into(),
                ))
            })
            .unwrap_or_else(|err| panic!("adapt {} to proto: {err}", path.display()));
        Recipe::try_from(proto)
            .unwrap_or_else(|err| panic!("convert {} from proto: {err}", path.display()));
    }
}

/// A step that interpolates `$VAR` receives the value only if that same step
/// also names it in `env_refs`. Nothing rejects the mismatch: `TomlNamedStep`
/// does not deny unknown fields, so a misspelled `env_refs` key deserializes
/// to an empty list and the step runs with the variable unset. For a secret
/// that is the worst shape of failure — the build proceeds and bakes an
/// unconfigured artifact.
///
/// Host-supplied names come from two sources. Most are taken from the recipes
/// themselves — a name is host-supplied if some step somewhere declares it —
/// which avoids classifying `$VAR` occurrences by parsing shell, where loop
/// variables and local assignments are indistinguishable from host references
/// without implementing scoping rules. That rule alone is circular for a name
/// declared correctly nowhere, so `KNOWN_HOST_ENV` names the host variables
/// independently of any declaration and covers first use.
///
/// The misspelling half of the problem is caught structurally by
/// `bundled_recipe_steps_use_only_known_keys`, which rejects an `env_ref` or
/// `env-refs` key outright instead of relying on the name being known here.
#[test]
fn bundled_recipes_declare_every_host_env_var_their_steps_interpolate() {
    let recipes = bundled_recipes();
    let host_env: std::collections::BTreeSet<&str> = recipes
        .iter()
        .flat_map(|(_, recipe)| recipe.steps.iter())
        .flat_map(|step| step.env_refs.iter())
        .map(String::as_str)
        .chain(KNOWN_HOST_ENV.iter().copied())
        .collect();
    assert!(
        !host_env.is_empty(),
        "no recipe declares any env_refs; the guard is not exercising anything"
    );

    // Collected rather than panicking on the first hit, so one run reports every
    // undeclared reference instead of only the earliest.
    let mut undeclared = Vec::new();
    for (path, recipe) in &recipes {
        for (index, step) in recipe.steps.iter().enumerate() {
            let declared = |name: &str| step.env_refs.iter().any(|declared| declared == name);
            let location = |name: &str| {
                format!(
                    "{} step {index} ({}) interpolates ${name} without declaring {} in env_refs",
                    path.display(),
                    step.name.as_deref().unwrap_or("unnamed"),
                    name,
                )
            };

            for text in interpolating_fields(&step.step) {
                undeclared.extend(
                    host_env
                        .iter()
                        .copied()
                        .filter(|name| interpolates(text, name))
                        .filter(|name| !declared(name))
                        .map(location),
                );
            }
        }
    }
    assert!(
        undeclared.is_empty(),
        "these steps would run with the named variable unset:\n{}",
        undeclared.join("\n")
    );
}

/// Every step field whose `$VAR` references are expanded against the step
/// environment, and so depend on that step's `env_refs`.
///
/// `Run` commands are expanded by the guest shell from the environment
/// `ssh.exec` is given. `PipInstall` extra arguments are expanded by the builder
/// through `resolve_env_var_in_string`. `Env` values are copied literally and
/// the remaining variants carry no interpolated field, so neither is included.
fn interpolating_fields(step: &TomlStep) -> Vec<&str> {
    match step {
        TomlStep::Run { command, .. } => vec![command.as_str()],
        TomlStep::PipInstall { extra_args, .. } => extra_args.iter().map(String::as_str).collect(),
        TomlStep::AptInstall { .. }
        | TomlStep::NpmInstall { .. }
        | TomlStep::NpmInstallGlobal { .. }
        | TomlStep::Env { .. }
        | TomlStep::Copy { .. }
        | TomlStep::PlaywrightInstall { .. }
        | TomlStep::SetRuntime { .. }
        | TomlStep::ClearRuntime => Vec::new(),
    }
}

/// Host variables the builder passes into steps, named independently of any
/// `env_refs` declaration.
///
/// Without this, "host-supplied" would be defined only by what the recipes
/// already declare, which cannot cover a name whose sole declaration is the one
/// being checked: a new step interpolating `$SOME_TOKEN` with no correct
/// declaration anywhere would leave the name out of the set and pass. Extend
/// this when a recipe starts taking a new host variable; an entry no recipe
/// references yet is harmless and pre-covers its first use.
const KNOWN_HOST_ENV: &[&str] = &["EXAMPLE_HOST_INDEX_PASSWORD"];

/// Keys `TomlNamedStep` reads itself, whatever the step's `type`.
///
/// `TomlNamedStep` flattens `TomlStep`, and serde rejects
/// `deny_unknown_fields` on a struct with a flattened field, so a misspelled key
/// cannot be refused at deserialization: `env_ref = [...]` is silently dropped
/// and `env_refs` defaults to empty. Validating raw keys is the only place that
/// mismatch can be caught, and it catches every misspelling rather than only
/// those on names some other recipe happens to declare correctly.
const COMMON_STEP_KEYS: &[&str] = &["name", "env_refs", "type"];

/// The keys each `TomlStep` variant reads, by its `type` tag.
///
/// Per variant rather than one union of every field: serde drops a key that
/// belongs to a *different* variant exactly as silently as one that belongs to no
/// variant, so `user` on a `pip-install` step is the same defect with the same
/// invisible failure, and a union accepts it.
const VARIANT_STEP_KEYS: &[(&str, &[&str])] = &[
    ("apt-install", &["packages"]),
    ("pip-install", &["packages", "extra_args"]),
    ("npm-install", &["packages"]),
    ("npm-install-global", &["packages"]),
    ("run", &["command", "user"]),
    ("env", &["vars"]),
    ("copy", &["source", "dest"]),
    ("playwright-install", &["browsers"]),
    ("set-runtime", &["declaration"]),
    ("clear-runtime", &[]),
];

/// Keys in `table` that `TomlNamedStep` would drop, judged against the step's own
/// `type` rather than against every type at once.
///
/// Fails closed on the type as well as the keys: a missing, non-string, or
/// unrecognized `type` is reported rather than skipped, so a new `TomlStep`
/// variant fails this guard until it is listed above.
fn unknown_step_keys(table: &toml::Table) -> Vec<String> {
    let Some(step_type) = table.get("type").and_then(toml::Value::as_str) else {
        return vec!["missing or non-string `type`".to_string()];
    };
    let Some((_, variant_keys)) = VARIANT_STEP_KEYS
        .iter()
        .find(|(variant, _)| *variant == step_type)
    else {
        return vec![format!("unrecognized step type {step_type:?}")];
    };
    table
        .keys()
        .filter(|key| {
            !COMMON_STEP_KEYS.contains(&key.as_str()) && !variant_keys.contains(&key.as_str())
        })
        .map(|key| format!("key {key:?} is not read by a {step_type:?} step"))
        .collect()
}

fn step_table(body: &str) -> toml::Table {
    body.parse::<toml::Table>()
        .expect("step table should parse")
}

#[test]
fn step_key_guard_rejects_a_key_belonging_to_another_variant() {
    for (label, body, stray) in [
        (
            "user on pip-install",
            "type = \"pip-install\"\npackages = []\nuser = \"root\"\n",
            "user",
        ),
        (
            "extra_args on run",
            "type = \"run\"\ncommand = \"true\"\nextra_args = []\n",
            "extra_args",
        ),
    ] {
        let table = step_table(body);

        // The premise, proven rather than asserted: serde accepts the table and
        // silently drops the stray key, so nothing else in the pipeline reports it.
        toml::Value::Table(table.clone())
            .try_into::<super::TomlNamedStep>()
            .unwrap_or_else(|err| panic!("{label}: step should deserialize: {err}"));

        let reported = unknown_step_keys(&table);
        assert!(
            reported.iter().any(|entry| entry.contains(stray)),
            "{label}: serde drops {stray:?} for this step type, so the guard has to \
             report it; got {reported:?}"
        );
    }
}

#[test]
fn step_key_guard_accepts_each_variant_with_its_own_keys() {
    for (step_type, keys) in VARIANT_STEP_KEYS {
        // Value type is irrelevant here: the guard reads key names only.
        let mut body = format!("type = \"{step_type}\"\nname = \"x\"\nenv_refs = []\n");
        for key in *keys {
            body.push_str(&format!("{key} = \"x\"\n"));
        }

        let reported = unknown_step_keys(&step_table(&body));
        assert!(
            reported.is_empty(),
            "{step_type}: its own keys must pass; got {reported:?}"
        );
    }
}

#[test]
fn step_key_guard_reports_an_unusable_type() {
    for (label, body) in [
        ("absent", "command = \"true\"\n"),
        ("non-string", "type = 7\n"),
        ("unrecognized", "type = \"rsync\"\n"),
    ] {
        assert!(
            !unknown_step_keys(&step_table(body)).is_empty(),
            "{label} `type`: the guard cannot judge the keys, so it must fail closed"
        );
    }
}

#[test]
fn bundled_recipe_steps_use_only_known_keys() {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../..")
        .canonicalize()
        .expect("repo root should resolve");
    let recipe_dir = repo_root.join("templates/firecracker/recipes");
    let mut unknown = Vec::new();
    let mut checked = 0usize;
    for entry in std::fs::read_dir(&recipe_dir)
        .unwrap_or_else(|err| panic!("read {}: {err}", recipe_dir.display()))
    {
        let path = entry.expect("recipe dir entry should read").path();
        if path.extension().is_none_or(|ext| ext != "toml") {
            continue;
        }
        let contents = std::fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
        let document = contents
            .parse::<toml::Table>()
            .unwrap_or_else(|err| panic!("parse {}: {err}", path.display()));
        let Some(steps) = document.get("steps").and_then(toml::Value::as_array) else {
            continue;
        };
        for (index, step) in steps.iter().enumerate() {
            let Some(table) = step.as_table() else {
                continue;
            };
            checked += 1;
            unknown.extend(
                unknown_step_keys(table)
                    .into_iter()
                    .map(|entry| format!("{} step {index}: {entry}", path.display())),
            );
        }
    }
    assert!(
        checked > 0,
        "no step tables found; the guard checks nothing"
    );
    assert!(
        unknown.is_empty(),
        "step keys outside the known set are dropped silently, so a misspelled \
         env_refs would leave its variable unset:\n{}",
        unknown.join("\n")
    );
}

/// The single definition of recipe discovery for these guards.
///
/// Enumerated rather than listed by name: a recipe reaches a build node only by
/// being checked in, so a hard-coded list silently exempts every new recipe until
/// someone remembers to extend it. Every guard reads this so a change to what
/// counts as a recipe cannot make one of them diverge from the others.
fn bundled_recipes() -> Vec<(std::path::PathBuf, TomlRecipe)> {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../..")
        .canonicalize()
        .expect("repo root should resolve");
    let recipe_dir = repo_root.join("templates/firecracker/recipes");
    let mut recipes: Vec<_> = std::fs::read_dir(&recipe_dir)
        .unwrap_or_else(|err| panic!("read {}: {err}", recipe_dir.display()))
        .map(|entry| entry.expect("recipe dir entry should read").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "toml"))
        .map(|path| {
            let contents = std::fs::read_to_string(&path)
                .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
            let recipe = toml::from_str::<TomlRecipe>(&contents)
                .unwrap_or_else(|err| panic!("parse {}: {err}", path.display()));
            (path, recipe)
        })
        .collect();
    recipes.sort_by(|left, right| left.0.cmp(&right.0));
    assert!(!recipes.is_empty(), "no bundled recipes found");
    recipes
}

/// Whether `command` expands `name` as `$name` or `${name}`, requiring the
/// full name to match so `$FOO` does not count as a use of `$FOO_BAR`.
fn interpolates(command: &str, name: &str) -> bool {
    // `${NAME}` is self-delimiting, so a substring match settles it.
    if command.contains(&format!("${{{name}}}")) {
        return true;
    }
    // `$NAME` needs the following character checked, otherwise `$FOO_BAR` would
    // count as a use of `$FOO`. `get` rather than a slice: the index is a char
    // boundary here, but slicing a `str` by byte index panics if it ever is not.
    let bare = format!("${name}");
    command.match_indices(bare.as_str()).any(|(start, _)| {
        command
            .get(start + bare.len()..)
            .and_then(|rest| rest.chars().next())
            .is_none_or(|next| !next.is_ascii_alphanumeric() && next != '_')
    })
}

#[test]
fn proto_origin_controls_template_base() {
    let recipe = Recipe::try_from(ProtoRecipe {
        metadata: Some(recipe::Metadata { description: None }),
        build_config: Some(recipe::BuildConfig::default()),
        origin: Some(recipe::Origin {
            kind: Some(recipe::origin::Kind::FromTemplate(
                recipe::origin::FromTemplate {
                    base: "seed".to_string(),
                },
            )),
        }),
        steps: vec![],
    })
    .expect("proto recipe should convert");

    assert!(matches!(
        recipe.origin,
        Origin::FromTemplate { base, base_raw }
            if base.to_string() == "seed:latest" && base_raw == "seed"
    ));
}

#[test]
fn proto_origin_accepts_exact_template_base() {
    let hash = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let recipe = Recipe::try_from(ProtoRecipe {
        metadata: Some(recipe::Metadata { description: None }),
        build_config: Some(recipe::BuildConfig::default()),
        origin: Some(recipe::Origin {
            kind: Some(recipe::origin::Kind::FromTemplate(
                recipe::origin::FromTemplate {
                    base: format!("code-runtime@{hash}"),
                },
            )),
        }),
        steps: vec![],
    })
    .expect("proto recipe should convert");

    assert!(matches!(
        recipe.origin,
        Origin::FromTemplate { base, base_raw }
            if base.name().as_str() == "code-runtime"
                && base_raw == format!("code-runtime@{hash}")
                && base.artifact_digest().is_some_and(|digest| digest.content_hash().to_string() == hash)
    ));
}

#[test]
fn proto_origin_rejects_template_base_digest_prefix() {
    let error = Recipe::try_from(ProtoRecipe {
        metadata: Some(recipe::Metadata { description: None }),
        build_config: Some(recipe::BuildConfig::default()),
        origin: Some(recipe::Origin {
            kind: Some(recipe::origin::Kind::FromTemplate(
                recipe::origin::FromTemplate {
                    base: "code-runtime@sha256:0123456789abcdef".to_string(),
                },
            )),
        }),
        steps: vec![],
    })
    .expect_err("digest prefixes are not accepted as build bases");

    assert!(matches!(
        error,
        RecipeProtoError::BaseDigestPrefix { value }
            if value == "code-runtime@sha256:0123456789abcdef"
    ));
}

#[test]
fn proto_run_step_preserves_absent_user() {
    let recipe = Recipe::try_from(ProtoRecipe {
        metadata: Some(recipe::Metadata { description: None }),
        build_config: Some(recipe::BuildConfig::default()),
        origin: Some(recipe::Origin {
            kind: Some(recipe::origin::Kind::FromTemplate(
                recipe::origin::FromTemplate {
                    base: "seed".to_string(),
                },
            )),
        }),
        steps: vec![recipe::Step {
            display_name: None,
            env_refs: vec![],
            body: Some(recipe::step::Body::Run(recipe::Run {
                command: "true".to_string(),
                user: None,
            })),
        }],
    })
    .expect("proto recipe should convert");

    assert!(matches!(
        &recipe.steps[0].step,
        Step::Run { user, .. } if user.is_none()
    ));
}

#[test]
fn proto_copy_step_requires_absolute_dest_and_valid_blob() {
    let recipe = ProtoRecipe {
        metadata: Some(recipe::Metadata { description: None }),
        build_config: Some(recipe::BuildConfig::default()),
        origin: Some(recipe::Origin {
            kind: Some(recipe::origin::Kind::FromTemplate(
                recipe::origin::FromTemplate {
                    base: "seed".to_string(),
                },
            )),
        }),
        steps: vec![recipe::Step {
            display_name: None,
            env_refs: vec![],
            body: Some(recipe::step::Body::Copy(recipe::Copy {
                payload: Some(copy::Payload::BlobSha256("not-a-sha".to_string())),
                dest: "tmp/file".to_string(),
                source_path: None,
            })),
        }],
    };

    assert!(matches!(
        Recipe::try_from(recipe),
        Err(RecipeProtoError::RelativeCopyDest { index: 1 })
    ));

    let recipe = ProtoRecipe {
        metadata: Some(recipe::Metadata { description: None }),
        build_config: Some(recipe::BuildConfig::default()),
        origin: Some(recipe::Origin {
            kind: Some(recipe::origin::Kind::FromTemplate(
                recipe::origin::FromTemplate {
                    base: "seed".to_string(),
                },
            )),
        }),
        steps: vec![recipe::Step {
            display_name: None,
            env_refs: vec![],
            body: Some(recipe::step::Body::Copy(recipe::Copy {
                payload: Some(copy::Payload::BlobSha256("not-a-sha".to_string())),
                dest: "/tmp/file".to_string(),
                source_path: None,
            })),
        }],
    };

    assert!(matches!(
        Recipe::try_from(recipe),
        Err(RecipeProtoError::InvalidBlobSha256 { index: 1 })
    ));
}

#[test]
fn package_install_uses_single_structural_step() {
    let recipe = Recipe::try_from(ProtoRecipe {
        metadata: Some(recipe::Metadata { description: None }),
        build_config: Some(recipe::BuildConfig::default()),
        origin: Some(recipe::Origin {
            kind: Some(recipe::origin::Kind::FromTemplate(
                recipe::origin::FromTemplate {
                    base: "seed".to_string(),
                },
            )),
        }),
        steps: vec![recipe::Step {
            display_name: Some("install".to_string()),
            env_refs: vec!["TOKEN".to_string()],
            body: Some(recipe::step::Body::PackageInstall(recipe::PackageInstall {
                manager: recipe::PackageManager::Pip as i32,
                packages: vec!["foo".to_string()],
                extra_args: vec!["--extra-index-url".to_string()],
            })),
        }],
    })
    .expect("proto recipe should convert");

    assert!(matches!(
        &recipe.steps[0].step,
        Step::PackageInstall {
            manager: PackageManager::Pip,
            packages,
            extra_args
        } if packages == &["foo"] && extra_args == &["--extra-index-url"]
    ));
    assert_eq!(
        recipe.steps[0].env_refs,
        vec![pplx_space_common_core::types::EnvKey::parse("TOKEN").unwrap()]
    );
}

#[test]
fn resolves_env_refs_for_toml_env_values() {
    let resolved =
        super::resolve_env_refs_with("https://user:$TOKEN@$HOST/path", |name| match name {
            "TOKEN" => "secret".to_string(),
            "HOST" => "example.test".to_string(),
            _ => String::new(),
        });

    assert_eq!(resolved, "https://user:secret@example.test/path");
}

#[test]
fn runtime_recipe_round_trip_and_fail_closed_policy() {
    let input = r#"
[metadata]
name = "native-runtime-example"
base = "seed"
[[steps]]
type = "set-runtime"
[steps.declaration]
schema_version = 1
[[steps.declaration.apps]]
name = "app"
command = ["/bin/sleep", "infinity"]
user = "root"
working_directory = "/"
startup_timeout_secs = 10
[steps.declaration.apps.env]
EXAMPLE_MODE = "template"
[steps.declaration.apps.readiness]
type = "launch"
[[steps.declaration.apps]]
name = "worker"
command = ["/usr/local/bin/worker"]
user = "root"
working_directory = "/"
startup_timeout_secs = 20
[steps.declaration.apps.readiness]
type = "launch"
"#;
    let recipe: TomlRecipe = toml::from_str(input).unwrap();
    let wire = recipe
        .into_proto("example.toml", None, |_, _| unreachable!())
        .unwrap();
    let native = Recipe::try_from(wire.clone()).unwrap();
    let Step::SetRuntime(runtime) = &native.steps[0].step else {
        panic!("runtime step missing")
    };
    assert_eq!(
        runtime.apps()[0].command().argv(),
        ["/bin/sleep", "infinity"]
    );
    assert_eq!(runtime.apps()[0].startup_timeout().as_secs(), 10);
    assert_eq!(runtime.apps().len(), 2);
    assert_eq!(runtime.apps()[1].name().as_str(), "worker");
    assert_eq!(
        runtime.apps()[1].command().argv(),
        ["/usr/local/bin/worker"]
    );
    assert_eq!(runtime.apps()[1].startup_timeout().as_secs(), 20);
    assert!(!format!("{:?}", wire.steps[0]).contains("EXAMPLE_MODE"));

    for steps in [
        vec![wire.steps[0].clone(), wire.steps[0].clone()],
        vec![
            wire.steps[0].clone(),
            recipe::Step {
                body: Some(recipe::step::Body::Run(recipe::Run {
                    command: "true".into(),
                    user: None,
                })),
                ..Default::default()
            },
        ],
        vec![recipe::Step {
            env_refs: vec!["EXAMPLE_MODE".into()],
            ..wire.steps[0].clone()
        }],
    ] {
        assert!(matches!(
            Recipe::try_from(ProtoRecipe {
                steps,
                ..wire.clone()
            }),
            Err(RecipeProtoError::RuntimePlacement)
        ));
    }
    // An older protobuf intermediary retains Step but drops the unknown body.
    #[derive(Clone, PartialEq, Message)]
    struct OldStep {
        #[prost(string, optional, tag = "1")]
        display_name: Option<String>,
    }
    for body in [
        wire.steps[0].body.clone().unwrap(),
        recipe::step::Body::ClearRuntime(recipe::ClearRuntime {}),
    ] {
        let step = recipe::Step {
            body: Some(body),
            ..Default::default()
        };
        let old = OldStep::decode(step.encode_to_vec().as_slice()).unwrap();
        let dropped = recipe::Step::decode(old.encode_to_vec().as_slice()).unwrap();
        assert!(matches!(
            Recipe::try_from(ProtoRecipe {
                steps: vec![dropped],
                ..wire.clone()
            }),
            Err(RecipeProtoError::MissingStep { index: 1 })
        ));
    }
    let Some(recipe::step::Body::SetRuntime(mut runtime)) = wire.steps[0].body.clone() else {
        unreachable!()
    };
    runtime.apps[0].readiness = None;
    pplx_space_build_mgr_client::runtime::decode(runtime).unwrap_err();
}

#[test]
fn proto_steps_validate_environment_names_before_execution() {
    use super::ProtoStepExt;

    for step in [
        recipe::Step {
            env_refs: vec!["TOKEN; echo injected".into()],
            body: Some(recipe::step::Body::Run(recipe::Run {
                command: "true".into(),
                user: None,
            })),
            ..Default::default()
        },
        recipe::Step {
            body: Some(recipe::step::Body::Env(recipe::Env {
                vars: [("TOKEN'".into(), "secret-value".into())].into(),
            })),
            ..Default::default()
        },
    ] {
        let result = step.try_into_named_step(1);

        let error = result.unwrap_err();
        assert!(matches!(
            error,
            RecipeProtoError::Environment { index: 1, .. }
        ));
        assert!(error.to_string().contains("recipe step 1 env"));
        assert!(!format!("{error:?}").contains("secret-value"));
        assert!(!format!("{error:?}").contains("TOKEN"));
    }
}
