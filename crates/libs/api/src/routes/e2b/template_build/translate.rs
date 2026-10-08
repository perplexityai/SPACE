//! e2b instruction list -> SPACE Recipe-proto translator.
//!
//! Mirrors the five e2b `InstructionType`s the Python SDK emits (see
//! `e2b/template/types.py`): `RUN`, `ENV`, `WORKDIR`, `USER`, `COPY`.
//!
//! Translation strategy:
//!
//! * `RUN [cmd, user]` -> `Recipe.Step.Run { cmd, user }`. WORKDIR is sticky
//!   (e2b matches Docker), so if a previous WORKDIR set `current_workdir`
//!   we wrap the command in `cd <wd> && (<cmd>)`. `user` falls back to
//!   `current_user`, which tracks the most recent USER step.
//! * `ENV [k1, v1, k2, v2, ...]` -> a `Run` step that appends
//!   `k=v` lines to `/etc/environment`. e2b's contract requires ENV to
//!   persist into the runtime sandbox,
//!   which the SPACE `Recipe.Env` step type doesn't do — it only
//!   propagates env into subsequent *build* steps' shells.
//! * `WORKDIR [path]` -> `Run { mkdir -p path, root }` plus update
//!   `current_workdir`.
//! * `USER [name]` -> no recipe step; updates `current_user` for the
//!   subsequent RUNs.
//! * `COPY [src, dest, user, mode]` with `filesHash` -> `Recipe.Step.Copy
//!   { blob_sha256, dest }`. The blob_sha256 is resolved from the upload
//!   state populated when the SDK PUTs the tar to our file-upload
//!   endpoint.
//!
//! Plus two cross-cutting fields on `TemplateBuildStartV2`:
//!
//! * `startCmd` and `readyCmd` lower to native SetRuntime intent. The
//!   guest owns startup and readiness; this adapter never writes unit text.
//!   ENV becomes template defaults. Startup retains SPACE's root, `/`,
//!   `/bin/sh -lc` defaults. Readiness is enforced at boot/snapshot creation,
//!   not before publishing the built rootfs.
//!
//! Base-template alias: `code-interpreter-v1` (the E2B base)
//! becomes `code-runtime` before the Recipe is forwarded. Template selectors
//! are preserved unless an alias would move an exact digest into another
//! template namespace.

use std::{collections::BTreeMap, str::FromStr};

use oci_spec::distribution::Reference as OciReference;
use pplx_space_build_mgr_client::proto as build_proto;
use pplx_space_common_core::types::{OrgId, TemplateName, TemplateReference, TemplateSelector};

use super::state::TemplateBuildState;
use super::wire::{TemplateBuildStartV2, TemplateStep};
use crate::e2b::errors::E2bApiError;

/// `code-interpreter-v1` (the E2B base)
/// has no SPACE equivalent template registered; the closest analogue is
/// SPACE's `code-runtime` (Ubuntu 26.04 base built from `seed`). Aliasing
/// in the translator means callers don't have to fork their per-provider
/// template-authoring code.
const BASE_TEMPLATE_ALIASES: &[(&str, &str)] = &[("code-interpreter-v1", "code-runtime")];

/// Per-base hardcoded initial `current_user` / `current_workdir` so that
/// a downstream `fromTemplate` build inherits the same identity defaults
/// the base would have set with `.set_user(...).set_workdir(...)` in a
/// real Docker-style image, mirroring upstream e2b's
/// `code-interpreter-v1` (`USER user`, `WORKDIR /home/user`).
///
/// Why hardcoded: the SPACE Recipe proto's `FromTemplate { base }` carries
/// only the base reference, not the base's final user/workdir. The
/// translator does not (yet) call into template-manager to read the
/// parent recipe back, so we encode the small, stable set of bases here.
/// Both the alias name (`code-interpreter-v1`) and the post-alias name
/// (`code-runtime`) are listed because callers can request either.
///
/// Unknown bases (including `seed`, custom user templates) fall back to
/// `root` / no-workdir, matching the prior behavior — this only adjusts
/// the user-facing baselines that previously diverged from E2B.
const BASE_TEMPLATE_DEFAULTS: &[(&str, &str, &str)] = &[
    ("code-runtime", "user", "/home/user"),
    ("code-interpreter-v1", "user", "/home/user"),
];

fn base_template_defaults(base: &str) -> (String, Option<String>) {
    BASE_TEMPLATE_DEFAULTS
        .iter()
        .find_map(|(name, user, workdir)| {
            (*name == base).then(|| ((*user).to_string(), Some((*workdir).to_string())))
        })
        .unwrap_or_else(|| (String::from("root"), None))
}

/// Disk sizes affect both the build VM and the template's runtime metadata.
/// The SDK has no disk-size field, so deployments supply exact-name or trailing-`*`
/// prefix rules as a JSON object. Unconfigured templates use build-manager defaults.
const TEMPLATE_BUILD_DISK_MIB_ENV: &str = "SPACE_TEMPLATE_BUILD_DISK_MIB";
const MIB: u64 = 1024 * 1024;

pub(super) async fn build_recipe(
    org_id: OrgId,
    template_name: &TemplateName,
    req: &TemplateBuildStartV2,
    state: &TemplateBuildState,
    cpu_count: Option<u32>,
    memory_mb: Option<u32>,
) -> Result<build_proto::Recipe, E2bApiError> {
    let from_template = req
        .from_template
        .as_deref()
        .filter(|value| !value.is_empty());
    let from_image = req.from_image.as_deref().filter(|value| !value.is_empty());
    if from_image.is_some() && from_template.is_some() {
        return Err(E2bApiError::bad_request(
            "fromImage and fromTemplate are mutually exclusive",
        ));
    }
    if req.from_image_registry.is_some() {
        return Err(E2bApiError::bad_request(
            "fromImageRegistry credentials are not supported",
        ));
    }

    let (origin, mut current_user, mut current_workdir) = if let Some(image) = from_image {
        let image = resolve_oci_image(image)?;
        (
            build_proto::recipe::origin::Kind::FromOciImage(
                build_proto::recipe::origin::FromOciImage {
                    base: "seed".to_string(),
                    reference: image.to_string(),
                },
            ),
            String::from("root"),
            None,
        )
    } else {
        let base = from_template.unwrap_or("seed");
        let resolved_base = resolve_base_template(base)?;
        let (initial_user, initial_workdir) = base_template_defaults(resolved_base.name().as_str());
        (
            build_proto::recipe::origin::Kind::FromTemplate(
                build_proto::recipe::origin::FromTemplate {
                    base: resolved_base.to_string(),
                },
            ),
            initial_user,
            initial_workdir,
        )
    };

    let mut steps = Vec::new();
    for step in &req.steps {
        translate_step(
            step,
            org_id,
            template_name,
            state,
            &mut current_user,
            &mut current_workdir,
            &mut steps,
        )
        .await?;
    }

    if let Some(runtime) = runtime_step(req)? {
        steps.push(runtime);
    }

    Ok(build_proto::Recipe {
        metadata: Some(build_proto::recipe::Metadata { description: None }),
        build_config: Some(build_proto::recipe::BuildConfig {
            vcpu: cpu_count,
            memory_bytes: memory_mb.map(|mb| u64::from(mb) * MIB),
            disk_bytes: default_disk_mib(template_name.as_str())?.map(|disk_mib| disk_mib * MIB),
            balloon: None,
        }),
        origin: Some(build_proto::recipe::Origin { kind: Some(origin) }),
        steps,
    })
}

fn resolve_oci_image(raw: &str) -> Result<OciReference, E2bApiError> {
    OciReference::from_str(raw).map_err(|_| E2bApiError::bad_request("invalid fromImage reference"))
}

fn apply_base_alias(raw: &str) -> &str {
    BASE_TEMPLATE_ALIASES
        .iter()
        .find_map(|(from, to)| (*from == raw).then_some(*to))
        .unwrap_or(raw)
}

fn resolve_base_template(raw: &str) -> Result<TemplateReference, E2bApiError> {
    let reference = TemplateReference::from_str(raw).map_err(|error| {
        E2bApiError::bad_request(format!("invalid fromTemplate {raw:?}: {error}"))
    })?;
    let raw_name = reference.name().as_str();
    let mapped_name = apply_base_alias(raw_name);
    if mapped_name != raw_name
        && matches!(
            reference.selector(),
            TemplateSelector::ArtifactDigest(_) | TemplateSelector::ArtifactDigestPrefix(_)
        )
    {
        return Err(E2bApiError::bad_request(format!(
            "fromTemplate {raw:?} cannot combine alias {raw_name:?} with an artifact digest selector; \
             digest selectors are scoped to the template name, so use {mapped_name}:latest or an exact \
             {mapped_name}@sha256:... reference"
        )));
    }
    let name = TemplateName::parse(mapped_name).map_err(|error| {
        E2bApiError::internal(format!(
            "invalid base template alias target {mapped_name:?}: {error}"
        ))
    })?;
    // Existence is checked by build-manager through template-manager GetTemplate; this translator
    // only normalizes the e2b-facing alias and rejects shapes that would be unresolvable later.
    Ok(TemplateReference::new(name, reference.selector().clone()))
}

fn default_disk_mib(template_id: &str) -> Result<Option<u64>, E2bApiError> {
    disk_mib_with_override(
        template_id,
        None,
        std::env::var(TEMPLATE_BUILD_DISK_MIB_ENV).ok().as_deref(),
    )
}

fn disk_mib_with_override(
    template_id: &str,
    override_mib: Option<&str>,
    template_disks: Option<&str>,
) -> Result<Option<u64>, E2bApiError> {
    if let Some(override_mib) = override_mib
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|v| *v > 0)
    {
        validate_disk_mib(override_mib)?;
        return Ok(Some(override_mib));
    }

    let rules: BTreeMap<String, Option<u64>> =
        serde_json::from_str(template_disks.unwrap_or("{}")).map_err(|_| {
            E2bApiError::internal(format!("{TEMPLATE_BUILD_DISK_MIB_ENV} must be a JSON object of template names or prefixes ending in * to MiB sizes or null"))
        })?;
    for (pattern, disk_mib) in &rules {
        TemplateName::parse(pattern.strip_suffix('*').unwrap_or(pattern)).map_err(|_| {
            E2bApiError::internal(format!(
                "invalid template pattern in {TEMPLATE_BUILD_DISK_MIB_ENV}"
            ))
        })?;
        if let Some(disk_mib) = disk_mib {
            validate_disk_mib(*disk_mib)?;
        }
    }
    // Exact names (including explicit nulls) take precedence over family rules.
    if let Some(disk_mib) = rules.get(template_id) {
        return Ok(*disk_mib);
    }
    Ok(rules
        .iter()
        .filter_map(|(pattern, disk_mib)| {
            let prefix = pattern.strip_suffix('*')?;
            template_id
                .starts_with(prefix)
                .then_some((prefix.len(), *disk_mib))
        })
        .max_by_key(|(len, _)| *len)
        .and_then(|(_, disk_mib)| disk_mib))
}

fn validate_disk_mib(disk_mib: u64) -> Result<(), E2bApiError> {
    if disk_mib == 0 || disk_mib.checked_mul(MIB).is_none() {
        return Err(E2bApiError::internal(
            "template build disk MiB must be positive and fit in u64 bytes",
        ));
    }
    Ok(())
}

async fn translate_step(
    step: &TemplateStep,
    org_id: OrgId,
    template_name: &TemplateName,
    state: &TemplateBuildState,
    current_user: &mut String,
    current_workdir: &mut Option<String>,
    steps: &mut Vec<build_proto::recipe::Step>,
) -> Result<(), E2bApiError> {
    match step.kind.to_ascii_uppercase().as_str() {
        "RUN" => {
            // e2b SDK shape: args = [command, user]. `user` is empty when
            // the caller didn't override the current_user.
            let command = step.args.first().ok_or_else(|| {
                E2bApiError::bad_request("RUN step requires at least one arg (the command)")
            })?;
            let user = step
                .args
                .get(1)
                .filter(|s| !s.is_empty())
                .cloned()
                .unwrap_or_else(|| current_user.clone());
            let command = match current_workdir {
                Some(wd) => run_in_workdir(command, wd),
                None => command.clone(),
            };
            steps.push(run_step(&command, &user));
        }
        "ENV" => {
            let pairs = parse_env_args(&step.args)?;
            let mut command = String::new();
            for (idx, (k, v)) in pairs.iter().enumerate() {
                if idx > 0 {
                    command.push_str(" && ");
                }
                use std::fmt::Write as _;
                write!(
                    command,
                    "echo {} >> /etc/environment",
                    shell_quote(&format!("{k}={v}"))
                )
                .expect("writing to String cannot fail");
            }
            steps.push(run_step(&command, "root"));
        }
        "WORKDIR" => {
            let path = step
                .args
                .first()
                .ok_or_else(|| E2bApiError::bad_request("WORKDIR step requires exactly 1 arg"))?;
            steps.push(run_step(&format!("mkdir -p {}", shell_quote(path)), "root"));
            *current_workdir = Some(path.clone());
        }
        "USER" => {
            let name = step
                .args
                .first()
                .ok_or_else(|| E2bApiError::bad_request("USER step requires exactly 1 arg"))?;
            *current_user = name.clone();
        }
        "COPY" => {
            // e2b SDK shape: args = [src, dest, user, mode]. user and
            // mode can be empty strings; SDK pads `mode` to octal.
            let source_path = step.args.first().cloned().ok_or_else(|| {
                E2bApiError::bad_request("COPY step requires args=[src, dest, ...]")
            })?;
            let raw_dest = step.args.get(1).cloned().ok_or_else(|| {
                E2bApiError::bad_request("COPY step requires args=[src, dest, ...]")
            })?;
            let user_arg = step.args.get(2).cloned().unwrap_or_default();
            let mode_arg = step.args.get(3).cloned().unwrap_or_default();
            // Vendored e2b resolves relative COPY destinations against
            // the current WORKDIR; when no WORKDIR has been declared,
            // `copy_script.sh:11` falls back to `cmdMetadata.User`'s
            // $HOME — i.e. the *command* user (the one set by the most
            // recent USER step / SDK set_user), NOT the COPY-owner
            // (`args[2]`). Mirror that: `current_user` is the right
            // fallback owner. SPACE's daemon Recipe.Copy validator
            // rejects non-absolute dest, so we have to resolve here.
            let dest = if raw_dest.starts_with('/') {
                raw_dest
            } else {
                let workdir = current_workdir
                    .as_deref()
                    .map(|wd| wd.trim_end_matches('/').to_string())
                    .unwrap_or_else(|| home_dir_for_user(current_user));
                format!("{workdir}/{raw_dest}")
            };
            let files_hash = step.files_hash.as_deref().ok_or_else(|| {
                E2bApiError::bad_request(
                    "COPY step is missing filesHash; the SDK should have uploaded the tar \
                     and populated filesHash before triggering the build",
                )
            })?;
            let blob_sha256 = state
                .lookup_blob(org_id, template_name, files_hash)
                .await
                .map_err(|err| super::template_build_sql_error("lookup template build blob", &err))?
                .ok_or_else(|| {
                    E2bApiError::bad_request(format!(
                        "COPY references filesHash={files_hash:?} which space-api never \
                         received an upload for. Did the SDK skip the PUT?"
                    ))
                })?;
            steps.push(copy_blob_step(
                files_hash,
                blob_sha256,
                source_path,
                dest.clone(),
            ));
            // build-mgr's tar extractor calls `archive.set_preserve_ownerships(false)`
            // and `set_preserve_permissions(false)`;
            // the daemon scp's the extracted entry in as root and ignores
            // any per-file metadata. e2b SDK callers passing `user`/`mode`
            // expect those to land — emit follow-up chown/chmod steps as
            // root so the SDK's contract holds. Empty user/mode are no-ops.
            //
            // Ownership parsing matches vendored e2b
            // (infra/self-hosted/e2b/aws/infra/packages/orchestrator/internal/template/build/commands/copy.go:196):
            // `user:group` is used verbatim; a bare `user` expands to
            // `user:user`. The daemon already produces root-owned files,
            // so an empty `user` is a no-op (skip the chown step entirely).
            // No `id -u` guard — vendored e2b doesn't have one and an
            // unknown owner should fail the build step the same way it
            // would under their copy command.
            if !user_arg.is_empty() {
                let owner = expand_copy_owner(&user_arg);
                let cmd = format!("chown -R {} {}", shell_quote(&owner), shell_quote(&dest),);
                steps.push(run_step(&cmd, "root"));
            }
            if !mode_arg.is_empty() {
                let cmd = format!(
                    "chmod -R {m} {d}",
                    m = shell_quote(&mode_arg),
                    d = shell_quote(&dest),
                );
                steps.push(run_step(&cmd, "root"));
            }
        }
        other => {
            return Err(E2bApiError::bad_request(format!(
                "unsupported e2b instruction type {other:?}. Supported: RUN, ENV, \
                 WORKDIR, USER, COPY."
            )));
        }
    }
    Ok(())
}

fn copy_blob_step(
    files_hash: &str,
    blob_sha256: String,
    source_path: String,
    dest: String,
) -> build_proto::recipe::Step {
    // Iterate by chars, not bytes — the PUT handler accepts any
    // URL segment for `files_hash`, so a multibyte path would
    // panic on a non-char-boundary slice. e2b's filesHash is hex
    // in practice, but we don't want server-side panics from
    // malformed client input.
    let short_hash: String = files_hash.chars().take(8).collect();
    build_proto::recipe::Step {
        display_name: Some(format!("copy from blob {short_hash} to {dest}")),
        env_refs: vec![],
        body: Some(build_proto::recipe::step::Body::Copy(
            build_proto::recipe::Copy {
                payload: Some(build_proto::recipe::copy::Payload::BlobSha256(blob_sha256)),
                dest,
                source_path: Some(source_path),
            },
        )),
    }
}

fn run_step(command: &str, user: &str) -> build_proto::recipe::Step {
    build_proto::recipe::Step {
        display_name: None,
        env_refs: vec![],
        body: Some(build_proto::recipe::step::Body::Run(
            build_proto::recipe::Run {
                command: command.to_string(),
                user: Some(user.to_string()),
            },
        )),
    }
}

/// `args` from the SDK is either a flat alternating `[k1, v1, k2, v2, ...]`
/// list (current SDK) or `["KEY=VALUE", ...]` (older clients). Accept both
/// — if every entry contains an `=` we treat them as pre-joined, otherwise
/// pair them positionally.
fn parse_env_args(args: &[String]) -> Result<Vec<(String, String)>, E2bApiError> {
    let pairs: Vec<(String, String)> = if args.iter().all(|a| a.contains('=')) {
        args.iter()
            .map(|a| {
                let mut parts = a.splitn(2, '=');
                let k = parts.next().unwrap_or("").to_string();
                let v = parts.next().unwrap_or("").to_string();
                (k, v)
            })
            .collect()
    } else {
        if !args.len().is_multiple_of(2) {
            return Err(E2bApiError::bad_request(format!(
                "ENV step args must be alternating KEY, VALUE pairs (or KEY=VALUE strings); got {args:?}"
            )));
        }
        args.as_chunks::<2>()
            .0
            .iter()
            .map(|[key, value]| (key.clone(), value.clone()))
            .collect()
    };
    if pairs.is_empty() {
        return Err(E2bApiError::bad_request(
            "ENV step requires at least one pair",
        ));
    }
    Ok(pairs)
}

/// e2b's COPY `args[2]` is either `user` or `user:group`. A bare user
/// expands to `user:user`. Mirrors vendored e2b's parseCopyArgs:
/// `orchestrator/internal/template/build/commands/copy.go:196`.
fn expand_copy_owner(raw: &str) -> String {
    if raw.contains(':') {
        raw.to_string()
    } else {
        format!("{raw}:{raw}")
    }
}

/// Resolve the default workdir for a COPY with no WORKDIR set, mirroring
/// `commands/copy_script.sh:11` — falls back to the command-user's
/// `$HOME` (the user set by the most recent USER / set_user, which is
/// `current_user` in this translator). Best effort without an actual
/// getent on the build VM: root → `/root`, anything else → `/home/<user>`.
/// Recipe authors can always set WORKDIR explicitly to override.
fn home_dir_for_user(user: &str) -> String {
    if user.is_empty() || user == "root" {
        "/root".to_string()
    } else {
        format!("/home/{user}")
    }
}

fn shell_quote(value: &str) -> String {
    let escaped = value.replace('\'', "'\\''");
    format!("'{escaped}'")
}

fn run_in_workdir(command: &str, workdir: &str) -> String {
    format!("cd {} && {{\n{command}\n}}", shell_quote(workdir))
}

fn runtime_step(
    req: &TemplateBuildStartV2,
) -> Result<Option<build_proto::recipe::Step>, E2bApiError> {
    use build_proto::recipe::{RuntimeApp, SetRuntime, runtime_app};
    let start = req.start_cmd.as_deref().filter(|value| !value.is_empty());
    let ready = req.ready_cmd.as_deref().filter(|value| !value.is_empty());
    let Some(start) = start else {
        if ready.is_some() {
            return Err(E2bApiError::bad_request("readyCmd requires startCmd"));
        }
        return Ok(None); // Native parent declaration is inherited unchanged.
    };
    let mut env = std::collections::HashMap::new();
    for step in &req.steps {
        if step.kind == "ENV" {
            env.extend(parse_env_args(&step.args)?);
        }
    }
    let shell = |command: &str| vec!["/bin/sh".into(), "-lc".into(), command.to_owned()];
    let runtime = SetRuntime {
        schema_version: 1,
        apps: vec![RuntimeApp {
            name: "app".into(),
            command: shell(start),
            user: "root".into(),
            working_directory: "/".into(),
            env,
            readiness: Some(match ready {
                Some(command) => runtime_app::Readiness::Exec(runtime_app::ExecReady {
                    command: shell(command),
                }),
                None => runtime_app::Readiness::Launch(runtime_app::LaunchReady {}),
            }),
            startup_timeout_secs: 60,
            startup_policy: 0,
        }],
    };
    pplx_space_build_mgr_client::runtime::decode(runtime.clone())
        .map_err(|_| E2bApiError::bad_request("invalid template runtime command or environment"))?;
    Ok(Some(build_proto::recipe::Step {
        display_name: None,
        env_refs: Vec::new(),
        body: Some(build_proto::recipe::step::Body::SetRuntime(runtime)),
    }))
}

#[cfg(test)]
mod tests;
