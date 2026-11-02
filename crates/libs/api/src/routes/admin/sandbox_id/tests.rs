use pplx_space_common_core::types::SandboxId;
use pplx_space_public_api::PublicSandboxId;

use super::*;

const UUID: &str = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b";

fn sandbox() -> SandboxId {
    SandboxId::from_uuid(Uuid::parse_str(UUID).expect("fixture uuid"))
}

/// Every form an operator may hold for one sandbox resolves to it.
#[test]
fn every_accepted_form_names_the_same_sandbox() {
    let undashed = UUID.replace('-', "");
    let accepted = [
        PublicSandboxId::from(sandbox()).to_string(),
        format!("sbx_{}", UUID.to_uppercase()),
        format!("sbx_{undashed}"),
        UUID.to_string(),
        UUID.to_uppercase(),
        undashed.clone(),
        undashed.to_uppercase(),
        format!("space-sbx-{undashed}-g0"),
        format!("space-sbx-{undashed}-g17"),
        format!("space-sbx-{undashed}"),
        format!(" {UUID}\t"),
        format!("\nsbx_{UUID} "),
    ];
    for form in accepted {
        assert_eq!(
            parse_admin_sandbox_id(&form),
            Ok(sandbox()),
            "{form} should resolve"
        );
    }
}

/// The pod-name form is the one the pod builder actually produces.
#[cfg(feature = "kubernetes")]
#[test]
fn the_pod_builder_names_resolve() {
    use pplx_space_common_core::kubernetes::sandbox_pod_name;

    for generation in [0, 1, i64::MAX] {
        let name = sandbox_pod_name(sandbox(), generation);
        assert_eq!(parse_admin_sandbox_id(&name), Ok(sandbox()), "{name}");
    }
}

#[test]
fn unknown_forms_are_refused() {
    let undashed = UUID.replace('-', "");
    let refused = [
        String::new(),
        "sbx_".to_string(),
        format!("{{{UUID}}}"),
        format!("urn:uuid:{UUID}"),
        format!("sbx_{{{UUID}}}"),
        format!("sandbox:{UUID}"),
        UUID.chars().take(35).collect(),
        undashed.chars().take(31).collect(),
        format!("space-sbx-{UUID}"),
        format!("space-sbx-{undashed}-g"),
        format!("space-sbx-{undashed}-gx"),
        format!("space-sbx-{undashed}-g1-g2"),
        format!("space-sbx-{undashed}g1"),
        format!(
            "space-sbx-{}",
            undashed.chars().take(31).collect::<String>()
        ),
        format!("sbx_space-sbx-{undashed}"),
        format!("SBX_{UUID}"),
        "my-sandbox".to_string(),
    ];
    for form in refused {
        assert_eq!(
            parse_admin_sandbox_id(&form),
            Err(AdminSandboxIdError),
            "{form:?} should be refused"
        );
    }
}
