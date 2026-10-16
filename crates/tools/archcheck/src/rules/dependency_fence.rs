//! Workspace members that may not depend on certain packages.
//!
//! Each `[rules.dependency_fence.<instance>]` lists Cargo package names and
//! the members it checks. A checked member outside its `allow` globs that
//! declares a dependency on one of the packages violates it (`no_crossing`),
//! whatever the dependency's kind or target and whatever the manifest calls
//! it: the dependency records come from `cargo metadata`, which names the
//! depended-on package even when `package = "…"` renames it. There is no
//! baseline; an existing dependency is removed or allowed.

use crate::{
    config::{CONFIG_FILE, DependencyFenceConfig, InstanceName},
    context::{Context, Crate, Dependency},
    finding::{Finding, Location},
    rules::Rule,
};

pub struct DependencyFence;

const RULE: &str = "dependency_fence";
const NO_CROSSING: &str = "no_crossing";

impl Rule for DependencyFence {
    fn check(&self, ctx: &Context) -> Vec<Finding> {
        ctx.config
            .rules
            .dependency_fence
            .iter()
            .flat_map(|(instance, config)| {
                ctx.crates
                    .iter()
                    .filter(|krate| {
                        config.members.is_match(&krate.member)
                            && !config.allow.is_match(&krate.member)
                    })
                    .flat_map(move |krate| {
                        krate
                            .dependencies
                            .iter()
                            .filter(|dependency| config.packages.contains(&dependency.package))
                            .map(move |dependency| crossing(instance, config, krate, dependency))
                    })
            })
            .collect()
    }
}

fn crossing(
    instance: &InstanceName,
    config: &DependencyFenceConfig,
    krate: &Crate,
    dependency: &Dependency,
) -> Finding {
    let manifest = krate.manifest();
    let mut declared = Vec::new();
    if let Some(rename) = &dependency.rename {
        declared.push(format!("renamed {rename}"));
    }
    declared.push(match dependency.kind.as_deref() {
        None => "a normal dependency".to_owned(),
        Some(kind) => format!("a {kind}-dependency"),
    });
    if let Some(target) = &dependency.target {
        declared.push(format!("for target {target}"));
    }
    Finding {
        rule: RULE,
        instance: Some(instance.to_string()),
        behaviour: NO_CROSSING,
        detail: format!(
            ": {} depends on {} ({}), which only the members [rules.{RULE}.{instance}] in \
             {CONFIG_FILE} allows may. Remove the dependency, or use what it provides through \
             an allowed member.",
            krate.name,
            dependency.package,
            declared.join(", "),
        ),
        path: manifest.clone(),
        mode: config.no_crossing.mode,
        location: Location {
            path: manifest,
            line: dependency.line,
        },
    }
}
