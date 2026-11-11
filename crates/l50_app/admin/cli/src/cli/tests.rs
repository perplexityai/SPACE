use super::*;
use clap::CommandFactory;

fn list_nodes_args(argv: &[&str]) -> ListNodesArgs {
    let cli = Cli::try_parse_from(argv).expect("argv should parse");
    match cli.command {
        Commands::ListNodes(args) => args,
        _ => panic!("expected list-nodes"),
    }
}

/// `--sort` is parsed by [`AdminNodeSort`]'s `FromStr` rather than a
/// CLI-local mirror of the vocabulary, so this pins the `value_parser` wiring:
/// an accepted value lands as the shared enum and a rejected one fails at
/// parse time instead of reaching the server.
#[test]
fn list_nodes_parses_the_sort_flag_into_the_shared_enum() {
    let args = list_nodes_args(&[
        "space-admin-cli",
        "list-nodes",
        "--sort",
        "drainability",
        "--role",
        "worker",
    ]);
    assert_eq!(args.sort, Some(AdminNodeSort::Drainability));

    let args = list_nodes_args(&["space-admin-cli", "list-nodes", "--sort", "name"]);
    assert_eq!(args.sort, Some(AdminNodeSort::Name));

    assert!(
        Cli::try_parse_from(["space-admin-cli", "list-nodes", "--sort", "Drainability"]).is_err(),
        "an unknown ordering must fail at parse time"
    );
}

/// The ranking has no role term and builders and cluster nodes host no
/// sandboxes, so they win every count and would lead an unfiltered ranked
/// listing. Handing that first row to `drain-node` drains a builder, so the
/// role has to be named rather than defaulted — a default would silently
/// answer a different question than the one asked.
///
/// Scoped to `drainability`: `--sort name` is the existing ordering and carries
/// no such hazard, so requiring a role there would be friction for nothing.
#[test]
fn list_nodes_requires_a_role_only_when_ranking() {
    // `.map(|_| ())` because `expect_err` would need `Cli: Debug` to render the
    // unexpected success, and deriving it on the whole command tree to satisfy
    // one test is the wrong trade.
    let err = Cli::try_parse_from(["space-admin-cli", "list-nodes", "--sort", "drainability"])
        .map(|_| ())
        .expect_err("ranking without a role must not parse");
    let message = err.to_string();
    assert!(message.contains("--role"), "{message}");

    for argv in [
        vec!["space-admin-cli", "list-nodes"],
        vec!["space-admin-cli", "list-nodes", "--sort", "name"],
        vec![
            "space-admin-cli",
            "list-nodes",
            "--long-runner-age-hours",
            "168",
        ],
    ] {
        Cli::try_parse_from(&argv).unwrap_or_else(|err| panic!("{argv:?} should parse: {err}"));
    }

    for role in ["worker", "builder", "cluster"] {
        let args = list_nodes_args(&[
            "space-admin-cli",
            "list-nodes",
            "--sort",
            "drainability",
            "--role",
            role,
        ]);
        assert_eq!(args.sort, Some(AdminNodeSort::Drainability));
    }
}

#[test]
fn list_nodes_defaults_leave_the_drainability_knobs_unset() {
    let args = list_nodes_args(&["space-admin-cli", "list-nodes"]);

    assert!(args.sort.is_none());
    assert!(args.long_runner_age_hours.is_none());
    assert!(!args.verbose);
    assert!(!args.json);
}

#[test]
fn list_nodes_parses_the_long_runner_age_and_existing_filters() {
    let args = list_nodes_args(&[
        "space-admin-cli",
        "list-nodes",
        "--role",
        "worker",
        "--drain-phase",
        "none",
        "--long-runner-age-hours",
        "168",
        "--include-deleted",
        "-v",
    ]);

    assert_eq!(args.long_runner_age_hours, Some(168));
    assert!(matches!(args.role, Some(RoleFilter::Worker)));
    assert!(matches!(args.drain_phase, Some(DrainPhaseFilter::None)));
    assert!(args.include_deleted);
    assert!(args.verbose);
}

/// The threshold is an hour count, so a fractional or unit-suffixed value is
/// not a threshold at all and has to fail before any request is built. `-1` is
/// in the list for completeness, but note it is rejected as an unknown flag
/// rather than by the `u32` — it would fail here even if the field were signed.
/// `0` does parse; what the server makes of it is the server's call.
#[test]
fn list_nodes_rejects_a_non_integer_long_runner_age() {
    for value in ["-1", "1.5", "24h"] {
        assert!(
            Cli::try_parse_from([
                "space-admin-cli",
                "list-nodes",
                "--long-runner-age-hours",
                value
            ])
            .is_err(),
            "{value} should not parse as an hour count"
        );
    }
}

/// The clap constraint names the string `"drainability"`, but the property that
/// makes a role necessary is `AdminNodeSort::needs_drainability()`. Nothing in
/// the type system ties those together, so a variant added to the shared enum
/// could rank unfiltered while the constraint stays silent — the exact failure
/// the constraint exists to prevent, reintroduced by growing the vocabulary.
///
/// Walking `ALL` makes that a CI failure rather than a latent hazard: every
/// ranking variant must require `--role`, and every non-ranking one must not.
#[test]
fn list_nodes_requires_a_role_for_every_ranking_variant() {
    for sort in AdminNodeSort::ALL {
        let bare = Cli::try_parse_from(["space-admin-cli", "list-nodes", "--sort", sort.as_str()])
            .map(|_| ());
        if sort.needs_drainability() {
            assert!(
                bare.is_err(),
                "--sort {} ranks, so it must require --role; give it an arm in \
                 `required_if_eq` on `role`",
                sort.as_str()
            );
            let args = list_nodes_args(&[
                "space-admin-cli",
                "list-nodes",
                "--sort",
                sort.as_str(),
                "--role",
                "worker",
            ]);
            assert_eq!(args.sort.as_ref(), Some(sort));
        } else {
            assert!(
                bare.is_ok(),
                "--sort {} does not rank, so it must parse without --role",
                sort.as_str()
            );
        }
    }
}

/// `sort_parser`'s whole contribution is the surface an operator discovers
/// values through — `--help`'s `[possible values]` and shell completion — and
/// none of that shows up in parse results. Swapping it back for a plain
/// `value_parser = AdminNodeSort::from_str` leaves every other test green while
/// silently emptying that surface, so assert clap actually holds the set, and
/// that it is the shared enum's rather than a copy.
#[test]
fn list_nodes_offers_the_shared_sort_vocabulary_in_help() {
    let sort = Cli::command()
        .find_subcommand("list-nodes")
        .expect("list-nodes subcommand")
        .get_arguments()
        .find(|arg| arg.get_id() == "sort")
        .expect("--sort argument")
        .clone();

    let offered: Vec<String> = sort
        .get_possible_values()
        .iter()
        .map(|value| value.get_name().to_string())
        .collect();
    let expected: Vec<String> = AdminNodeSort::ALL
        .iter()
        .map(|sort| sort.as_str().to_string())
        .collect();

    assert_eq!(
        offered, expected,
        "--sort must offer exactly AdminNodeSort::ALL"
    );
}
