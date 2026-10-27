use super::*;

#[test]
fn chunk_mode_is_confined_to_the_explicit_experiment_namespace() {
    let config = Config {
        selection: "chunks:4194304".parse().unwrap(),
        prefix: "chunk-fixture-".parse().unwrap(),
    };
    assert!(matches!(
        config.for_template("chunk-fixture-target"),
        Some(Selection::Chunks(ChunkSize::Mib4, _))
    ));
    for existing in [
        "seed",
        "chunk-fixture",
        "chunk-fixturex-target",
        "another-template",
    ] {
        assert!(config.for_template(existing).is_none());
    }
    for invalid in ["", "-", "../", "prefix", "bad/name-"] {
        invalid
            .parse::<pplx_space_common_core::types::template_chunks::TemplatePrefix>()
            .unwrap_err();
    }
    for concurrency in [None, Some("1"), Some("4"), Some("16")] {
        let config =
            Config::from_values("chunks:4194304", "chunk-fixture-", concurrency, None, None)
                .unwrap();
        assert!(matches!(
            config.for_template("chunk-fixture-target"),
            Some(Selection::Chunks(ChunkSize::Mib4, _))
        ));
        assert!(config.for_template("another-template").is_none());
    }
    for invalid in ["", "0", "17", "-1", "unlimited"] {
        Config::from_values(
            "chunks:4194304",
            "chunk-fixture-",
            Some(invalid),
            None,
            None,
        )
        .unwrap_err();
    }
    for target in [None, Some("2097152"), Some("8388608"), Some("33554432")] {
        for schedule in [
            None,
            Some("coupled"),
            Some("pipeline"),
            Some("parallel-frames"),
            Some("overlap-reuse"),
            Some("combined"),
        ] {
            Config::from_values(
                "chunks:4194304",
                "chunk-fixture-",
                Some("16"),
                target,
                schedule,
            )
            .unwrap();
        }
    }
    for invalid in ["", "0", "1048575", "33554433", "-1", "8MiB"] {
        Config::from_values(
            "chunks:4194304",
            "chunk-fixture-",
            None,
            Some(invalid),
            None,
        )
        .unwrap_err();
    }
    Config::from_values(
        "chunks:4194304",
        "chunk-fixture-",
        None,
        None,
        Some("unknown"),
    )
    .unwrap_err();
}
