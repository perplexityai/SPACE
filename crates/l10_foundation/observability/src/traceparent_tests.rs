use super::{MAX_ORIGIN_LINKS, is_storable_traceparent, sampled_origins_first};

#[test]
fn storable_traceparents_follow_the_w3c_version_00_shape() {
    for (traceparent, storable) in [
        (
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            true,
        ),
        (
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00",
            true,
        ),
        // Uppercase hex, an all-zero id, another version, a wrong length and
        // a misplaced separator are each rejected.
        (
            "00-0AF7651916CD43DD8448EB211C80319C-B7AD6B7169203331-01",
            false,
        ),
        (
            "00-00000000000000000000000000000000-b7ad6b7169203331-01",
            false,
        ),
        (
            "00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01",
            false,
        ),
        (
            "01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            false,
        ),
        (
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01-",
            false,
        ),
        (
            "00-0af7651916cd43dd8448eb211c80319cb-7ad6b7169203331-01",
            false,
        ),
        ("", false),
    ] {
        assert_eq!(
            is_storable_traceparent(traceparent),
            storable,
            "{traceparent:?}"
        );
    }
}

#[test]
fn sampled_origins_come_first_and_survive_the_link_cap() {
    let unsampled = (1..=MAX_ORIGIN_LINKS + 1)
        .map(|n| format!("00-{n:032x}-{n:016x}-00"))
        .collect::<Vec<_>>();
    let sampled = [
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-03",
    ];
    // Every unsampled origin is given before the sampled ones, and one invalid
    // value sits among them.
    let given = unsampled
        .iter()
        .map(String::as_str)
        .chain(["not-a-traceparent", sampled[0], sampled[1]])
        .collect::<Vec<_>>();

    let ordered = sampled_origins_first(given);

    let expected = sampled
        .into_iter()
        .chain(unsampled.iter().map(String::as_str))
        .collect::<Vec<_>>();
    assert_eq!(ordered, expected);
    assert!(
        ordered[..MAX_ORIGIN_LINKS].contains(&sampled[0]),
        "a sampled origin is within the cap"
    );
}
