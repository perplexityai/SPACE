use super::is_storable_traceparent;

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
