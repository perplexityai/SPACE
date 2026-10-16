//! `git diff -z --name-status` output, as git writes it and as it is not.

use super::*;

/// Each record of `input` is read, and only renames are kept, whatever
/// else the diff has.
#[test]
fn renames_are_read_from_git_s_records() {
    // Name, input and each `(new, old)` path.
    type Case<'a> = (&'a str, &'a [u8], &'a [(&'a str, &'a str)]);
    let cases: &[Case] = &[
        ("an empty diff", b"", &[]),
        ("a rename", b"R100\0a.rs\0b.rs\0", &[("b.rs", "a.rs")]),
        (
            "every other status, around a rename",
            b"M\0m.rs\0A\0n.rs\0D\0d.rs\0T\0t.rs\0R061\0a.rs\0b.rs\0C075\0c.rs\0e.rs\0",
            &[("b.rs", "a.rs")],
        ),
        (
            "a path with a tab",
            b"R100\0a\tb.rs\0c.rs\0",
            &[("c.rs", "a\tb.rs")],
        ),
        ("a path that is not UTF-8", b"R100\0\xff.rs\0b.rs\0", &[]),
    ];
    for (name, input, expected) in cases {
        let renames = Renames::parse(input).expect(name);

        let expected: BTreeMap<String, String> = expected
            .iter()
            .map(|(new, old)| ((*new).to_owned(), (*old).to_owned()))
            .collect();
        assert_eq!(renames.sources, expected, "{name}");
    }
}

/// Input git would not write is an error, not a partial set of renames.
#[test]
fn input_git_would_not_write() {
    let status = |status: &str| RenamesError::Status {
        status: status.to_owned(),
    };
    let cases: &[(&str, &[u8], RenamesError)] = &[
        (
            "without -z",
            b"M\tm.rs\nR100\ta.rs\tb.rs\n",
            RenamesError::Unterminated,
        ),
        (
            "cut off in a path",
            b"R100\0a.rs\0b.rs.backup\0R100\0c.rs\0d.rs",
            RenamesError::Unterminated,
        ),
        (
            "a field that is not a status",
            b"M\tm.rs\0",
            status("M\tm.rs"),
        ),
        ("a lower-case status", b"r100\0a.rs\0b.rs\0", status("r100")),
        ("an empty status", b"M\0m.rs\0\0", status("")),
        (
            "a rename without its new path",
            b"R100\0a.rs\0",
            RenamesError::Truncated {
                status: "R100".to_owned(),
            },
        ),
        (
            "a path renamed twice",
            b"R100\0a.rs\0b.rs\0R100\0a.rs\0c.rs\0",
            RenamesError::Repeated {
                path: "a.rs".to_owned(),
            },
        ),
        (
            "two paths renamed to one",
            b"R100\0a.rs\0c.rs\0R100\0b.rs\0c.rs\0",
            RenamesError::Repeated {
                path: "c.rs".to_owned(),
            },
        ),
        (
            "a chain",
            b"R100\0a.rs\0b.rs\0R100\0b.rs\0c.rs\0",
            RenamesError::Repeated {
                path: "b.rs".to_owned(),
            },
        ),
        (
            "a swap",
            b"R100\0a.rs\0b.rs\0R100\0b.rs\0a.rs\0",
            RenamesError::Repeated {
                path: "b.rs".to_owned(),
            },
        ),
        (
            "a path renamed to itself",
            b"R100\0a.rs\0a.rs\0",
            RenamesError::Repeated {
                path: "a.rs".to_owned(),
            },
        ),
    ];
    for (name, input, expected) in cases {
        assert_eq!(Renames::parse(input).as_ref(), Err(expected), "{name}");
    }
}
