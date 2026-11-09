use super::read_only_file_mappings;

#[test]
fn selects_read_only_private_file_mappings() {
    let maps = "\
55d0c0000000-55d0c0100000 r--p 00000000 fe:01 1234 /usr/local/bin/spaced-svc
55d0c0100000-55d0c0800000 r-xp 00100000 fe:01 1234 /usr/local/bin/spaced-svc
55d0c0800000-55d0c0900000 rw-p 00800000 fe:01 1234 /usr/local/bin/spaced-svc
55d0c1000000-55d0c1100000 rw-p 00000000 00:00 0 [heap]
7f0000000000-7f0000200000 r-xp 00028000 fe:01 99 /usr/lib/x86_64-linux-gnu/libc.so.6
7f0000300000-7f0000301000 r--s 00000000 00:05 7 /dev/zero
7f0000400000-7f0000401000 r--s 00000000 fe:01 55 /var/lib/shared.db
7f0000500000-7f0000501000 r--p 00000000 00:01 8 /memfd:buffer (deleted)
7f0000600000-7f0000601000 r-xp 00000000 fe:01 77 /usr/local/bin/old (deleted)
7ffd00000000-7ffd00021000 rw-p 00000000 00:00 0 [stack]
7ffd00100000-7ffd00102000 r-xp 00000000 00:00 0 [vdso]
";
    let selected: Vec<_> = read_only_file_mappings(maps).collect();
    assert_eq!(
        selected,
        vec![
            (0x55d0c0000000, 0x55d0c0100000),
            (0x55d0c0100000, 0x55d0c0800000),
            (0x7f0000000000, 0x7f0000200000),
        ]
    );
}
