use super::*;

#[test]
fn argv_quoting_round_trips_through_windows_parser() {
    use windows_sys::Win32::{Foundation::LocalFree, UI::Shell::CommandLineToArgvW};
    for arg in [
        "",
        "plain",
        "two words",
        "a\"b",
        "trailing\\",
        "a\\\"b",
        "\t",
        "λ 📁",
    ] {
        let command = wide(&format!("probe.exe {}", quote(arg))).unwrap();
        let mut count = 0;
        // SAFETY: terminated command and writable count; Windows allocates argv.
        let argv = unsafe { CommandLineToArgvW(command.as_ptr(), &mut count) };
        assert!(!argv.is_null());
        assert_eq!(count, 2);
        let second = argv.wrapping_add(1);
        // SAFETY: parser reported exactly two argument pointers.
        let text = unsafe { *second };
        let mut length = 0;
        loop {
            let current = text.wrapping_add(length);
            // SAFETY: Windows guarantees this argument is NUL terminated.
            if unsafe { *current } == 0 {
                break;
            }
            length += 1;
        }
        // SAFETY: measured the initialized argument up to its terminator.
        let parsed = unsafe { std::slice::from_raw_parts(text, length) };
        assert_eq!(String::from_utf16(parsed).unwrap(), arg);
        // SAFETY: unique allocation from CommandLineToArgvW, no remaining use.
        unsafe {
            LocalFree(argv.cast());
        }
    }
}
