use super::*;

#[test]
fn cpu_stat_usage_usec_parses_the_usage_line() {
    let content = "usage_usec 41398047\nuser_usec 40000000\nsystem_usec 1398047\n";
    assert_eq!(cpu_stat_usage_usec(content), Some(41_398_047));
    assert_eq!(cpu_stat_usage_usec("user_usec 1\n"), None);
}
