//! Structural validation for the IAM role ARNs credmgr assumes.

/// Whether `raw` is `arn:<partition>:iam::<12-digit account>:role/<name>` with
/// a nonempty name of IAM-permitted characters.
pub(crate) fn is_iam_role_arn(raw: &str) -> bool {
    let mut parts = raw.splitn(6, ':');
    parts.next() == Some("arn")
        && parts
            .next()
            .is_some_and(|partition| matches!(partition, "aws" | "aws-us-gov" | "aws-cn"))
        && parts.next() == Some("iam")
        && parts.next() == Some("")
        && parts.next().is_some_and(|account| {
            account.len() == 12 && account.bytes().all(|byte| byte.is_ascii_digit())
        })
        && parts.next().is_some_and(|resource| {
            resource.strip_prefix("role/").is_some_and(|role| {
                !role.is_empty()
                    && role.len() <= 512
                    && role.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric()
                            || matches!(byte, b'+' | b'=' | b',' | b'.' | b'@' | b'_' | b'-' | b'/')
                    })
            })
        })
}
