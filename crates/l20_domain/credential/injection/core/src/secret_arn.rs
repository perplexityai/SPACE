//! Validated Secrets Manager ARN syntax shared by registration and resolution.

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct SecretArn {
    value: String,
    region: String,
}

impl SecretArn {
    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidSecretArn> {
        let value = value.into();
        let (_, region) = parse_secret_arn(&value)?;
        Ok(Self {
            region: region.to_owned(),
            value,
        })
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.value
    }

    #[must_use]
    pub fn region(&self) -> &str {
        &self.region
    }
}

impl std::fmt::Debug for SecretArn {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SecretArn(<redacted>)")
    }
}

#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("credential reference is malformed")]
pub struct InvalidSecretArn;

pub fn parse_secret_arn(secret_arn: &str) -> Result<(&str, &str), InvalidSecretArn> {
    let parts: Vec<_> = secret_arn.splitn(6, ':').collect();
    match parts.as_slice() {
        ["arn", "aws", "secretsmanager", region, account, resource]
            if !region.is_empty()
                && region.bytes().all(|byte| {
                    byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'
                })
                && account.len() == 12
                && account.bytes().all(|byte| byte.is_ascii_digit())
                && resource.starts_with("secret:")
                && resource.len() > "secret:".len()
                && resource.bytes().all(|byte| byte.is_ascii_graphic()) =>
        {
            Ok((account, region))
        }
        _ => Err(InvalidSecretArn),
    }
}

#[cfg(test)]
mod tests;
