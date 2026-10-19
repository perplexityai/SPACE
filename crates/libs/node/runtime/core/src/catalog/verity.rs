//! Measurements authored from the existing SHA-256-pinned bytes using
//! fsverity-utils: SHA-256, 4096-byte blocks, no salt. They are local integrity
//! metadata and do not change release, kernel or environment identities.

use crate::verity::VerityDigest;
use pplx_space_common_core::types::Digest;
use std::collections::BTreeMap;

pub fn compiled_verity_digest(
    content: Digest,
) -> Result<Option<VerityDigest>, &'static super::CatalogError> {
    compiled_pins().map(|pins| pins.get(&content).copied())
}

fn compiled_pins() -> Result<&'static BTreeMap<Digest, VerityDigest>, &'static super::CatalogError>
{
    const PINS: &[(&str, &str)] = &[
        (
            "sha256:7e8b57e88c459396d4680d83dcdd8c7f72305447cb55b11f4ac98ad70a3f7825",
            "sha256:a5da051d29f43f6f9d0ceb1621157588b5178d140e989b82db25e09550482411",
        ),
        (
            "sha256:4830a9b1fc6cece036d8992ff12f1fe9c5247aacad77f42c7aba683c7a08622e",
            "sha256:1bbcda6cb3099423c7c48bb0fc2d18f895c83afc5b3da66f728f949f15ddd560",
        ),
        (
            "sha256:99ad0f5cd0514a88aad0e9ae8cfdb3cc3b4ab9d190e1194602406c786b5de7a5",
            "sha256:0beb9b64bac35ce390745782bdabcf8d955e0fc4314e56e3430eaf5fe969f26d",
        ),
        (
            "sha256:65ef226e96f0ceda55ba643f445801ef2cc0ea667ef67cad8ac4f406c9c8434f",
            "sha256:dd85bbc9fffbbbf42a1726f48a31dcc5e7f90411edb8c50e0db20a549ae4a2f3",
        ),
        (
            "sha256:e20e46d0c36c55c0d1014eb20576171b3f3d922260d9f792017aeff53af3d4f2",
            "sha256:346294cd981e5f4bc7af2d4d68f47a3987ccd421af0d1e44d4717403275ce2fa",
        ),
        (
            "sha256:d8ced68bd61e27b6813e2c993cc53a4029c59e13210672180591c84109684fe4",
            "sha256:45ef19d3bc9a6ff4c929ff5dcc462a8052cee82c68043d9067f2ee57a5f6df8e",
        ),
    ];
    static PINS_CACHE: std::sync::OnceLock<
        Result<BTreeMap<Digest, VerityDigest>, super::CatalogError>,
    > = std::sync::OnceLock::new();
    PINS_CACHE
        .get_or_init(|| {
            PINS.iter()
                .map(|(content, measured)| {
                    let invalid = |field, value| super::CatalogError::Compiled {
                        entry: (*content).to_owned(),
                        field,
                        value,
                    };
                    Ok((
                        content
                            .parse()
                            .map_err(|_| invalid("content digest", *content))?,
                        measured
                            .parse()
                            .map_err(|_| invalid("verity digest", *measured))?,
                    ))
                })
                .collect()
        })
        .as_ref()
}
