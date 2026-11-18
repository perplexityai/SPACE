//! Size-only Elastic Volumes requests. A receipt is not filesystem growth.

use std::{fmt, num::NonZeroU32, str::FromStr};

use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VolumeModificationState {
    Modifying,
    Optimizing,
    Completed,
    Failed,
}

impl VolumeModificationState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Modifying => "modifying",
            Self::Optimizing => "optimizing",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Display for VolumeModificationState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for VolumeModificationState {
    type Err = XmlDecodeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "modifying" => Ok(Self::Modifying),
            "optimizing" => Ok(Self::Optimizing),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            _ => Err(XmlDecodeError::custom("unknown EBS modification state")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumeModification {
    pub volume_id: String,
    pub state: VolumeModificationState,
    pub original_size_gib: NonZeroU32,
    pub target_size_gib: NonZeroU32,
    pub original_volume_type: String,
    pub target_volume_type: String,
}

impl<T: QueryTransport> Client<T> {
    /// One size-only request, with the transport's ordinary request timeout.
    ///
    /// Persist the absolute target and establish grow-only eligibility before
    /// calling. EC2 has no ClientToken for ModifyVolume: after an uncertain
    /// result, observe this same volume's modification and backing size before
    /// deciding whether another submission is safe. No retries or polling run
    /// here, and a successful receipt does not establish usable filesystem size.
    /// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_ModifyVolume.html
    pub async fn modify_volume_size(
        &self,
        volume_id: &str,
        target_size_gib: NonZeroU32,
    ) -> Result<VolumeModification, AwsClientError> {
        let body = self
            .query
            .send_action(
                "ModifyVolume",
                EC2_API_VERSION,
                vec![
                    ("VolumeId", volume_id.to_owned()),
                    ("Size", target_size_gib.to_string()),
                ],
            )
            .await?;
        let modification = parse_receipt(&body)?;
        if modification.volume_id != volume_id || modification.target_size_gib != target_size_gib {
            return Err(AwsClientError::Xml(
                "volume modification receipt does not match the requested identity or size".into(),
            ));
        }
        Ok(modification)
    }

    /// Exactly one volume lookup. Missing history is distinct from an error;
    /// duplicate, foreign or paginated successful results are refused.
    /// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DescribeVolumesModifications.html
    pub async fn describe_volume_modification(
        &self,
        volume_id: &str,
    ) -> Result<Option<VolumeModification>, AwsClientError> {
        let body = self
            .query
            .send_action(
                "DescribeVolumesModifications",
                EC2_API_VERSION,
                vec![("VolumeId.1", volume_id.to_owned())],
            )
            .await?;
        let modification = parse_exact_lookup(&body)?;
        if modification
            .as_ref()
            .is_some_and(|row| row.volume_id != volume_id)
        {
            return Err(AwsClientError::Xml(
                "volume modification lookup returned a different volume".into(),
            ));
        }
        Ok(modification)
    }
}

fn parse_modification(
    decoder: &mut ScopedDecoder<'_, '_>,
) -> Result<VolumeModification, XmlDecodeError> {
    let mut fields = BTreeMap::new();
    while let Some(mut tag) = decoder.next_tag() {
        let key = match tag.start_el().local() {
            "volumeId" => "volumeId",
            "modificationState" => "modificationState",
            "originalSize" => "originalSize",
            "targetSize" => "targetSize",
            "originalVolumeType" => "originalVolumeType",
            "targetVolumeType" => "targetVolumeType",
            _ => continue,
        };
        if fields
            .insert(key, try_data(&mut tag)?.into_owned())
            .is_some()
        {
            return Err(XmlDecodeError::custom("duplicate EBS modification field"));
        }
    }
    let original_size_gib = required(fields.remove("originalSize"), "originalSize")?
        .parse::<NonZeroU32>()
        .map_err(|_| XmlDecodeError::custom("invalid original EBS modification size"))?;
    let target_size_gib = required(fields.remove("targetSize"), "targetSize")?
        .parse::<NonZeroU32>()
        .map_err(|_| XmlDecodeError::custom("invalid target EBS modification size"))?;
    if target_size_gib < original_size_gib {
        return Err(XmlDecodeError::custom(
            "EBS modification reports shrinking capacity",
        ));
    }
    Ok(VolumeModification {
        volume_id: required(fields.remove("volumeId"), "volumeId")?,
        state: required(fields.remove("modificationState"), "modificationState")?.parse()?,
        original_size_gib,
        target_size_gib,
        original_volume_type: required(fields.remove("originalVolumeType"), "originalVolumeType")?,
        target_volume_type: required(fields.remove("targetVolumeType"), "targetVolumeType")?,
    })
}

fn parse_receipt(body: &str) -> Result<VolumeModification, XmlDecodeError> {
    let mut document = Document::new(body);
    let mut root = document.root_element()?;
    require_root(&root, "ModifyVolumeResponse")?;
    let mut modification = None;
    while let Some(mut tag) = root.next_tag() {
        if tag.start_el().local() == "volumeModification" {
            if modification.is_some() {
                return Err(XmlDecodeError::custom("duplicate EBS modification receipt"));
            }
            modification = Some(parse_modification(&mut tag)?);
        }
    }
    modification.ok_or_else(|| XmlDecodeError::custom("missing EBS modification receipt"))
}

fn parse_exact_lookup(body: &str) -> Result<Option<VolumeModification>, XmlDecodeError> {
    let mut document = Document::new(body);
    let mut root = document.root_element()?;
    require_root(&root, "DescribeVolumesModificationsResponse")?;
    let mut modification = None;
    let mut set_seen = false;
    while let Some(mut tag) = root.next_tag() {
        match tag.start_el().local() {
            "volumeModificationSet" => {
                if set_seen {
                    return Err(XmlDecodeError::custom("duplicate EBS modification set"));
                }
                set_seen = true;
                while let Some(mut item) = tag.next_tag() {
                    if item.start_el().local() == "item" {
                        if modification.is_some() {
                            return Err(XmlDecodeError::custom(
                                "ambiguous EBS modification lookup",
                            ));
                        }
                        modification = Some(parse_modification(&mut item)?);
                    }
                }
            }
            "nextToken" => {
                let token = try_data(&mut tag)?;
                if !token.is_empty() {
                    return Err(XmlDecodeError::custom(
                        "paginated exact EBS modification lookup",
                    ));
                }
            }
            _ => {}
        }
    }
    if !set_seen {
        return Err(XmlDecodeError::custom("missing EBS modification set"));
    }
    Ok(modification)
}

#[cfg(test)]
mod tests;
