//! EBS query operations. Callers own authorization, durable intent, retries,
//! pagination bounds, and attachment fencing; no operation force-detaches.

use std::collections::BTreeMap;

use super::{Client, EC2_API_VERSION};
use crate::query::{AwsClientError, QueryTransport};
use aws_smithy_xml::decode::{Document, ScopedDecoder, XmlDecodeError, try_data};

/// A fresh, encrypted gp3 disk. There is deliberately no snapshot source.
pub struct CreateEncryptedGp3Volume<'a> {
    pub availability_zone: &'a str,
    pub size_gib: u32,
    /// Persist this token before calling AWS, and reuse it after ambiguous errors.
    pub client_token: &'a str,
    pub tags: &'a BTreeMap<String, String>,
}

/// A creation receipt, not evidence of availability, ownership, or attachments.
/// Callers must reconcile the tagged inventory before using the disk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreatedVolume {
    pub volume_id: String,
}

/// AWS wire values are retained verbatim, including unknown future states.
/// Controllers must allowlist states before performing a lifecycle transition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Volume {
    pub volume_id: String,
    pub availability_zone: String,
    pub size_gib: u32,
    pub encrypted: bool,
    pub volume_type: String,
    pub state: String,
    pub attachments: Vec<VolumeAttachment>,
    pub tags: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumeAttachment {
    pub instance_id: String,
    pub device: String,
    pub state: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumePage {
    pub volumes: Vec<Volume>,
    pub next_token: Option<String>,
}

impl<T: QueryTransport> Client<T> {
    pub async fn create_encrypted_gp3_volume(
        &self,
        request: &CreateEncryptedGp3Volume<'_>,
    ) -> Result<CreatedVolume, AwsClientError> {
        let mut params = vec![
            (
                "AvailabilityZone".to_string(),
                request.availability_zone.to_owned(),
            ),
            ("Size".to_string(), request.size_gib.to_string()),
            ("VolumeType".to_string(), "gp3".to_string()),
            ("Encrypted".to_string(), "true".to_string()),
            ("ClientToken".to_string(), request.client_token.to_owned()),
        ];
        if !request.tags.is_empty() {
            params.push((
                "TagSpecification.1.ResourceType".to_string(),
                "volume".to_string(),
            ));
        }
        for (index, (key, value)) in request.tags.iter().enumerate() {
            let prefix = format!("TagSpecification.1.Tag.{}", index + 1);
            params.push((format!("{prefix}.Key"), key.clone()));
            params.push((format!("{prefix}.Value"), value.clone()));
        }
        let body = self
            .query
            .send_action(
                "CreateVolume",
                EC2_API_VERSION,
                params
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.clone()))
                    .collect(),
            )
            .await?;
        parse_creation_receipt(&body).map_err(Into::into)
    }

    /// One inventory page, scoped by all supplied ownership tags. Keeping
    /// pagination explicit lets a controller fail closed at its inventory cap.
    pub async fn describe_volumes_page(
        &self,
        tags: &BTreeMap<String, String>,
        next_token: Option<&str>,
    ) -> Result<VolumePage, AwsClientError> {
        let mut params = vec![("MaxResults".to_string(), "1000".to_string())];
        for (index, (key, value)) in tags.iter().enumerate() {
            let prefix = format!("Filter.{}", index + 1);
            params.push((format!("{prefix}.Name"), format!("tag:{key}")));
            params.push((format!("{prefix}.Value.1"), value.clone()));
        }
        if let Some(token) = next_token {
            params.push(("NextToken".to_string(), token.to_owned()));
        }
        let body = self
            .query
            .send_action(
                "DescribeVolumes",
                EC2_API_VERSION,
                params
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.clone()))
                    .collect(),
            )
            .await?;
        parse_volume_page(&body).map_err(Into::into)
    }

    /// Absence is authoritative only for the exact ID, never an ownership-tag
    /// filter whose membership can change independently of the disk.
    pub async fn describe_volume(&self, volume_id: &str) -> Result<Option<Volume>, AwsClientError> {
        let body = match self
            .query
            .send_action(
                "DescribeVolumes",
                EC2_API_VERSION,
                vec![("VolumeId.1", volume_id.to_owned())],
            )
            .await
        {
            Ok(body) => body,
            Err(AwsClientError::AwsService { code, .. }) if code == "InvalidVolume.NotFound" => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let page = parse_volume_page(&body)?;
        let mut volumes = page.volumes.into_iter();
        let volume = volumes.next().ok_or_else(|| {
            AwsClientError::Xml("exact volume lookup returned an empty successful result".into())
        })?;
        if volume.volume_id != volume_id || volumes.next().is_some() || page.next_token.is_some() {
            return Err(AwsClientError::Xml(
                "exact volume lookup returned an ambiguous result".into(),
            ));
        }
        Ok(Some(volume))
    }

    pub async fn attach_volume(
        &self,
        volume_id: &str,
        instance_id: &str,
        device: &str,
    ) -> Result<VolumeAttachment, AwsClientError> {
        let body = self
            .query
            .send_action(
                "AttachVolume",
                EC2_API_VERSION,
                vec![
                    ("VolumeId", volume_id.to_owned()),
                    ("InstanceId", instance_id.to_owned()),
                    ("Device", device.to_owned()),
                ],
            )
            .await?;
        let mut document = Document::new(&body);
        let mut root = document.root_element()?;
        require_root(&root, "AttachVolumeResponse")?;
        parse_attachment(&mut root).map_err(Into::into)
    }

    /// Detach only the specified attachment, without Force. The caller must
    /// already have established that the VM and its device references are gone.
    pub async fn detach_volume(
        &self,
        volume_id: &str,
        instance_id: &str,
        device: &str,
    ) -> Result<VolumeAttachment, AwsClientError> {
        let body = self
            .query
            .send_action(
                "DetachVolume",
                EC2_API_VERSION,
                vec![
                    ("VolumeId", volume_id.to_owned()),
                    ("InstanceId", instance_id.to_owned()),
                    ("Device", device.to_owned()),
                    ("Force", "false".to_string()),
                ],
            )
            .await?;
        let mut document = Document::new(&body);
        let mut root = document.root_element()?;
        require_root(&root, "DetachVolumeResponse")?;
        parse_attachment(&mut root).map_err(Into::into)
    }

    pub async fn delete_volume(&self, volume_id: &str) -> Result<(), AwsClientError> {
        let body = self
            .query
            .send_action(
                "DeleteVolume",
                EC2_API_VERSION,
                vec![("VolumeId", volume_id.to_owned())],
            )
            .await?;
        let mut document = Document::new(&body);
        let mut root = document.root_element()?;
        require_root(&root, "DeleteVolumeResponse")?;
        while let Some(mut tag) = root.next_tag() {
            if tag.start_el().local() == "return" {
                return match try_data(&mut tag)?.as_ref() {
                    "true" => Ok(()),
                    _ => Err(AwsClientError::Xml(
                        "DeleteVolume did not confirm deletion".into(),
                    )),
                };
            }
        }
        Err(AwsClientError::Xml(
            "DeleteVolume response omitted return".into(),
        ))
    }
}

fn require_root(
    decoder: &ScopedDecoder<'_, '_>,
    expected: &'static str,
) -> Result<(), XmlDecodeError> {
    if decoder.start_el().local() != expected {
        return Err(XmlDecodeError::custom("unexpected EBS response document"));
    }
    Ok(())
}

fn required(value: Option<String>, field: &'static str) -> Result<String, XmlDecodeError> {
    value
        .filter(|value| !value.is_empty())
        .ok_or_else(|| XmlDecodeError::custom(format!("EBS response omitted {field}")))
}

fn parse_creation_receipt(body: &str) -> Result<CreatedVolume, XmlDecodeError> {
    let mut document = Document::new(body);
    let mut root = document.root_element()?;
    require_root(&root, "CreateVolumeResponse")?;
    let mut volume_id = None;
    while let Some(mut tag) = root.next_tag() {
        if tag.start_el().local() == "volumeId" {
            if volume_id.is_some() {
                return Err(XmlDecodeError::custom("duplicate EBS creation volumeId"));
            }
            volume_id = Some(try_data(&mut tag)?.into_owned());
        }
    }
    // CreateVolume deliberately omits attachmentSet. Do not weaken inventory
    // parsing or fabricate an empty attachment list from this receipt.
    // https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_CreateVolume.html
    Ok(CreatedVolume {
        volume_id: required(volume_id, "volumeId")?,
    })
}

fn parse_attachment(
    decoder: &mut ScopedDecoder<'_, '_>,
) -> Result<VolumeAttachment, XmlDecodeError> {
    let (mut instance, mut device, mut state) = (None, None, None);
    while let Some(mut tag) = decoder.next_tag() {
        match tag.start_el().local() {
            "instanceId" => instance = Some(try_data(&mut tag)?.into_owned()),
            "device" => device = Some(try_data(&mut tag)?.into_owned()),
            "status" => state = Some(try_data(&mut tag)?.into_owned()),
            _ => {}
        }
    }
    Ok(VolumeAttachment {
        instance_id: required(instance, "instanceId")?,
        device: required(device, "device")?,
        state: required(state, "attachment status")?,
    })
}

fn parse_volume(decoder: &mut ScopedDecoder<'_, '_>) -> Result<Volume, XmlDecodeError> {
    let (mut id, mut zone, mut size, mut encrypted, mut kind, mut state) =
        (None, None, None, None, None, None);
    let mut attachments = Vec::new();
    let mut attachments_seen = false;
    let mut tags = BTreeMap::new();
    while let Some(mut tag) = decoder.next_tag() {
        match tag.start_el().local() {
            "volumeId" => id = Some(try_data(&mut tag)?.into_owned()),
            "availabilityZone" => zone = Some(try_data(&mut tag)?.into_owned()),
            "size" => size = Some(try_data(&mut tag)?.into_owned()),
            "encrypted" => encrypted = Some(try_data(&mut tag)?.into_owned()),
            "volumeType" => kind = Some(try_data(&mut tag)?.into_owned()),
            "status" => state = Some(try_data(&mut tag)?.into_owned()),
            "attachmentSet" => {
                if attachments_seen {
                    return Err(XmlDecodeError::custom("duplicate EBS attachment set"));
                }
                attachments_seen = true;
                while let Some(mut attachment) = tag.next_tag() {
                    if attachment.start_el().local() == "item" {
                        attachments.push(parse_attachment(&mut attachment)?);
                    }
                }
            }
            "tagSet" => {
                while let Some(mut item) = tag.next_tag() {
                    if item.start_el().local() != "item" {
                        continue;
                    }
                    let (mut key, mut value) = (None, None);
                    while let Some(mut entry) = item.next_tag() {
                        match entry.start_el().local() {
                            "key" => key = Some(try_data(&mut entry)?.into_owned()),
                            "value" => value = Some(try_data(&mut entry)?.into_owned()),
                            _ => {}
                        }
                    }
                    let key = required(key, "tag key")?;
                    let value =
                        value.ok_or_else(|| XmlDecodeError::custom("EBS tag omitted value"))?;
                    if tags.insert(key, value).is_some() {
                        return Err(XmlDecodeError::custom(
                            "EBS response contains duplicate tags",
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    if !attachments_seen {
        return Err(XmlDecodeError::custom("EBS response omitted attachmentSet"));
    }
    Ok(Volume {
        volume_id: required(id, "volumeId")?,
        availability_zone: required(zone, "availabilityZone")?,
        size_gib: required(size, "size")?
            .parse()
            .map_err(|_| XmlDecodeError::custom("invalid EBS size"))?,
        encrypted: match required(encrypted, "encrypted")?.as_str() {
            "true" => true,
            "false" => false,
            _ => return Err(XmlDecodeError::custom("invalid EBS encryption flag")),
        },
        volume_type: required(kind, "volumeType")?,
        state: required(state, "status")?,
        attachments,
        tags,
    })
}

fn parse_volume_page(body: &str) -> Result<VolumePage, XmlDecodeError> {
    let mut document = Document::new(body);
    let mut root = document.root_element()?;
    require_root(&root, "DescribeVolumesResponse")?;
    let mut page = VolumePage {
        volumes: Vec::new(),
        next_token: None,
    };
    let mut volumes_seen = false;
    while let Some(mut tag) = root.next_tag() {
        match tag.start_el().local() {
            "volumeSet" => {
                if volumes_seen {
                    return Err(XmlDecodeError::custom("duplicate EBS volume set"));
                }
                volumes_seen = true;
                while let Some(mut item) = tag.next_tag() {
                    if item.start_el().local() == "item" {
                        page.volumes.push(parse_volume(&mut item)?);
                    }
                }
            }
            "nextToken" => {
                let token = try_data(&mut tag)?.into_owned();
                page.next_token = (!token.is_empty()).then_some(token);
            }
            _ => {}
        }
    }
    if !volumes_seen {
        return Err(XmlDecodeError::custom("EBS response omitted volumeSet"));
    }
    Ok(page)
}

#[cfg(test)]
mod tests;
