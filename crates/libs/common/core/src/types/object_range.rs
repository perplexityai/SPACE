//! A bounded byte range of one immutable object, including its expected EOF.

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "Wire", into = "Wire"))]
pub struct ObjectByteRange {
    offset: u64,
    length: u64,
    object_size: u64,
    version_id: Option<String>,
}

#[derive(Debug, thiserror::Error)]
#[error(
    "object range must be nonempty, at most 128 MiB, within the declared object, with a valid optional version"
)]
pub struct ObjectByteRangeError;

impl ObjectByteRange {
    pub fn new(
        offset: u64,
        length: u64,
        object_size: u64,
        version_id: Option<String>,
    ) -> Result<Self, ObjectByteRangeError> {
        if length == 0
            || length > (128 << 20)
            || object_size == 0
            || offset
                .checked_add(length)
                .is_none_or(|end| end > object_size)
            || version_id.as_ref().is_some_and(|id| {
                id.is_empty() || id == "null" || id.len() > 1024 || id.chars().any(char::is_control)
            })
        {
            return Err(ObjectByteRangeError);
        }
        Ok(Self {
            offset,
            length,
            object_size,
            version_id,
        })
    }
    pub const fn offset(&self) -> u64 {
        self.offset
    }
    pub const fn length(&self) -> u64 {
        self.length
    }
    pub const fn object_size(&self) -> u64 {
        self.object_size
    }
    pub fn version_id(&self) -> Option<&str> {
        self.version_id.as_deref()
    }
    pub fn request_header(&self) -> String {
        format!("bytes={}-{}", self.offset, self.offset + self.length - 1)
    }
    pub fn response_header(&self) -> String {
        format!(
            "bytes {}-{}/{}",
            self.offset,
            self.offset + self.length - 1,
            self.object_size
        )
    }
}

#[cfg(feature = "serde")]
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Wire {
    offset: u64,
    length: u64,
    object_size: u64,
    version_id: Option<String>,
}

#[cfg(feature = "serde")]
impl TryFrom<Wire> for ObjectByteRange {
    type Error = ObjectByteRangeError;
    fn try_from(value: Wire) -> Result<Self, Self::Error> {
        Self::new(
            value.offset,
            value.length,
            value.object_size,
            value.version_id,
        )
    }
}

#[cfg(feature = "serde")]
impl From<ObjectByteRange> for Wire {
    fn from(value: ObjectByteRange) -> Self {
        Self {
            offset: value.offset,
            length: value.length,
            object_size: value.object_size,
            version_id: value.version_id,
        }
    }
}
