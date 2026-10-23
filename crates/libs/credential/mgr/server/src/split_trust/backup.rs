//! KMS-encrypted, content-addressed backups written before Secret publication.
use aws_lc_rs::{
    aead,
    rand::{SecureRandom, SystemRandom},
};
use aws_sdk_kms::primitives::Blob;
use pplx_space_sandbox_anchor::Keyring;
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

const PURPOSE: &str = "space-sandbox-anchor-keyring-v1";
const MAGIC: &[u8] = b"SPACE-ANCHOR-BACKUP-V1\n";
const MAX_KEYRING_BYTES: usize = 512 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum BackupError {
    #[error("anchor backup configuration: {0}")]
    Configuration(#[from] std::env::VarError),
    #[error("anchor keyring serialization: {0}")]
    Keyring(#[from] pplx_space_sandbox_anchor::AnchorError),
    #[error("anchor backup operation failed: {0}")]
    Operation(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("anchor backup exceeds its bounded size")]
    TooLarge,
    #[error("anchor backup contained no key material")]
    Empty,
    #[error("anchor backup bucket must have S3 versioning enabled")]
    VersioningRequired,
    #[error("anchor backup envelope: {0}")]
    Envelope(#[from] serde_json::Error),
    #[error("anchor backup encryption or authentication failed")]
    Crypto,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    encrypted_data_key: Vec<u8>,
    nonce: [u8; 12],
    ciphertext: Vec<u8>,
}

impl Envelope {
    fn seal(
        plaintext: &[u8],
        data_key: &[u8],
        encrypted_data_key: Vec<u8>,
    ) -> Result<Self, BackupError> {
        if plaintext.len() > MAX_KEYRING_BYTES {
            return Err(BackupError::TooLarge);
        }
        let key = aead::LessSafeKey::new(
            aead::UnboundKey::new(&aead::AES_256_GCM, data_key).map_err(|_| BackupError::Crypto)?,
        );
        let mut nonce = [0u8; 12];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| BackupError::Crypto)?;
        let mut ciphertext = Zeroizing::new(plaintext.to_vec());
        key.seal_in_place_append_tag(
            aead::Nonce::assume_unique_for_key(nonce),
            aead::Aad::from(PURPOSE),
            &mut *ciphertext,
        )
        .map_err(|_| BackupError::Crypto)?;
        Ok(Self {
            encrypted_data_key,
            nonce,
            ciphertext: ciphertext.to_vec(),
        })
    }

    fn encode(&self) -> Result<Vec<u8>, BackupError> {
        let mut bytes = MAGIC.to_vec();
        serde_json::to_writer(&mut bytes, self)?;
        Ok(bytes)
    }

    fn open(self, data_key: &[u8]) -> Result<Zeroizing<Vec<u8>>, BackupError> {
        let key = aead::LessSafeKey::new(
            aead::UnboundKey::new(&aead::AES_256_GCM, data_key).map_err(|_| BackupError::Crypto)?,
        );
        let mut plaintext = Zeroizing::new(self.ciphertext);
        let length = key
            .open_in_place(
                aead::Nonce::assume_unique_for_key(self.nonce),
                aead::Aad::from(PURPOSE),
                &mut plaintext,
            )
            .map_err(|_| BackupError::Crypto)?
            .len();
        plaintext.truncate(length);
        if length > MAX_KEYRING_BYTES {
            return Err(BackupError::TooLarge);
        }
        Ok(plaintext)
    }
}

#[derive(Clone)]
pub(super) struct Backup {
    kms: aws_sdk_kms::Client,
    s3: aws_sdk_s3::Client,
    key_id: String,
    bucket: String,
}

impl Backup {
    pub async fn from_env() -> Result<Self, BackupError> {
        let key_id = std::env::var("SPACE_ANCHOR_KMS_KEY_ID")?;
        let bucket = std::env::var("SPACE_ANCHOR_BACKUP_BUCKET")?;
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let mut kms = aws_sdk_kms::config::Builder::from(&config);
        let mut s3 = aws_sdk_s3::config::Builder::from(&config).force_path_style(true);
        if let Some(endpoint) = std::env::var("SPACE_ANCHOR_BACKUP_ENDPOINT")
            .ok()
            .filter(|value| !value.is_empty())
        {
            // This override is exclusively for the explicit local prototype
            // emulator. Do not alter ambient storage-exchange credentials.
            let credentials = aws_sdk_kms::config::Credentials::new(
                "testing",
                "testing",
                None,
                None,
                "sandbox-anchor-local-emulator",
            );
            kms = kms
                .endpoint_url(&endpoint)
                .credentials_provider(credentials.clone());
            s3 = s3.endpoint_url(&endpoint).credentials_provider(credentials);
        }
        let backup = Self {
            kms: aws_sdk_kms::Client::from_conf(kms.build()),
            s3: aws_sdk_s3::Client::from_conf(s3.build()),
            key_id,
            bucket,
        };
        let versioning = backup
            .s3
            .get_bucket_versioning()
            .bucket(&backup.bucket)
            .send()
            .await
            .map_err(|error| BackupError::Operation(Box::new(error)))?;
        if versioning.status() != Some(&aws_sdk_s3::types::BucketVersioningStatus::Enabled) {
            return Err(BackupError::VersioningRequired);
        }
        Ok(backup)
    }

    pub async fn has_existing_backup(&self) -> Result<bool, BackupError> {
        let response = self
            .s3
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix("sandbox-anchor/")
            .max_keys(1)
            .send()
            .await
            .map_err(|error| BackupError::Operation(Box::new(error)))?;
        Ok(!response.contents().is_empty())
    }

    pub async fn persist(&self, ring: &Keyring) -> Result<String, BackupError> {
        let plaintext = ring.encode()?;
        if plaintext.len() > MAX_KEYRING_BYTES {
            return Err(BackupError::TooLarge);
        }
        let generated = self
            .kms
            .generate_data_key()
            .key_id(&self.key_id)
            .number_of_bytes(32)
            .encryption_context("purpose", PURPOSE)
            .send()
            .await
            .map_err(|error| BackupError::Operation(Box::new(error)))?;
        let data_key = Zeroizing::new(generated.plaintext.ok_or(BackupError::Empty)?.into_inner());
        let encrypted_data_key = generated
            .ciphertext_blob
            .ok_or(BackupError::Empty)?
            .into_inner();
        let envelope = Envelope::seal(&plaintext, &data_key, encrypted_data_key)?.encode()?;
        let version = ring.versions().last().ok_or(BackupError::Empty)?.0;
        let object = format!("sandbox-anchor/v{version}/{}.kms", ring.hash()?);
        // Content addressing makes duplicate writes harmless. S3 versioning
        // provides an additional recovery history; no plaintext ever goes to S3.
        self.s3
            .put_object()
            .bucket(&self.bucket)
            .key(&object)
            .body(aws_sdk_s3::primitives::ByteStream::from(envelope))
            .send()
            .await
            .map_err(|error| BackupError::Operation(Box::new(error)))?;
        Ok(object)
    }

    pub async fn recover(&self, object: &str) -> Result<Keyring, BackupError> {
        let response = self
            .s3
            .get_object()
            .bucket(&self.bucket)
            .key(object)
            .send()
            .await
            .map_err(|error| BackupError::Operation(Box::new(error)))?;
        if response
            .content_length()
            .is_some_and(|length| length > 4 * 1024 * 1024)
        {
            return Err(BackupError::TooLarge);
        }
        let encrypted = response
            .body
            .collect()
            .await
            .map_err(|error| BackupError::Operation(Box::new(error)))?
            .into_bytes();
        let plaintext = if let Some(payload) = encrypted.strip_prefix(MAGIC) {
            let envelope: Envelope = serde_json::from_slice(payload)?;
            let key = self.unwrap(&envelope.encrypted_data_key).await?;
            envelope.open(&key)?
        } else {
            // Read the first prototype's small, direct-KMS backups as well.
            // New writes always use the authenticated envelope above.
            self.unwrap(&encrypted).await?
        };
        let ring = Keyring::decode(&plaintext)?;
        if !object.ends_with(&format!("/{}.kms", ring.hash()?)) {
            return Err(BackupError::Empty);
        }
        Ok(ring)
    }

    async fn unwrap(&self, ciphertext: &[u8]) -> Result<Zeroizing<Vec<u8>>, BackupError> {
        let decrypted = self
            .kms
            .decrypt()
            .key_id(&self.key_id)
            .encryption_context("purpose", PURPOSE)
            .ciphertext_blob(Blob::new(ciphertext.to_vec()))
            .send()
            .await
            .map_err(|error| BackupError::Operation(Box::new(error)))?
            .plaintext
            .ok_or(BackupError::Empty)?;
        Ok(Zeroizing::new(decrypted.into_inner()))
    }
}

#[cfg(test)]
mod tests;
