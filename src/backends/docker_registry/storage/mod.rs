mod sqlite;

use std::path::PathBuf;

use async_trait::async_trait;
use uuid::Uuid;

use crate::{artifact::ArtifactItemInfo, error::Result, source::Sha256};

#[async_trait]
pub trait Storage {
    #[must_use]
    async fn create_class(&self, name: &str) -> Result<()>;

    #[must_use]
    async fn create_upload(&self, name: &str, upload_uuid: Uuid) -> Result<()>;

    #[must_use]
    async fn lock_upload(&self, upload_uuid: Uuid) -> Result<()>;

    #[must_use]
    async fn unlock_upload(&self, upload_uuid: Uuid) -> Result<()>;

    #[must_use]
    async fn remove_upload(&self, upload_uuid: Uuid) -> Result<()>;

    #[must_use]
    async fn commit_blob(&self, digest: Sha256, size: i64, upload_uuid: Uuid) -> Result<()>;

    #[must_use]
    async fn link_blob(&self, digest: Sha256, class_name: &str) -> Result<()>;

    #[must_use]
    async fn commit_manifest(
        &self,
        class_name: &str,
        manifest_digest: Sha256,
        manifest_size: i64,
        manifest_type: &str,
        blob_digests: Vec<Sha256>,
    ) -> Result<()>;

    #[must_use]
    async fn commit_tag(
        &self,
        artifact_uuid: Uuid,
        class_name: &str,
        manifest_digest: Sha256,
    ) -> Result<()>;

    #[must_use]
    async fn remove_tag_if_exists(&self, artifact_uuid: Uuid) -> Result<()>;

    #[must_use]
    async fn get_artifact_items(&self, artifact_uuid: Uuid) -> Result<Vec<ArtifactItemInfo>>;
}

pub async fn new_storage(path: &PathBuf) -> Result<Box<dyn Storage + Send + Sync>> {
    Ok(Box::new(sqlite::SqliteStorage::new(path).await?))
}
