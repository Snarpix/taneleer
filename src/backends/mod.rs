pub mod docker_registry;
mod fs;

use std::{path::PathBuf, fs::Metadata};

use async_trait::async_trait;
use tokio::fs::File;
use url::Url;
use uuid::Uuid;

use crate::{
    artifact::ArtifactItemInfo,
    class::{ArtifactClassData, ArtifactType},
    config::backend::ConfigBackendTypes,
    api::Hashsum, manifest::ManifestInfo,
};

#[derive(Debug)]
pub enum BackendError {
    InternalError,
    RootIsNotDir,
    NoDigest,
    InvalidDigest,
    InvalidArtifactType,
    EmptyCommit,
    NotImplemented,
    IoError(std::io::Error),
    UrlError(url::ParseError),
    StorageError(docker_registry::StorageError)
}

impl std::fmt::Display for BackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for BackendError {}

impl From<std::io::Error> for BackendError {
    fn from(e: std::io::Error) -> Self {
        Self::IoError(e)
    }
}

impl From<url::ParseError> for BackendError {
    fn from(e: url::ParseError) -> Self {
        Self::UrlError(e)
    }
}

impl From<docker_registry::StorageError> for BackendError {
    fn from(e: docker_registry::StorageError) -> Self {
        Self::StorageError(e)
    }
}

pub type Result<T> = std::result::Result<T, BackendError>;

#[async_trait]
pub trait Backend {
    #[must_use]
    async fn create_class(&mut self, name: &str, data: &ArtifactClassData) -> Result<()>;

    #[must_use]
    async fn reserve_artifact(
        &mut self,
        class_name: &str,
        art_type: ArtifactType,
        uuid: Uuid,
    ) -> Result<Url>;

    #[must_use]
    async fn abort_reserve(
        &mut self,
        class_name: &str,
        art_type: ArtifactType,
        uuid: Uuid,
    ) -> Result<()>;

    #[must_use]
    async fn commit_artifact(
        &mut self,
        class_name: &str,
        art_type: ArtifactType,
        uuid: Uuid,
    ) -> Result<Vec<ArtifactItemInfo>>;

    #[must_use]
    async fn get_artifact(
        &mut self,
        class_name: &str,
        art_type: ArtifactType,
        uuid: Uuid,
    ) -> Result<Url>;

    #[must_use]
    async fn create_upload(
        &mut self,
        class_name: &str,
    ) -> Result<Uuid>;

    #[must_use]
    async fn lock_upload(
        &mut self,
        class_name: &str,
        upload_uuid: Uuid,
    ) -> Result<File>;

    #[must_use]
    async fn unlock_upload(
        &mut self,
        class_name: &str,
        upload_uuid: Uuid,
    ) -> Result<()>;

    #[must_use]
    async fn commit_upload(
        &mut self,
        class_name: &str,
        upload_uuid: Uuid,
        hash: Hashsum,
    ) -> Result<()>;

    #[must_use]
    async fn commit_manifest_upload(
        &mut self,
        class_name: &str,
        upload_uuid: Uuid,
        hash: Hashsum,
    ) -> Result<()>;

    #[must_use]
    async fn check_upload(
        &mut self,
        class_name: &str,
        hash: Hashsum,
    ) -> Result<Metadata>;

    #[must_use]
    async fn get_upload(
        &mut self,
        class_name: &str,
        hash: Hashsum,
    ) -> Result<File>;

    #[must_use]
    async fn check_manifest_by_hash(
        &mut self,
        class_name: &str,
        hash: Hashsum,
    ) -> Result<ManifestInfo>;

    #[must_use]
    async fn check_manifest_by_uuid(
        &mut self,
        class_name: &str,
        uuid: Uuid,
    ) -> Result<ManifestInfo>;

    #[must_use]
    async fn get_manifest_by_hash(
        &mut self,
        class_name: &str,
        hash: Hashsum,
    ) -> Result<(File, ManifestInfo)>;

    #[must_use]
    async fn get_manifest_by_uuid(
        &mut self,
        class_name: &str,
        uuid: Uuid,
    ) -> Result<(File, ManifestInfo)>;
}

pub async fn backend_from_config(
    config: &ConfigBackendTypes,
) -> Result<Box<dyn Backend + Send + Sync>> {
    match config {
        ConfigBackendTypes::Fs(cfg) => Ok(Box::new(fs::FsBackend::new(cfg).await?)),
        ConfigBackendTypes::DockerRegistry(cfg) => Ok(Box::new(
            docker_registry::DockerRegistryBackend::new(cfg).await?,
        )),
    }
}
