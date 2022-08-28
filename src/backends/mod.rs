mod fs;

use async_trait::async_trait;
use url::Url;
use uuid::Uuid;

use crate::{
    artifact::ArtifactItemInfo,
    class::{ArtifactClassData, ArtifactType},
    config::backend::{ConfigBackendTypes, ConfigFsBackend},
    error::Result,
};

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
}

pub async fn backend_from_config(
    config: &ConfigBackendTypes,
) -> Result<Box<dyn Backend + Send + Sync>> {
    match config {
        ConfigBackendTypes::Fs(ConfigFsBackend { root_path }) => {
            Ok(Box::new(fs::FsBackend::new(root_path).await?))
        }
    }
}
