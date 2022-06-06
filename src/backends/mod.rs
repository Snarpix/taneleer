mod fs;

use std::collections::HashMap;

use async_trait::async_trait;
use uuid::Uuid;

use crate::{
    class::{ArtifactClassData, ArtifactType},
    config::backend::{ConfigBackend, ConfigFsBackend},
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
    ) -> Result<String>;
}

pub type Backends = HashMap<String, Box<dyn Backend + Send + Sync>>;

pub async fn from_config(config: &ConfigBackend) -> Result<Box<dyn Backend + Send + Sync>> {
    match config {
        ConfigBackend::Fs(ConfigFsBackend { root_path }) => {
            Ok(Box::new(fs::FsBackend::new(root_path).await?))
        }
    }
}
