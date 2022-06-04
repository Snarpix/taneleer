mod fs;

use std::collections::HashMap;

use async_trait::async_trait;

use crate::{
    class::ArtifactClassData,
    config::backend::{ConfigBackend, ConfigFsBackend},
    error::Result,
};

#[async_trait]
pub trait Backend {
    async fn create_class(&mut self, name: &str, data: &ArtifactClassData) -> Result<()>;
}

pub type Backends = HashMap<String, Box<dyn Backend + Send + Sync>>;

pub async fn from_config(config: &ConfigBackend) -> Result<Box<dyn Backend + Send + Sync>> {
    match config {
        ConfigBackend::Fs(ConfigFsBackend { root_path }) => {
            Ok(Box::new(fs::FsBackend::new(root_path).await?))
        }
    }
}
