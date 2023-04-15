mod docker;

use crate::config::frontend::ConfigFrontend;
use crate::error::Result;
use crate::manager::SharedArtifactManager;

pub trait Frontend {}

pub async fn from_config(
    config: &ConfigFrontend,
    manager: SharedArtifactManager,
) -> Result<Box<dyn Frontend>> {
    match config {
        ConfigFrontend::DockerRegistry(cfg) => Ok(Box::new(docker::DockerRegistry::new(cfg, manager).await?)),
    }
}
