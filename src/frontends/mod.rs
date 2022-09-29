pub mod wsjsonrpc;

use crate::config::frontend::ConfigFrontend;
use crate::error::Result;
use crate::manager::SharedArtifactManager;

pub trait Frontend {}

pub async fn from_config(
    config: &ConfigFrontend,
    manager: SharedArtifactManager,
) -> Result<Box<dyn Frontend>> {
    match config {
        ConfigFrontend::WSJsonRPC(cfg) => {
            Ok(Box::new(wsjsonrpc::WSFrontend::new(cfg, manager).await?))
        }
    }
}
