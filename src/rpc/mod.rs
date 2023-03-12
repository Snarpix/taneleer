pub mod wsjsonrpc;

use crate::config::rpc::ConfigRPC;
use crate::error::Result;
use crate::manager::SharedArtifactManager;

pub trait Rpc {}

pub async fn from_config(
    config: &ConfigRPC,
    manager: SharedArtifactManager,
) -> Result<Box<dyn Rpc>> {
    match config {
        ConfigRPC::WSJsonRPC(cfg) => Ok(Box::new(wsjsonrpc::WsRpc::new(cfg, manager).await?)),
    }
}
