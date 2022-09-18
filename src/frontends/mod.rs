mod dbus;
mod wsjsonrpc;

use crate::config::frontend::{ConfigDBusFrontend, ConfigFrontend};
use crate::error::Result;
use crate::manager::SharedArtifactManager;

pub trait Frontend {}

pub async fn from_config(
    config: &ConfigFrontend,
    manager: SharedArtifactManager,
) -> Result<Box<dyn Frontend>> {
    match config {
        ConfigFrontend::DBus(ConfigDBusFrontend { dbus_name, bus }) => Ok(Box::new(
            dbus::DBusFrontend::new(dbus_name.as_str(), bus.into(), manager).await?,
        )),
        ConfigFrontend::WSJsonRPC(cfg) => {
            Ok(Box::new(wsjsonrpc::WSFrontend::new(cfg, manager).await?))
        }
    }
}
