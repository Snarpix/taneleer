mod dbus;

use crate::config::frontend::{ConfigDBusFrontend, ConfigFrontend};
use crate::manager::SharedArtifactManager;

pub trait Frontend {}

pub async fn from_config(
    config: &ConfigFrontend,
    manager: SharedArtifactManager,
) -> Result<Box<dyn Frontend>, Box<dyn std::error::Error>> {
    match config {
        ConfigFrontend::DBus(ConfigDBusFrontend { bus }) => Ok(Box::new(
            dbus::DBusFrontend::new(bus.into(), manager).await?,
        )),
    }
}
