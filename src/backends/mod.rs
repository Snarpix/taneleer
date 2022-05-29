mod fs;

use crate::config::backend::{ConfigBackend, ConfigFsBackend};

pub trait Backend {}

pub async fn from_config(
    config: &ConfigBackend,
) -> Result<Box<dyn Backend>, Box<dyn std::error::Error>> {
    match config {
        ConfigBackend::Fs(ConfigFsBackend { root_path }) => {
            Ok(Box::new(fs::FsBackend::new(root_path).await?))
        }
    }
}
