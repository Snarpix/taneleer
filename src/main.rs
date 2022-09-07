mod artifact;
mod backend_pack;
mod backends;
mod class;
mod config;
mod error;
mod frontends;
mod manager;
mod proxies;
mod source;
mod storage;
mod util;

use std::sync::Arc;

use tokio::sync::Mutex;

use backend_pack::Backends;
use config::Config;
use error::Result;
use manager::ArtifactManager;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let config = Config::parse(std::path::Path::new("examples/config.yaml"))?;
    let storage = storage::from_config(&config.storage).await?;

    let mut backends = Backends::with_capacity(config.backends.len());
    for (backend_name, backend_config) in &config.backends {
        backends.insert(
            backend_name.clone(),
            backend_pack::from_config(backend_config).await?,
        );
    }

    let mng = Arc::new(Mutex::new(ArtifactManager::new(storage, backends)));

    let mut frontends = Vec::with_capacity(config.frontends.len());
    for f in &config.frontends {
        frontends.push(frontends::from_config(f, mng.clone()).await?);
    }

    tokio::signal::ctrl_c().await?;

    drop(frontends);
    Ok(())
}
