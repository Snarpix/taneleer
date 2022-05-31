mod artifact;
mod backends;
mod class;
mod config;
mod frontends;
mod manager;
mod storage;

use std::sync::Arc;

use tokio::sync::Mutex;

use backends::Backends;
use config::Config;
use manager::ArtifactManager;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::parse(std::path::Path::new("examples/config.yaml"))?;
    let storage = storage::from_config(&config.storage).await?;

    let mut backends = Backends::with_capacity(config.backends.len());
    for (backend_name, backend_config) in &config.backends {
        backends.insert(
            backend_name.clone(),
            backends::from_config(backend_config).await?,
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
