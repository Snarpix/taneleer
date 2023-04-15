mod api;
mod artifact;
mod backends;
mod class;
mod config;
mod frontends;
mod error;
mod manager;
mod rpc;
mod source;
mod storage;
mod tag;
mod usage;
mod manifest;
mod util;

use std::sync::Arc;

use tokio::sync::Mutex;

use config::Config;
use error::Result;
use manager::{ArtifactManager, Backends};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("taneleer=info"))
        .init();

    let config = Config::parse(std::path::Path::new("examples/config.yaml"))?;
    let storage = storage::from_config(&config.storage).await?;

    let mut backends = Backends::with_capacity(config.backends.len());
    for (backend_name, backend_config) in &config.backends {
        backends.insert(
            backend_name.clone(),
            backends::backend_from_config(backend_config).await?,
        );
    }

    let mng = Arc::new(Mutex::new(ArtifactManager::new(storage, backends)));

    let mut rpc = Vec::with_capacity(config.rpc.len());
    for f in &config.rpc {
        rpc.push(rpc::from_config(f, mng.clone()).await?);
    }

    let mut frontends = Vec::with_capacity(config.frontends.len());
    for f in &config.frontends {
        frontends.push(frontends::from_config(f, mng.clone()).await?);
    }
    tokio::signal::ctrl_c().await?;

    drop(rpc);
    Ok(())
}
