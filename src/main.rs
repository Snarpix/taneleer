mod artifact;
mod backends;
mod class;
mod config;
mod frontends;
mod manager;
mod storage;

use config::Config;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::parse(std::path::Path::new("examples/config.yaml"))?;
    let storage = storage::from_config(&config.storage).await?;
    let backend = backends::from_config(&config.backend).await?;
    let mut frontends = Vec::with_capacity(config.frontends.len());
    for f in &config.frontends {
        frontends.push(frontends::from_config(f).await?);
    }
    tokio::signal::ctrl_c().await?;
    Ok(())
}
