pub mod backend;
pub mod frontend;
pub mod storage;

use serde_derive::Deserialize;

use backend::ConfigBackend;
use frontend::ConfigFrontend;
use storage::ConfigStorage;

#[derive(Deserialize)]
pub struct Config {
    pub frontends: Vec<ConfigFrontend>,
    pub backend: ConfigBackend,
    pub storage: ConfigStorage,
}

impl Config {
    pub fn parse(path: &std::path::Path) -> Result<Config, Box<dyn std::error::Error>> {
        let file = std::fs::OpenOptions::new().read(true).open(path)?;
        Ok(serde_yaml::from_reader::<_, Config>(file)?)
    }
}
