pub mod backend;
pub mod frontend;
pub mod rpc;
pub mod storage;

use std::collections::HashMap;

use serde_derive::Deserialize;

use crate::error::Result;
use rpc::ConfigRPC;
use storage::ConfigStorage;
use frontend::ConfigFrontend;

use self::backend::ConfigBackendTypes;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)] 
pub struct Config {
    pub rpc: Vec<ConfigRPC>,
    #[serde(with = "serde_with::rust::maps_duplicate_key_is_error")]
    pub backends: HashMap<String, ConfigBackendTypes>,
    pub frontends: Vec<ConfigFrontend>,
    pub storage: ConfigStorage,
}

impl Config {
    pub fn parse(path: &std::path::Path) -> Result<Config> {
        let file = std::fs::OpenOptions::new().read(true).open(path)?;
        Ok(serde_yaml::from_reader::<_, Config>(file)?)
    }
}
