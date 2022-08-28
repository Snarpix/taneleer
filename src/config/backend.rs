use std::collections::HashMap;

use serde_derive::Deserialize;

use super::proxy::ConfigBackendProxy;

#[derive(Deserialize)]
pub struct ConfigBackendCommon {
    #[serde(default)]
    pub proxies: HashMap<String, ConfigBackendProxy>,
}

#[derive(Deserialize)]
pub struct ConfigFsBackend {
    pub root_path: std::path::PathBuf,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "lowercase")]
pub enum ConfigBackendTypes {
    Fs(ConfigFsBackend),
}

#[derive(Deserialize)]
pub struct ConfigBackend {
    #[serde(flatten)]
    pub common: ConfigBackendCommon,
    #[serde(flatten)]
    pub specific: ConfigBackendTypes,
}
