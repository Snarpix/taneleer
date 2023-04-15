use std::collections::HashMap;

use serde_derive::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)] 
pub struct ConfigFsBackend {
    pub root_path: std::path::PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)] 
pub struct ConfigDockerRegistry {
    pub root_path: std::path::PathBuf,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum ConfigBackendTypes {
    Fs(ConfigFsBackend),
    DockerRegistry(ConfigDockerRegistry),
}
