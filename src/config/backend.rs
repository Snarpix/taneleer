use serde_derive::Deserialize;

#[derive(Deserialize)]
pub struct ConfigFsBackend {
    pub root_path: std::path::PathBuf,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "lowercase")]
pub enum ConfigBackend {
    Fs(ConfigFsBackend),
}
