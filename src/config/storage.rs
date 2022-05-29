use serde_derive::Deserialize;

#[derive(Deserialize)]
pub struct ConfigSqliteStorage {
    pub path: std::path::PathBuf,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "lowercase")]
pub enum ConfigStorage {
    Sqlite(ConfigSqliteStorage),
}
