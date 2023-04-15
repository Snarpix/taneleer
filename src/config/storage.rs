use serde_derive::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigSqliteStorage {
    pub path: std::path::PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)] 
#[serde(tag = "type")]
#[serde(rename_all = "lowercase")]
pub enum ConfigStorage {
    Sqlite(ConfigSqliteStorage),
}
