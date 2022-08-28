use serde_derive::Deserialize;

#[derive(Deserialize)]
pub struct ConfigRewriteProxy {
    pub from: String,
    pub to: String,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "lowercase")]
pub enum ConfigBackendProxy {
    Rewrite(ConfigRewriteProxy),
}
