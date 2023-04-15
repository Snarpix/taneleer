use serde_derive::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)] 
pub struct ConfigRewriteProxy {
    pub from: String,
    pub to: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)] 
#[serde(tag = "type")]
#[serde(rename_all = "lowercase")]
pub enum ConfigBackendProxy {
    Rewrite(ConfigRewriteProxy),
}
