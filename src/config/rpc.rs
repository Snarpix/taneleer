use serde_derive::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)] 
pub struct ConfigWSJsonRPC {
    pub address: std::net::IpAddr,
    pub port: u16,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)] 
#[serde(tag = "type")]
#[serde(rename_all = "lowercase")]
pub enum ConfigRPC {
    WSJsonRPC(ConfigWSJsonRPC),
}
