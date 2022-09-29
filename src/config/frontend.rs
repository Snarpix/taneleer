use serde_derive::Deserialize;

#[derive(Deserialize)]
pub struct ConfigWSJsonRPC {
    pub address: std::net::IpAddr,
    pub hostname: String,
    pub port: u16,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "lowercase")]
pub enum ConfigFrontend {
    WSJsonRPC(ConfigWSJsonRPC),
}
