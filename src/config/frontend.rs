use serde_derive::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)] 
pub struct ConfigDockerFrontend {
    pub default_backend: String,
    pub address: std::net::IpAddr,
    pub hostname: String,
    pub port: u16,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum ConfigFrontend {
    DockerRegistry(ConfigDockerFrontend),
}
