use serde_derive::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BusType {
    System,
    Session,
}

impl Into<dbus::channel::BusType> for &BusType {
    fn into(self) -> dbus::channel::BusType {
        match self {
            BusType::System => dbus::channel::BusType::System,
            BusType::Session => dbus::channel::BusType::Session,
        }
    }
}

#[derive(Deserialize)]
pub struct ConfigDBusFrontend {
    pub bus: BusType,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "lowercase")]
pub enum ConfigFrontend {
    DBus(ConfigDBusFrontend),
}
