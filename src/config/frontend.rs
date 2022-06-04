use serde_derive::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BusType {
    System,
    Session,
}

impl From<&BusType> for dbus::channel::BusType {
    fn from(bus: &BusType) -> Self {
        match bus {
            BusType::System => dbus::channel::BusType::System,
            BusType::Session => dbus::channel::BusType::Session,
        }
    }
}

#[derive(Deserialize)]
pub struct ConfigDBusFrontend {
    pub dbus_name: String,
    pub bus: BusType,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "lowercase")]
pub enum ConfigFrontend {
    DBus(ConfigDBusFrontend),
}
