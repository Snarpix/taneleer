use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use strum::{EnumDiscriminants, EnumString, IntoStaticStr};
use uuid::Uuid;

pub type Sha1 = [u8; 20];
pub type Sha256 = [u8; 32];

#[serde_as]
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "val", rename_all = "lowercase")]
pub enum Hashsum {
    Sha256(#[serde_as(as = "serde_with::hex::Hex")] Sha256),
}

#[derive(Debug)]
pub enum HashsumParseError {
    InvalidType,
    InvalidLength,
}

impl std::fmt::Display for HashsumParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for HashsumParseError {}

impl Hashsum {
    pub fn from_split(name: &str, bytes: &[u8]) -> Result<Self, HashsumParseError> {
        if name != "sha256" {
            return Err(HashsumParseError::InvalidType);
        }
        Ok(Hashsum::Sha256(
            bytes
                .try_into()
                .map_err(|_| HashsumParseError::InvalidLength)?,
        ))
    }
}

#[serde_as]
#[derive(Serialize, Deserialize, EnumDiscriminants, IntoStaticStr)]
#[serde(rename_all = "snake_case")]
#[strum_discriminants(derive(EnumString))]
#[strum_discriminants(strum(serialize_all = "snake_case"))]
#[strum(serialize_all = "snake_case")]
pub enum SourceType {
    Url {
        url: String,
        hash: Hashsum,
    },
    Git {
        repo: String,
        #[serde_as(as = "serde_with::hex::Hex")]
        commit: Sha1,
    },
    Artifact {
        uuid: Uuid,
    },
}

#[derive(Serialize, Deserialize)]
pub struct Source {
    pub name: String,
    pub source: SourceType,
}
