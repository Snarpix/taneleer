use uuid::Uuid;

pub type Sha1 = [u8; 20];
pub type Sha256 = [u8; 32];

pub enum Hashsum {
    Sha256(Sha256),
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

pub enum SourceType {
    Url { url: String, hash: Hashsum },
    Git { repo: String, commit: Sha1 },
    Artifact { uuid: Uuid },
}

pub struct Source {
    pub name: String,
    pub source: SourceType,
}
